package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/gorilla/websocket"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"reflect"
	"sort"
	"sync"
	"time"
)

// PubSubConfig contains the configuration for PubSub.
type PubSubConfig struct {
	Logger Logger
}

// NewPubSub creates a PubSub.
func NewPubSub(conf PubSubConfig) (*PubSub, error) {
	l := conf.Logger
	if l == nil {
		l = log.Default()
	}
	ps := &PubSub{
		topics:       make(map[string]map[chan []byte]struct{}),
		chanMap:      make(map[<-chan []byte]chan []byte),
		unsubscribeC: make(chan struct{}),
		log:          l,
	}
	return ps, nil
}

// PubSub is responsible for holding the data for the message topics and
// allowing subscription to the topics.
type PubSub struct {
	topics       map[string]map[chan []byte]struct{}
	chanMap      map[<-chan []byte]chan []byte
	topicsMu     sync.Mutex
	log          Logger
	unsubscribeC chan struct{}
}

// Publish publishes a message to a given topic. If the topic does not exist, it
// will be created.
func (s *PubSub) Publish(ctx context.Context, topic string, message []byte) error {
	// Check if context is finished with a non-blocking receive before entering
	// main select to ensure no attempt to send on the channel if the context is
	// done.
	// This is due to the "uniform pseudo-random selection" for selects. See:
	//   https://go.dev/ref/spec#Select_statements
	select {
	case <-ctx.Done():
		return fmt.Errorf("unable to publish message: %w", ctx.Err())
	default:
	}

	// Send message to a dynamic set of channels.
	// Track channels already sent to to not send to them again.
	sentTo := make(map[chan []byte]struct{})
	for {
		subsSet := s.getSubsForTopic(topic)

		// Nothing to do if there are no active subscribers as this is intended to
		// only output to actively connected HTTP clients.
		if len(subsSet) == 0 {
			return nil
		}

		var cases []reflect.SelectCase
		cases = append(cases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ctx.Done()),
		})
		// If an unsubscription event occurs, restart from the beginning of the
		// loop to see if there is any more work to do.
		cases = append(cases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(s.unsubscribeC),
		})
		msgVal := reflect.ValueOf(message)
		for sub := range subsSet {
			// Skip channels already sent to.
			if _, ok := sentTo[sub]; ok {
				continue
			}
			cases = append(cases, reflect.SelectCase{
				Dir:  reflect.SelectSend,
				Chan: reflect.ValueOf(sub),
				Send: msgVal,
			})
		}
		chosen, _, _ := reflect.Select(cases)
		switch chosen {
		case 0: // ctx.Done.
			return fmt.Errorf("unable to publish message: %w", ctx.Err())
		case 1: // unsubscribeC.
			continue
		default:
			sentTo[cases[chosen].Chan.Interface().(chan []byte)] = struct{}{}
		}
		// If only ctx.Done and unsubscribeC cases remain.
		if len(sentTo) == len(subsSet) {
			return nil
		}
	}
}

// Subscribe subscribes to the given topic. If the topic does not exist yet, it
// will be created.
func (s *PubSub) Subscribe(_ context.Context, topic string) (<-chan []byte, error) {
	s.log.Printf("Added subscription to %q", topic)
	return s.addTopicSub(topic), nil
}

// Unsubscribe unsubscribes from the given topic with the given subscription
// channel.
func (s *PubSub) Unsubscribe(_ context.Context, topic string, subC <-chan []byte) error {
	if err := s.removeTopicSub(topic, subC); err != nil {
		return fmt.Errorf("error removing topic subscription: %w", err)
	}
	s.log.Printf("Removed subscription from %q", topic)
	return nil
}

func (s *PubSub) getSubsForTopic(topic string) map[chan []byte]struct{} {
	s.topicsMu.Lock()
	defer s.topicsMu.Unlock()

	// Get topic if it exists, otherwise create it.
	subsSet, ok := s.topics[topic]
	if !ok {
		subsSet = make(map[chan []byte]struct{})
		s.topics[topic] = subsSet
	}
	return subsSet
}

func (s *PubSub) addTopicSub(topic string) <-chan []byte {
	s.topicsMu.Lock()
	defer s.topicsMu.Unlock()

	// Get topic if it exists, otherwise create it.
	subsSet, ok := s.topics[topic]
	if !ok {
		subsSet = make(map[chan []byte]struct{})
		s.topics[topic] = subsSet
	}
	subC := make(chan []byte)
	subsSet[subC] = struct{}{}
	out := (<-chan []byte)(subC)
	s.chanMap[out] = subC
	return out
}

func (s *PubSub) removeTopicSub(topic string, sub <-chan []byte) error {
	s.topicsMu.Lock()
	defer s.topicsMu.Unlock()

	// Get topic if it exists, otherwise create it.
	subsSet, ok := s.topics[topic]
	if !ok {
		return fmt.Errorf("unknown subscription topic %q", topic)
	}
	delete(subsSet, s.chanMap[sub])
	delete(s.chanMap, sub)
	// Try to issue an unsubscription request.
	// If there are no pending publish calls, then nothing will be sent on the
	// channel.
	select {
	case s.unsubscribeC <- struct{}{}:
	default:
	}
	return nil
}

func (s *PubSub) getTopics() []string {
	var topics []string
	s.topicsMu.Lock()
	defer s.topicsMu.Unlock()
	for topic := range s.topics {
		topics = append(topics, topic)
	}
	return topics
}

// PubSubHTTPConfig contains the configuration for PubSubHTTP.
type PubSubHTTPConfig struct {
	PubSub          *PubSub
	MaxPublishBytes int64
	Logger          Logger
}

// NewPubSubHTTP creates a new PubSubHTTP.
func NewPubSubHTTP(conf PubSubHTTPConfig) (*PubSubHTTP, error) {
	l := conf.Logger
	if l == nil {
		l = log.Default()
	}
	if conf.PubSub == nil {
		return nil, errors.New("invalid nil PubSub")
	}
	return &PubSubHTTP{
		ps:              conf.PubSub,
		maxPublishBytes: conf.MaxPublishBytes,
		log:             l,
	}, nil
}

// PubSubHTTP is responsible for the HTTP handlers for interacting with a
// PubSub.
type PubSubHTTP struct {
	ps              *PubSub
	maxPublishBytes int64
	log             Logger
}

// HandlePublish handles HTTP requests to publish a message to a topic.
func (psh *PubSubHTTP) HandlePublish(w http.ResponseWriter, r *http.Request) {
	cts := r.Header["Content-Type"]
	if len(cts) != 1 || (len(cts) == 1 && cts[0] != "application/json") {
		psh.log.Printf("Invalid Content-Type for publish request")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprint(w, `Invalid Content-Type header. Only "application/json supported"`)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, psh.maxPublishBytes)
	topic, ok := psh.getTopic(w, r.URL)
	if !ok {
		return
	}
	msg, err := io.ReadAll(r.Body)
	if err != nil {
		psh.log.Printf("Unable to complete reading publish message body: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprint(w, "Unable to complete reading of message")
		return
	}
	if !json.Valid(msg) {
		psh.log.Printf("Invalid JSON for publish request")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprint(w, "Invalid JSON message")
		return
	}
	if err := psh.ps.Publish(r.Context(), topic, msg); err != nil {
		psh.log.Printf("Unable to publish to topic %q: %v", topic, err)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprint(w, "Internal error: unable to publish to topic")
		return
	}
	w.WriteHeader(http.StatusCreated)
}

// HandleSubscribe handles subscription requests to a given topic.
//
// This handler assumes that the given topic will be read from forever, even if
// there are no messages remaining to read from the topic.
//
// Websockets are normally rejected with request methods other than GET, but
// this handler works around that limitation.
func (psh *PubSubHTTP) HandleSubscribe(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	// Get the topic name and subscribe.
	topic, ok := psh.getTopic(w, r.URL)
	if !ok {
		return
	}
	subC, err := psh.ps.Subscribe(ctx, topic)
	if err != nil {
		psh.log.Printf("Unable to subscribe to topic %q: %v", topic, err)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprint(w, "Internal error: unable to subscribe to topic")
		return
	}
	defer func() {
		if err := psh.ps.Unsubscribe(ctx, topic, subC); err != nil {
			psh.log.Printf("Error unsubscribing from topic %q: %v", topic, err)
		}
	}()

	// Upgrade to the websocket connection.
	u := websocket.Upgrader{
		HandshakeTimeout: 30 * time.Second,
		ReadBufferSize:   1 << 10,
		WriteBufferSize:  1 << 10,
	}
	// Change request method to GET as using non-GET request methods seems to be
	// non-conformant and will result in an error.
	r.Method = http.MethodGet
	conn, err := u.Upgrade(w, r, nil)
	if err != nil {
		psh.log.Printf("Unable to upgrade to websocket connection: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprintf(w, "Unable to upgrade to websocket connection: %v", err)
		return
	}
	defer func() {
		psh.log.Printf("Closing /subscribe websocket connection")
		if err := conn.Close(); err != nil {
			psh.log.Printf("Error closing websocket connection: %v", err)
		}
	}()

	// Asynchronously read all incoming messages in case a close control message
	// is received.
	go func() {
		for {
			if _, _, err := conn.NextReader(); err != nil {
				cancel()
				return
			}
		}
	}()

	// Write all message from the topic to the websocket connection.
	for {
		select {
		case <-ctx.Done():
			psh.log.Printf("Stopping websocket connection (context cancellation: %v)", ctx.Err())
			msg := "connection closed (cancelled)"
			if _, ok := r.Context().Deadline(); ok {
				msg = "connection closed (timeout)"
			}
			_ = conn.WriteMessage(websocket.CloseMessage, []byte(msg))
			return
		case m := <-subC:
			if err := conn.WriteMessage(websocket.TextMessage, m); err != nil {
				psh.log.Printf("Error writing message to websocket: %v", err)
				_ = conn.WriteMessage(websocket.CloseMessage, []byte("error writing to websocket; closing"))
				return
			}
		}
	}
}

// Logger wraps the Printf method for logging.
//
// Logger implementations must be thread-safe.
type Logger interface {
	Printf(format string, a ...interface{})
}

// getTopic gets the topic query parameter and handles setting the status code
// and response body if there is an error.
func (psh *PubSubHTTP) getTopic(w http.ResponseWriter, u *url.URL) (topic string, ok bool) {
	topics, ok := u.Query()["topic"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprint(w, `Missing required "topic" query parameter`)
		return "", false
	}
	if len(topics) > 1 {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprint(w, "Too many topics requested. Only one topic subscription allowed per request")
		return "", false
	}
	return topics[0], true
}

// MethodFilter is an HTTP handler middleware that restricts the set of allowed
// HTTP methods for a given HandlerFunc.
func MethodFilter(allowedMethods map[string]struct{}, f http.HandlerFunc) http.HandlerFunc {
	if allowedMethods == nil {
		panic("invalid nil allowed method map")
	}
	var methods []string
	for m := range allowedMethods {
		methods = append(methods, m)
	}
	sort.Strings(methods)
	return func(w http.ResponseWriter, r *http.Request) {
		if _, ok := allowedMethods[r.Method]; !ok {
			w.WriteHeader(http.StatusMethodNotAllowed)
			_, _ = fmt.Fprintf(w, "Invalid method. Allowed methods are %v", methods)
			return
		}
		f(w, r)
	}
}

// RequestLogger is an HTTP middleware that logs the method and path of each
// request.
func RequestLogger(l Logger, f http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l.Printf("%s %s", r.Method, r.URL.Path)
		f(w, r)
	}
}

func createHandler(logger Logger) (http.Handler, error) {
	ps, err := NewPubSub(PubSubConfig{
		Logger: logger,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create PubSub: %w", err)
	}
	psh, err := NewPubSubHTTP(PubSubHTTPConfig{
		PubSub:          ps,
		MaxPublishBytes: 1 << 10, // 1 KiB.
		Logger:          logger,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create PubSubHTTP: %w", err)
	}
	l := logger
	if l == nil {
		l = log.Default()
	}
	mux := http.NewServeMux()

	// Adds endpoint with all middleware applied.
	// TODO: Clean up to be able to apply slice of middlewares.
	addEndpoint := func(path string, f http.HandlerFunc, methods ...string) {
		mm := make(map[string]struct{})
		for _, m := range methods {
			mm[m] = struct{}{}
		}
		mux.HandleFunc(path, RequestLogger(l, MethodFilter(mm, f)))
	}

	// Add http.MethodGet here to allow /subscribe to work with other websocket
	//clients (Postman, etc.).
	addEndpoint("/subscribe", psh.HandleSubscribe, http.MethodPost, http.MethodGet)
	addEndpoint("/publish", psh.HandlePublish, http.MethodPost)
	return mux, nil
}

// SubscriberConfig contains the configuration for a Subscriber.
type SubscriberConfig struct {
	Topic    string
	Writer   io.Writer
	HostPort string
	Logger   Logger
}

// NewSubscriber creates a Subscriber for a given topic.
//
// To start the Subscriber, call the Subscriber.Start method.
func NewSubscriber(conf SubscriberConfig) (*Subscriber, error) {
	u, err := url.Parse("ws://" + conf.HostPort)
	if err != nil {
		return nil, fmt.Errorf("invalid websocket host:port: %w", err)
	}
	if u.Host != conf.HostPort {
		return nil, fmt.Errorf("invalid websocket host:port (only host or host:port is required)")
	}
	if u.Port() == "" {
		u.Host += ":8081"
	}
	//u.Path = fmt.Sprintf("/subscribe?topic=%s", conf.Topic)
	if conf.Writer == nil {
		return nil, fmt.Errorf("invalid nil Writer")
	}
	l := conf.Logger
	if l == nil {
		l = log.Default()
	}
	return &Subscriber{
		w:     conf.Writer,
		topic: conf.Topic,
		wsURL: u.String() + fmt.Sprintf("/subscribe?topic=%s", conf.Topic),
		log:   l,
	}, nil
}

// Start starts the Subscriber. This function will block until either an error
// is encountered or ctx is done. A non-nil error should always be returned from
// this function.
func (s *Subscriber) Start(ctx context.Context) (e error) {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, s.wsURL, nil)
	if err != nil {
		return fmt.Errorf("unable to connect to websocket server at %q: %w", s.wsURL, err)
	}

	// Close the connection if the context is canceled.
	// This is required as calls to conn are blocking.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var closeOnce sync.Once
	closeConn := func() {
		// Notify the server that the client is finished.
		if err := conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseGoingAway, "")); err != nil {
			s.log.Printf("Error send close message to server: %v", err)
		}
		if err := conn.Close(); err != nil {
			s.log.Printf("Error closing Subscriber websocket connection: %v", err)
		}
	}
	// Handle converting the returned error to a context error if the context
	// cancellation lead to an error using the closed websocket connection.
	defer func() {
		// Check if the context is done.
		ctxIsDone := false
		select {
		case <-ctx.Done():
			ctxIsDone = true
		default:
		}
		// Likely the connection was closed due to the context cancellation, so
		// convert the error into a context cancellation error.
		if errors.Is(e, net.ErrClosed) && ctxIsDone {
			e = ctx.Err()
		}
	}()
	// Close the connection if either the context is cancelled or an error
	// occurs in the read loop.
	defer closeOnce.Do(closeConn)
	go func() {
		<-ctx.Done()
		closeOnce.Do(closeConn)
	}()
	for {
		_, m, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("unable to read message from websocket: %w", err)
		}
		_, err = fmt.Fprintf(s.w, "Received message [topic=%s]:\n%s\n", s.topic, m)
		if err != nil {
			return fmt.Errorf("unable to write out message from websocket: %w", err)
		}
	}
}

// Subscriber subscribes to a given /subscribe endpoint and outputs the message
// to the configured writer.
type Subscriber struct {
	w     io.Writer
	topic string
	wsURL string
	log   Logger
}

func runServer(port uint16) error {
	handler, err := createHandler(nil)
	if err != nil {
		return fmt.Errorf("unable to create HTTP handler: %w", err)
	}
	addr := fmt.Sprintf(":%d", port)
	log.Printf("Listening on port %d\nHTTP Endpoints:\nPOST /subscribe\nPOST /publish", port)
	if err := http.ListenAndServe(addr, handler); err != nil {
		return fmt.Errorf("error running HTTP server: %w", err)
	}
	return nil
}

func main() {
	var (
		isClient = flag.Bool("client", false, "Set to run in client/subscriber mode")
		host     = flag.String("host", "", "Host to connect to (client only)")
		topic    = flag.String("topic", "", "Topic to subscribe to (client only)")
		port     = flag.Uint("port", 8081, "Port to listen on (server only)")
	)
	flag.Parse()

	// Client.
	if *isClient {
		if *host == "" {
			log.Fatalf("invalid host, must be in form <host>:<port>")
		}
		sub, err := NewSubscriber(SubscriberConfig{
			Topic:    *topic,
			Writer:   os.Stdout,
			HostPort: *host,
		})
		if err != nil {
			log.Fatalf("Error creating subscriber client: %v", err)
		}
		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
		defer cancel()
		if err := sub.Start(ctx); err != nil {
			// Don't treat context cancellation as an error.
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Fatalf("Subscriber stopped: %v", err)
		}
		return
	}

	// Server.
	if *port > math.MaxUint16 || *port == 0 {
		log.Fatalf("invalid server port number %d", *port)
	}
	if err := runServer(uint16(*port)); err != nil {
		log.Fatalf("Error running HTTP server: %v", err)
	}
}
