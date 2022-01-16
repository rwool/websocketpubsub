package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type testLogger struct {
	t *testing.T
	// Race detector indicates that t.LogX is not thread-safe. Adding this mutex
	// to prevent race conditions in tests.
	mu sync.Mutex
}

func (t *testLogger) Printf(format string, a ...interface{}) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.t.Logf(format, a...)
}

func TestPubSub(t *testing.T) {
	t.Parallel()
	type testDependencies struct {
		PS   *PubSub
		Ctx  context.Context
		SubC <-chan []byte
	}
	setup := func(t *testing.T, topic string) testDependencies {
		t.Parallel()
		tl := &testLogger{t: t}
		ps, err := NewPubSub(PubSubConfig{
			Logger: tl,
		})
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		t.Cleanup(cancel)
		subC, err := ps.Subscribe(ctx, topic)
		require.NoError(t, err)
		return testDependencies{
			PS:   ps,
			Ctx:  ctx,
			SubC: subC,
		}
	}
	t.Run("publish one empty message with empty named topic with subscriber", func(t *testing.T) {
		td := setup(t, "")
		g, ctx := errgroup.WithContext(td.Ctx)
		g.Go(func() error {
			return td.PS.Publish(ctx, "", nil)
		})
		g.Go(func() error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case m := <-td.SubC:
				assert.Equal(t, ([]byte)(nil), m)
			}
			return nil
		})
		assert.NoError(t, g.Wait())
	})
	t.Run("publish one empty message with empty named topic without subscriber", func(t *testing.T) {
		td := setup(t, "")
		assert.NoError(t, td.PS.Publish(td.Ctx, "x", nil))
	})
	t.Run("publish one non-empty message to empty named topic with subscriber", func(t *testing.T) {
		td := setup(t, "")
		g, ctx := errgroup.WithContext(td.Ctx)
		g.Go(func() error {
			return td.PS.Publish(ctx, "", []byte("a"))
		})
		g.Go(func() error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case m := <-td.SubC:
				assert.Equal(t, []byte("a"), m)
			}
			return nil
		})
		assert.NoError(t, g.Wait())
	})
	t.Run("publish and subscribe one million messages", func(t *testing.T) {
		td := setup(t, "1 million")
		// Make messages to send.
		ms := make([][]byte, 1_000_000)
		for i := range ms {
			ms[i] = []byte(strconv.Itoa(i))
		}

		g, ctx := errgroup.WithContext(td.Ctx)
		g.Go(func() error {
			for _, v := range ms {
				if err := td.PS.Publish(ctx, "1 million", v); err != nil {
					return err
				}
			}
			return nil
		})
		g.Go(func() error {
			for i := 0; i < 1_000_000; i++ {
				s := []byte(strconv.Itoa(i))
				select {
				case <-ctx.Done():
					return ctx.Err()
				case m := <-td.SubC:
					if !assert.Equal(t, s, m) {
						return errors.New("unequal numbers")
					}
				}
			}
			return nil
		})
		assert.NoError(t, g.Wait())
	})
	t.Run("publish and subscribe multiple different topics", func(t *testing.T) {
		td := setup(t, "abc")
		g, _ := errgroup.WithContext(td.Ctx)
		xyzC, err := td.PS.Subscribe(td.Ctx, "xyz")
		require.NoError(t, err)
		g.Go(func() error {
			return td.PS.Publish(td.Ctx, "xyz", []byte("toXYZ"))
		})
		g.Go(func() error {
			return td.PS.Publish(td.Ctx, "abc", []byte("toABC"))
		})
		select {
		case <-td.Ctx.Done():
			t.Fatalf("timeout waiting for published message")
		case m := <-xyzC:
			assert.Equal(t, []byte("toXYZ"), m)
		}
		select {
		case <-td.Ctx.Done():
			t.Fatalf("timeout waiting for published message")
		case m := <-td.SubC:
			assert.Equal(t, []byte("toABC"), m)
		}
		require.NoError(t, g.Wait())
	})
	t.Run("publish once and subscribe multiple", func(t *testing.T) {
		td := setup(t, "abc")
		g, _ := errgroup.WithContext(td.Ctx)
		abcC, err := td.PS.Subscribe(td.Ctx, "abc")
		require.NoError(t, err)
		g.Go(func() error {
			select {
			case <-td.Ctx.Done():
				return td.Ctx.Err()
			case m := <-abcC:
				t.Log("GOT TD ABC")
				assert.Equal(t, []byte("toABC"), m)
			}
			return nil
		})
		g.Go(func() error {
			select {
			case <-td.Ctx.Done():
				return td.Ctx.Err()
			case m := <-td.SubC:
				t.Log("GOT NEW ABC")
				assert.Equal(t, []byte("toABC"), m)
			}
			return nil
		})
		g.Go(func() error {
			return td.PS.Publish(td.Ctx, "abc", []byte("toABC"))
		})
		require.NoError(t, g.Wait())
	})
}

func TestPubSubHTTP(t *testing.T) {
	t.Parallel()
	type testDependencies struct {
		Server    *httptest.Server
		Ctx       context.Context
		WSURLBase string
		Logger    Logger
	}
	setup := func(t *testing.T) testDependencies {
		t.Parallel()
		tl := &testLogger{t: t}
		h, err := createHandler(tl)
		require.NoError(t, err)
		s := httptest.NewServer(h)
		t.Cleanup(s.Close)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		t.Cleanup(cancel)
		return testDependencies{
			Server:    s,
			Ctx:       ctx,
			WSURLBase: strings.Replace(s.URL, "http", "ws", 1),
			Logger:    tl,
		}
	}
	t.Run("publish succeeds", func(t *testing.T) {
		td := setup(t)
		pubURL := fmt.Sprintf("%s/publish?topic=abc", td.Server.URL)
		r, err := http.NewRequest(http.MethodPost, pubURL, bytes.NewReader([]byte(`{"hello": true}`)))
		require.NoError(t, err)
		r.Header = map[string][]string{
			"Content-Type": {"application/json"},
		}
		r = r.WithContext(td.Ctx)
		resp, err := http.DefaultClient.Do(r)
		require.NoError(t, err)
		assert.Equal(t, http.StatusCreated, resp.StatusCode)
	})
	t.Run("publish no Content-Type", func(t *testing.T) {
		td := setup(t)
		pubURL := fmt.Sprintf("%s/publish?topic=abc", td.Server.URL)
		r, err := http.NewRequest(http.MethodPost, pubURL, bytes.NewReader([]byte(`{"hello": true}`)))
		require.NoError(t, err)
		r.Header = map[string][]string{
			"Content-Type": {"application/json"},
		}
		r.Header = map[string][]string{}
		r = r.WithContext(td.Ctx)
		resp, err := http.DefaultClient.Do(r)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})
	t.Run("publish invalid JSON", func(t *testing.T) {
		td := setup(t)
		pubURL := fmt.Sprintf("%s/publish?topic=abc", td.Server.URL)
		r, err := http.NewRequest(http.MethodPost, pubURL, bytes.NewReader([]byte(`{{"hello": true}`)))
		require.NoError(t, err)
		r.Header = map[string][]string{
			"Content-Type": {"application/json"},
		}
		r = r.WithContext(td.Ctx)
		resp, err := http.DefaultClient.Do(r)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})
	t.Run("publish too large message", func(t *testing.T) {
		td := setup(t)
		pubURL := fmt.Sprintf("%s/publish?topic=abc", td.Server.URL)
		msg := make([]byte, 1<<11)
		r, err := http.NewRequest(http.MethodPost, pubURL, bytes.NewReader(msg))
		require.NoError(t, err)
		r.Header = map[string][]string{
			"Content-Type": {"application/json"},
		}
		r = r.WithContext(td.Ctx)
		resp, err := http.DefaultClient.Do(r)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})
	t.Run("publish with no topic", func(t *testing.T) {
		td := setup(t)
		pubURL := fmt.Sprintf("%s/publish", td.Server.URL)
		r, err := http.NewRequest(http.MethodPost, pubURL, bytes.NewReader([]byte(`{"hello": true}`)))
		require.NoError(t, err)
		r.Header = map[string][]string{
			"Content-Type": {"application/json"},
		}
		r = r.WithContext(td.Ctx)
		resp, err := http.DefaultClient.Do(r)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})
	t.Run("publish with more than one topic", func(t *testing.T) {
		td := setup(t)
		subURL := fmt.Sprintf("%s/publish?topic=abc&topic=xyz", td.Server.URL)
		r, err := http.NewRequest(http.MethodPost, subURL, bytes.NewReader([]byte(`{"hello": true}`)))
		require.NoError(t, err)
		r.Header = map[string][]string{
			"Content-Type": {"application/json"},
		}
		r = r.WithContext(td.Ctx)
		resp, err := http.DefaultClient.Do(r)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})
	t.Run("publish with incorrect method", func(t *testing.T) {
		td := setup(t)
		pubURL := fmt.Sprintf("%s/publish?topic=abc", td.Server.URL)
		r, err := http.NewRequest(http.MethodGet, pubURL, bytes.NewReader([]byte(`{"hello": true}`)))
		require.NoError(t, err)
		r.Header = map[string][]string{
			"Content-Type": {"application/json"},
		}
		r = r.WithContext(td.Ctx)
		resp, err := http.DefaultClient.Do(r)
		require.NoError(t, err)
		assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	})
	t.Run("subscribe succeeds", func(t *testing.T) {
		td := setup(t)
		subURL := fmt.Sprintf("%s/subscribe?topic=abc", td.WSURLBase)
		conn, _, err := websocket.DefaultDialer.DialContext(td.Ctx, subURL, nil)
		require.NoError(t, err)
		assert.NoError(t, conn.Close())
	})
	t.Run("publish and subscribe one", func(t *testing.T) {
		td := setup(t)

		g, ctx := errgroup.WithContext(td.Ctx)
		subURL := fmt.Sprintf("%s/subscribe?topic=abc", td.WSURLBase)
		conn, _, err := websocket.DefaultDialer.DialContext(ctx, subURL, nil)
		require.NoError(t, err)

		g.Go(func() error {
			mtype, msg, err := conn.ReadMessage()
			assert.NoError(t, err)
			assert.Equal(t, websocket.TextMessage, mtype)
			assert.Equal(t, []byte(`{"hello": true}`), msg)
			assert.NoError(t, conn.Close())
			return nil
		})
		g.Go(func() error {
			pubURL := fmt.Sprintf("%s/publish?topic=abc", td.Server.URL)
			r, err := http.NewRequest(http.MethodPost, pubURL, bytes.NewReader([]byte(`{"hello": true}`)))
			require.NoError(t, err)
			r.Header = map[string][]string{
				"Content-Type": {"application/json"},
			}
			r = r.WithContext(ctx)
			resp, err := http.DefaultClient.Do(r)
			require.NoError(t, err)
			require.Equal(t, http.StatusCreated, resp.StatusCode)
			return nil
		})
		require.NoError(t, g.Wait())

	})
	t.Run("publish and subscribe with Subscriber", func(t *testing.T) {
		td := setup(t)

		// Create Subscriber.
		var buf strings.Builder
		u, err := url.Parse(td.WSURLBase)
		require.NoError(t, err)
		sub, err := NewSubscriber(SubscriberConfig{
			Topic:    "abc",
			Writer:   &buf,
			HostPort: u.Host,
			Logger:   td.Logger,
		})
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(td.Ctx, 2*time.Second)
		defer cancel()
		g, ctx := errgroup.WithContext(ctx)
		g.Go(func() error {
			// Start Subscriber.
			return sub.Start(ctx)
		})
		time.Sleep(100 * time.Millisecond) // Wait for subscription to start.
		g.Go(func() error {
			pubURL := fmt.Sprintf("%s/publish?topic=abc", td.Server.URL)
			r, err := http.NewRequest(http.MethodPost, pubURL, bytes.NewReader([]byte(`{"hello": true}`)))
			require.NoError(t, err)
			r.Header = map[string][]string{
				"Content-Type": {"application/json"},
			}
			r = r.WithContext(td.Ctx)
			resp, err := http.DefaultClient.Do(r)
			require.NoError(t, err)
			require.Equal(t, http.StatusCreated, resp.StatusCode)
			return nil
		})
		// Deadline exceeded is expected from Subscriber context timeout.
		assert.ErrorIs(t, g.Wait(), context.DeadlineExceeded)

	})
}
