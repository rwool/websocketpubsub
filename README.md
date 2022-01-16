# websocketpubsub

## Requirements
* Go 1.17 or later

# Usage
## Server
Start the server with the following command:
```shell
go run ./main.go
```
#### Example Output
```shell
$ go run ./main.go
2022/01/16 15:37:23 Listening on port 8081
HTTP Endpoints:
POST /subscribe
POST /publish
2022/01/16 15:37:40 GET /subscribe
2022/01/16 15:37:40 Added subscription to "abc"
2022/01/16 15:37:49 POST /publish
2022/01/16 15:37:50 POST /publish
2022/01/16 15:37:51 POST /publish
2022/01/16 15:37:55 Stopping websocket connection (context cancellation: context canceled)
2022/01/16 15:37:55 Closing /subscribe websocket connection
2022/01/16 15:37:55 Removed subscription from "abc"
```

## Client
### Go Client
Start the subscriber client with the following command:
```shell
go run ./main.go -client -host=localhost:8081 -topic=abc
```
Where "abc" can be replaced with the name of the topic to subscribe to.

#### Example Output
```shell
go run ./main.go -client -host=localhost:8081 -topic=abc
Received message [topic=abc]:
{"hello": "abc123"}
Received message [topic=abc]:
{"hello": "abc"}
Received message [topic=abc]:
{"goodbye": true}
```

### curl Client
curl can also be used to connect to the subscription websocket. Note that this uses "http" as the schema instead of "ws", but otherwise works as expected.
```shell
curl -XPOST --include \
     --no-buffer \
     --header "Connection: Upgrade" \
     --header "Upgrade: websocket" \
     --header "Host: example.com:80" \
     --header "Origin: http://example.com:80" \
     --header "Sec-WebSocket-Key: SGVsbG8sIHdvcmxkIQ==" \
     --header "Sec-WebSocket-Version: 13" \
     'http://localhost:8081/subscribe?topic=abc'
```

## Publishing
To publish to a client the following command may be used:
```shell
curl -XPOST -H 'Content-Type: application/json' 'http://localhost:8081/publish?topic=abc' -d '{"hello": true}'
```
Where "abc" can be replaced with any topic name and `{"hello": true}` can be replaced with any valid JSON text.

## Code Organization
The code is separated into three main types:
* `PubSub`
* `PubSubHTTP`
* `Subscriber`

`PubSub` is responsible for storing the received messages in memory and allowing subscription to the messages on a per-topic basis. All operations with PubSub are thread-safe.

`PubSubHTTP` is responsible for handling the HTTP operations related to publishing and subscribing and is responsible for request validation and interaction with the internal PubSub object.

`Subscriber` is responsible for subscribing to a single topic and giving "pretty" output of the messages that are seen.


# Notes
* If there are multiple subscribers to a single topic, then any published messages will be delivered to all subscribers.
* If topics become empty with no subscribers, they are not currently removed internally.
* Websocket ping and pong control messages are not currently supported.
* The HTTP POST method is supported for the /subscribe endpoint despite appearing to be non-standard for websocket connections. HTTP GET requests are also supported to allow for support with most clients.
* Any messages published without active subscribers will be dropped.
* Much of the HTTP handler code could be simplified with third-party libraries.
* Running the tests with the race detector enabled can occasionally fail due to race condition related to logging in the testing.common methods. It is not clear at this time if this is known bug, an unknown bug, or if I'm missing documentation that states that the log methods require special usage to guarantee thread-safety.