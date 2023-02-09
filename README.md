[![Go Reference](https://pkg.go.dev/badge/github.com/bartke/datastream.svg)](https://pkg.go.dev/github.com/bartke/datastream)
[![Go Report Card](https://goreportcard.com/badge/github.com/bartke/datastream)](https://goreportcard.com/report/github.com/bartke/datastream)

# datastream

Datastream is a gRPC service definition that provides a consistent interface for
requesting, propagating and receiving real-time data updates. It aims to be a
simple interface for a common set of problems around use cases such as
configuration management, real-time data such as exchange rates, notification
subscribers, file management and similar.

- `ListCapabilities`: lists available keys for subscription
- `Sync`: sync with a server and receive the current state
- `Subscribe`: subscribe to the data stream and receive updates, initially syncs all keys
- `PushUpdate`: if supported, update and push a value update back on the server

Note: Make sure you have installed protoc and the Go protobuf plugin on your system.

## Backing stores

A storage interface and datastream grpc implementation example exists for
- **git repository** - key=file path, value=file content
- **sqlite3** - key=table column, value=table column
- **postgresql** - key=table column, value=table column
- **S3/minio compatible storage** - key=path, valu=file content

There is also a freestanding settings server implementation example using
sqlite3 with a local gRPC service implementation under `examples/server/` and a
self-communication example.

## Examples

The example setting service shows how a datastream service can be used to
requesting, subscribing and push updates.

```sh
make
./server &
./client &
./updater
kill %1 # also terminates the client
```

server
```
2023/01/17 18:58:05 Starting gRPC server on :8080
2023/01/17 18:58:08 method: /datastream.DataService/ListCapabilities, duration: 190.625µs, error: <nil>
2023/01/17 18:58:12 method: /datastream.DataService/PushUpdate, duration: 1.05157ms, error: <nil>
```

client
```
2023/01/17 18:58:08 Capabilities: [key:"max_connections" value_type:"int" key:"rate_limit" value_type:"int" key:"debug_enabled" value_type:"bool"]
2023/01/17 18:58:08 Subscribing to key: max_connections
2023/01/17 18:58:08 Received update for key max_connections value [49 48], stringified data 10
2023/01/17 18:58:12 Received update for key max_connections value [50 48], stringified data 20
```
