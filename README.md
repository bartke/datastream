# datastream

Simple gRPC service definition to request data and receive updates by push from
the source. The idea is to solve the common problem of different approaches to
data streaming and provide a consistent interface for requesting, propagating
and receiving data updates.

This is applicable to use cases such as configuration management, real-time data
such as exchange rates, notification subscribers, file management and similar.

- `ListCapabilities`: lists available keys for subscription
- `Sync`: sync with a server and receive the current state
- `Subscribe`: subscribe to the data stream and receive updates, initially syncs all keys
- `PushUpdate`: if supported, update and push a value update back on the server

Note: Make sure you have installed protoc and the Go protobuf plugin on your system.

## Backing stores

A storage interface and datastream grpc implementation example exists for
- **git repository** - key=file path, value=file content
- **sqlite3** - key=table column, value=table column
- **postgres** - key=table column, value=table column

There is also a freestanding settings server implementation example using
sqlite3 with a local gRPC service implementation under `examples/server/`.

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
