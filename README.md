# datastream

Simple gRPC service definition to request data and receive updates by push from
the source. The idea is to solve the common problem of different approaches to
data streaming and provide a consistent interface for requesting and receiving
data updates.

This is applicable to data such as settings and excange rates.

- ListCapabilities: lists available keys for subscription
- Sync: sync with a server and receive the current state
- Subscribe: subscribe to the data stream and receive updates
- PushUpdate: if supported, update a push a value update back on the server

