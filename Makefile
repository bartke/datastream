
PROTO_SRC=api/datastream
PROTO_DST=generated/datastream

PROTOC_CMD = protoc
PROTOC_OPTS = -I $(PROTO_SRC) $< --go-grpc_out=. --go_out=.

# The target of the makefile
.PHONY: all
all: $(PROTO_DST)/*.pb.go server client updater

# Compile the Protocol Buffer definitions
$(PROTO_DST)/%.pb.go: $(PROTO_SRC)/*.proto
	@mkdir -p api
	$(PROTOC_CMD) $(PROTOC_OPTS) $<


# Compile the server
server: $(PROTO_DST)/*.pb.go examples/server/*.go
	go build -o $@ examples/server/main.go

# Compile the client
client: $(PROTO_DST)/*.pb.go examples/client/*.go
	go build -o $@ examples/client/main.go

# Compile the client
updater: $(PROTO_DST)/*.pb.go examples/updater/*.go
	go build -o $@ examples/updater/main.go
