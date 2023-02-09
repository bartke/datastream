
PROTO_SRC=api/datastream
PROTO_DST=generated/datastream

PROTOC_CMD = protoc
PROTOC_OPTS = -I $(PROTO_SRC) $< --go-grpc_out=. --go_out=.

# The target of the makefile
.PHONY: all
all: $(PROTO_DST)/*.pb.go server server-sqlite server-git server-s3 client updater

# Compile the Protocol Buffer definitions
$(PROTO_DST)/%.pb.go: $(PROTO_SRC)/*.proto
	@mkdir -p $(PROTO_DST)
	$(PROTOC_CMD) $(PROTOC_OPTS) $<


# Compile the server
server: $(PROTO_DST)/*.pb.go examples/server/*.go
	go build -o $@ examples/server/main.go

# Compile the server
server-sqlite: $(PROTO_DST)/*.pb.go examples/server-sqlite/*.go
	go build -o $@ examples/server-sqlite/main.go

# Compile the server
server-git: $(PROTO_DST)/*.pb.go examples/server-git/*.go
	go build -o $@ examples/server-git/main.go

# Compile the server
server-s3: $(PROTO_DST)/*.pb.go examples/server-s3/*.go
	go build -o $@ examples/server-s3/main.go

# Compile the client
client: $(PROTO_DST)/*.pb.go examples/client/*.go
	go build -o $@ examples/client/main.go

# Compile the client
updater: $(PROTO_DST)/*.pb.go examples/updater/*.go
	go build -o $@ examples/updater/main.go
