PROTO_DIR   := proto
GEN_DIR     := internal/gen/proto
BROKER_BIN  := bin/broker
CLI_BIN     := bin/cli
GO          := go

.PHONY: all proto build clean run-broker run-cluster stop-cluster web bench test

all: proto build

proto:
	@mkdir -p $(GEN_DIR)
	protoc \
		--go_out=$(GEN_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_DIR) --go-grpc_opt=paths=source_relative \
		-I $(PROTO_DIR) \
		$(PROTO_DIR)/broker.proto $(PROTO_DIR)/consumer.proto $(PROTO_DIR)/raft.proto
	@echo "proto generation done"

build:
	@mkdir -p bin
	$(GO) build -o $(BROKER_BIN) ./cmd/broker
	$(GO) build -o $(CLI_BIN) ./cmd/cli
	@echo "build done → $(BROKER_BIN) $(CLI_BIN)"

test:
	$(GO) test ./... -v -timeout 30s

clean:
	rm -rf bin data/broker*
	find $(GEN_DIR) -name '*.go' -delete

# Single-node quick start
run-broker:
	@mkdir -p data
	./bin/broker -id 1 -grpc :9092 -ws :9095

# 3-node cluster (each in background)
run-cluster: build
	@mkdir -p data/broker1 data/broker2 data/broker3 logs
	./bin/broker -config configs/broker1.yaml > logs/broker1.log 2>&1 &
	./bin/broker -config configs/broker2.yaml > logs/broker2.log 2>&1 &
	./bin/broker -config configs/broker3.yaml > logs/broker3.log 2>&1 &
	@echo "cluster started — logs in ./logs/"

stop-cluster:
	@pkill -f "bin/broker" || true
	@echo "cluster stopped"

web:
	cd web && npm install && npm run dev

# Quick benchmark (requires broker running on :9092)
bench: build
	./bin/cli -mode create-topic -topic bench-topic -partitions 3 -addr localhost:9092 || true
	./bin/cli -mode bench -topic bench-topic -count 100000 -batch 10 -partitions 3 -addr localhost:9092

tidy:
	$(GO) mod tidy
