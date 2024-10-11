ROOT_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

.PHONY: all build test proto clean
all: build test

build:
	go build ./...

test:
	go test -v ./...

format:
	gofumpt -l -w .

proto: internal/proto/asynq.proto
	protoc -I=$(ROOT_DIR)/internal/proto \
				 --go_out=$(ROOT_DIR)/internal/proto \
				 --go_opt=module=github.com/AsynqLab/asynq/internal/proto \
				 $(ROOT_DIR)/internal/proto/asynq.proto

generate: proto

clean:
	go clean
	rm -f $(ROOT_DIR)/internal/proto/*.pb.go
