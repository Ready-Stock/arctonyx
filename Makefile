.PHONY: default build protos
default: build

build: protos
	go build
	go test -v ./

PROTOS_DIRECTORY = ./protos

protos:
	protoc -I=$(PROTOS_DIRECTORY) --go_out=plugins=grpc:./ $(PROTOS_DIRECTORY)/raft.proto
	protoc -I=$(PROTOS_DIRECTORY) --go_out=plugins=grpc:./ $(PROTOS_DIRECTORY)/sequence.proto
	protoc -I=$(PROTOS_DIRECTORY) --go_out=./ $(PROTOS_DIRECTORY)/fsm.proto
