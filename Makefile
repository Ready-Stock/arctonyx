.PHONY: default build protos
default: build

build:
	protos
	go build

PROTOS_DIRECTORY = ./protos

protos:
	protoc -I=$(PROTOS_DIRECTORY) --go_out=plugins=grpc:./cluster $(PROTOS_DIRECTORY)/raft.proto
	protoc -I=$(PROTOS_DIRECTORY) --go_out=plugins=grpc:./ $(PROTOS_DIRECTORY)/sequence.proto
