package raft_badger

import (
	"context"
	"github.com/kataras/go-errors"
)

// ClusterServiceServer is the server API for ClusterService service.
type clusterServer struct {
	Store
}

func (server *clusterServer) SendCommand(ctx context.Context, command *Command) (*CommandResponse, error) {
	switch command.Operation {
	case Operation_DELETE:
		return server.serverDelete(*command)
	case Operation_SET:
		return server.serverSet(*command)
	case Operation_GET:
		return nil, nil
	default:
		return nil, errors.New("could not handle operation %d").Format(command.Operation)
	}
}

func (server *clusterServer) Join(ctx context.Context, join *JoinRequest) (*JoinResponse, error) {
	response := &JoinResponse{}
	if err := server.Store.Join(join.Id, join.RaftAddress, join.ServerAddress); err != nil {
		response.IsSuccess = false
		response.ErrorMessage = err.Error()
	} else {
		response.IsSuccess = true
	}
	return response, nil
}

func (server *clusterServer) serverSet(command Command) (*CommandResponse, error) {
	response := &CommandResponse{}
	if err := server.Store.Set(command.Key, command.Value); err != nil {
		response.ErrorMessage = err.Error()
		response.IsSuccess = false
	} else {
		response.IsSuccess = true
	}
	response.Operation = command.Operation
	return response, nil
}

func (server *clusterServer) serverDelete(command Command) (*CommandResponse, error) {
	response := &CommandResponse{}
	if err := server.Store.Delete(command.Key); err != nil {
		response.ErrorMessage = err.Error()
		response.IsSuccess = false
	} else {
		response.IsSuccess = true
	}
	response.Operation = command.Operation
	return response, nil
}