package raft_badger

import (
	"context"
	"github.com/hashicorp/raft"
	"github.com/kataras/go-errors"
	"sync"
)

type clusterClient struct {
	sync    *sync.Mutex
	addr    raft.ServerAddress
	cluster *clusterServiceClient
}

func (client *clusterClient) validateConnection(leaderAddr raft.ServerAddress) error {
	client.sync.Lock()
	defer client.sync.Unlock()
	if leaderAddr != client.addr {
		// If the address is not the same (the leader has changed) then update the connection and reconnect.
	}
	return nil
}

func (client *clusterClient) sendCommand(leaderAddr raft.ServerAddress, command *Command) (*CommandResponse, error) {
	if err := client.validateConnection(leaderAddr); err != nil {
		return nil, err
	}
	if result, err := client.cluster.SendCommand(context.Background(), command); err != nil {
		return nil, err
	} else if !result.IsSuccess {
		return nil, errors.New(result.ErrorMessage)
	} else {
		return result, nil
	}
}

func (client *clusterClient) joinCluster(addr string) (*JoinResponse, error) {
	return nil, nil
}