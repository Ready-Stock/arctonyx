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

func (client *clusterClient) set(leaderAddr raft.ServerAddress, key, value []byte) error {
	client.sync.Lock()
	if leaderAddr != client.addr {
		// If the address is not the same (the leader has changed) then update the connection and reconnect.
	}
	client.sync.Unlock()

	if result, err := client.cluster.Set(context.Background(), &SetRequest{Key:key, Value:value}); err != nil {
		return err
	} else if !result.IsSuccess {
		return errors.New(result.ErrorMessage)
	} else {
		return nil
	}
}