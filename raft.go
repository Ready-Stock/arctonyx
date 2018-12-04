package arctonyx

import (
	"context"
	"github.com/hashicorp/raft"
	"github.com/kataras/go-errors"
	"google.golang.org/grpc"
	"sync"
)

type clusterClient struct {
	Store
	conn    *grpc.ClientConn
	sync    *sync.Mutex
	addr    raft.ServerAddress
	cluster *clusterServiceClient
}

func (client *clusterClient) validateConnection(leaderAddr raft.ServerAddress) error {
	client.sync.Lock()
	defer client.sync.Unlock()
	if leaderAddr != client.addr {
		// If the address is not the same (the leader has changed) then update the connection and reconnect.
		newAddr, err := client.Store.getPeer(leaderAddr)
		if err != nil {
			return err
		}
		if client.conn != nil {
			client.conn.Close()
		}

		conn, err := grpc.Dial(newAddr, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			return err
		}
		client.cluster = &clusterServiceClient{cc: conn}
	}
	return nil
}

func (client *clusterClient) sendCommand(command *Command) (*CommandResponse, error) {
	if err := client.validateConnection(client.Store.raft.Leader()); err != nil {
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

func (client *clusterClient) getNextChunkInSequence(sequenceName string) (*SequenceChunkResponse, error) {
	if err := client.validateConnection(client.Store.raft.Leader()); err != nil {
		return nil, err
	}
	if result, err := client.cluster.GetSequenceChunk(context.Background(), &SequenceChunkRequest{SequenceName: sequenceName}); err != nil {
		return nil, err
	} else {
		return result, nil
	}
}

func (client *clusterClient) joinCluster(addr string) (*JoinResponse, error) {
	return client.cluster.Join(context.Background(), &JoinRequest{RaftAddress: client.listen, ChatterAddress: client.chatterListen, Id: client.nodeId})
}
