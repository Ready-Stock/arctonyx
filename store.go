package raft_badger

import (
	"fmt"
	"github.com/Ready-Stock/badger"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/kataras/go-errors"
	"github.com/kataras/golog"
	uuid2 "github.com/satori/go.uuid"
	"google.golang.org/grpc"
	"net"
	"os"
	"sync"
	"time"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

var (
	ServerIdPath = []byte("/_server_id_/")
)

type Store struct {
	raft        *raft.Raft
	badger      *badger.DB
	sequenceIds *badger.Sequence

	chunkMapMutex  *sync.Mutex
	sequenceChunks map[string]*SequenceChunk

	sequenceClient *sequenceClient
	clusterClient  *clusterClient
	server         *clusterServer

	nodeId string
}

// Creates and possibly joins a cluster.
func CreateStore(directory string, listen string, protoListen string, joinAddr string) (*Store, error) {
	// Setup Raft configuration.
	config := raft.DefaultConfig()
	config.CommitTimeout = 10 * time.Millisecond
	store := Store{}

	if listen == "" {
		listen = ":6543"
	}

	addr, err := net.ResolveTCPAddr("tcp", listen)
	if err != nil {
		return nil, err
	}

	transport, err := raft.NewTCPTransport(listen, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}

	opts := badger.DefaultOptions
	opts.Dir = directory
	opts.ValueDir = directory
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	store.badger = db

	stable := stableStore(store)
	log := logStore(store)
	nodeId := ""
	if id, err := stable.Get(ServerIdPath); err != nil {
		if err.Error() == "Key not found" {
			if uuid, err := uuid2.NewV4(); err != nil {
				return nil, err
			} else {
				stable.Set(ServerIdPath, []byte(uuid.String()))
				nodeId = string(uuid.String())
			}
		} else {
			return nil, err
		}
	} else {
		if string(id) == "" {
			if uuid, err := uuid2.NewV4(); err != nil {
				return nil, err
			} else {
				stable.Set(ServerIdPath, []byte(uuid.String()))
				nodeId = string(uuid.String())
			}
		} else {
			nodeId = string(id)
		}
	}
	config.LocalID = raft.ServerID(nodeId)
	store.nodeId = nodeId
	snapshots, err := raft.NewFileSnapshotStore(directory, retainSnapshotCount, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("file snapshot store: %s", err)
	}

	ra, err := raft.NewRaft(config, (*fsm)(&store), &log, &stable, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("new raft: %s", err)
	}
	store.raft = ra
	if joinAddr == "" {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		f := store.raft.BootstrapCluster(configuration)
		if f.Error() != nil {
			return nil, f.Error()
		}
	}
	if protoListen != "" {
		lis, err := net.Listen("tcp", protoListen)
		if err != nil {
			return nil, err
		}
		grpcServer := grpc.NewServer()
		RegisterClusterServiceServer(grpcServer, &clusterServer{store})
		go grpcServer.Serve(lis)
	}
	return &store, nil
}

func (store *Store) Join(nodeId, addr, chatter string) error {
	golog.Debugf("received join request from remote node [%s] at [%s]", nodeId, addr)

	configFuture := store.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		golog.Errorf("failed to get raft configuration: %s", err.Error())
	}

	for _, srv := range configFuture.Configuration().Servers {
		// If a node already exists with either the joining node's ID or address,
		// that node may need to be removed from the config first.
		if srv.ID == raft.ServerID(nodeId) || srv.Address == raft.ServerAddress(addr) {
			// However if *both* the ID and the address are the same, then nothing -- not even
			// a join operation -- is needed.
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(nodeId) {
				golog.Errorf("node %s at %s already member of cluster, ignoring join request", nodeId, addr)
				return nil
			}

			future := store.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return fmt.Errorf("error removing existing node %s at %s: %s", nodeId, addr, err)
			}
		}
	}
	f := store.raft.AddVoter(raft.ServerID(nodeId), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}
	golog.Infof("node %s at %s joined successfully", nodeId, addr)

	return nil
}

func (store *Store) Get(key []byte) (value []byte, err error) {
	resetCount := 0
latencyReset:
	err = store.badger.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if err.Error() != "Key not found" {
				return err
			} else {
				value = make([]byte, 0)
				return nil
			}
		}
		value, err = item.Value()
		return err
	})
	if resetCount == 0 {
		if len(value) == 0 && err == nil {
			resetCount++
			time.Sleep(100 * time.Millisecond)
			goto latencyReset
		}
	}
	return value, err
}

func (store *Store) Set(key, value []byte) (err error) {
	c := &Command{Operation: Operation_SET, Key: key, Value: value, Timestamp: uint64(time.Now().UnixNano())}
	if store.raft.State() != raft.Leader {
		if store.raft.Leader() == "" {
			return errors.New("no leader in cluster")
		}
		if _, err := store.clusterClient.sendCommand(store.raft.Leader(), c); err != nil {
			return err
		}
		return nil
	}
	b, err := proto.Marshal(c)
	if err != nil {
		return err
	}
	r := store.raft.Apply(b, raftTimeout)
	return r.Error()
}

func (store *Store) Delete(key []byte) (err error) {
	c := &Command{Operation: Operation_DELETE, Key: key, Value: nil, Timestamp: uint64(time.Now().UnixNano())}
	if store.raft.State() != raft.Leader {
		if _, err := store.clusterClient.sendCommand(store.raft.Leader(), c); err != nil {
			return err
		}
		return nil
	}
	b, err := proto.Marshal(c)
	if err != nil {
		return err
	}
	r := store.raft.Apply(b, raftTimeout)
	return r.Error()
}

func (store *Store) NodeID() string {
	return store.nodeId
}
