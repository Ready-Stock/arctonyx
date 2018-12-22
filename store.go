package arctonyx

import (
	"context"
	"fmt"
	"github.com/dgraph-io/badger"
	"github.com/golang/protobuf/proto"
	"github.com/kataras/go-errors"
	"github.com/kataras/golog"
	"github.com/readystock/raft"
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

type KeyValue struct {
	Key   []byte
	Value []byte
}

type Store struct {
	raft              *raft.Raft
	badger            *badger.DB
	sequenceIds       *badger.Sequence
	chunkMapMutex     *sync.Mutex
	sequenceCacheSync *sync.Mutex
	sequenceChunks    map[string]*SequenceChunk
	sequenceCache     map[string]*Sequence
	clusterClient     *clusterClient
	server            *grpc.Server
	nodeId            uint64
	listen            string
}

// Creates and possibly joins a cluster.
func CreateStore(directory string, listen string, joinAddr string) (*Store, error) {
	// Setup Raft configuration.
	config := raft.DefaultConfig()
	config.CommitTimeout = 1 * time.Second
	store := Store{
		chunkMapMutex:     new(sync.Mutex),
		sequenceCacheSync: new(sync.Mutex),
		sequenceCache:     map[string]*Sequence{},
		sequenceChunks:    map[string]*SequenceChunk{},
		listen:            listen,
	}

	if listen == "" {
		listen = ":6543"
	}

	// addr, err := net.ResolveTCPAddr("tcp", listen)
	// if err != nil {
	// 	return nil, err
	// }

	lis, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}
	grpcServer := grpc.NewServer()
	//ctx := context.Background()
	//transport:= raftgrpc.NewTransport(ctx, "")

	transport, err := raft.NewGrpcTransport(grpcServer, lis.Addr().String())
	// transport, err := raft.NewTCPTransport(listen, addr, 3, 10*time.Second, os.Stderr)
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

	nodeId := uint64(0)

	clusterExists := false

	if nodeIdBytes, _ := store.Get(serverIdPath); len(nodeIdBytes) > 0 {
		clusterExists = true
		nodeId = bytesToUint64(nodeIdBytes)
	}

	if joinAddr != "" {
		conn, err := grpc.Dial(joinAddr, grpc.WithInsecure())
		if err != nil {
			return nil, err
		}
		tempClient := &clusterServiceClient{cc: conn}

		if !clusterExists {
			response, err := tempClient.GetNodeID(context.Background(), &GetNodeIdRequest{})
			if err != nil {
				return nil, err
			}
			nodeId = response.NodeId
			stable.Set(serverIdPath, uint64ToBytes(nodeId))
		}

		defer func() {
			golog.Debugf("node %d joining cluster at addr %s!", nodeId, joinAddr)
			tempClient.Join(context.Background(), &JoinRequest{RaftAddress: listen, Id: nodeId})
		}()
	} else {
		if !clusterExists {
			stable.Set(serverIdPath, uint64ToBytes(nodeId))
		}
	}

	config.LocalID = raft.ServerID(nodeId)
	store.nodeId = nodeId
	snapshots, err := raft.NewFileSnapshotStore(directory, retainSnapshotCount, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("file snapshot store: %s", err)
	}

	if clusterExists {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		err := raft.RecoverCluster(config, (*fsm)(&store), &log, &stable, snapshots, transport, configuration)
		if err != nil {
			return nil, fmt.Errorf("recover raft: %s", err)
		}
	}
	ra, err := raft.NewRaft(config, (*fsm)(&store), &log, &stable, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("new raft: %s", err)
	}
	store.raft = ra
	RegisterClusterServiceServer(grpcServer, &clusterServer{store})
	//raftgrpc.RegisterRaftServiceServer(grpcServer, )
	go grpcServer.Serve(lis)
	if joinAddr == "" && nodeId == 0 && !clusterExists {
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
		time.Sleep(5 * time.Second)
		store.setPeer(nodeId, listen)
	}


	store.server = grpcServer
	store.clusterClient = &clusterClient{Store: store, sync: new(sync.Mutex)}
	return &store, nil
}

func (store *Store) join(nodeId uint64, addr string) error {
	golog.Debugf("received join request from remote node [%d] at [%s]", nodeId, addr)

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
				golog.Errorf("node %d at %s already member of cluster, ignoring join request", nodeId, addr)
				return nil
			}

			future := store.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return fmt.Errorf("error removing existing node %d at %s: %s", nodeId, addr, err)
			}
		}
	}
	f := store.raft.AddVoter(raft.ServerID(nodeId), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}
	store.setPeer(nodeId, addr)
	golog.Infof("node %d at %s joined successfully", nodeId, addr)
	return nil
}

func (store *Store) setPeer(nodeId uint64, addr string) error {
	peer := &Peer{
		NodeId:      nodeId,
		RaftAddr:    addr,
	}
	b, err := proto.Marshal(peer)
	if err != nil {
		return err
	}
	return store.Set([]byte(fmt.Sprintf("%s%s", peerPath, addr)), b)
}

func (store *Store) getPeer(server raft.ServerAddress) (addr string, err error) {
	peer := Peer{}
	bytes, err := store.Get([]byte(fmt.Sprintf("%s%s", peerPath, server)))
	if err != nil {
		return addr, err
	}
	err = proto.Unmarshal(bytes, &peer)
	addr = peer.RaftAddr
	return addr, nil
}

func (store *Store) GetPrefix(prefix []byte) (values []KeyValue, err error) {
	values = make([]KeyValue, 0)
	err = store.badger.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			valueBytes := make([]byte, 0)
			if valueBytes, err = item.ValueCopy(valueBytes); err != nil {
				return err
			} else {
				values = append(values, KeyValue{Key: item.Key(), Value: valueBytes})
			}
		}
		return nil
	})
	return values, err
}

func (store *Store) Get(key []byte) (value []byte, err error) {
	golog.Debugf("[%d] Getting key: %s", store.nodeId, string(key))
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
		value, err = item.ValueCopy(value)
		return err
	})
	return value, err
}

func (store *Store) Set(key, value []byte) (err error) {
	c := &Command{Operation: Operation_SET, Key: key, Value: value, Timestamp: uint64(time.Now().UnixNano())}
	if store.raft.State() != raft.Leader {
		if store.raft.Leader() == "" {
			return errors.New("no leader in cluster")
		}
		golog.Debugf("[%d] Proxying set key: %s to %s", store.nodeId, string(key), string(value))
		if _, err := store.clusterClient.sendCommand(c); err != nil {
			return err
		}
		return nil
	}
	golog.Debugf("[%d] Initiating set key: %s to %s", store.nodeId, string(key), string(value))
	b, err := proto.Marshal(c)
	if err != nil {
		return err
	}
	r := store.raft.Apply(b, raftTimeout)
	if err := r.Error(); err != nil {
		return err
	} else if resp, ok := r.Response().(CommandResponse); ok {
		golog.Debugf("[%d] Delay Total [%s] Response [%s]", store.nodeId, time.Since(time.Unix(0, int64(resp.Timestamp))), time.Since(time.Unix(0, int64(resp.AppliedTimestamp))))
	}
	return nil
}

func (store *Store) Delete(key []byte) (err error) {
	c := &Command{Operation: Operation_DELETE, Key: key, Value: nil, Timestamp: uint64(time.Now().UnixNano())}
	if store.raft.State() != raft.Leader {
		if _, err := store.clusterClient.sendCommand(c); err != nil {
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

func (store *Store) NodeID() uint64 {
	return store.nodeId
}

func (store *Store) IsLeader() bool {
	return store.raft.State() == raft.Leader
}

func (store *Store) Close() {
	snap := store.raft.Snapshot()
	if snap.Error() != nil {
		golog.Error(snap.Error())
	}
	store.raft.Shutdown()
	store.badger.Close()
	store.server.Stop()
}
