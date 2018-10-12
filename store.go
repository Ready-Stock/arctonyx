package raft_badger

import (
	"github.com/Ready-Stock/badger"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/kataras/golog"
	"sync"
	"time"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 5 * time.Second
)


type Store struct {
	raft        *raft.Raft
	badger      *badger.DB
	sequenceIds *badger.Sequence

	chunkMapMutex  *sync.Mutex
	sequenceChunks map[string]*SequenceChunk

	sequenceClient *sequenceClient
	clusterClient  *clusterClient
	NodeId         uint64
}

// Creates and possibly joins a cluster.
func CreateStore(directory string, joinAddr *string) (*Store, error) {
	store := Store{

	}
	opts := badger.DefaultOptions

	opts.Dir = directory
	opts.ValueDir = directory
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	store.badger = db

	// nodeId := uint64(-1)
	// if joinAddr != nil {
	//
	// } else if nId, err := store.getNextNodeID(); err != nil {
	// 	return nil, err
	// } else {
	// 	nodeId = nId
	// }
	//
	// store.NodeId = nodeId

	return &store, nil
}

func (store *Store) Join(nodeId, addr string) error {
	golog.Debugf("received join request from remote node [%s] at [%s]", nodeId, addr)

	configFuture := store.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		golog.Errorf("failed to get raft configuration: %s", err.Error())
	}
	return nil
}

func (store *Store) Get(key []byte) (value []byte, err error) {
	err = store.badger.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		value, err = item.Value()
		return err
	})
	return value, err
}

func (store *Store) Set(key, value []byte) (err error) {
	if store.raft.State() != raft.Leader {
		return store.clusterClient.set(store.raft.Leader(), key, value)
	}
	c := &Command{
		Operation:Operation_SET,
		Key:key,
		Value:value,
	}
	b, err := proto.Marshal(c)
	if err != nil {
		return err
	}
	return store.raft.Apply(b, raftTimeout).Error()
}



