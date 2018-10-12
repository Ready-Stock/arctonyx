package raft_badger

import (
	"github.com/Ready-Stock/badger"
	"github.com/hashicorp/raft"
	"sync"
)

type Store struct {
	raft        *raft.Raft
	badger      *badger.DB
	sequenceIds *badger.Sequence

	chunkMapMutex  *sync.Mutex
	sequenceChunks map[uint64]*SequenceChunk

	sequenceServiceC *sequenceServiceClient

	NodeId uint64
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

	nodeId := uint64(-1)
	if joinAddr != nil {

	} else if nId, err := store.getNextNodeID(); err != nil {
		return nil, err
	} else {
		nodeId = *nId
	}

	store.NodeID = nodeId

	return &store, nil
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

	} else {

	}
	return nil
}