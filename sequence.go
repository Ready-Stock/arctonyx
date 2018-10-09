package raft_badger

import (
	"github.com/hashicorp/raft"
	"sync"
)

func (store *Store) getNextNodeID() (uint64, error) {
	if store.raft != nil && store.raft.State() != raft.Leader {
		// Send request to leader.
	}
}

func (store *Store) CreateNewSequence(name *string) (*uint64, error) {

}

type SequenceChunk struct {
	response SequenceChunkResponse
	sync     *sync.Mutex
}

func (store *Store) NextSequenceValue(sequenceId uint64) (*uint64, error) {
	store.chunkMapMutex.Lock()
	defer store.chunkMapMutex.Unlock()
	if sequence, ok := store.sequenceChunks[sequenceId]; !ok {
		if store.raft != nil && store.raft.State() != raft.Leader {
			// Send request to leader.
		} else {

		}
	}
}
