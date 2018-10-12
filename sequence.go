package raft_badger

import (
	"context"
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
	current *SequenceChunkResponse
	next    *SequenceChunkResponse
	index   uint64
	sync    *sync.Mutex
}

func (store *Store) NextSequenceValueById(sequenceId uint64) (*uint64, error) {
	store.chunkMapMutex.Lock()
	defer store.chunkMapMutex.Unlock()
Reset:
	if sequence, ok := store.sequenceChunks[sequenceId]; !ok {
		if store.raft != nil && store.raft.State() != raft.Leader {
			// Send request to leader.
			if chunk, err := store.sequenceServiceC.GetSequenceChunkById(context.Background(), &SequenceChunkRequestById{SequenceId:sequenceId}); err != nil {
				return nil, err
			} else {
				store.sequenceChunks[sequenceId] = &SequenceChunk{
					current:chunk,
					next:nil,
					index:1,
					sync:new(sync.Mutex),
				}
				goto Reset
			}
		} else {

		}
	} else {
		return sequence.Next()
	}
}

func (sequence *SequenceChunk) Next() (*uint64, error) {
	sequence.sync.Lock()
	defer sequence.sync.Unlock()

}
