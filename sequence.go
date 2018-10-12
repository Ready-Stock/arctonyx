package raft_badger

import (
	"context"
	"github.com/hashicorp/raft"
	"sync"
)

type sequenceClient struct {
	sequenceServiceC *sequenceServiceClient
}


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

func (store *Store) NextSequenceValueById(sequenceName string) (*uint64, error) {
	store.chunkMapMutex.Lock()
	defer store.chunkMapMutex.Unlock()
Reset:
	if sequence, ok := store.sequenceChunks[sequenceName]; !ok {
		if store.raft != nil && store.raft.State() != raft.Leader {
			// Send request to leader.
			if chunk, err := store.sequenceServiceC.GetSequenceChunk(context.Background(), &SequenceChunkRequest{SequenceName:sequenceName}); err != nil {
				return nil, err
			} else {
				store.sequenceChunks[sequenceName] = &SequenceChunk{
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
