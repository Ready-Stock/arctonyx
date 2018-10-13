package raft_badger

import (
	"github.com/kataras/go-errors"
	"sync"
)

type sequenceClient struct {
	sequenceServiceC *sequenceServiceClient
}

// func (store *Store) getNextNodeID() (uint64, error) {
// 	if store.raft != nil && store.raft.State() != raft.Leader {
// 		// Send request to leader.
// 	}
// }
//
// func (store *Store) CreateNewSequence(name *string) (*uint64, error) {
//
// }

type SequenceChunk struct {
	current *SequenceChunkResponse
	next    *SequenceChunkResponse
	index   uint64
	sync    *sync.Mutex
}

func (store *Store) NextSequenceValueById(sequenceName string) (*uint64, error) {
	store.chunkMapMutex.Lock()
	defer store.chunkMapMutex.Unlock()

	// Reset:
	// 	if sequence, ok := store.sequenceChunks[sequenceName]; !ok {
	// 		if store.raft != nil && store.raft.State() != raft.Leader {
	// 			// Send request to leader.
	// 			if chunk, err := store.sequenceClient.GetSequenceChunk(context.Background(), &SequenceChunkRequest{SequenceName:sequenceName}); err != nil {
	// 				return nil, err
	// 			} else {
	// 				store.sequenceChunks[sequenceName] = &SequenceChunk{
	// 					current:chunk,
	// 					next:nil,
	// 					index:1,
	// 					sync:new(sync.Mutex),
	// 				}
	// 				goto Reset
	// 			}
	// 		} else {
	//
	// 		}
	// 	} else {
	// 		return sequence.Next()
	// 	}
	return nil, nil
}

func (sequence *SequenceChunk) Next() (*uint64, error) {
	sequence.sync.Lock()
	defer sequence.sync.Unlock()
	if sequence.current == nil {
		return nil, errors.New("current sequence is nil")
	}
NewId:
	nextId := sequence.current.Start + sequence.current.Offset + (sequence.current.Count * sequence.index) - (sequence.current.Count - 1)
	if nextId > sequence.current.End {
		if sequence.next != nil {
			sequence.current = sequence.next
			sequence.next = nil
			sequence.index = 1
			goto NewId
		} else {
			return nil, errors.New("reached end of available sequence ranges")
		}
	}
	sequence.index++
	return &nextId, nil
}
