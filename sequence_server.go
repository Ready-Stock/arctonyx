package raft_badger

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
)

const (
	SequenceRangeSize  = 10
	SequencePartitions = 2
)

type sequenceServer struct {
	Store
}

func (server *sequenceServer) GetSequenceChunk(ctx context.Context, request *SequenceChunkRequest) (*SequenceChunkResponse, error) {
	return server.getSequenceChunk(request.SequenceName)
}

func (server *sequenceServer) getSequenceChunk(sequenceName string) (*SequenceChunkResponse, error) {
	if server.Store.raft.State() != raft.Leader { // Only the leader can manage sequences
		return nil, ErrNotLeader
	}
	server.sequenceCacheSync.Lock()
	defer server.sequenceCacheSync.Unlock()
	sequenceCache, ok := server.Store.sequenceCache[sequenceName]
	path := []byte(fmt.Sprintf("%s%s", sequencePath, sequenceName))
	if !ok {
		seq, err := server.Store.Get(path)
		if err != nil {
			return nil, err
		}
		if len(seq) == 0 {
			sequenceCache = &Sequence{
				CurrentValue:       0,
				LastPartitionIndex: 0,
				MaxPartitionIndex:  SequencePartitions,
				Partitions:         SequencePartitions,
			}
		} else {
			err = proto.Unmarshal(seq, sequenceCache)
			if err != nil {
				return nil, err
			}
		}
		server.Store.sequenceCache[sequenceName] = sequenceCache
	}
	if sequenceCache.LastPartitionIndex > sequenceCache.MaxPartitionIndex {
		sequenceCache.CurrentValue += SequenceRangeSize
		sequenceCache.LastPartitionIndex = 0
		sequenceCache.MaxPartitionIndex = SequencePartitions
	}
	index := sequenceCache.LastPartitionIndex
	sequenceCache.LastPartitionIndex++
	b, err := proto.Marshal(sequenceCache)
	if err != nil {
		return nil, err
	}
	err = server.Store.Set(path, b)
	if err != nil {
		return nil, err
	}
	return &SequenceChunkResponse{
		Start:  sequenceCache.CurrentValue,
		End:    sequenceCache.CurrentValue + SequenceRangeSize,
		Offset: index,
		Count:  sequenceCache.MaxPartitionIndex,
	}, nil
}
