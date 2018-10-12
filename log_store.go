package raft_badger

import (
	"github.com/hashicorp/raft"
)

type logStore Store

func (log *logStore) FirstIndex() (uint64, error) {
	return 0, nil
}

func (log *logStore) LastIndex() (uint64, error) {
	return 0, nil
}

func (log *logStore) GetLog(index uint64, raftLog *raft.Log) error {
	return nil
}

func (log *logStore) StoreLog(raftLog *raft.Log) error {
	return nil
}

func (log *logStore) StoreLogs(raftLogs []*raft.Log) error {
	return nil
}

func (log *logStore) DeleteRange(min, max uint64) error {
	return nil
}