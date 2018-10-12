package raft_badger

import (
	"encoding/hex"
	"github.com/Ready-Stock/badger"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/kataras/go-errors"
	"github.com/kataras/golog"
)

type fsm Store

func (f *fsm) Apply(l *raft.Log) error {
	var c Command
	if err := proto.Unmarshal(l.Data, &c); err != nil {
		golog.Fatalf("failed to unmarshal command: %s. %s", err.Error(), hex.Dump(l.Data))
		return err
	}

	switch c.Operation {
	case Operation_GET:
		return nil
	case Operation_SET:
		return f.applySet(c.Key, c.Value)
	case Operation_DELETE:
	default:
		return errors.New("unrecognized command operation: %d").Format(c.Operation)
	}
	return nil
}

func (f *fsm) applySet(key, value []byte) error {
	return f.badger.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func (f *fsm) applyDelete(key []byte) error {
	return f.badger.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}


