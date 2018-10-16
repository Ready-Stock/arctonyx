package raft_badger

import (
	"bytes"
	"encoding/hex"
	"github.com/Ready-Stock/badger"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/kataras/go-errors"
	"github.com/kataras/golog"
	"io"
	"time"
)

type fsm Store

func (f *fsm) Apply(l *raft.Log) interface{} {
	var c Command
	if err := proto.Unmarshal(l.Data, &c); err != nil {
		golog.Fatalf("failed to unmarshal command: %s. %s", err.Error(), hex.Dump(l.Data))
		return err
	}
	golog.Debugf("[%d] Delay [%s]\n%s", f.nodeId, time.Since(time.Unix(0, int64(c.Timestamp))), hex.Dump(l.Data))
	switch c.Operation {
	case Operation_SET:
		return f.applySet(c.Key, c.Value)
	case Operation_DELETE:
		return f.applyDelete(c.Key)
	default:
		return errors.New("unsupported command operation: %d").Format(c.Operation)
	}
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	return f.badger.Load(rc)
}

// Snapshot returns a snapshot of the key-value store.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	w := &bytes.Buffer{}
	f.badger.Backup(w, 0)
	return &snapshot{
		store:w.Bytes(),
	}, nil
}

func (f *fsm) applySet(key, value []byte) error {
	return f.badger.Update(func(txn *badger.Txn) error {
		golog.Debugf("[%d] Setting Key: %s To Value: %s", f.nodeId, string(key), string(value))
		return txn.Set(key, value)
	})
}

func (f *fsm) applyDelete(key []byte) error {
	return f.badger.Update(func(txn *badger.Txn) error {
		golog.Debugf("[%d] Deleting Key: %s", f.nodeId, string(key))
		return txn.Delete(key)
	})
}




