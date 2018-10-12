package raft_badger

import (
	"github.com/Ready-Stock/badger"
)

type stableStore Store

func (stable *stableStore) Set(key, val []byte) error {
	return stable.badger.Update(func(txn *badger.Txn) error {
		return txn.Set(key, val)
	})
}

func (stable *stableStore) Get(key []byte) (val []byte, err error) {
	err = stable.badger.View(func(txn *badger.Txn) error {
		if item, err := txn.Get(key); err != nil {
			return err
		} else {
			if value, err := item.Value(); err != nil {
				return err
			} else {
				val = value
				return nil
			}
		}
	})
	return val, err
}

func (stable *stableStore) SetUint64(key []byte, val uint64) error {
	return stable.Set(key, uint64ToBytes(val))
}

func (stable *stableStore) GetUint64(key []byte) (uint64, error) {
	if val, err := stable.Get(key); err != nil {
		return 0, err
	} else {
		return bytesToUint64(val), nil
	}
}

