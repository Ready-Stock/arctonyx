package raft_badger

import (
	"encoding/hex"
	"fmt"
	"github.com/Ready-Stock/badger"
	"io/ioutil"
	"os"
	"testing"
)

func TestLogStore_FirstIndex(t *testing.T) {
	store := Store{}
	tmpDir, _ := ioutil.TempDir("", "store_test")
	defer os.RemoveAll(tmpDir)
	opts := badger.DefaultOptions
	opts.Dir = tmpDir
	opts.ValueDir = tmpDir
	db, err := badger.Open(opts)
	if err != nil {
		t.Error(err)
		t.Fail()
		return
	}
	store.badger = db
	ls := logStore(store)
	if index, err := ls.FirstIndex(); err != nil {
		t.Error(err)
		t.Fail()
		return
	} else {
		if index != 0 {
			t.Error("first index should be 0")
			t.Fail()
			return
		}
	}

	if index, err := ls.LastIndex(); err != nil {
		t.Error(err)
		t.Fail()
		return
	} else {
		if index != 0 {
			t.Error("last index should be 0")
			t.Fail()
			return
		}
	}
}

func TestLogStore_GetKeyForIndex(t *testing.T) {
	uints := []uint64{ 1, 2, 3, 15831904231, 35183541, 489156156156 }
	for _, u := range uints {
		key := getKeyForIndex(u)
		index := getIndexForKey(key)
		fmt.Print(hex.Dump(key))
		if index != u {
			t.Error("decoded index does not match input index")
			t.Fail()
			return
		}
	}
}
