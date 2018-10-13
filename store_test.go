package raft_badger_test

import (
	"github.com/Ready-Stock/raft-badger"
	"testing"
	"time"
)

func TestCreateStore(t *testing.T) {
	store1, err := raft_badger.CreateStore("test1", "127.0.0.1:0", "")
	if err != nil {
		t.Error(err)
		t.Fail()
		return
	}
	// Simple way to ensure there is a leader.
	time.Sleep(30 * time.Second)
	err = store1.Set([]byte("test"), []byte("value"))
	if err != nil {
		t.Error(err)
		t.Fail()
		return
	}

	val, err := store1.Get([]byte("test"))
	if err != nil {
		t.Error(err)
		t.Fail()
		return
	}

	if string(val) != "value" {
		t.Error("value did not match")
		t.Fail()
		return
	}

}