package raft_badger_test

import (
	"github.com/Ready-Stock/raft-badger"
	"testing"
)

func TestCreateStore(t *testing.T) {
	store, err := raft_badger.CreateStore("test", nil)
	if err != nil {
		t.Error(err)
		t.Fail()
		return
	}
	store.Set([]byte("test"), []byte("value"))
}