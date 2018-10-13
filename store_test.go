package raft_badger_test

import (
	"github.com/Ready-Stock/raft-badger"
	"github.com/kataras/golog"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	golog.SetLevel("debug")
	code := m.Run()
	os.Exit(code)
}

func TestCreateStore(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "store_test")
	defer os.RemoveAll(tmpDir)
	store1, err := raft_badger.CreateStore(tmpDir, "127.0.0.1:0", "")
	if err != nil {
		t.Error(err)
		t.Fail()
		return
	}
	// Simple way to ensure there is a leader.
	time.Sleep(5 * time.Second)
	err = store1.Set([]byte("test"), []byte("value3"))
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