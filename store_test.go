package arctonyx_test

import (
	"github.com/Ready-Stock/arctonyx"
	"github.com/ahmetb/go-linq"
	"github.com/kataras/golog"
	"io/ioutil"
	"os"
	"sort"
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
	store1, err := arctonyx.CreateStore(tmpDir, "127.0.0.1:0", "", "")
	if err != nil {
		t.Error(err)
		t.Fail()
		return
	}
	defer store1.Close()
	// Simple way to ensure there is a leader.
	time.Sleep(5 * time.Second)
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

func TestCreateStoreMultipleServers(t *testing.T) {
	tmpDir1, _ := ioutil.TempDir("", "store_test")
	defer os.RemoveAll(tmpDir1)

	tmpDir2, _ := ioutil.TempDir("", "store_test2")
	defer os.RemoveAll(tmpDir2)
	store1, err := arctonyx.CreateStore(tmpDir1, ":6543", ":6500", "")
	if err != nil {
		t.Error(err)
		t.Fail()
		return
	}
	defer store1.Close()
	// Simple way to ensure there is a leader.
	time.Sleep(5 * time.Second)

	store2, err := arctonyx.CreateStore(tmpDir2, ":6544",":6501", ":6500")
	if err != nil {
		t.Error(err)
		t.Fail()
		return
	}
	defer store2.Close()
	//store1.Join(store2.NodeID(), ":6544", ":6501")
	time.Sleep(5 * time.Second)
	err = store1.Set([]byte("test"), []byte("value"))
	if err != nil {
		t.Error(err)
		t.Fail()
		return
	}
	val, err := store2.Get([]byte("test"))
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

	store1.Set([]byte("test"), []byte("value1"))
	time.Sleep(100 * time.Millisecond)
	val1, err := store2.Get([]byte("test"))
	if err != nil {
		t.Error(err)
		t.Fail()
		return
	}

	if string(val1) != "value1" {
		t.Errorf("value did not match, found: %s", val1)
		t.Fail()
		return
	}

	store1.Delete([]byte("test"))
	time.Sleep(100 * time.Millisecond)
	val2, err := store2.Get([]byte("test"))
	if err != nil {
		t.Error(err)
		t.Fail()
		return
	}

	if string(val2) == "value1" {
		t.Errorf("value did not match, found: %s", val1)
		t.Fail()
		return
	}
}


func TestGetPrefix(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "store_test")
	defer os.RemoveAll(tmpDir)
	store1, err := arctonyx.CreateStore(tmpDir, "127.0.0.1:0", "", "")
	if err != nil {
		t.Error(err)
		t.Fail()
		return
	}
	defer store1.Close()
	// Simple way to ensure there is a leader.
	time.Sleep(5 * time.Second)
	err = store1.Set([]byte("/test"), []byte("value"))
	if err != nil {
		t.Error(err)
		t.Fail()
		return
	}

	val, err := store1.GetPrefix([]byte("/"))
	if err != nil {
		t.Error(err)
		t.Fail()
		return
	}
	if len(val) == 0 {
		t.Error("no values found")
		t.Fail()
		return
	}
	for _, kv := range val {
		golog.Debugf("Key: %s Value: %s", string(kv.Key), string(kv.Value))
	}
}

func TestSequence(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "store_test")
	defer os.RemoveAll(tmpDir)
	store1, err := arctonyx.CreateStore(tmpDir, "127.0.0.1:0", "", "")
	if err != nil {
		t.Error(err)
		t.Fail()
		return
	}
	defer store1.Close()
	// Simple way to ensure there is a leader.
	time.Sleep(5 * time.Second)
	numberOfIds := 10000
	Ids := make([]int, 0)
	for i := 0; i < numberOfIds; i++ {
		id, err := store1.NextSequenceValueById("public.users.user_id")
		if err != nil {
			t.Error(err)
			t.Fail()
			return
		}
		Ids = append(Ids, int(*id))
		//golog.Infof("New user_id: %d", *id)
	}
	sort.Ints(Ids)
	if len(Ids) != numberOfIds {
		t.Error("number of ids do not match")
		t.Fail()
		return
	}

}

func TestSequenceMulti(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "store_test")
	defer os.RemoveAll(tmpDir)
	tmpDir2, _ := ioutil.TempDir("", "store_test2")
	defer os.RemoveAll(tmpDir2)
	store1, err := arctonyx.CreateStore(tmpDir, "127.0.0.1:0", ":6502", "")
	if err != nil {
		t.Error(err)
		t.Fail()
		return
	}
	defer store1.Close()
	// Simple way to ensure there is a leader.
	time.Sleep(5 * time.Second)

	store2, err := arctonyx.CreateStore(tmpDir2, ":6546",":6501", ":6502")
	if err != nil {
		t.Error(err)
		t.Fail()
		return
	}
	defer store2.Close()
	//store1.Join(store2.NodeID(), ":6546", ":6501")
	time.Sleep(5 * time.Second)

	numberOfIds := 1000000
	Ids := make([]int, 0)
	for i := 0; i < numberOfIds; i++ {
		switch i % 2 {
		case 0:
			id, err := store2.NextSequenceValueById("public.users.user_id")
			if err != nil {
				panic(err)
				t.Fail()
				return
			}
			Ids = append(Ids, int(*id))
			//golog.Infof("New user_id on node 2: %d", *id)
		default:
			id, err := store1.NextSequenceValueById("public.users.user_id")
			if err != nil {
				panic(err)
				t.Fail()
				return
			}
			Ids = append(Ids, int(*id))
			//golog.Infof("New user_id on node 1: %d", *id)
		}

	}
	sort.Ints(Ids)
	if len(Ids) != numberOfIds {
		t.Error("number of ids do not match")
		t.Fail()
		return
	}
	linq.From(Ids).Distinct().ToSlice(&Ids)
	if len(Ids) != numberOfIds {
		t.Error("distinct number of ids do not match")
		t.Fail()
		return
	}
}

func TestCreateStoreWithClose(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "store_test")
	defer os.RemoveAll(tmpDir)
	store1, err := arctonyx.CreateStore(tmpDir, "127.0.0.1:0", "", "")
	if err != nil {
		t.Error(err)
		t.Fail()
		return
	}
	// Simple way to ensure there is a leader.
	time.Sleep(5 * time.Second)
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

	golog.Warnf("shutting down lone node and restarting it")
	store1.Close()
	store1 = nil
	time.Sleep(5 * time.Second)
	store1, err = arctonyx.CreateStore(tmpDir, "127.0.0.1:0", "", "")
	defer store1.Close()
	// Simple way to ensure there is a leader.
	time.Sleep(5 * time.Second)
	err = store1.Set([]byte("test1"), []byte("value"))
	if err != nil {
		t.Error(err)
		t.Fail()
		return
	}

	val, err = store1.Get([]byte("test1"))
	if err != nil {
		t.Error(err)
		t.Fail()
		return
	}
}