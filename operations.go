package arctonyx

import (
    "errors"
    "github.com/dgraph-io/badger"
    "github.com/golang/protobuf/proto"
    "github.com/kataras/golog"
    "github.com/readystock/raft"
    "time"
)

func (store *Store) GetPrefix(prefix []byte) (values []KeyValue, err error) {
    values = make([]KeyValue, 0)
    err = store.badger.View(func(txn *badger.Txn) error {
        it := txn.NewIterator(badger.DefaultIteratorOptions)
        defer it.Close()
        for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
            item := it.Item()
            valueBytes := make([]byte, 0)
            if valueBytes, err = item.ValueCopy(valueBytes); err != nil {
                return err
            } else {
                values = append(values, KeyValue{Key: item.Key(), Value: valueBytes})
            }
        }
        return nil
    })
    return values, err
}

func (store *Store) Get(key []byte) (value []byte, err error) {
    golog.Debugf("[%d] Getting key: %s", store.nodeId, string(key))
    //isSet := true
    err = store.badger.View(func(txn *badger.Txn) error {
        item, err := txn.Get(key)
        if err != nil {
            if err.Error() != "Key not found" {
                return err
            } else {
                // If the key is not found normally we want to just return an empty byte array.
                // but only if we are the leader, if we are not the leader then we want to send
                // a request to the leader.
                //isSet = false
                value = make([]byte, 0)
                return nil
            }
        }
        value, err = item.ValueCopy(value)
        return err
    })
    // if !isSet {
    //     value, err = store.clusterClient.Get(key)
    // }
    return value, err
}

func (store *Store) Set(key, value []byte) (err error) {
    c := &Command{Operation: Operation_SET, Key: key, Value: value, Timestamp: uint64(time.Now().UnixNano())}
    if store.raft.State() != raft.Leader {
        if store.raft.Leader() == "" {
            return errors.New("no leader in cluster")
        }
        golog.Debugf("[%d] Proxying set key: %s to %s", store.nodeId, string(key), string(value))
        if _, err := store.clusterClient.sendCommand(c); err != nil {
            return err
        }
        return nil
    }
    golog.Debugf("[%d] Initiating set key: %s to %s", store.nodeId, string(key), string(value))
    b, err := proto.Marshal(c)
    if err != nil {
        return err
    }
    r := store.raft.Apply(b, raftTimeout)
    if err := r.Error(); err != nil {
        return err
    } else if resp, ok := r.Response().(CommandResponse); ok {
        golog.Debugf("[%d] Delay Total [%s] Response [%s]", store.nodeId, time.Since(time.Unix(0, int64(resp.Timestamp))), time.Since(time.Unix(0, int64(resp.AppliedTimestamp))))
    }
    return nil
}

func (store *Store) Delete(key []byte) (err error) {
    c := &Command{Operation: Operation_DELETE, Key: key, Value: nil, Timestamp: uint64(time.Now().UnixNano())}
    if store.raft.State() != raft.Leader {
        if _, err := store.clusterClient.sendCommand(c); err != nil {
            return err
        }
        return nil
    }
    b, err := proto.Marshal(c)
    if err != nil {
        return err
    }
    r := store.raft.Apply(b, raftTimeout)
    return r.Error()
}