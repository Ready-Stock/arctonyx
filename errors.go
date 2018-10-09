package raft_badger

import (
	"github.com/kataras/go-errors"
)

var (
	ErrNotLeader = errors.New("the current node is not the leader and cannot fulfill your request")
)
