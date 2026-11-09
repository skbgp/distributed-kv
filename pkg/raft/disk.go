package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/shubham/distributed-kv/pkg/persist"
)

type diskState struct {
	Term   uint64
	Vote   int32
	Log    []LogEntry
	Commit uint64
}

func (rn *RaftNode) diskPath() string {
	return filepath.Join(rn.engine.DataDir(), "raft-state.json")
}

func (rn *RaftNode) loadState() error {
	data, err := os.ReadFile(rn.diskPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var state diskState
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	if len(state.Log) == 0 || state.Commit >= uint64(len(state.Log)) {
		return fmt.Errorf("invalid Raft log or commit index")
	}
	for i, entry := range state.Log {
		if entry.Index != uint64(i) || entry.Term > state.Term {
			return fmt.Errorf("invalid Raft log entry %d", i)
		}
	}
	rn.currentTerm = state.Term
	rn.votedFor = state.Vote
	rn.log = state.Log
	rn.commitIndex = state.Commit
	return nil
}

// saveState is called with rn.mu held before any success can escape to a
// voter, leader, or client. After an I/O failure, restart from disk.
func (rn *RaftNode) saveState() error {
	data, err := json.Marshal(diskState{rn.currentTerm, rn.votedFor, rn.log, rn.commitIndex})
	if err == nil {
		err = persist.Replace(rn.diskPath(), data)
	}
	if err != nil {
		rn.diskErr = fmt.Errorf("persist Raft state: %w", err)
		rn.state = Follower
		rn.leaderID = 0
		log.Printf("[node %d] %v", rn.id, rn.diskErr)
	}
	return rn.diskErr
}

func (rn *RaftNode) stopped() bool {
	select {
	case <-rn.stopCh:
		return true
	default:
		return false
	}
}
