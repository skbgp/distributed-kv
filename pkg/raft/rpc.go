package raft

import "encoding/json"

// RequestVoteArgs is sent by a candidate to ask for votes during election.
type RequestVoteArgs struct {
	Term         uint64
	CandidateID  uint32
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

// AppendEntriesArgs is sent by the leader for two purposes:
//  1. Heartbeats (when Entries is empty) — prevents election timeouts
//  2. Log replication (when Entries has data) — replicates new commands
type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     uint32
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term       uint64
	Success    bool
	MatchIndex uint64
}

// LogEntry is a single entry in the Raft log. The Command field holds
// a serialized KV operation (put/delete) that will be applied to the
// storage engine once the entry is committed.
type LogEntry struct {
	Term    uint64
	Index   uint64
	Command []byte
}

type CommandType byte

const (
	CmdPut    CommandType = 1
	CmdDelete CommandType = 2
)

type Command struct {
	Type  CommandType `json:"type"`
	Key   string      `json:"key"`
	Value []byte      `json:"value,omitempty"`
}

// EncodeCommand serializes a command for storage in a log entry.
func EncodeCommand(cmd Command) ([]byte, error) {
	return json.Marshal(cmd)
}

// DecodeCommand deserializes a command from a log entry.
func DecodeCommand(data []byte) (Command, error) {
	var cmd Command
	err := json.Unmarshal(data, &cmd)
	return cmd, err
}
