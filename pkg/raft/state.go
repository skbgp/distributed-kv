package raft

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/shubham/distributed-kv/pkg/storage"
)

// NodeState represents the three possible states of a Raft node.
type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// Timing constants. The election timeout is randomized between min and max
// to avoid all nodes starting elections at the same time (split votes).
// The heartbeat interval must be much shorter than the election timeout.
const (
	electionTimeoutMin = 300 * time.Millisecond
	electionTimeoutMax = 500 * time.Millisecond
	heartbeatInterval  = 100 * time.Millisecond
)

// RaftNode is a single node in the Raft cluster.
type RaftNode struct {
	currentTerm uint64
	votedFor    int32
	log         []LogEntry

	commitIndex uint64
	lastApplied uint64

	nextIndex  map[uint32]uint64
	matchIndex map[uint32]uint64

	id       uint32
	peers    []string
	state    NodeState
	leaderID uint32

	engine *storage.Engine

	mu            sync.RWMutex
	electionTimer *time.Timer
	applyCh       chan LogEntry
	stopCh        chan struct{}
	rpcServer     *rpc.Server
	listener      net.Listener
	address       string
}

// NewRaftNode creates a new Raft node. It doesn't start the node — call
// Start() to begin participating in the cluster.
func NewRaftNode(id uint32, address string, peers []string, engine *storage.Engine) *RaftNode {
	node := &RaftNode{
		id:          id,
		address:     address,
		peers:       peers,
		state:       Follower,
		votedFor:    -1,
		currentTerm: 0,
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make(map[uint32]uint64),
		matchIndex:  make(map[uint32]uint64),
		engine:      engine,
		applyCh:     make(chan LogEntry, 100),
		stopCh:      make(chan struct{}),
	}

	node.log = append(node.log, LogEntry{Term: 0, Index: 0})

	return node
}

// Start begins the Raft node: starts the RPC server, resets the election
// timer, and begins the main event loop.
func (rn *RaftNode) Start() error {

	if err := rn.startRPCServer(); err != nil {
		return fmt.Errorf("start RPC server: %w", err)
	}

	rn.resetElectionTimer()

	go rn.applyLoop()

	log.Printf("[node %d] started as %s at %s (peers: %v)\n",
		rn.id, rn.state, rn.address, rn.peers)

	return nil
}

// Stop gracefully shuts down the node.
func (rn *RaftNode) Stop() {
	close(rn.stopCh)
	if rn.listener != nil {
		rn.listener.Close()
	}
	if rn.electionTimer != nil {
		rn.electionTimer.Stop()
	}
}

func (rn *RaftNode) startElection() {
	rn.mu.Lock()
	rn.state = Candidate
	rn.currentTerm++
	rn.votedFor = int32(rn.id)
	currentTerm := rn.currentTerm
	lastLogIndex := rn.lastLogIndex()
	lastLogTerm := rn.lastLogTerm()
	rn.mu.Unlock()

	log.Printf("[node %d] starting election for term %d\n", rn.id, currentTerm)

	votesReceived := 1
	votesNeeded := (len(rn.peers)+1)/2 + 1

	for _, peerAddr := range rn.peers {
		go func(addr string) {
			args := &RequestVoteArgs{
				Term:         currentTerm,
				CandidateID:  rn.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			var reply RequestVoteReply

			if err := rn.callRPC(addr, "RaftNode.RequestVote", args, &reply); err != nil {
				return
			}

			rn.mu.Lock()
			defer rn.mu.Unlock()

			if reply.Term > rn.currentTerm {
				rn.stepDown(reply.Term)
				return
			}

			if rn.state != Candidate || rn.currentTerm != currentTerm {
				return
			}

			if reply.VoteGranted {

				votesReceived++
				if votesReceived >= votesNeeded {
					rn.becomeLeader()
				}
			}
		}(peerAddr)
	}

	rn.resetElectionTimer()
}

// becomeLeader transitions this node to the leader state.
// Must be called with rn.mu held.
func (rn *RaftNode) becomeLeader() {
	if rn.state != Candidate {
		return
	}
	rn.state = Leader
	rn.leaderID = rn.id

	log.Printf("[node %d] won election, now leader for term %d\n", rn.id, rn.currentTerm)

	lastIdx := rn.lastLogIndex()
	for _, addr := range rn.peers {

		_ = addr
	}

	for id := uint32(1); id <= uint32(len(rn.peers)+1); id++ {
		if id != rn.id {
			rn.nextIndex[id] = lastIdx + 1
			rn.matchIndex[id] = 0
		}
	}

	go rn.heartbeatLoop()
}

// RequestVote is called by candidates asking for our vote.
// This is an exported method so Go's net/rpc can call it.
func (rn *RaftNode) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	reply.Term = rn.currentTerm
	reply.VoteGranted = false

	if args.Term < rn.currentTerm {
		return nil
	}

	if args.Term > rn.currentTerm {
		rn.stepDown(args.Term)
	}

	if rn.votedFor != -1 && rn.votedFor != int32(args.CandidateID) {
		return nil
	}

	myLastTerm := rn.lastLogTerm()
	myLastIndex := rn.lastLogIndex()

	logIsUpToDate := args.LastLogTerm > myLastTerm ||
		(args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIndex)

	if !logIsUpToDate {
		return nil
	}

	rn.votedFor = int32(args.CandidateID)
	reply.VoteGranted = true
	rn.resetElectionTimer()

	log.Printf("[node %d] voted for node %d in term %d\n",
		rn.id, args.CandidateID, rn.currentTerm)

	return nil
}

// AppendEntries is called by the leader to replicate log entries or
// send heartbeats. This is the workhorse of Raft.
func (rn *RaftNode) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	reply.Term = rn.currentTerm
	reply.Success = false

	if args.Term < rn.currentTerm {
		return nil
	}

	if args.Term >= rn.currentTerm {
		if args.Term > rn.currentTerm {
			rn.stepDown(args.Term)
		}
		rn.state = Follower
		rn.leaderID = args.LeaderID
	}

	rn.resetElectionTimer()

	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex >= uint64(len(rn.log)) {

			reply.MatchIndex = uint64(len(rn.log)) - 1
			return nil
		}
		if rn.log[args.PrevLogIndex].Term != args.PrevLogTerm {

			rn.log = rn.log[:args.PrevLogIndex]
			reply.MatchIndex = uint64(len(rn.log)) - 1
			return nil
		}
	}

	for i, entry := range args.Entries {
		logIndex := args.PrevLogIndex + 1 + uint64(i)

		if logIndex < uint64(len(rn.log)) {

			if rn.log[logIndex].Term != entry.Term {

				rn.log = rn.log[:logIndex]
				rn.log = append(rn.log, args.Entries[i:]...)
				break
			}

		} else {

			rn.log = append(rn.log, args.Entries[i:]...)
			break
		}
	}

	if args.LeaderCommit > rn.commitIndex {
		lastIdx := rn.lastLogIndex()
		if args.LeaderCommit < lastIdx {
			rn.commitIndex = args.LeaderCommit
		} else {
			rn.commitIndex = lastIdx
		}

		rn.signalApply()
	}

	reply.Success = true
	reply.MatchIndex = rn.lastLogIndex()
	return nil
}

// heartbeatLoop runs while this node is leader, periodically sending
// AppendEntries RPCs to all followers. If there are new log entries,
// they're included in the heartbeat. Otherwise, it's an empty heartbeat
// just to prevent election timeouts.
func (rn *RaftNode) heartbeatLoop() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rn.mu.RLock()
			isLeader := rn.state == Leader
			rn.mu.RUnlock()

			if !isLeader {
				return
			}
			rn.sendHeartbeats()

		case <-rn.stopCh:
			return
		}
	}
}

// sendHeartbeats sends AppendEntries to each peer. For each peer,
// we send any log entries they're missing (based on nextIndex).
func (rn *RaftNode) sendHeartbeats() {
	rn.mu.RLock()
	if rn.state != Leader {
		rn.mu.RUnlock()
		return
	}
	currentTerm := rn.currentTerm
	commitIndex := rn.commitIndex
	rn.mu.RUnlock()

	for peerID := uint32(1); peerID <= uint32(len(rn.peers)+1); peerID++ {
		if peerID == rn.id {
			continue
		}

		go func(pid uint32) {
			rn.mu.RLock()
			nextIdx, ok := rn.nextIndex[pid]
			if !ok {
				rn.mu.RUnlock()
				return
			}

			var entries []LogEntry
			if nextIdx <= rn.lastLogIndex() {
				entries = make([]LogEntry, len(rn.log[nextIdx:]))
				copy(entries, rn.log[nextIdx:])
			}

			prevLogIndex := nextIdx - 1
			prevLogTerm := uint64(0)
			if prevLogIndex > 0 && prevLogIndex < uint64(len(rn.log)) {
				prevLogTerm = rn.log[prevLogIndex].Term
			}
			rn.mu.RUnlock()

			peerAddr := rn.peerAddress(pid)
			if peerAddr == "" {
				return
			}

			args := &AppendEntriesArgs{
				Term:         currentTerm,
				LeaderID:     rn.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: commitIndex,
			}
			var reply AppendEntriesReply

			if err := rn.callRPC(peerAddr, "RaftNode.AppendEntries", args, &reply); err != nil {
				return
			}

			rn.mu.Lock()
			defer rn.mu.Unlock()

			if reply.Term > rn.currentTerm {
				rn.stepDown(reply.Term)
				return
			}

			if rn.state != Leader || rn.currentTerm != currentTerm {
				return
			}

			if reply.Success {

				if len(entries) > 0 {
					rn.nextIndex[pid] = entries[len(entries)-1].Index + 1
					rn.matchIndex[pid] = entries[len(entries)-1].Index
				}
				rn.advanceCommitIndex()
			} else {

				if reply.MatchIndex+1 < rn.nextIndex[pid] {
					rn.nextIndex[pid] = reply.MatchIndex + 1
				} else if rn.nextIndex[pid] > 1 {
					rn.nextIndex[pid]--
				}
			}
		}(peerID)
	}
}

// advanceCommitIndex checks if there's a new index that a majority of
// nodes have replicated. If so, we can safely advance the commit index.
// Must be called with rn.mu held.
func (rn *RaftNode) advanceCommitIndex() {

	for idx := rn.commitIndex + 1; idx <= rn.lastLogIndex(); idx++ {

		if rn.log[idx].Term != rn.currentTerm {
			continue
		}

		replicatedCount := 1
		for pid := uint32(1); pid <= uint32(len(rn.peers)+1); pid++ {
			if pid != rn.id && rn.matchIndex[pid] >= idx {
				replicatedCount++
			}
		}

		if replicatedCount > (len(rn.peers)+1)/2 {
			rn.commitIndex = idx
			rn.signalApply()
		}
	}
}

// Propose submits a new command to the Raft cluster. Only the leader
// can accept proposals. If this node isn't the leader, it returns an
// error with the leader's address so the client can redirect.
func (rn *RaftNode) Propose(cmd Command) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.state != Leader {
		return fmt.Errorf("not the leader (leader is node %d)", rn.leaderID)
	}

	data, err := EncodeCommand(cmd)
	if err != nil {
		return fmt.Errorf("encode command: %w", err)
	}

	entry := LogEntry{
		Term:    rn.currentTerm,
		Index:   rn.lastLogIndex() + 1,
		Command: data,
	}

	rn.log = append(rn.log, entry)

	log.Printf("[node %d] proposed command at index %d (term %d)\n",
		rn.id, entry.Index, entry.Term)

	return nil
}

func (rn *RaftNode) applyLoop() {
	for {
		select {
		case <-rn.applyCh:
			rn.applyCommitted()
		case <-rn.stopCh:
			return
		}
	}
}

func (rn *RaftNode) applyCommitted() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	for rn.lastApplied < rn.commitIndex {
		rn.lastApplied++
		entry := rn.log[rn.lastApplied]

		cmd, err := DecodeCommand(entry.Command)
		if err != nil {
			log.Printf("[node %d] failed to decode command at index %d: %v\n",
				rn.id, entry.Index, err)
			continue
		}

		switch cmd.Type {
		case CmdPut:
			if err := rn.engine.Put(cmd.Key, cmd.Value); err != nil {
				log.Printf("[node %d] failed to apply PUT %q: %v\n", rn.id, cmd.Key, err)
			}
		case CmdDelete:
			if err := rn.engine.Delete(cmd.Key); err != nil {
				log.Printf("[node %d] failed to apply DELETE %q: %v\n", rn.id, cmd.Key, err)
			}
		}

		log.Printf("[node %d] applied command at index %d: %s %q\n",
			rn.id, entry.Index, cmdTypeName(cmd.Type), cmd.Key)
	}
}

func (rn *RaftNode) signalApply() {
	select {
	case rn.applyCh <- LogEntry{}:
	default:

	}
}

func (rn *RaftNode) stepDown(newTerm uint64) {
	rn.currentTerm = newTerm
	rn.state = Follower
	rn.votedFor = -1
	rn.resetElectionTimer()
}

func (rn *RaftNode) lastLogIndex() uint64 {
	return uint64(len(rn.log)) - 1
}

func (rn *RaftNode) lastLogTerm() uint64 {
	return rn.log[len(rn.log)-1].Term
}

func (rn *RaftNode) resetElectionTimer() {
	timeout := electionTimeoutMin +
		time.Duration(rand.Int63n(int64(electionTimeoutMax-electionTimeoutMin)))

	if rn.electionTimer != nil {
		rn.electionTimer.Stop()
	}

	rn.electionTimer = time.AfterFunc(timeout, func() {
		rn.mu.RLock()
		state := rn.state
		rn.mu.RUnlock()

		if state != Leader {
			rn.startElection()
		}
	})
}

// peerAddress maps a peer ID to its network address.
// Our peer IDs are 1-indexed and sequential. We skip our own ID when
// mapping to the peers array.
func (rn *RaftNode) peerAddress(peerID uint32) string {

	idx := 0
	for id := uint32(1); id <= uint32(len(rn.peers)+1); id++ {
		if id == rn.id {
			continue
		}
		if id == peerID {
			if idx < len(rn.peers) {
				return rn.peers[idx]
			}
			return ""
		}
		idx++
	}
	return ""
}

func (rn *RaftNode) startRPCServer() error {
	rn.rpcServer = rpc.NewServer()
	rn.rpcServer.Register(rn)

	listener, err := net.Listen("tcp", rn.address)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", rn.address, err)
	}
	rn.listener = listener

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				select {
				case <-rn.stopCh:
					return
				default:
					continue
				}
			}
			go rn.rpcServer.ServeConn(conn)
		}
	}()

	return nil
}

func (rn *RaftNode) callRPC(addr string, method string, args interface{}, reply interface{}) error {
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer client.Close()
	return client.Call(method, args, reply)
}

func (rn *RaftNode) IsLeader() bool {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.state == Leader
}

func (rn *RaftNode) LeaderID() uint32 {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.leaderID
}

func (rn *RaftNode) CurrentTerm() uint64 {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.currentTerm
}

func (rn *RaftNode) State() NodeState {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.state
}

func (rn *RaftNode) LogLength() int {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return len(rn.log) - 1
}

func cmdTypeName(t CommandType) string {
	switch t {
	case CmdPut:
		return "PUT"
	case CmdDelete:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}
