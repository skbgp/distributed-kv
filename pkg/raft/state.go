package raft

import (
	"context"
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

	mu                 sync.RWMutex
	electionTimer      *time.Timer
	electionGeneration uint64
	applyCh            chan LogEntry
	stopCh             chan struct{}
	stopOnce           sync.Once
	applyWG            sync.WaitGroup
	diskErr            error
	rpcServer          *rpc.Server
	listener           net.Listener
	address            string

	// commitWaiters holds callers blocked inside ProposeAndWait until their
	// entry is committed by a majority. Without this the HTTP layer had no
	// way to know a write was durable and simply guessed with a sleep.
	commitWaiters []commitWaiter
}

// commitWaiter is one caller waiting for a specific log index to commit.
type commitWaiter struct {
	index uint64
	ch    chan struct{}
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
	if err := rn.loadState(); err != nil {
		return fmt.Errorf("recover Raft state: %w", err)
	}
	// PUT and DELETE replay safely in log order. Rebuild the applied
	// prefix before accepting traffic, even after a crash during apply.
	if err := rn.applyCommitted(); err != nil {
		return fmt.Errorf("replay committed entries: %w", err)
	}

	if err := rn.startRPCServer(); err != nil {
		return fmt.Errorf("start RPC server: %w", err)
	}

	rn.mu.Lock()
	rn.resetElectionTimer()
	rn.mu.Unlock()

	rn.applyWG.Add(1)
	go rn.applyLoop()

	log.Printf("[node %d] started as %s at %s (peers: %v)\n",
		rn.id, rn.State(), rn.address, rn.peers)

	return nil
}

// Stop gracefully shuts down the node.
//
// The timer and listener are read under the lock: the election timer is
// rewritten by resetElectionTimer from timer goroutines, so reading it bare
// here was a data race. Stop is also safe to call more than once.
func (rn *RaftNode) Stop() {
	rn.stopOnce.Do(func() {
		close(rn.stopCh)
	})

	rn.mu.Lock()
	listener := rn.listener
	timer := rn.electionTimer
	rn.mu.Unlock()

	if listener != nil {
		listener.Close()
	}
	if timer != nil {
		timer.Stop()
	}
	rn.applyWG.Wait()
}

func (rn *RaftNode) startElection(generation uint64) {
	rn.mu.Lock()
	if rn.electionGeneration != generation || rn.state == Leader || rn.stopped() || rn.diskErr != nil {
		rn.mu.Unlock()
		return
	}
	rn.state = Candidate
	rn.currentTerm++
	rn.votedFor = int32(rn.id)
	rn.leaderID = 0
	if rn.saveState() != nil {
		rn.mu.Unlock()
		return
	}
	rn.resetElectionTimer()
	if len(rn.peers) == 0 {
		rn.becomeLeader()
		rn.mu.Unlock()
		return
	}
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

			if rn.stopped() || rn.diskErr != nil {
				return
			}
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
	for id := uint32(1); id <= uint32(len(rn.peers)+1); id++ {
		if id != rn.id {
			rn.nextIndex[id] = lastIdx + 1
			rn.matchIndex[id] = 0
		}
	}

	// A current-term no-op lets a replacement leader safely commit a
	// previous term's replicated prefix even when no client is writing.
	rn.log = append(rn.log, LogEntry{Term: rn.currentTerm, Index: lastIdx + 1})
	if rn.saveState() != nil {
		return
	}
	rn.advanceCommitIndex()
	go rn.heartbeatLoop(rn.currentTerm)
}

// RequestVote is called by candidates asking for our vote.
// This is an exported method so Go's net/rpc can call it.
func (rn *RaftNode) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.stopped() || rn.diskErr != nil {
		return fmt.Errorf("node unavailable")
	}
	if args.Term > rn.currentTerm {
		if err := rn.stepDown(args.Term); err != nil {
			return err
		}
	}
	reply.Term = rn.currentTerm
	reply.VoteGranted = false
	if args.Term < rn.currentTerm {
		return nil
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

	if rn.votedFor != int32(args.CandidateID) {
		rn.votedFor = int32(args.CandidateID)
		if err := rn.saveState(); err != nil {
			return err
		}
	}
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

	if rn.stopped() || rn.diskErr != nil {
		return fmt.Errorf("node unavailable")
	}
	if args.Term > rn.currentTerm {
		if err := rn.stepDown(args.Term); err != nil {
			return err
		}
	}
	reply.Term = rn.currentTerm
	reply.Success = false
	reply.MatchIndex = 0
	if args.Term < rn.currentTerm {
		return nil
	}
	rn.state = Follower
	rn.leaderID = args.LeaderID
	rn.resetElectionTimer()

	if args.PrevLogIndex > rn.lastLogIndex() {
		reply.MatchIndex = rn.lastLogIndex()
		return nil
	}
	if rn.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		if args.PrevLogIndex > 0 {
			reply.MatchIndex = args.PrevLogIndex - 1
		}
		return nil
	}

	changed := false
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + uint64(i)
		if index <= rn.lastLogIndex() && rn.log[index].Term == entry.Term {
			continue
		}
		if index <= rn.commitIndex {
			return fmt.Errorf("refusing to overwrite committed entry %d", index)
		}
		rn.log = append(rn.log[:index], args.Entries[i:]...)
		changed = true
		break
	}
	matched := args.PrevLogIndex + uint64(len(args.Entries))
	commit := min(args.LeaderCommit, matched)
	if commit > rn.commitIndex {
		rn.commitIndex = commit
		changed = true
	}
	if changed {
		if err := rn.saveState(); err != nil {
			return err
		}
		rn.signalApply()
	}

	reply.Success = true
	reply.MatchIndex = matched
	return nil
}

// heartbeatLoop runs while this node is leader, periodically sending
// AppendEntries RPCs to all followers. If there are new log entries,
// they're included in the heartbeat. Otherwise, it's an empty heartbeat
// just to prevent election timeouts.
func (rn *RaftNode) heartbeatLoop(term uint64) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rn.mu.RLock()
			isLeader := rn.state == Leader && rn.currentTerm == term
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
	if rn.state != Leader || rn.stopped() {
		rn.mu.RUnlock()
		return
	}
	currentTerm := rn.currentTerm
	rn.mu.RUnlock()

	for peerID := uint32(1); peerID <= uint32(len(rn.peers)+1); peerID++ {
		if peerID == rn.id {
			continue
		}

		go func(pid uint32) {
			rn.mu.RLock()
			nextIdx, ok := rn.nextIndex[pid]
			if !ok || rn.currentTerm != currentTerm || rn.state != Leader {
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
			commitIndex := rn.commitIndex
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

			if rn.stopped() || rn.diskErr != nil {
				return
			}
			if reply.Term > rn.currentTerm {
				rn.stepDown(reply.Term)
				return
			}

			if rn.state != Leader || rn.currentTerm != currentTerm {
				return
			}

			if reply.Success {
				rn.matchIndex[pid] = max(rn.matchIndex[pid], reply.MatchIndex)
				rn.nextIndex[pid] = rn.matchIndex[pid] + 1
				rn.advanceCommitIndex()
			} else if rn.nextIndex[pid] == nextIdx && nextIdx > 1 {
				rn.nextIndex[pid] = max(rn.matchIndex[pid]+1, min(nextIdx-1, reply.MatchIndex+1))
			}
		}(peerID)
	}
}

// advanceCommitIndex checks if there's a new index that a majority of
// nodes have replicated. If so, we can safely advance the commit index.
// Must be called with rn.mu held.
func (rn *RaftNode) advanceCommitIndex() {

	for idx := rn.lastLogIndex(); idx > rn.commitIndex; idx-- {
		if rn.log[idx].Term != rn.currentTerm {
			break
		}
		count := 1
		for _, matched := range rn.matchIndex {
			if matched >= idx {
				count++
			}
		}
		if count > (len(rn.peers)+1)/2 {
			rn.commitIndex = idx
			if rn.saveState() == nil {
				rn.signalApply()
			}
			return
		}
	}
}

// releaseCommitWaitersLocked wakes callers whose committed entries have
// been applied locally. Must be called with rn.mu held.
func (rn *RaftNode) releaseCommitWaitersLocked() {
	if len(rn.commitWaiters) == 0 {
		return
	}

	remaining := rn.commitWaiters[:0]
	for _, w := range rn.commitWaiters {
		if w.index <= rn.lastApplied {
			close(w.ch)
		} else {
			remaining = append(remaining, w)
		}
	}
	rn.commitWaiters = remaining
}

// cancelCommitWaiter removes a waiter that gave up (timeout or shutdown), so
// the slice doesn't grow with abandoned entries.
func (rn *RaftNode) cancelCommitWaiter(ch chan struct{}) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	for i, w := range rn.commitWaiters {
		if w.ch == ch {
			rn.commitWaiters = append(rn.commitWaiters[:i], rn.commitWaiters[i+1:]...)
			return
		}
	}
}

// Propose appends a command to the leader's log and returns immediately,
// without waiting for replication. Callers that need to know the write is
// durable should use ProposeAndWait instead.
func (rn *RaftNode) Propose(cmd Command) error {
	_, _, err := rn.propose(cmd)
	return err
}

// propose appends the command to the local log, returning the index and term
// it was written at.
func (rn *RaftNode) propose(cmd Command) (uint64, uint64, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.state != Leader || rn.stopped() || rn.diskErr != nil {
		return 0, 0, fmt.Errorf("not the leader (leader is node %d)", rn.leaderID)
	}

	data, err := EncodeCommand(cmd)
	if err != nil {
		return 0, 0, fmt.Errorf("encode command: %w", err)
	}

	entry := LogEntry{
		Term:    rn.currentTerm,
		Index:   rn.lastLogIndex() + 1,
		Command: data,
	}

	rn.log = append(rn.log, entry)
	if err := rn.saveState(); err != nil {
		return 0, 0, err
	}
	rn.advanceCommitIndex()

	log.Printf("[node %d] proposed command at index %d (term %d)\n",
		rn.id, entry.Index, entry.Term)

	return entry.Index, entry.Term, nil
}

// ProposeAndWait waits for durable majority replication and local apply.
func (rn *RaftNode) ProposeAndWait(ctx context.Context, cmd Command) error {
	index, term, err := rn.propose(cmd)
	if err != nil {
		return err
	}

	ch := make(chan struct{})

	rn.mu.Lock()
	// The apply loop may have finished before we got the lock back.
	if rn.lastApplied >= index {
		rn.mu.Unlock()
		return rn.verifyCommitted(index, term)
	}
	rn.commitWaiters = append(rn.commitWaiters, commitWaiter{index: index, ch: ch})
	rn.mu.Unlock()

	select {
	case <-ch:
		return rn.verifyCommitted(index, term)

	case <-ctx.Done():
		rn.cancelCommitWaiter(ch)
		return fmt.Errorf("timed out waiting for entry %d to commit", index)

	case <-rn.stopCh:
		rn.cancelCommitWaiter(ch)
		return fmt.Errorf("node shutting down before entry %d committed", index)
	}
}

// verifyCommitted confirms that the entry sitting at index is still the one
// we appended. If leadership changed, a new leader may have overwritten that
// slot with a different entry from a later term — in which case our write did
// not survive and the client must not be told it succeeded.
func (rn *RaftNode) verifyCommitted(index uint64, term uint64) error {
	rn.mu.RLock()
	defer rn.mu.RUnlock()

	if index >= uint64(len(rn.log)) {
		return fmt.Errorf("entry %d disappeared from the log after a leader change", index)
	}
	if rn.log[index].Term != term {
		return fmt.Errorf("entry %d was overwritten by a new leader (term %d, expected %d)",
			index, rn.log[index].Term, term)
	}
	return nil
}

func (rn *RaftNode) applyLoop() {
	defer rn.applyWG.Done()
	for {
		select {
		case <-rn.applyCh:
			if err := rn.applyCommitted(); err != nil {
				log.Printf("[node %d] apply: %v", rn.id, err)
			}
		case <-rn.stopCh:
			return
		}
	}
}

func (rn *RaftNode) applyCommitted() error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	for rn.diskErr == nil && rn.lastApplied < rn.commitIndex {
		entry := rn.log[rn.lastApplied+1]
		if len(entry.Command) != 0 {
			cmd, err := DecodeCommand(entry.Command)
			if err != nil {
				return err
			}
			switch cmd.Type {
			case CmdPut:
				err = rn.engine.Put(cmd.Key, cmd.Value)
			case CmdDelete:
				err = rn.engine.Delete(cmd.Key)
			default:
				err = fmt.Errorf("unknown command type %d", cmd.Type)
			}
			if err != nil {
				rn.diskErr = fmt.Errorf("apply entry %d: %w", entry.Index, err)
				rn.state = Follower
				rn.leaderID = 0
				return rn.diskErr
			}
		}
		rn.lastApplied++
		rn.releaseCommitWaitersLocked()
	}
	return rn.diskErr
}

func (rn *RaftNode) signalApply() {
	select {
	case rn.applyCh <- LogEntry{}:
	default:

	}
}

func (rn *RaftNode) stepDown(newTerm uint64) error {
	rn.currentTerm = newTerm
	rn.state = Follower
	rn.leaderID = 0
	rn.votedFor = -1
	if err := rn.saveState(); err != nil {
		return err
	}
	rn.resetElectionTimer()
	return nil
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

	rn.electionGeneration++
	generation := rn.electionGeneration
	rn.electionTimer = time.AfterFunc(timeout, func() { rn.startElection(generation) })
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
	if err := rn.rpcServer.Register(rn); err != nil {
		return err
	}

	listener, err := net.Listen("tcp", rn.address)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", rn.address, err)
	}

	rn.mu.Lock()
	rn.listener = listener
	rn.mu.Unlock()

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
	conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
	if err != nil {
		return err
	}
	client := rpc.NewClient(conn)
	defer client.Close()
	if err := conn.SetDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
		return err
	}
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
