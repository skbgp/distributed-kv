package raft

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/shubham/distributed-kv/pkg/storage"
)

// These tests bring up a real 3-node cluster in-process. The nodes talk to
// each other over actual TCP via net/rpc — the same path used in production —
// so elections and replication are exercised end to end rather than mocked.

// testCluster owns a set of running nodes and their temp data dirs.
type testCluster struct {
	t       *testing.T
	nodes   []*RaftNode
	stopped []bool
	mu      sync.Mutex
}

// freePort grabs a port the OS is willing to hand out, then releases it so
// the node can bind it a moment later.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// newTestCluster starts n nodes wired together as peers. Node IDs are
// 1-indexed, and each node's peer list holds the other nodes' addresses in
// ascending ID order — the layout peerAddress() expects.
func newTestCluster(t *testing.T, n int) *testCluster {
	t.Helper()

	addrs := make([]string, n)
	for i := 0; i < n; i++ {
		addrs[i] = fmt.Sprintf("localhost:%d", freePort(t))
	}

	c := &testCluster{t: t, stopped: make([]bool, n)}

	for i := 0; i < n; i++ {
		var peers []string
		for j := 0; j < n; j++ {
			if j != i {
				peers = append(peers, addrs[j])
			}
		}

		engine, err := storage.NewEngine(t.TempDir())
		if err != nil {
			t.Fatalf("node %d: open engine: %v", i+1, err)
		}

		node := NewRaftNode(uint32(i+1), addrs[i], peers, engine)
		if err := node.Start(); err != nil {
			t.Fatalf("node %d: start: %v", i+1, err)
		}
		c.nodes = append(c.nodes, node)
	}

	t.Cleanup(c.stopAll)
	return c
}

func (c *testCluster) stopAll() {
	for i := range c.nodes {
		c.stop(i)
	}
}

// stop shuts a node down, tolerating a node that is already stopped.
func (c *testCluster) stop(i int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped[i] {
		return
	}
	c.stopped[i] = true
	c.nodes[i].Stop()
}

func (c *testCluster) isStopped(i int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stopped[i]
}

// leaders returns the indexes of every live node currently claiming leadership.
func (c *testCluster) leaders() []int {
	var out []int
	for i, n := range c.nodes {
		if c.isStopped(i) {
			continue
		}
		if n.IsLeader() {
			out = append(out, i)
		}
	}
	return out
}

// waitForLeader blocks until exactly one live node is leader, or fails.
func (c *testCluster) waitForLeader(timeout time.Duration) int {
	c.t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ls := c.leaders(); len(ls) == 1 {
			return ls[0]
		}
		time.Sleep(20 * time.Millisecond)
	}

	c.t.Fatalf("no single leader elected within %s (leaders=%v)", timeout, c.leaders())
	return -1
}

// TestCluster_ElectsSingleLeader is the baseline liveness + safety check:
// a fresh cluster must converge on exactly one leader, and only one.
func TestCluster_ElectsSingleLeader(t *testing.T) {
	c := newTestCluster(t, 3)

	leader := c.waitForLeader(5 * time.Second)
	t.Logf("node %d elected leader in term %d",
		c.nodes[leader].id, c.nodes[leader].CurrentTerm())

	// Hold for a few heartbeat rounds — a stable leader must not be displaced,
	// and no second node may declare itself leader in the meantime.
	for i := 0; i < 20; i++ {
		time.Sleep(50 * time.Millisecond)
		if ls := c.leaders(); len(ls) > 1 {
			t.Fatalf("split brain: %d nodes claim leadership", len(ls))
		}
	}

	if !c.nodes[leader].IsLeader() {
		t.Fatal("leader lost leadership despite a healthy cluster")
	}
}

// TestCluster_LeaderFailover kills the leader and requires the remaining two
// nodes — still a majority of three — to elect a replacement.
func TestCluster_LeaderFailover(t *testing.T) {
	c := newTestCluster(t, 3)

	oldLeader := c.waitForLeader(5 * time.Second)
	oldTerm := c.nodes[oldLeader].CurrentTerm()
	t.Logf("killing leader node %d (term %d)", c.nodes[oldLeader].id, oldTerm)

	c.stop(oldLeader)

	newLeader := c.waitForLeader(5 * time.Second)
	if newLeader == oldLeader {
		t.Fatal("the dead node is still reported as leader")
	}

	newTerm := c.nodes[newLeader].CurrentTerm()
	t.Logf("node %d took over in term %d", c.nodes[newLeader].id, newTerm)

	// A new leader must serve a strictly later term, or the old leader's
	// entries could be silently overwritten.
	if newTerm <= oldTerm {
		t.Fatalf("new leader's term %d did not advance past the old term %d", newTerm, oldTerm)
	}
}

// TestCluster_ReplicatesToMajority proposes a write on the leader and requires
// it to reach the followers' storage engines. This is the claim that matters
// for a KV store: a committed write is durable on a majority, not just locally.
func TestCluster_ReplicatesToMajority(t *testing.T) {
	c := newTestCluster(t, 3)

	leaderIdx := c.waitForLeader(5 * time.Second)
	leader := c.nodes[leaderIdx]

	const numKeys = 10
	for i := 0; i < numKeys; i++ {
		cmd := Command{
			Type:  CmdPut,
			Key:   fmt.Sprintf("key-%02d", i),
			Value: []byte(fmt.Sprintf("value-%02d", i)),
		}
		if err := leader.Propose(cmd); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}

	// Give replication a generous window: several heartbeat intervals.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if allApplied(c, numKeys) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Every live node must hold every proposed key.
	for i, node := range c.nodes {
		if c.isStopped(i) {
			continue
		}
		for k := 0; k < numKeys; k++ {
			key := fmt.Sprintf("key-%02d", k)
			want := fmt.Sprintf("value-%02d", k)

			val, found, err := node.engine.Get(key)
			if err != nil {
				t.Fatalf("node %d: get %q: %v", node.id, key, err)
			}
			if !found {
				t.Fatalf("node %d never received committed key %q", node.id, key)
			}
			if string(val) != want {
				t.Fatalf("node %d: key %q = %q, want %q", node.id, key, val, want)
			}
		}
	}

	// The leader's commit index has to actually cover what it proposed;
	// otherwise the entries were replicated but never declared committed.
	leader.mu.RLock()
	commitIndex, lastIndex := leader.commitIndex, leader.lastLogIndex()
	leader.mu.RUnlock()

	if commitIndex < lastIndex {
		t.Fatalf("leader commit index %d lags its last log index %d: entries replicated but not committed",
			commitIndex, lastIndex)
	}
}

// TestCluster_ProposeAndWaitBlocksUntilCommitted checks that when
// ProposeAndWait returns, the write is genuinely on a majority — not merely
// in the leader's local log. The followers are read immediately after the
// call returns, with no sleep to paper over a gap.
func TestCluster_ProposeAndWaitBlocksUntilCommitted(t *testing.T) {
	c := newTestCluster(t, 3)

	leaderIdx := c.waitForLeader(5 * time.Second)
	leader := c.nodes[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cmd := Command{Type: CmdPut, Key: "durable", Value: []byte("committed")}
	if err := leader.ProposeAndWait(ctx, cmd); err != nil {
		t.Fatalf("ProposeAndWait: %v", err)
	}

	// The guarantee at this instant is that a majority holds the entry in
	// their logs. Followers learn it is *committed* on the next AppendEntries,
	// so checking their commitIndex here would be testing heartbeat timing,
	// not durability.
	replicated := 0
	for i, node := range c.nodes {
		if c.isStopped(i) {
			continue
		}
		node.mu.RLock()
		hasIt := node.lastLogIndex() >= 1
		node.mu.RUnlock()
		if hasIt {
			replicated++
		}
	}

	majority := len(c.nodes)/2 + 1
	if replicated < majority {
		t.Fatalf("ProposeAndWait returned with the entry on only %d/%d nodes, need %d",
			replicated, len(c.nodes), majority)
	}

	// And the leader must consider it committed.
	leader.mu.RLock()
	commitIndex := leader.commitIndex
	leader.mu.RUnlock()
	if commitIndex < 1 {
		t.Fatalf("ProposeAndWait returned before the leader committed (commitIndex=%d)", commitIndex)
	}
}

// TestCluster_WriteRejectedWithoutQuorum is the safety case. With both
// followers down the leader cannot commit anything, so a write must fail
// rather than be acknowledged.
//
// The old HTTP path slept 200ms and returned 200 OK here, telling the client
// a write had succeeded when it had reached exactly one node.
func TestCluster_WriteRejectedWithoutQuorum(t *testing.T) {
	c := newTestCluster(t, 3)

	leaderIdx := c.waitForLeader(5 * time.Second)
	leader := c.nodes[leaderIdx]

	// Kill every node except the leader — no majority remains.
	for i := range c.nodes {
		if i != leaderIdx {
			c.stop(i)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()

	cmd := Command{Type: CmdPut, Key: "orphan", Value: []byte("must-not-be-acked")}
	err := leader.ProposeAndWait(ctx, cmd)

	if err == nil {
		t.Fatal("write was acknowledged with no majority available: a client would " +
			"believe this write is durable when only one node has it")
	}
	t.Logf("correctly refused: %v", err)
}

// allApplied reports whether every live node has applied all numKeys writes.
func allApplied(c *testCluster, numKeys int) bool {
	for i, node := range c.nodes {
		if c.isStopped(i) {
			continue
		}
		for k := 0; k < numKeys; k++ {
			_, found, err := node.engine.Get(fmt.Sprintf("key-%02d", k))
			if err != nil || !found {
				return false
			}
		}
	}
	return true
}
