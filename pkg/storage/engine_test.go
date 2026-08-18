package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// These tests exercise the recovery path by simulating an unclean shutdown:
// we stop using an engine WITHOUT calling Close(), so the memtable is never
// gracefully flushed. Anything that survives must have come back through WAL
// replay or from an SSTable that was already on disk — which is exactly what
// a real crash recovery has to rely on.

// mustPut fails the test immediately on write error.
func mustPut(t *testing.T, e *Engine, key string, val []byte) {
	t.Helper()
	if err := e.Put(key, val); err != nil {
		t.Fatalf("put %q: %v", key, err)
	}
}

// assertValue checks that a key is present with the expected value.
func assertValue(t *testing.T, e *Engine, key string, want []byte) {
	t.Helper()
	got, found, err := e.Get(key)
	if err != nil {
		t.Fatalf("get %q: %v", key, err)
	}
	if !found {
		t.Fatalf("key %q missing after recovery (data loss)", key)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("key %q: got %q, want %q", key, got, want)
	}
}

// TestEngine_CrashRecovery_MemTableOnly kills the engine while all writes are
// still in the memtable and WAL, with nothing flushed to an SSTable yet.
// Every acknowledged write must come back.
func TestEngine_CrashRecovery_MemTableOnly(t *testing.T) {
	dir := t.TempDir()

	engine, err := NewEngine(dir)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}

	const numKeys = 100
	for i := 0; i < numKeys; i++ {
		mustPut(t, engine, fmt.Sprintf("key-%03d", i), []byte(fmt.Sprintf("value-%03d", i)))
	}

	// Crash: no Close(), so no graceful flush of the memtable.
	recovered, err := NewEngine(dir)
	if err != nil {
		t.Fatalf("reopen engine after crash: %v", err)
	}
	defer recovered.Close()

	for i := 0; i < numKeys; i++ {
		assertValue(t, recovered,
			fmt.Sprintf("key-%03d", i),
			[]byte(fmt.Sprintf("value-%03d", i)))
	}
}

// TestEngine_CrashRecovery_TombstoneSurvives makes sure a delete is not
// silently undone by recovery. Replaying a WAL that resurrects deleted keys
// is a classic LSM recovery bug.
func TestEngine_CrashRecovery_TombstoneSurvives(t *testing.T) {
	dir := t.TempDir()

	engine, err := NewEngine(dir)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}

	mustPut(t, engine, "alive", []byte("yes"))
	mustPut(t, engine, "doomed", []byte("temporary"))
	if err := engine.Delete("doomed"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Crash without Close().
	recovered, err := NewEngine(dir)
	if err != nil {
		t.Fatalf("reopen engine after crash: %v", err)
	}
	defer recovered.Close()

	assertValue(t, recovered, "alive", []byte("yes"))

	// Get reports (nil, true) for a tombstone: the key is known, value is gone.
	val, found, err := recovered.Get("doomed")
	if err != nil {
		t.Fatalf("get doomed: %v", err)
	}
	if found && val != nil {
		t.Fatalf("deleted key came back to life after recovery: %q", val)
	}
}

// TestEngine_CrashRecovery_AfterFlush writes enough data to push a memtable
// out to an SSTable, then keeps writing and crashes. Data must be recovered
// from both sources: the SSTable on disk and the WAL tail.
//
// This is the interesting case, because the engine resets the WAL right after
// a flush — if that reset races with concurrent writes, those writes exist
// only in the new memtable and are lost on crash.
func TestEngine_CrashRecovery_AfterFlush(t *testing.T) {
	dir := t.TempDir()

	engine, err := NewEngine(dir)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}

	// The default memtable threshold is 4MB, so use fat values to trigger a
	// real flush without paying for tens of thousands of fsyncs.
	bigVal := bytes.Repeat([]byte("x"), 8*1024)

	const preFlush = 700 // ~5.6MB, comfortably past the threshold
	for i := 0; i < preFlush; i++ {
		mustPut(t, engine, fmt.Sprintf("bulk-%04d", i), bigVal)
	}

	// Wait for the background flusher to actually produce an SSTable.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if engine.Stats().SSTableCount > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if engine.Stats().SSTableCount == 0 {
		t.Fatal("no SSTable was produced; flush never happened")
	}

	// More writes after the flush — these live in the fresh memtable + WAL.
	const postFlush = 50
	for i := 0; i < postFlush; i++ {
		mustPut(t, engine, fmt.Sprintf("tail-%03d", i), []byte(fmt.Sprintf("tail-value-%03d", i)))
	}

	// Crash without Close().
	recovered, err := NewEngine(dir)
	if err != nil {
		t.Fatalf("reopen engine after crash: %v", err)
	}
	defer recovered.Close()

	for i := 0; i < preFlush; i++ {
		assertValue(t, recovered, fmt.Sprintf("bulk-%04d", i), bigVal)
	}
	for i := 0; i < postFlush; i++ {
		assertValue(t, recovered,
			fmt.Sprintf("tail-%03d", i),
			[]byte(fmt.Sprintf("tail-value-%03d", i)))
	}
}

// TestEngine_CrashRecovery_TornWriteAtWALTail simulates the machine dying
// mid-append, leaving a partial record at the end of the log. Recovery must
// discard only the damaged tail and keep every complete record before it.
func TestEngine_CrashRecovery_TornWriteAtWALTail(t *testing.T) {
	dir := t.TempDir()

	engine, err := NewEngine(dir)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}

	const numKeys = 20
	for i := 0; i < numKeys; i++ {
		mustPut(t, engine, fmt.Sprintf("key-%02d", i), []byte(fmt.Sprintf("value-%02d", i)))
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Close() flushes the memtable and retires the segments behind it, so
	// rewrite the log by hand to get a clean set of records plus a torn tail.
	wal, err := OpenWAL(dir)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	activeSegment := filepath.Join(dir, segmentName(wal.ActiveSegment()))
	for i := 0; i < numKeys; i++ {
		rec := WALRecord{
			Op:    OpPut,
			Key:   fmt.Sprintf("torn-%02d", i),
			Value: []byte(fmt.Sprintf("torn-value-%02d", i)),
		}
		if err := wal.Write(rec); err != nil {
			t.Fatalf("wal write: %v", err)
		}
	}
	wal.Close()

	// Append a truncated record: a length header promising more bytes than follow.
	f, err := os.OpenFile(activeSegment, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("open wal for corruption: %v", err)
	}
	if _, err := f.Write([]byte{0x40, 0x00, 0x00, 0x00, 0xde, 0xad}); err != nil {
		t.Fatalf("write torn record: %v", err)
	}
	f.Close()

	recovered, err := NewEngine(dir)
	if err != nil {
		t.Fatalf("reopen engine after torn write: %v", err)
	}
	defer recovered.Close()

	// Everything written before the tear must survive.
	for i := 0; i < numKeys; i++ {
		assertValue(t, recovered,
			fmt.Sprintf("torn-%02d", i),
			[]byte(fmt.Sprintf("torn-value-%02d", i)))
	}

	// And the engine must still be writable afterwards.
	mustPut(t, recovered, "after-repair", []byte("ok"))
	assertValue(t, recovered, "after-repair", []byte("ok"))
}

// TestEngine_WALSegmentsAreRetired checks the other half of segmentation: old
// segments must actually be deleted once their data is in an SSTable, or the
// log grows without bound and recovery gets slower on every restart.
func TestEngine_WALSegmentsAreRetired(t *testing.T) {
	dir := t.TempDir()

	engine, err := NewEngine(dir)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()

	bigVal := bytes.Repeat([]byte("x"), 8*1024)

	// Enough for two flushes.
	for i := 0; i < 1200; i++ {
		mustPut(t, engine, fmt.Sprintf("bulk-%04d", i), bigVal)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if engine.Stats().SSTableCount >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := engine.Stats().SSTableCount; got < 2 {
		t.Fatalf("expected at least 2 SSTables, got %d", got)
	}

	// Give the flusher a moment to finish retiring segments.
	time.Sleep(200 * time.Millisecond)

	// Two flushes happened, so the log should have been trimmed back to
	// roughly the active segment rather than accumulating one per flush.
	if segments := engine.Stats().WALSegments; segments > 2 {
		t.Fatalf("WAL segments not being retired: %d still on disk", segments)
	}
}

// TestEngine_CrashRecovery_Repeated restarts the engine several times in a
// row, adding data each round, to catch recovery bugs that only appear once
// the WAL and SSTable set have some history behind them.
func TestEngine_CrashRecovery_Repeated(t *testing.T) {
	dir := t.TempDir()

	const rounds = 5
	const perRound = 20

	for r := 0; r < rounds; r++ {
		engine, err := NewEngine(dir)
		if err != nil {
			t.Fatalf("round %d: open engine: %v", r, err)
		}

		for i := 0; i < perRound; i++ {
			mustPut(t, engine, fmt.Sprintf("r%d-k%02d", r, i), []byte(fmt.Sprintf("r%d-v%02d", r, i)))
		}

		// Everything from every previous round must still be readable.
		for pr := 0; pr <= r; pr++ {
			for i := 0; i < perRound; i++ {
				assertValue(t, engine,
					fmt.Sprintf("r%d-k%02d", pr, i),
					[]byte(fmt.Sprintf("r%d-v%02d", pr, i)))
			}
		}
		// Crash, no Close().
	}
}
