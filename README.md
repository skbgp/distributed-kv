# DistKV - Distributed Key-Value Store

A distributed key-value store built from scratch in Go, featuring an LSM-tree storage engine and Raft consensus for fault-tolerant replication.

## Architecture

```
Client (CLI / HTTP)
       │
       ▼
┌─────────────┐
│  HTTP Server│ ← REST API + Web Dashboard
└──────┬──────┘
       │
┌──────┴──────┐
│  Raft Node  │ ← Leader Election + Log Replication
└──────┬──────┘
       │
┌──────┴──────┐
│   Storage   │ ← LSM Tree (MemTable → WAL → SSTable → Compaction)
│   Engine    │
└─────────────┘
```

## Components

### Storage Engine (LSM Tree)
- **MemTable**: Skip list for sorted in-memory writes (O(log n) insert/lookup)
- **WAL**: Write-ahead log with CRC32 checksums for crash recovery
- **SSTable**: Immutable sorted files on disk with sparse index and Bloom filter
- **Bloom Filter**: Probabilistic filter (~1% false positive rate) to skip unnecessary disk reads
- **Compaction**: Background merging of SSTables to reclaim space and remove stale data

### Raft Consensus (Simplified)
- **Leader Election**: Randomized timeouts to prevent split votes
- **Log Replication**: AppendEntries RPC with consistency checks
- **Commit Advancement**: Majority-based commit with safety guarantees
- **Scope Limitations**: This is a simplified educational implementation. The Raft state (term, votedFor) and log are kept in-memory rather than being durably persisted, and snapshotting/log-compaction is omitted for brevity.

### HTTP Server
- REST API: `PUT/GET/DELETE /api/kv/{key}`
- Status endpoint: `GET /api/status`
- Web dashboard for cluster monitoring

## Quick Start

The fastest way to spin up a 3-node cluster:
```bash
./start-cluster.sh
```

Or you can run it manually:

```bash
# Build
make build

# Start a 3-node cluster (in separate terminals)
./bin/dkv-server --id 1 --port 9001 --peers "localhost:9002,localhost:9003" --data-dir /tmp/dkv-1
./bin/dkv-server --id 2 --port 9002 --peers "localhost:9001,localhost:9003" --data-dir /tmp/dkv-2
./bin/dkv-server --id 3 --port 9003 --peers "localhost:9001,localhost:9002" --data-dir /tmp/dkv-3

# Use the CLI
./bin/dkv-cli --server localhost:10001
dkv> put name shubham
OK
dkv> get name
"shubham"
dkv> status

# Or use curl
curl -X PUT localhost:10001/api/kv/city -d '{"value": "chennai"}'
curl localhost:10001/api/kv/city

# Dashboard
open http://localhost:10001
```

## Project Structure

```
distributed-kv/
├── cmd/
│   ├── server/main.go          # Server entry point
│   └── cli/main.go             # Interactive CLI client
├── pkg/
│   ├── storage/
│   │   ├── skiplist.go          # Skip list (memtable data structure)
│   │   ├── memtable.go          # In-memory write buffer
│   │   ├── wal.go               # Write-ahead log
│   │   ├── bloom.go             # Bloom filter
│   │   ├── sstable.go           # On-disk sorted tables
│   │   ├── compaction.go        # Background compaction
│   │   └── engine.go            # Storage engine facade
│   ├── raft/
│   │   ├── rpc.go               # Raft message types
│   │   └── state.go             # Raft consensus (election + replication)
│   └── server/
│       ├── server.go            # HTTP API + dashboard server
│       └── static/index.html    # Web dashboard
├── proto/
│   └── raft.proto               # Protobuf definitions (reference)
├── go.mod
├── Makefile
└── README.md
```

## Design Decisions

These are the engineering tradeoffs I made and why.

### Crash Recovery

The WAL (Write-Ahead Log) is the backbone of durability. Every write is first appended to the WAL and fsynced to disk, then inserted into the in-memory memtable. If the server crashes:

- **Before WAL write**: Data is lost, but the client never received an acknowledgment, so this is correct behavior.
- **After WAL write, before memtable**: On restart, the WAL is replayed to reconstruct the memtable. No data loss.
- **During memtable flush to SSTable**: The WAL is only cleared *after* the SSTable is fully written and fsynced. So if we crash mid-flush, the WAL still has the data and we can recover it.
- **Corrupt WAL tail**: If the server crashes mid-write, the last WAL record may be partially written. On recovery, we detect the corruption (via CRC32 checksum), truncate the corrupt tail, and recover everything before it.

### Write Path

```
Client → WAL (fsync) → MemTable (skip list) → [if full] → SSTable (disk)
```

Writes are always sequential (WAL append) and in-memory (skip list insert), making them fast. The expensive part (flushing to SSTable) happens in the background and doesn't block the write path.

### Read Path

```
Client → MemTable → Immutable MemTable → SSTables (newest first, bloom filter check)
```

I check layers from newest to oldest, stopping at the first match. The Bloom filter on each SSTable skips ~99% of unnecessary disk reads (1% false positive rate with 10 bits per key).

### Consistency Model

- **Writes** go through Raft consensus. A write is only acknowledged after a majority of nodes have replicated it.
- **Reads** go directly to the local storage engine without a Raft round. This is an "eventually consistent" read, so it might return slightly stale data if a write was just committed but not yet applied locally. A linearizable read would require a Raft round-trip, but for this project scope, the simpler approach is fine and easy to explain.

### Compaction

I implemented leveled compaction (L0 → L1). Level 0 tables can have overlapping key ranges (they come from separate memtable flushes). When L0 has ≥4 tables, a k-way merge runs against L1 to produce a single sorted, deduplicated SSTable. Tombstones are dropped at this stage since L1 is the bottom level.

## Benchmarks

The storage engine was benchmarked on an Apple M-series processor (SSD).

| Operation | Latency/Op | Bottleneck / Explanation |
|-----------|------------|--------------------------|
| **PUT** | `~3.7 ms` | Bound by `fsync()` on the WAL. Every write is guaranteed to be on disk before returning to ensure strict durability. |
| **GET (MemTable)** | `~1.5 µs` | Immediate O(log n) lookup in the skip list. |
| **GET (SSTable)** | `~15 µs` | Fast rejection via Bloom Filter (`O(1)`), followed by sparse index lookup and bounded disk read. |

*(Note: Run `go test -bench .` in `pkg/storage` to run local benchmarks).*

## Tech Stack

| Component    | Technology     |
|-------------|---------------|
| Language     | Go 1.22       |
| RPC          | Go net/rpc    |
| HTTP         | Go net/http   |
| Storage      | Custom LSM    |
| Dashboard    | Vanilla HTML/CSS/JS |

