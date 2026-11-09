# DistKV - Distributed Key-Value Store

A distributed key-value store built from scratch in Go, featuring an LSM-tree storage engine and an educational Raft implementation for replication.

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
│   Storage   │ ← LSM Tree (WAL → MemTable → SSTable → Compaction)
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
- **Commit Advancement**: Persist entries before acknowledging replication. HTTP writes wait for a majority and successful local application
- **Persistence and scope**: Term, vote, log and commit index are persisted atomically. Startup reapplies the committed PUT and DELETE prefix. Snapshots, dynamic membership and log compaction remain outside this implementation.

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

# Discover the leader with /api/status on ports 10001, 10002 and 10003.
# Point the CLI at the leader for writes.
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

The engine appends the WAL and updates a skip list. Background flushes serialize with writes when publishing an immutable memtable and retiring its WAL. Compaction also serializes table publication and disk reads to keep each lookup on a consistent table set.

### Read Path

```
Client → MemTable → Immutable MemTable → SSTables (newest first, bloom filter check)
```

I check layers from newest to oldest, stopping at the first match. The Bloom filter can skip an SSTable without opening it on a negative result. With ten bits per key, its theoretical false-positive rate is around 1% under the usual hash assumptions.

### Consistency model

- The leader acknowledges writes after a majority persists the Raft entry and the local engine applies it. Without quorum the HTTP write times out. A timed-out write can still commit later.
- Reads use the local engine. A follower can lag, and the read API does not perform a Raft read-index round.
- A replacement leader appends a current-term no-op so it can safely commit prior entries after an election. Existing data directories from pre-persistence versions retain their local values. Those releases kept Raft history in memory, so use a fresh cluster for replicated recovery testing.

### Compaction

Four L0 tables trigger a merge of the visible tables into one L1 table. The merge sorts all entries and keeps the newest value or tombstone. The engine syncs the replacement file and atomically publishes its manifest before removing retired inputs. Lookups hold a read lock while accessing table files. The merge holds its inputs in memory, and the Raft log is unbounded without snapshots.

### Benchmarks

```sh
go test ./pkg/storage -run '^$' -bench . -benchmem
```

The benchmarks separate local engine PUT, an in-memory GET hit and an SSTable GET hit. They measure the storage engine. For cluster write latency, time HTTP requests against the elected leader and report the test setup.

## Tech Stack

| Component    | Technology     |
|-------------|---------------|
| Language     | Go 1.22       |
| RPC          | Go net/rpc    |
| HTTP         | Go net/http   |
| Storage      | Custom LSM    |
| Dashboard    | Vanilla HTML/CSS/JS |

