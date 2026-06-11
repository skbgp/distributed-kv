package server

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"time"

	"github.com/shubham/distributed-kv/pkg/raft"
	"github.com/shubham/distributed-kv/pkg/storage"
)

//go:embed static/*
var staticFiles embed.FS

// Server is the HTTP frontend for the KV store.
type Server struct {
	node    *raft.RaftNode
	engine  *storage.Engine
	address string
	nodeID  uint32
}

// NewServer creates a new HTTP server.
func NewServer(node *raft.RaftNode, engine *storage.Engine, address string, nodeID uint32) *Server {
	return &Server{
		node:    node,
		engine:  engine,
		address: address,
		nodeID:  nodeID,
	}
}

// Start begins serving HTTP requests.
func (s *Server) Start() error {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/kv/", s.handleKV)
	mux.HandleFunc("/api/status", s.handleStatus)

	staticContent, _ := fs.Sub(staticFiles, "static")
	mux.Handle("/", http.FileServer(http.FS(staticContent)))

	log.Printf("[http] listening on %s\n", s.address)
	return http.ListenAndServe(s.address, mux)
}

// handleKV routes to the appropriate handler based on HTTP method.
// URL format: /api/kv/{key}
func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[len("/api/kv/"):]
	if key == "" {
		http.Error(w, `{"error": "key is required"}`, http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.handleGet(w, key)
	case http.MethodPut:
		s.handlePut(w, r, key)
	case http.MethodDelete:
		s.handleDelete(w, key)
	default:
		http.Error(w, `{"error": "method not allowed"}`, http.StatusMethodNotAllowed)
	}
}

// handleGet reads a key from the local storage engine.
// This is a "stale read" — it might return slightly old data if a write
// was recently committed but not yet applied. For a distributed KV store
// used in interviews, this tradeoff is fine and easy to explain.
func (s *Server) handleGet(w http.ResponseWriter, key string) {
	val, found, err := s.engine.Get(key)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": err.Error(),
		})
		return
	}

	if !found || val == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{
			"error": "key not found",
			"key":   key,
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"key":   key,
		"value": string(val),
	})
}

// handlePut writes a key-value pair through Raft consensus.
// The write only succeeds if this node is the leader and a majority
// of nodes confirm the replication.
func (s *Server) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	var body struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid JSON body, expected {\"value\": \"...\"}",
		})
		return
	}

	cmd := raft.Command{
		Type:  raft.CmdPut,
		Key:   key,
		Value: []byte(body.Value),
	}

	if err := s.node.Propose(cmd); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
			"error":     err.Error(),
			"leader_id": s.node.LeaderID(),
		})
		return
	}

	time.Sleep(200 * time.Millisecond)

	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
		"key":    key,
	})
}

// handleDelete removes a key through Raft consensus.
func (s *Server) handleDelete(w http.ResponseWriter, key string) {
	cmd := raft.Command{
		Type: raft.CmdDelete,
		Key:  key,
	}

	if err := s.node.Propose(cmd); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
			"error":     err.Error(),
			"leader_id": s.node.LeaderID(),
		})
		return
	}

	time.Sleep(200 * time.Millisecond)

	writeJSON(w, http.StatusOK, map[string]string{
		"status": "deleted",
		"key":    key,
	})
}

// handleStatus returns the current node's status — used by the dashboard.
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	stats := s.engine.Stats()

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"node_id":    s.nodeID,
		"state":      s.node.State().String(),
		"term":       s.node.CurrentTerm(),
		"is_leader":  s.node.IsLeader(),
		"leader_id":  s.node.LeaderID(),
		"log_length": s.node.LogLength(),
		"storage": map[string]interface{}{
			"memtable_entries":      stats.MemTableEntries,
			"memtable_bytes":        stats.MemTableBytes,
			"immutable_entries":     stats.ImmutableEntries,
			"sstable_count":         stats.SSTableCount,
			"total_sstable_entries": stats.TotalSSTableEntries,
		},
	})
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

// FormatAddress builds the HTTP address from Raft port.
// We use Raft port + 1000 for HTTP. So node on :9001 (Raft) serves HTTP on :10001.
func FormatAddress(raftPort int) string {
	return fmt.Sprintf(":%d", raftPort+1000)
}
