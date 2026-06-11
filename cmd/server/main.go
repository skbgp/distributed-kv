package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/shubham/distributed-kv/pkg/raft"
	"github.com/shubham/distributed-kv/pkg/server"
	"github.com/shubham/distributed-kv/pkg/storage"
)

func main() {

	nodeID := flag.Int("id", 1, "Node ID (unique within the cluster)")
	port := flag.Int("port", 9001, "Raft RPC port")
	peersStr := flag.String("peers", "", "Comma-separated list of peer Raft addresses")
	dataDir := flag.String("data-dir", "/tmp/dkv-node", "Directory for storing data files")

	flag.Parse()

	if *peersStr == "" {
		log.Fatal("--peers is required (comma-separated peer addresses)")
	}

	peers := strings.Split(*peersStr, ",")
	for i := range peers {
		peers[i] = strings.TrimSpace(peers[i])
	}

	raftAddr := fmt.Sprintf("localhost:%d", *port)
	httpAddr := server.FormatAddress(*port)

	log.Printf("Starting node %d\n", *nodeID)
	log.Printf("  Raft address: %s\n", raftAddr)
	log.Printf("  HTTP address: %s\n", httpAddr)
	log.Printf("  Peers: %v\n", peers)
	log.Printf("  Data dir: %s\n", *dataDir)

	engine, err := storage.NewEngine(*dataDir)
	if err != nil {
		log.Fatalf("Failed to create storage engine: %v", err)
	}

	node := raft.NewRaftNode(uint32(*nodeID), raftAddr, peers, engine)
	if err := node.Start(); err != nil {
		log.Fatalf("Failed to start Raft node: %v", err)
	}

	httpServer := server.NewServer(node, engine, httpAddr, uint32(*nodeID))
	go func() {
		if err := httpServer.Start(); err != nil {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	log.Printf("Node %d is ready. Dashboard at http://localhost%s\n", *nodeID, httpAddr)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Printf("Shutting down node %d...\n", *nodeID)
	node.Stop()
	engine.Close()
	log.Println("Goodbye.")
}
