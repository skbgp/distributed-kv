#!/bin/bash
# start-cluster.sh - Bootstraps a local 3-node Raft cluster

make build >/dev/null 2>&1

./bin/dkv-server --id 1 --port 9001 --peers "localhost:9002,localhost:9003" --data-dir /tmp/dkv-1 &
PID1=$!

./bin/dkv-server --id 2 --port 9002 --peers "localhost:9001,localhost:9003" --data-dir /tmp/dkv-2 &
PID2=$!

./bin/dkv-server --id 3 --port 9003 --peers "localhost:9001,localhost:9002" --data-dir /tmp/dkv-3 &
PID3=$!

echo "Cluster started."
echo "Dashboard: http://localhost:10001"
echo "CLI: ./bin/dkv-cli --server localhost:10001"

trap "kill $PID1 $PID2 $PID3; exit" SIGINT SIGTERM
wait
