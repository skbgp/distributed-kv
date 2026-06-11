package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

func main() {
	serverAddr := flag.String("server", "localhost:10001", "HTTP address of a KV node")
	flag.Parse()

	baseURL := fmt.Sprintf("http://%s", *serverAddr)

	fmt.Println("DistKV CLI")
	fmt.Printf("Connected to %s\n", baseURL)
	fmt.Println("Type 'help' for commands.")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("dkv> ")

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			fmt.Print("dkv> ")
			continue
		}

		parts := strings.SplitN(line, " ", 3)
		cmd := strings.ToLower(parts[0])

		switch cmd {
		case "put":
			if len(parts) < 3 {
				fmt.Println("Usage: put <key> <value>")
			} else {
				doPut(baseURL, parts[1], parts[2])
			}

		case "get":
			if len(parts) < 2 {
				fmt.Println("Usage: get <key>")
			} else {
				doGet(baseURL, parts[1])
			}

		case "del", "delete":
			if len(parts) < 2 {
				fmt.Println("Usage: del <key>")
			} else {
				doDelete(baseURL, parts[1])
			}

		case "status":
			doStatus(baseURL)

		case "help":
			printHelp()

		case "exit", "quit":
			fmt.Println("Bye.")
			return

		default:
			fmt.Printf("Unknown command: %s (type 'help')\n", cmd)
		}

		fmt.Print("dkv> ")
	}
}

func doPut(base, key, value string) {

	payload, _ := json.Marshal(map[string]string{"value": value})
	req, _ := http.NewRequest("PUT", base+"/api/kv/"+key, strings.NewReader(string(payload)))
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	if errMsg, ok := result["error"]; ok {
		fmt.Printf("Error: %v\n", errMsg)
	} else {
		fmt.Printf("OK\n")
	}
}

func doGet(base, key string) {
	resp, err := http.Get(base + "/api/kv/" + key)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	if resp.StatusCode == 404 {
		fmt.Printf("(nil)\n")
	} else if errMsg, ok := result["error"]; ok {
		fmt.Printf("Error: %v\n", errMsg)
	} else {
		fmt.Printf("\"%v\"\n", result["value"])
	}
}

func doDelete(base, key string) {
	req, _ := http.NewRequest("DELETE", base+"/api/kv/"+key, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	if errMsg, ok := result["error"]; ok {
		fmt.Printf("Error: %v\n", errMsg)
	} else {
		fmt.Printf("OK (deleted)\n")
	}
}

func doStatus(base string) {
	resp, err := http.Get(base + "/api/status")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	json.Unmarshal(data, &result)

	pretty, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(pretty))
}

func printHelp() {
	fmt.Println("Commands:")
	fmt.Println("  put <key> <value>   Store a key-value pair")
	fmt.Println("  get <key>           Retrieve a value")
	fmt.Println("  del <key>           Delete a key")
	fmt.Println("  status              Show node status (Raft state, term, etc)")
	fmt.Println("  help                Show this help")
	fmt.Println("  exit                Quit")
}
