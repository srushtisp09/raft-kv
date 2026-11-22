package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/srushtisp09/raft-kv/kvstore"
)

func usage() {
	fmt.Fprintln(os.Stderr, "usage: client -servers addr1,addr2,addr3 <command>")
	fmt.Fprintln(os.Stderr, "  get KEY")
	fmt.Fprintln(os.Stderr, "  put KEY VALUE")
	fmt.Fprintln(os.Stderr, "  delete KEY")
	os.Exit(2)
}

func main() {
	serversFlag := flag.String("servers", "", "Comma-separated KV server addresses")
	flag.Parse()

	args := flag.Args()
	if *serversFlag == "" || len(args) == 0 {
		usage()
	}

	// Check the command and its argument count before connecting.
	cmd := args[0]
	switch {
	case cmd == "get" && len(args) == 2:
	case cmd == "put" && len(args) == 3:
	case cmd == "delete" && len(args) == 2:
	default:
		usage()
	}

	// Connect to every server that is reachable; skip the ones that are down.
	var services []kvstore.KVService
	for _, addr := range strings.Split(*serversFlag, ",") {
		addr = strings.TrimSpace(addr)
		c, err := kvstore.NewKVClient(addr)
		if err != nil {
			log.Printf("warning: cannot reach %s: %v", addr, err)
			continue
		}
		defer c.Close()
		services = append(services, c)
	}
	if len(services) == 0 {
		log.Fatal("no KV servers reachable")
	}

	ck := kvstore.MakeClerk(services)

	switch cmd {
	case "get":
		fmt.Println(ck.Get(args[1]))
	case "put":
		ck.Put(args[1], args[2])
		fmt.Println("OK")
	case "delete":
		ck.Delete(args[1])
		fmt.Println("OK")
	}
}
