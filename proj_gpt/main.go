package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Println("Usage: swim [localAddr] [seedAddress]")
		os.Exit(1)
	}
	localAddr := os.Args[1]
	seedAddress := os.Args[2]

	cluster := &Cluster{
		LocalAddr: localAddr,
	}

	swim := &SWIM{Cluster: cluster}
	_ = rpc.Register(swim)
	l, err := net.Listen("tcp", localAddr)
	if err != nil {
		log.Fatal(err)
	}
	go rpc.Accept(l)
	if seedAddress != "" {
		cluster.Join(seedAddress)
	}

	go cluster.StartPinging()

	select {}
}
