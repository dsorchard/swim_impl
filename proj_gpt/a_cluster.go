package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/rpc"
	"sync"
	"time"
)

type SWIM struct {
	Cluster *Cluster
}

type Cluster struct {
	Mutex     sync.Mutex
	LocalAddr string
	Members   []Member
}

func (c *Cluster) Join(addr string) {
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		log.Fatal("Dialing:", err)
	}

	req := &PingRequest{
		From:    Member{Address: c.LocalAddr},
		Message: "Join",
	}

	var res PingResponse
	err = client.Call("SWIM.Ping", req, &res)
	if err != nil {
		log.Fatal("RPC failed:", err)
	}

	fmt.Println("Joined cluster through", addr)
}

func (c *Cluster) StartPinging() {
	for {
		c.Mutex.Lock()
		if len(c.Members) > 0 {
			index := rand.Intn(len(c.Members))
			member := c.Members[index]
			c.Mutex.Unlock()

			client, err := rpc.Dial("tcp", member.Address)
			if err != nil {
				fmt.Println("Failed to dial:", member.Address)
				continue
			}

			req := PingRequest{
				From:    Member{Address: c.LocalAddr},
				Message: "Ping",
			}

			var res PingResponse
			err = client.Call("SWIM.Ping", req, &res)
			if err != nil {
				fmt.Println("Ping failed:", err)
			} else {
				fmt.Println("Ping successful:", res.Message)
			}
		} else {
			c.Mutex.Unlock()
		}

		time.Sleep(time.Second * 5)
	}
}
