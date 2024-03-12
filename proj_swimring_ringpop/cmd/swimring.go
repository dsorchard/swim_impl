package main

import (
	"fmt"
	"net"
	"net/rpc"
	"swim/proj_swimring_ringpop/membership"
	"time"
)

type SwimRing struct {
	node         *membership.Node
	internalPort int
}

func NewSwimRing(internalPort int) *SwimRing {
	sr := &SwimRing{
		internalPort: internalPort,
	}
	return sr
}

func (sr *SwimRing) init() error {
	address := fmt.Sprintf("%s:%d", "127.0.0.1", sr.internalPort)

	sr.node = membership.NewNode(sr, address, &membership.Options{
		JoinTimeout:        time.Duration(200) * time.Millisecond,
		SuspectTimeout:     time.Duration(200) * time.Millisecond,
		PingTimeout:        time.Duration(200) * time.Millisecond,
		PingRequestTimeout: time.Duration(200) * time.Millisecond,
		MinProtocolPeriod:  time.Duration(200) * time.Millisecond,
		PingRequestSize:    200,
		BootstrapNodes:     []string{"127.0.0.1:8080"},
	})
	return nil
}

func (sr *SwimRing) Bootstrap() ([]string, error) {
	err := sr.init()
	if err != nil {
		return nil, err
	}

	err = sr.registerInternalRPCHandlers()
	if err != nil {
		return nil, err
	}

	joined, err := sr.node.Bootstrap()
	if err != nil {
		return nil, err
	}

	return joined, nil
}

func (sr *SwimRing) registerInternalRPCHandlers() error {

	// a. create server
	server := rpc.NewServer()
	err := sr.node.RegisterRPCHandlers(server)
	if err != nil {
		return err
	}

	// b. create listening address
	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf(":%d", sr.internalPort))
	if err != nil {
		return err
	}
	conn, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return err
	}

	go server.Accept(conn)

	return nil
}

func (sr *SwimRing) HandleChanges(changes []membership.Change) {
	var serversToAdd, serversToRemove []string

	for _, change := range changes {
		switch change.Status {
		case membership.Alive, membership.Suspect:
			serversToAdd = append(serversToAdd, change.Address)
		case membership.Faulty:
			serversToRemove = append(serversToRemove, change.Address)
		}
	}
}
