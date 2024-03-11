package main

import "fmt"

// PingRequest is the request struct for ping messages
type PingRequest struct {
	From    Member
	Message string
}

// PingResponse is the response struct for ping messages
type PingResponse struct {
	Message string
}

// Ping handles incoming ping requests
func (s *SWIM) Ping(req PingRequest, res *PingResponse) error {
	fmt.Printf("Received ping from %s: %s\n", req.From.Address, req.Message)
	res.Message = "Pong"
	return nil
}
