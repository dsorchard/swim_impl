package membership

import "time"

// JoinRequest is the payload of join request.
type JoinRequest struct {
	Source      string
	Incarnation int64
	Timeout     time.Duration
}

// JoinResponse is the payload of the response of join request.
type JoinResponse struct {
	Coordinator string
	Membership  []Change
	Checksum    uint32
}
