package membership

import (
	"errors"
	"sync"
	"time"
)

func (n *Node) joinCluster() []string {
	var nodesJoined []string
	var wg sync.WaitGroup

	logger.Infof("Trying to join the cluster...")
	for _, target := range n.bootstrapNodes {
		wg.Add(1)

		go func(target string) {
			defer wg.Done()
			res, err := sendJoin(n, target, n.joinTimeout)

			if err != nil {
				return
			}

			logger.Info("Join %s successfully, %d peers found", target, len(res.Membership))
			n.memberlist.AddJoinList(res.Membership)
			nodesJoined = append(nodesJoined, target)
		}(target)
	}

	wg.Wait()

	return nodesJoined
}

func sendJoin(node *Node, target string, timeout time.Duration) (*JoinResponse, error) {
	if target == node.Address() {
		logger.Error("Cannot join local node")
		return nil, errors.New("cannot join local node")
	}

	req := &JoinRequest{
		Source:      node.address,
		Incarnation: node.Incarnation(),
		Timeout:     timeout,
	}
	resp := &JoinResponse{}

	errCh := make(chan error, 1)
	go func() {
		client, err := node.memberlist.MemberClient(target)
		if err != nil {
			errCh <- err
			return
		}

		errCh <- client.Call("Protocol.Join", req, resp)
	}()

	var err error
	select {
	case err = <-errCh:
	case <-time.After(timeout):
		logger.Error("Join request timeout")
		err = errors.New("join timeout")
	}

	if err != nil {
		return nil, err
	}

	return resp, err
}

// Incarnation returns the incarnation number of the Node.
func (n *Node) Incarnation() int64 {
	if n.memberlist != nil && n.memberlist.local != nil {
		n.memberlist.local.RLock()
		incarnation := n.memberlist.local.Incarnation
		n.memberlist.local.RUnlock()
		return incarnation
	}
	return -1
}

func (n *Node) pinging() bool {
	n.status.RLock()
	pinging := n.status.pinging
	n.status.RUnlock()

	return pinging
}

func (n *Node) setPinging(pinging bool) {
	n.status.Lock()
	n.status.pinging = pinging
	n.status.Unlock()
}

func (n *Node) pingNextMember() {
	if n.pinging() {
		return
	}

	member, ok := n.memberiter.Next()
	if !ok {
		return
	}

	n.setPinging(true)
	defer n.setPinging(false)

	res, err := sendDirectPing(n, member.Address, n.pingTimeout)
	if err == nil {
		n.memberlist.Update(res.Changes)
		return
	}

	n.memberlist.CloseMemberClient(member.Address)
	targetReached, _ := sendIndirectPing(n, member.Address, n.pingRequestSize, n.pingRequestTimeout)

	if !targetReached {
		if member.Status != Suspect {
			logger.Errorf("Cannot reach %s, mark it suspect", member.Address)
		}
		n.memberlist.MarkSuspect(member.Address, member.Incarnation)
		return
	}
}
