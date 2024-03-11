package membership

import (
	"net/rpc"
	"sync"
	"time"
)

// Node is a SWIM member.
type Node struct {
	address string

	status struct {
		stopped, destroyed, pinging, ready bool
		sync.RWMutex
	}

	swimring         changeHandler
	memberlist       *memberlist
	disseminator     *disseminator
	stateTransitions *stateTransitions

	memberiter       *memberlistIter
	gossip           *gossip
	protocolHandlers *ProtocolHandlers

	joinTimeout, suspectTimeout, pingTimeout, pingRequestTimeout time.Duration

	pingRequestSize int
	bootstrapNodes  []string
}

type Options struct {
	JoinTimeout, SuspectTimeout,
	PingTimeout, PingRequestTimeout,
	MinProtocolPeriod time.Duration

	PingRequestSize int
	BootstrapNodes  []string
}

// NewNode returns a new SWIM node.
func NewNode(swimRing changeHandler, address string, opts *Options) *Node {
	node := &Node{
		address: address,
	}

	node.swimring = swimRing
	node.memberlist = newMemberlist(node)
	node.memberiter = newMemberlistIter(node.memberlist)
	node.disseminator = newDisseminator(node)
	node.stateTransitions = newStateTransitions(node)
	node.gossip = newGossip(node, opts.MinProtocolPeriod)
	node.protocolHandlers = NewProtocolHandler(node)

	node.joinTimeout = opts.JoinTimeout
	node.suspectTimeout = opts.SuspectTimeout
	node.pingTimeout = opts.PingTimeout
	node.pingRequestTimeout = opts.PingRequestTimeout
	node.pingRequestSize = opts.PingRequestSize
	node.bootstrapNodes = opts.BootstrapNodes

	return node
}

func (n *Node) RegisterRPCHandlers(server *rpc.Server) error {
	err := server.RegisterName("Protocol", n.protocolHandlers)
	logger.Info("SWIM protocol RPC handlers registered")
	return err
}

func (n *Node) Bootstrap() ([]string, error) {
	n.memberlist.Reincarnate()
	nodesJoined := n.joinCluster()
	n.gossip.Start()

	n.status.Lock()
	n.status.ready = true
	n.status.Unlock()

	return nodesJoined, nil
}

func (n *Node) Members() []Member {
	return n.memberlist.Members()
}

func (n *Node) MemberClient(address string) (*rpc.Client, error) {
	return n.memberlist.MemberClient(address)
}

func (n *Node) MemberReachable(address string) bool {
	member, ok := n.memberlist.Member(address)
	if !ok {
		return false
	}

	return member.isReachable()
}

func (n *Node) Address() string {
	return n.address
}
