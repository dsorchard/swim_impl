package memberlist

// NodeEventType are the types of events that can be sent from the
// ChannelEventDelegate.
type NodeEventType int

const (
	NodeJoin NodeEventType = iota
	NodeLeave
	NodeUpdate
)

// NodeEvent is a single event related to node activity in the memberlist.
// The Node member of this struct must not be directly modified. It is passed
// as a pointer to avoid unnecessary copies. If you wish to modify the node,
// make a copy first.
type NodeEvent struct {
	Event NodeEventType
	Node  *Node
}

type ChannelEventDelegate struct {
	Ch chan<- NodeEvent
}

func (c *ChannelEventDelegate) NotifyJoin(n *Node) {
	node := *n
	c.Ch <- NodeEvent{NodeJoin, &node}
}

func (c *ChannelEventDelegate) NotifyLeave(n *Node) {
	node := *n
	c.Ch <- NodeEvent{NodeLeave, &node}
}

func (c *ChannelEventDelegate) NotifyUpdate(n *Node) {
	node := *n
	c.Ch <- NodeEvent{NodeUpdate, &node}
}
