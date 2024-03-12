package membership

import "sync"

const defaultPFactor int = 15

// pChange is a Change with a piggyback counter.
type pChange struct {
	Change
	p int
}

type Disseminator interface {
	RecordChange(change Change)
	ClearChange(address string)

	// IssueAsSender Prepares a list of changes to be sent out during a gossip round.
	// It also returns a callback function to increment the piggyback counters of the
	// disseminated changes, indicating they have been sent once.
	IssueAsSender() (changes []Change, bumpPiggybackCounters func())

	// IssueAsReceiver Similar to IssueAsSender, but it automatically increments the piggyback counters
	// because it's assumed that the changes are being sent in response to a received message, and
	// there's no clear acknowledgment mechanism to trigger the increment.
	IssueAsReceiver(senderAddress string, senderIncarnation int64, senderChecksum uint32) (changes []Change)
	MembershipAsChanges() (changes []Change)
}

type disseminator struct {
	node    *Node
	changes map[string]*pChange

	maxP    int
	pFactor int

	sync.RWMutex
}

func newDisseminator(n *Node) *disseminator {
	d := &disseminator{
		node:    n,
		changes: make(map[string]*pChange),
		maxP:    defaultPFactor,
		pFactor: defaultPFactor,
	}

	return d
}

// RecordChange stores the Change into the disseminator.
func (d *disseminator) RecordChange(change Change) {
	d.Lock()
	d.changes[change.Address] = &pChange{change, 0}
	d.Unlock()
}

// ClearChange removes the Change record of specific address.
func (d *disseminator) ClearChange(address string) {
	d.Lock()
	delete(d.changes, address)
	d.Unlock()
}

// IssueAsSender collects all changes a node needs when sending a ping or
// ping-req. The second return value is a callback that raises the piggyback
// counters of the given changes.
func (d *disseminator) IssueAsSender() (changes []Change, bumpPiggybackCounters func()) {
	changes = d.issueChanges()
	return changes, func() {
		d.bumpPiggybackCounters(changes)
	}
}

func (d *disseminator) issueChanges() []Change {
	d.Lock()
	result := []Change{}
	for _, change := range d.changes {
		result = append(result, change.Change)
	}
	d.Unlock()
	return result
}

func (d *disseminator) bumpPiggybackCounters(changes []Change) {
	d.Lock()
	for _, change := range changes {
		c, ok := d.changes[change.Address]
		if !ok {
			continue
		}

		c.p++
		if c.p >= d.maxP {
			delete(d.changes, c.Address)
		}
	}
	d.Unlock()
}

// IssueAsReceiver collects all changes a node needs when responding to a ping
// or ping-req. Unlike IssueAsSender, IssueAsReceiver automatically increments
// the piggyback counters because it's difficult to find out whether a response
// reaches the client. The second return value indicates whether a full sync
// is triggered.
func (d *disseminator) IssueAsReceiver(senderAddress string, senderIncarnation int64, senderChecksum uint32) (changes []Change) {
	changes = d.filterChangesFromSender(d.issueChanges(), senderAddress, senderIncarnation)

	d.bumpPiggybackCounters(changes)
	// d.node.memberlist.Checksum() == senderChecksum
	if len(changes) > 0 {
		return changes
	}

	return d.MembershipAsChanges()
}

func (d *disseminator) filterChangesFromSender(cs []Change, source string, incarnation int64) []Change {
	var filtered []Change
	for _, change := range cs {
		if change.SourceIncarnation != incarnation || change.Source != source {
			filtered = append(filtered, change)
		}
	}
	return filtered
}

// MembershipAsChanges returns a Change array containing all the members
// in memberlist of Node.
func (d *disseminator) MembershipAsChanges() []Change {
	d.Lock()
	defer d.Unlock() // Ensures the lock is released in case of a panic

	members := d.node.memberlist.Members()
	changes := make([]Change, len(members))
	for i, member := range members {
		changes[i] = Change{
			Address:           member.Address,
			Incarnation:       member.Incarnation,
			Source:            d.node.Address(),
			SourceIncarnation: d.node.Incarnation(),
			Status:            member.Status,
		}
	}

	return changes
}
