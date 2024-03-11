package membership

import (
	"github.com/charmbracelet/log"
	"net/rpc"
	"os"
	"sync"
)

var logger = log.NewWithOptions(os.Stderr, log.Options{Prefix: "memberlist"})

type memberlist struct {
	node  *Node
	local *Member

	members struct {
		list       []*Member
		byAddress  map[string]*Member
		rpcClients map[string]*rpc.Client
		sync.RWMutex
	}

	sync.Mutex
}

func newMemberlist(n *Node) *memberlist {
	m := &memberlist{
		node: n,
	}

	m.members.byAddress = make(map[string]*Member)
	m.members.rpcClients = make(map[string]*rpc.Client)

	return m
}

// Member returns the member at a specific address.
func (m *memberlist) Member(address string) (*Member, bool) {
	m.members.RLock()
	member, ok := m.members.byAddress[address]
	m.members.RUnlock()

	return member, ok
}

// Members returns all the members in the memberlist.
func (m *memberlist) Members() (members []Member) {
	m.members.RLock()
	for _, member := range m.members.byAddress {
		members = append(members, *member)
	}
	m.members.RUnlock()

	return
}

func (m *memberlist) MemberClient(address string) (*rpc.Client, error) {
	m.members.Lock()
	client, ok := m.members.rpcClients[address]
	m.members.Unlock()

	if ok {
		return client, nil
	}

	logger.Debugf("Dialing to RPC server: %s", address)
	client, err := rpc.Dial("tcp", address)
	if err == nil {
		logger.Debugf("RPC connection established: %s", address)
		m.members.Lock()
		m.members.rpcClients[address] = client
		m.members.Unlock()
	} else {
		logger.Debugf("Cannot connect to RPC server: %s", address)
	}

	return client, err
}

// Pingable returns whether or not a member is pingable.
func (m *memberlist) Pingable(member Member) bool {
	return member.Address != m.local.Address && member.isReachable()
}

// CloseMemberClient removes the client instance of the member at a specific address.
func (m *memberlist) CloseMemberClient(address string) {
	m.members.Lock()
	delete(m.members.rpcClients, address)
	m.members.Unlock()
}

func (n *Node) handleChanges(changes []Change) {
	for _, change := range changes {
		n.disseminator.RecordChange(change)

		switch change.Status {
		case Alive:
			n.stateTransitions.Cancel(change)
		case Suspect:
			n.stateTransitions.ScheduleSuspectToFaulty(change)
		}
	}
}

// NumMembers returns the number of members in the memberlist.
func (m *memberlist) NumMembers() int {
	m.members.RLock()
	n := len(m.members.list)
	m.members.RUnlock()

	return n
}

// MemberAt returns the i-th member in the list.
func (m *memberlist) MemberAt(i int) *Member {
	m.members.RLock()
	member := m.members.list[i]
	m.members.RUnlock()

	return member
}
