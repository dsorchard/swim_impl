package membership

import (
	"github.com/charmbracelet/log"
	"math/rand"
	"net/rpc"
	"os"
	"sync"
	"time"
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

// AddJoinList adds the list to the membership with the Update function.
// However, as a side effect, Update adds changes to the disseminator as well.
// Since we don't want to disseminate the potentially very large join lists,
// we clear all the changes from the disseminator, except for the one change
// that refers to the make-alive of this node.
func (m *memberlist) AddJoinList(list []Change) {
	applied := m.Update(list)
	for _, member := range applied {
		if member.Address == m.node.Address() {
			continue
		}
		m.node.disseminator.ClearChange(member.Address)
	}
}

//-----------------------------Mark--------------------------------------

// Reincarnate sets the status of the node to Alive and updates the incarnation
// number. It adds the change to the disseminator as well.
func (m *memberlist) Reincarnate() []Change {
	return m.MarkAlive(m.node.Address(), time.Now().Unix())
}

// MarkAlive sets the status of the node at specific address to Alive and
// updates the incarnation number. It adds the change to the disseminator as well.
func (m *memberlist) MarkAlive(address string, incarnation int64) []Change {
	return m.MakeChange(address, incarnation, Alive)
}

// MarkSuspect sets the status of the node at specific address to Suspect and
// updates the incarnation number. It adds the change to the disseminator as well.
func (m *memberlist) MarkSuspect(address string, incarnation int64) []Change {
	return m.MakeChange(address, incarnation, Suspect)
}

// MarkFaulty sets the status of the node at specific address to Faulty and
// updates the incarnation number. It adds the change to the disseminator as well.
func (m *memberlist) MarkFaulty(address string, incarnation int64) []Change {
	return m.MakeChange(address, incarnation, Faulty)
}

// MakeChange makes a change to the memberlist.
func (m *memberlist) MakeChange(address string, incarnation int64, status string) []Change {
	if m.local == nil {
		m.local = &Member{
			Address:     m.node.Address(),
			Incarnation: 0,
			Status:      Alive,
		}
	}

	changes := m.Update([]Change{
		{
			Source:            m.local.Address,
			SourceIncarnation: m.local.Incarnation,
			Address:           address,
			Incarnation:       incarnation,
			Status:            status,
		},
	})

	return changes
}

// Update updates the memberlist with the slice of changes, applying selectively.
func (m *memberlist) Update(changes []Change) (applied []Change) {
	if len(changes) == 0 {
		return nil
	}

	m.Lock()
	m.members.Lock()

	for _, change := range changes {
		member, ok := m.members.byAddress[change.Address]

		if !ok {
			if m.applyChange(change) {
				applied = append(applied, change)
			}
			continue
		}

		if member.localOverride(m.node.Address(), change) {
			overrideChange := Change{
				Source:            change.Source,
				SourceIncarnation: change.SourceIncarnation,
				Address:           change.Address,
				Incarnation:       time.Now().Unix(),
				Status:            Alive,
			}

			if m.applyChange(overrideChange) {
				applied = append(applied, overrideChange)
			}

			continue
		}

		if member.nonLocalOverride(change) {
			if m.applyChange(change) {
				applied = append(applied, change)
			}
		}
	}

	m.members.Unlock()

	if len(applied) > 0 {
		m.node.handleChanges(applied)
		m.node.swimring.HandleChanges(applied)
	}

	m.Unlock()
	return applied
}

func (m *Member) localOverride(local string, change Change) bool {
	if m.Address != local {
		return false
	}
	return change.Status == Faulty || change.Status == Suspect
}

func (m *Member) nonLocalOverride(change Change) bool {
	if change.Incarnation > m.Incarnation {
		return true
	}

	if change.Incarnation < m.Incarnation {
		return false
	}

	return statePrecedence(change.Status) > statePrecedence(m.Status)
}

func (m *memberlist) applyChange(change Change) bool {
	member, ok := m.members.byAddress[change.Address]

	if !ok {
		member = &Member{
			Address:     change.Address,
			Status:      change.Status,
			Incarnation: change.Incarnation,
		}

		if member.Address == m.node.Address() {
			m.local = member
		}

		m.members.byAddress[change.Address] = member
		i := m.getJoinPosition()
		newList := make([]*Member, 0, len(m.members.list)+1)
		newList = append(newList, m.members.list[:i]...)
		newList = append(newList, member)
		newList = append(newList, m.members.list[i:]...)
		m.members.list = newList

		logger.Infof("Server %s added to memberlist", member.Address)
	}

	member.Lock()
	member.Status = change.Status
	member.Incarnation = change.Incarnation
	member.Unlock()

	logger.Infof("%s is marked as %s node", member.Address, change.Status)

	return true
}

func (m *memberlist) getJoinPosition() int {
	l := len(m.members.list)
	if l == 0 {
		return l
	}
	return rand.Intn(l)
}

// Shuffle shuffles the memberlist.
func (m *memberlist) Shuffle() {
	m.members.Lock()
	m.members.list = shuffle(m.members.list)
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

func (s *stateTransitions) ScheduleSuspectToFaulty(change Change) {
	s.Lock()
	s.schedule(change, Suspect, s.node.suspectTimeout, func() {
		logger.Info("Suspect timer expired, mark %s as faulty node", change.Address)
		s.node.memberlist.MarkFaulty(change.Address, change.Incarnation)
	})
	logger.Infof("Suspect timer for %s scheduled", change.Address)
	s.Unlock()
}

func (s *stateTransitions) schedule(change Change, state string, timeout time.Duration, transition func()) {
	if !s.enabled {
		return
	}

	if s.node.Address() == change.Address {
		return
	}

	if timer, ok := s.timers[change.Address]; ok {
		if timer.state == state {
			return
		}
		timer.Stop()
	}

	timer := time.AfterFunc(timeout, func() {
		transition()
	})

	s.timers[change.Address] = &transitionTimer{
		Timer: timer,
		state: state,
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

// RandomPingableMembers returns the number of pingable members in the memberlist.
func (m *memberlist) RandomPingableMembers(n int, excluding map[string]bool) []*Member {
	var members []*Member

	m.members.RLock()
	for _, member := range m.members.list {
		if m.Pingable(*member) && !excluding[member.Address] {
			members = append(members, member)
		}
	}
	m.members.RUnlock()

	members = shuffle(members)

	if n > len(members) {
		return members
	}
	return members[:n]
}
