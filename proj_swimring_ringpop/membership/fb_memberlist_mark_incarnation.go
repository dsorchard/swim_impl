package membership

import (
	"math/rand"
	"time"
)

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

//-----------------------------Update----------------------------

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
		m.node.changeHandler.HandleChanges(applied)
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
