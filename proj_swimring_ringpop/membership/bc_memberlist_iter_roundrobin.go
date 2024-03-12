package membership

import "math/rand"

// Shuffle shuffles the memberlist.
func (m *memberlist) Shuffle() {
	m.members.Lock()
	m.members.list = shuffle(m.members.list)
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

func shuffle(members []*Member) []*Member {
	newMembers := make([]*Member, len(members), cap(members))
	newIndexes := rand.Perm(len(members))

	for o, n := range newIndexes {
		newMembers[n] = members[o]
	}

	return newMembers
}

type memberlistIter struct {
	m            *memberlist
	currentIndex int
	currentRound int
}

func newMemberlistIter(m *memberlist) *memberlistIter {
	iter := &memberlistIter{
		m:            m,
		currentIndex: -1,
		currentRound: 0,
	}

	iter.m.Shuffle()

	return iter
}

// Next returns the next pingable member in the member list, if it
// visits all members but none are pingable returns nil, false.
func (i *memberlistIter) Next() (*Member, bool) {
	startIndex := i.currentIndex
	for {
		member := i.m.MemberAt(i.currentIndex)
		if i.m.Pingable(*member) {
			return member, true
		}

		// round-robin through the member list
		i.currentIndex = (i.currentIndex + 1) % i.m.NumMembers()
		if i.currentIndex == startIndex {
			return nil, false
		}

		if i.currentIndex == 0 {
			i.m.Shuffle()
		}
	}
}
