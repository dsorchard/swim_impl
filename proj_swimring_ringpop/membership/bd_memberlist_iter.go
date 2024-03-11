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
	numOfMembers := i.m.NumMembers()
	visited := make(map[string]bool)

	for len(visited) < numOfMembers {
		i.currentIndex++

		if i.currentIndex >= i.m.NumMembers() {
			i.currentIndex = 0
			i.currentRound++
			i.m.Shuffle()
		}

		member := i.m.MemberAt(i.currentIndex)
		visited[member.Address] = true

		if i.m.Pingable(*member) {
			return member, true
		}
	}

	return nil, false
}

//ok
