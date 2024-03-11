package membership

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
