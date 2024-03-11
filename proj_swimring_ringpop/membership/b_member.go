package membership

import (
	"math/rand"
	"sync"
)

type Member struct {
	sync.RWMutex
	Address     string
	Status      string
	Incarnation int64
}

func (m *Member) isReachable() bool {
	return m.Status == Alive || m.Status == Suspect
}

func shuffle(members []*Member) []*Member {
	newMembers := make([]*Member, len(members), cap(members))
	newIndexes := rand.Perm(len(members))

	for o, n := range newIndexes {
		newMembers[n] = members[o]
	}

	return newMembers
}

// ----------------change---------------------
const (
	Alive   = "alive"
	Suspect = "suspect"
	Faulty  = "faulty"
)

func statePrecedence(s string) int {
	switch s {
	case Alive:
		return 0
	case Suspect:
		return 1
	case Faulty:
		return 2
	default:
		return -1
	}
}

type Change struct {
	Source            string
	SourceIncarnation int64
	Address           string
	Incarnation       int64
	Status            string
}

type changeHandler interface {
	HandleChanges(changes []Change)
}
