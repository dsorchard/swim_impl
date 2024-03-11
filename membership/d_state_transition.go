package membership

import (
	"sync"
	"time"
)

type transitionTimer struct {
	*time.Timer
	state string
}

type stateTransitions struct {
	sync.Mutex

	node    *Node
	timers  map[string]*transitionTimer
	enabled bool
}

// Cancel cancels the scheduled transition for the change.
func (s *stateTransitions) Cancel(change Change) {
	s.Lock()

	if timer, ok := s.timers[change.Address]; ok {
		timer.Stop()
		delete(s.timers, change.Address)
	}

	s.Unlock()
}
