package membership

import "sync"

type pChange struct {
	Change
	p int
}

type disseminator struct {
	node    *Node
	changes map[string]*pChange

	maxP    int
	pFactor int

	sync.RWMutex
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
