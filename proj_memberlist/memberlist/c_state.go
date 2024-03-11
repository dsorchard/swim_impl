package memberlist

import (
	"fmt"
	"net"
	"time"
)

type NodeStateType int

func (t NodeStateType) metricsString() string {
	switch t {
	case StateAlive:
		return "alive"
	case StateDead:
		return "dead"
	case StateSuspect:
		return "suspect"
	case StateLeft:
		return "left"
	default:
		return fmt.Sprintf("unhandled-value-%d", t)
	}
}

const (
	StateAlive NodeStateType = iota
	StateSuspect
	StateDead
	StateLeft
)

// Node represents a node in the cluster.
type Node struct {
	Name  string
	Addr  net.IP
	Port  uint16
	Meta  []byte        // Metadata from the delegate for this node.
	State NodeStateType // State of the node.
	PMin  uint8         // Minimum protocol version this understands
	PMax  uint8         // Maximum protocol version this understands
	PCur  uint8         // Current version node is speaking
	DMin  uint8         // Min protocol version for the delegate to understand
	DMax  uint8         // Max protocol version for the delegate to understand
	DCur  uint8         // Current version delegate is speaking
}

// Schedule is used to ensure the Tick is performed periodically. This
// function is safe to call multiple times. If the memberlist is already
// scheduled, then it won't do anything.
func (m *Memberlist) schedule() {
	m.tickerLock.Lock()
	defer m.tickerLock.Unlock()

	// If we already have tickers, then don't do anything, since we're
	// scheduled
	if len(m.tickers) > 0 {
		return
	}

	// Create the stop tick channel, a blocking channel. We close this
	// when we should stop the tickers.
	stopCh := make(chan struct{})

	// Create a new probeTicker
	if m.config.ProbeInterval > 0 {
		t := time.NewTicker(m.config.ProbeInterval)
		go m.triggerFunc(m.config.ProbeInterval, t.C, stopCh, m.probe)
		m.tickers = append(m.tickers, t)
	}

	// Create a push pull ticker if needed
	if m.config.PushPullInterval > 0 {
		go m.pushPullTrigger(stopCh)
	}

	// Create a gossip ticker if needed
	if m.config.GossipInterval > 0 && m.config.GossipNodes > 0 {
		t := time.NewTicker(m.config.GossipInterval)
		go m.triggerFunc(m.config.GossipInterval, t.C, stopCh, m.gossip)
		m.tickers = append(m.tickers, t)
	}

	// If we made any tickers, then record the stopTick channel for
	// later.
	if len(m.tickers) > 0 {
		m.stopTick = stopCh
	}
}
