package memberlist

import (
	"container/list"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Memberlist struct {
	sequenceNum uint32 // Local sequence number
	incarnation uint32 // Local incarnation number
	numNodes    uint32 // Number of known nodes (estimate)
	pushPullReq uint32 // Number of push/pull requests

	advertiseLock sync.RWMutex
	advertiseAddr net.IP
	advertisePort uint16

	config         *Config
	shutdown       int32 // Used as an atomic boolean value
	shutdownCh     chan struct{}
	leave          int32 // Used as an atomic boolean value
	leaveBroadcast chan struct{}

	shutdownLock sync.Mutex // Serializes calls to Shutdown
	leaveLock    sync.Mutex // Serializes calls to Leave

	transport NodeAwareTransport

	handoffCh            chan struct{}
	highPriorityMsgQueue *list.List
	lowPriorityMsgQueue  *list.List
	msgQueueLock         sync.Mutex

	nodeLock   sync.RWMutex
	nodes      []*nodeState          // Known nodes
	nodeMap    map[string]*nodeState // Maps Node.Name -> NodeState
	nodeTimers map[string]*suspicion // Maps Node.Name -> suspicion timer
	awareness  *awareness

	tickerLock sync.Mutex
	tickers    []*time.Ticker
	stopTick   chan struct{}
	probeIndex int

	ackLock     sync.Mutex
	ackHandlers map[uint32]*ackHandler

	broadcasts *TransmitLimitedQueue

	logger *log.Logger

	// metricLabels is the slice of labels to put on all emitted metrics
	metricLabels []metrics.Label
}

// Create will create a new Memberlist using the given configuration.
// This will not connect to any other node (see Join) yet, but will start
// all the listeners to allow other nodes to join this memberlist.
// After creating a Memberlist, the configuration given should not be
// modified by the user anymore.
func Create(conf *Config) (*Memberlist, error) {
	m, err := newMemberlist(conf)
	if err != nil {
		return nil, err
	}
	if err := m.setAlive(); err != nil {
		m.Shutdown()
		return nil, err
	}
	m.schedule()
	return m, nil
}

// newMemberlist creates the network listeners.
// Does not schedule execution of background maintenance.
func newMemberlist(conf *Config) (*Memberlist, error) {
	logDest := conf.LogOutput
	if logDest == nil {
		logDest = os.Stderr
	}

	logger := conf.Logger
	if logger == nil {
		logger = log.New(logDest, "", log.LstdFlags)
	}

	// Set up a network transport by default if a custom one wasn't given
	// by the config.
	transport := conf.Transport
	if transport == nil {
		nc := &NetTransportConfig{
			BindAddrs:    []string{conf.BindAddr},
			BindPort:     conf.BindPort,
			Logger:       logger,
			MetricLabels: conf.MetricLabels,
		}

		// See comment below for details about the retry in here.
		makeNetRetry := func(limit int) (*NetTransport, error) {
			var err error
			for try := 0; try < limit; try++ {
				var nt *NetTransport
				if nt, err = NewNetTransport(nc); err == nil {
					return nt, nil
				}
				if strings.Contains(err.Error(), "address already in use") {
					logger.Printf("[DEBUG] memberlist: Got bind error: %v", err)
					continue
				}
			}

			return nil, fmt.Errorf("failed to obtain an address: %v", err)
		}

		// The dynamic bind port operation is inherently racy because
		// even though we are using the kernel to find a port for us, we
		// are attempting to bind multiple protocols (and potentially
		// multiple addresses) with the same port number. We build in a
		// few retries here since this often gets transient errors in
		// busy unit tests.
		limit := 1
		if conf.BindPort == 0 {
			limit = 10
		}

		nt, err := makeNetRetry(limit)
		if err != nil {
			return nil, fmt.Errorf("Could not set up network transport: %v", err)
		}
		if conf.BindPort == 0 {
			port := nt.GetAutoBindPort()
			conf.BindPort = port
			conf.AdvertisePort = port
			logger.Printf("[DEBUG] memberlist: Using dynamic bind port %d", port)
		}
		transport = nt
	}

	nodeAwareTransport, _ := transport.(NodeAwareTransport)

	if conf.Label != "" {
		nodeAwareTransport = &labelWrappedTransport{
			label:              conf.Label,
			NodeAwareTransport: nodeAwareTransport,
		}
	}

	m := &Memberlist{
		config:               conf,
		shutdownCh:           make(chan struct{}),
		leaveBroadcast:       make(chan struct{}, 1),
		transport:            nodeAwareTransport,
		handoffCh:            make(chan struct{}, 1),
		highPriorityMsgQueue: list.New(),
		lowPriorityMsgQueue:  list.New(),
		nodeMap:              make(map[string]*nodeState),
		nodeTimers:           make(map[string]*suspicion),
		awareness:            newAwareness(conf.AwarenessMaxMultiplier, conf.MetricLabels),
		ackHandlers:          make(map[uint32]*ackHandler),
		broadcasts:           &TransmitLimitedQueue{RetransmitMult: conf.RetransmitMult},
		logger:               logger,
		metricLabels:         conf.MetricLabels,
	}
	m.broadcasts.NumNodes = func() int {
		return m.estNumNodes()
	}

	// Get the final advertise address from the transport, which may need
	// to see which address we bound to. We'll refresh this each time we
	// send out an alive message.
	if _, _, err := m.refreshAdvertise(); err != nil {
		return nil, err
	}

	go m.streamListen()
	go m.packetListen()
	go m.packetHandler()
	go m.checkBroadcastQueueDepth()
	return m, nil
}

// Join is used to take an existing Memberlist and attempt to join a cluster
// by contacting all the given hosts and performing a state sync. Initially,
// the Memberlist only contains our own state, so doing this will cause
// remote nodes to become aware of the existence of this node, effectively
// joining the cluster.
//
// This returns the number of hosts successfully contacted and an error if
// none could be reached. If an error is returned, the node did not successfully
// join the cluster.
func (m *Memberlist) Join(existing []string) (int, error) {
	numSuccess := 0
	var errs error
	for _, exist := range existing {
		addrs, err := m.resolveAddr(exist)
		if err != nil {
			err = fmt.Errorf("Failed to resolve %s: %v", exist, err)
			errs = multierror.Append(errs, err)
			m.logger.Printf("[WARN] memberlist: %v", err)
			continue
		}

		for _, addr := range addrs {
			hp := joinHostPort(addr.ip.String(), addr.port)
			a := Address{Addr: hp, Name: addr.nodeName}
			if err := m.pushPullNode(a, true); err != nil {
				err = fmt.Errorf("Failed to join %s: %v", a.Addr, err)
				errs = multierror.Append(errs, err)
				m.logger.Printf("[DEBUG] memberlist: %v", err)
				continue
			}
			numSuccess++
		}

	}
	if numSuccess > 0 {
		errs = nil
	}
	return numSuccess, errs
}

func (m *Memberlist) refreshAdvertise() (net.IP, int, error) {
	addr, port, err := m.transport.FinalAdvertiseAddr(
		m.config.AdvertiseAddr, m.config.AdvertisePort)
	if err != nil {
		return nil, 0, fmt.Errorf("Failed to get final advertise address: %v", err)
	}
	m.setAdvertise(addr, port)
	return addr, port, nil
}

// checkBroadcastQueueDepth periodically checks the size of the broadcast queue
// to see if it is too large
func (m *Memberlist) checkBroadcastQueueDepth() {
	for {
		select {
		case <-time.After(m.config.QueueCheckInterval):
			numq := m.broadcasts.NumQueued()
			metrics.AddSampleWithLabels([]string{"memberlist", "queue", "broadcasts"}, float32(numq), m.metricLabels)
		case <-m.shutdownCh:
			return
		}
	}
}

// setAlive is used to mark this node as being alive. This is the same
// as if we received an alive notification our own network channel for
// ourself.
func (m *Memberlist) setAlive() error {
	// Get the final advertise address from the transport, which may need
	// to see which address we bound to.
	addr, port, err := m.refreshAdvertise()
	if err != nil {
		return err
	}

	// Check if this is a public address without encryption
	ipAddr, err := sockaddr.NewIPAddr(addr.String())
	if err != nil {
		return fmt.Errorf("Failed to parse interface addresses: %v", err)
	}
	ifAddrs := []sockaddr.IfAddr{
		sockaddr.IfAddr{
			SockAddr: ipAddr,
		},
	}
	_, publicIfs, err := sockaddr.IfByRFC("6890", ifAddrs)
	if len(publicIfs) > 0 && !m.config.EncryptionEnabled() {
		m.logger.Printf("[WARN] memberlist: Binding to public address without encryption!")
	}

	// Set any metadata from the delegate.
	var meta []byte
	if m.config.Delegate != nil {
		meta = m.config.Delegate.NodeMeta(MetaMaxSize)
		if len(meta) > MetaMaxSize {
			panic("Node meta data provided is longer than the limit")
		}
	}

	a := alive{
		Incarnation: m.nextIncarnation(),
		Node:        m.config.Name,
		Addr:        addr,
		Port:        uint16(port),
		Meta:        meta,
		Vsn:         m.config.BuildVsnArray(),
	}
	m.aliveNode(&a, nil, true)

	return nil
}

// Shutdown will stop any background maintenance of network activity
// for this memberlist, causing it to appear "dead". A leave message
// will not be broadcasted prior, so the cluster being left will have
// to detect this node's shutdown using probing. If you wish to more
// gracefully exit the cluster, call Leave prior to shutting down.
//
// This method is safe to call multiple times.
func (m *Memberlist) Shutdown() error {
	m.shutdownLock.Lock()
	defer m.shutdownLock.Unlock()

	if m.hasShutdown() {
		return nil
	}

	// Shut down the transport first, which should block until it's
	// completely torn down. If we kill the memberlist-side handlers
	// those I/O handlers might get stuck.
	if err := m.transport.Shutdown(); err != nil {
		m.logger.Printf("[ERR] Failed to shutdown transport: %v", err)
	}

	// Now tear down everything else.
	atomic.StoreInt32(&m.shutdown, 1)
	close(m.shutdownCh)
	m.deschedule()
	return nil
}
