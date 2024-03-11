package memberlist

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"net"
	"sync/atomic"
	"time"
)

// streamListen is a long running goroutine that pulls incoming streams from the
// transport and hands them off for processing.
func (m *Memberlist) streamListen() {
	for {
		select {
		case conn := <-m.transport.StreamCh():
			go m.handleConn(conn)

		case <-m.shutdownCh:
			return
		}
	}
}

// handleConn handles a single incoming stream connection from the transport.
func (m *Memberlist) handleConn(conn net.Conn) {
	defer conn.Close()
	m.logger.Printf("[DEBUG] memberlist: Stream connection %s", LogConn(conn))

	conn.SetDeadline(time.Now().Add(m.config.TCPTimeout))

	var (
		streamLabel string
		err         error
	)
	conn, streamLabel, err = RemoveLabelHeaderFromStream(conn)
	if err != nil {
		m.logger.Printf("[ERR] memberlist: failed to receive and remove the stream label header: %s %s", err, LogConn(conn))
		return
	}

	if m.config.SkipInboundLabelCheck {
		if streamLabel != "" {
			m.logger.Printf("[ERR] memberlist: unexpected double stream label header: %s", LogConn(conn))
			return
		}
		// Set this from config so that the auth data assertions work below.
		streamLabel = m.config.Label
	}

	if m.config.Label != streamLabel {
		m.logger.Printf("[ERR] memberlist: discarding stream with unacceptable label %q: %s", streamLabel, LogConn(conn))
		return
	}

	msgType, bufConn, dec, err := m.readStream(conn, streamLabel)
	if err != nil {
		if err != io.EOF {
			m.logger.Printf("[ERR] memberlist: failed to receive: %s %s", err, LogConn(conn))

			resp := errResp{err.Error()}
			out, err := encode(errMsg, &resp)
			if err != nil {
				m.logger.Printf("[ERR] memberlist: Failed to encode error response: %s", err)
				return
			}

			err = m.rawSendMsgStream(conn, out.Bytes(), streamLabel)
			if err != nil {
				m.logger.Printf("[ERR] memberlist: Failed to send error: %s %s", err, LogConn(conn))
				return
			}
		}
		return
	}

	switch msgType {
	case userMsg:
		if err := m.readUserMsg(bufConn, dec); err != nil {
			m.logger.Printf("[ERR] memberlist: Failed to receive user message: %s %s", err, LogConn(conn))
		}
	case pushPullMsg:
		// Increment counter of pending push/pulls
		numConcurrent := atomic.AddUint32(&m.pushPullReq, 1)
		defer atomic.AddUint32(&m.pushPullReq, ^uint32(0))

		// Check if we have too many open push/pull requests
		if numConcurrent >= maxPushPullRequests {
			m.logger.Printf("[ERR] memberlist: Too many pending push/pull requests")
			return
		}

		join, remoteNodes, userState, err := m.readRemoteState(bufConn, dec)
		if err != nil {
			m.logger.Printf("[ERR] memberlist: Failed to read remote state: %s %s", err, LogConn(conn))
			return
		}

		if err := m.sendLocalState(conn, join, streamLabel); err != nil {
			m.logger.Printf("[ERR] memberlist: Failed to push local state: %s %s", err, LogConn(conn))
			return
		}

		if err := m.mergeRemoteState(join, remoteNodes, userState); err != nil {
			m.logger.Printf("[ERR] memberlist: Failed push/pull merge: %s %s", err, LogConn(conn))
			return
		}
	case pingMsg:
		var p ping
		if err := dec.Decode(&p); err != nil {
			m.logger.Printf("[ERR] memberlist: Failed to decode ping: %s %s", err, LogConn(conn))
			return
		}

		if p.Node != "" && p.Node != m.config.Name {
			m.logger.Printf("[WARN] memberlist: Got ping for unexpected node %s %s", p.Node, LogConn(conn))
			return
		}

		ack := ackResp{p.SeqNo, nil}
		out, err := encode(ackRespMsg, &ack)
		if err != nil {
			m.logger.Printf("[ERR] memberlist: Failed to encode ack: %s", err)
			return
		}

		err = m.rawSendMsgStream(conn, out.Bytes(), streamLabel)
		if err != nil {
			m.logger.Printf("[ERR] memberlist: Failed to send ack: %s %s", err, LogConn(conn))
			return
		}
	default:
		m.logger.Printf("[ERR] memberlist: Received invalid msgType (%d) %s", msgType, LogConn(conn))
	}
}

// packetListen is a long running goroutine that pulls packets out of the
// transport and hands them off for processing.
func (m *Memberlist) packetListen() {
	for {
		select {
		case packet := <-m.transport.PacketCh():
			m.ingestPacket(packet.Buf, packet.From, packet.Timestamp)

		case <-m.shutdownCh:
			return
		}
	}
}

func (m *Memberlist) ingestPacket(buf []byte, from net.Addr, timestamp time.Time) {
	var (
		packetLabel string
		err         error
	)
	buf, packetLabel, err = RemoveLabelHeaderFromPacket(buf)
	if err != nil {
		m.logger.Printf("[ERR] memberlist: %v %s", err, LogAddress(from))
		return
	}

	if m.config.SkipInboundLabelCheck {
		if packetLabel != "" {
			m.logger.Printf("[ERR] memberlist: unexpected double packet label header: %s", LogAddress(from))
			return
		}
		// Set this from config so that the auth data assertions work below.
		packetLabel = m.config.Label
	}

	if m.config.Label != packetLabel {
		m.logger.Printf("[ERR] memberlist: discarding packet with unacceptable label %q: %s", packetLabel, LogAddress(from))
		return
	}

	// Check if encryption is enabled
	if m.config.EncryptionEnabled() {
		// Decrypt the payload
		authData := []byte(packetLabel)
		plain, err := decryptPayload(m.config.Keyring.GetKeys(), buf, authData)
		if err != nil {
			if !m.config.GossipVerifyIncoming {
				// Treat the message as plaintext
				plain = buf
			} else {
				m.logger.Printf("[ERR] memberlist: Decrypt packet failed: %v %s", err, LogAddress(from))
				return
			}
		}

		// Continue processing the plaintext buffer
		buf = plain
	}

	// See if there's a checksum included to verify the contents of the message
	if len(buf) >= 5 && messageType(buf[0]) == hasCrcMsg {
		crc := crc32.ChecksumIEEE(buf[5:])
		expected := binary.BigEndian.Uint32(buf[1:5])
		if crc != expected {
			m.logger.Printf("[WARN] memberlist: Got invalid checksum for UDP packet: %x, %x", crc, expected)
			return
		}
		m.handleCommand(buf[5:], from, timestamp)
	} else {
		m.handleCommand(buf, from, timestamp)
	}
}

// packetHandler is a long running goroutine that processes messages received
// over the packet interface, but is decoupled from the listener to avoid
// blocking the listener which may cause ping/ack messages to be delayed.
func (m *Memberlist) packetHandler() {
	for {
		select {
		case <-m.handoffCh:
			for {
				msg, ok := m.getNextMessage()
				if !ok {
					break
				}
				msgType := msg.msgType
				buf := msg.buf
				from := msg.from

				switch msgType {
				case suspectMsg:
					m.handleSuspect(buf, from)
				case aliveMsg:
					m.handleAlive(buf, from)
				case deadMsg:
					m.handleDead(buf, from)
				case userMsg:
					m.handleUser(buf, from)
				default:
					m.logger.Printf("[ERR] memberlist: Message type (%d) not supported %s (packet handler)", msgType, LogAddress(from))
				}
			}

		case <-m.shutdownCh:
			return
		}
	}
}
