// Package broker implements the MQTT broker core.
package broker

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/luma/broker/internal/metrics"
	"github.com/luma/broker/internal/protocol"
	"github.com/luma/broker/internal/router"
	"github.com/luma/broker/internal/session"
	"github.com/luma/broker/internal/storage"
	"github.com/luma/broker/pkg/config"
	"go.uber.org/zap"
)

// ─── Client State ─────────────────────────────────────────────────────────────

type connState int32

const (
	stateHandshaking connState = iota
	stateConnected
	stateClosing
	stateClosed
)

// writeMsg is an item in the write queue.
type writeMsg struct {
	data []byte
}

// Client represents a single connected MQTT client.
// It runs two goroutines: readLoop and writeLoop.
type Client struct {
	conn   net.Conn
	br     *bufio.Reader
	cfg    *config.BrokerConfig
	broker *Broker
	logger *zap.Logger

	// Identity (set during CONNECT)
	clientID   string
	username   string
	remoteAddr string
	version    byte

	// State machine
	state atomic.Int32

	// Session reference
	sess *session.Session

	// Write queue — buffered channel acting as a lock-free ring buffer
	writeQ    chan writeMsg
	closeOnce sync.Once
	closeCh   chan struct{}

	// Keep-alive deadline tracking
	keepAlive time.Duration

	// MQTT v5 incoming topic aliases (publisher → broker)
	topicAliasMu sync.RWMutex
	topicAliases map[uint16]string

	// Metrics
	rxPackets atomic.Uint64
	txPackets atomic.Uint64
	rxBytes   atomic.Uint64
	txBytes   atomic.Uint64
}

func newClient(conn net.Conn, b *Broker) *Client {
	c := &Client{
		conn:       conn,
		br:         bufio.NewReaderSize(conn, b.cfg.Broker.ReadBufSize),
		cfg:        &b.cfg.Broker,
		broker:     b,
		logger:     b.logger,
		remoteAddr: conn.RemoteAddr().String(),
		writeQ:     make(chan writeMsg, b.cfg.Broker.WriteQueueSize),
		closeCh:    make(chan struct{}),
	}
	c.state.Store(int32(stateHandshaking))
	return c
}

// Run blocks until the client disconnects. Must be called from a goroutine.
func (c *Client) Run(ctx context.Context) {
	defer c.cleanup()
	go c.writeLoop(ctx)
	c.readLoop(ctx)
}

// ─── Read Loop ────────────────────────────────────────────────────────────────

func (c *Client) readLoop(ctx context.Context) {
	for {
		// Apply keep-alive deadline
		if c.keepAlive > 0 {
			deadline := time.Now().Add(time.Duration(float64(c.keepAlive) * c.cfg.PingGrace))
			c.conn.SetReadDeadline(deadline)
		}

		fh, err := protocol.ReadFixed(c.br)
		if err != nil {
			if !isClosedErr(err) {
				c.logger.Debug("read error", zap.String("client", c.clientID), zap.Error(err))
			}
			return
		}

		// Must receive CONNECT as the very first packet
		if c.state.Load() == int32(stateHandshaking) && fh.Type != protocol.CONNECT {
			c.logger.Warn("expected CONNECT", zap.String("type", fh.Type.String()))
			return
		}

		// Read body
		var body []byte
		if fh.RemainingLength > 0 {
			if fh.RemainingLength > c.cfg.MaxPacketSize {
				c.logger.Warn("oversized packet", zap.Int("size", fh.RemainingLength))
				return
			}
			body = make([]byte, fh.RemainingLength)
			if _, err := io.ReadFull(c.br, body); err != nil {
				c.logger.Debug("body read error", zap.Error(err))
				return
			}
		}

		c.rxPackets.Add(1)
		c.rxBytes.Add(uint64(2 + fh.RemainingLength))

		if err := c.handle(fh, body); err != nil {
			c.logger.Debug("handle error",
				zap.String("client", c.clientID),
				zap.String("packet", fh.Type.String()),
				zap.Error(err))
			return
		}

		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

// ─── Packet Dispatcher ────────────────────────────────────────────────────────

func (c *Client) handle(fh protocol.FixedHeader, body []byte) error {
	// Rate limiter (skip during handshake)
	if c.clientID != "" && !c.broker.rateLimiter.Allow(c.clientID) {
		metrics.DroppedTotal.WithLabelValues("rate_limit").Inc()
		c.logger.Warn("rate limit", zap.String("client", c.clientID))
		c.enqueue([]byte{byte(protocol.DISCONNECT) << 4, 2, protocol.ReasonQuotaExceeded, 0})
		return fmt.Errorf("rate limited")
	}

	switch fh.Type {
	case protocol.CONNECT:
		return c.handleConnect(body)
	case protocol.PUBLISH:
		return c.handlePublish(fh, body)
	case protocol.PUBACK:
		return c.handlePuback(body)
	case protocol.PUBREC:
		return c.handlePubrec(body)
	case protocol.PUBREL:
		return c.handlePubrel(body)
	case protocol.PUBCOMP:
		return c.handlePubcomp(body)
	case protocol.SUBSCRIBE:
		return c.handleSubscribe(body)
	case protocol.UNSUBSCRIBE:
		return c.handleUnsubscribe(body)
	case protocol.PINGREQ:
		c.enqueue(protocol.Pingresp)
		c.sess.Touch()
		return nil
	case protocol.DISCONNECT:
		c.sess.WillSet = false // graceful disconnect — suppress LWT
		return fmt.Errorf("graceful disconnect")
	default:
		return fmt.Errorf("unexpected packet type: %s", fh.Type)
	}
}

// ─── CONNECT ─────────────────────────────────────────────────────────────────

// sendConnackFailure sends a refused CONNACK using MQTT v5 encoding when the peer is v5
// (known from c.version after CONNECT decode, or from peekBody before decode succeeds).
func (c *Client) sendConnackFailure(reasonV311, reasonV5 byte, peekBody []byte) {
	useV5 := c.version == protocol.V50
	if !useV5 && len(peekBody) > 0 {
		if v, ok := protocol.PeekConnectProtocolLevel(peekBody); ok && v == protocol.V50 {
			useV5 = true
		}
	}
	if useV5 {
		c.enqueue(protocol.EncodeConnackV5(false, reasonV5, 0))
		return
	}
	c.enqueue(protocol.EncodeConnack(false, reasonV311))
}

func (c *Client) handleConnect(body []byte) error {
	pkt, err := protocol.DecodeConnect(body)
	if err != nil {
		reasonV5 := protocol.ConnackReasonV5ForDecodeError(err)
		c.sendConnackFailure(protocol.ConnRefusedProtocol, reasonV5, body)
		return err
	}

	c.version = pkt.Version

	// Authentication
	if tc, ok := c.conn.(*tls.Conn); ok {
		if st := tc.ConnectionState(); len(st.PeerCertificates) > 0 {
			if identity, errX509 := c.broker.authn.AuthX509(st.PeerCertificates[0]); errX509 == nil {
				c.username = identity
			} else {
				metrics.AuthFailTotal.WithLabelValues("bad_x509").Inc()
				c.sendConnackFailure(protocol.ConnRefusedBadCredentials, protocol.ConnackReasonV5FromV311(protocol.ConnRefusedBadCredentials), nil)
				return errX509
			}
		}
	}
	if c.username == "" && pkt.HasUsername {
		if err := c.broker.authn.AuthPassword(pkt.Username, pkt.Password); err != nil {
			metrics.AuthFailTotal.WithLabelValues("bad_credentials").Inc()
			c.sendConnackFailure(protocol.ConnRefusedBadCredentials, protocol.ConnackReasonV5FromV311(protocol.ConnRefusedBadCredentials), nil)
			return err
		}
		c.username = pkt.Username
	} else if len(pkt.Password) > 0 {
		claims, err := c.broker.authn.AuthJWT(string(pkt.Password))
		if err != nil {
			metrics.AuthFailTotal.WithLabelValues("bad_token").Inc()
			c.sendConnackFailure(protocol.ConnRefusedBadCredentials, protocol.ConnackReasonV5FromV311(protocol.ConnRefusedBadCredentials), nil)
			return err
		}
		c.username = claims.Username
	}

	// Client ID
	cid := pkt.ClientID
	if cid == "" {
		if !pkt.CleanSession {
			c.sendConnackFailure(protocol.ConnRefusedIDRejected, protocol.ConnackReasonV5ClientIDNotValid, nil)
			return protocol.ErrInvalidClientID
		}
		cid = fmt.Sprintf("auto-%s-%d", c.remoteAddr, time.Now().UnixNano())
	}
	c.clientID = cid

	if err := c.broker.plugins.OnClientConnect(cid, c.username); err != nil {
		c.sendConnackFailure(protocol.ConnRefusedNotAuthorized, protocol.ConnackReasonV5FromV311(protocol.ConnRefusedNotAuthorized), nil)
		return err
	}

	// Takeover any existing connection with same ID
	c.broker.takeover(cid)

	// Session management
	sess, present := c.broker.sessions.GetOrCreate(cid, pkt.CleanSession)
	c.sess = sess

	if pkt.WillFlag {
		sess.WillTopic = pkt.WillTopic
		sess.WillPayload = pkt.WillPayload
		sess.WillQoS = pkt.WillQoS
		sess.WillRetain = pkt.WillRetain
		sess.WillSet = true
	}

	if pkt.KeepAlive > 0 {
		c.keepAlive = time.Duration(pkt.KeepAlive) * time.Second
		sess.SetKeepAlive(pkt.KeepAlive)
	}

	sess.SetConnected(true)
	c.state.Store(int32(stateConnected))

	// Register in broker + storage (async for Redis/PG)
	c.broker.register(c)
	go func() {
		ctx := context.Background()
		c.broker.redis.SetClient(ctx, &storage.ClientInfo{
			ClientID:    cid,
			Username:    c.username,
			BrokerID:    c.broker.id,
			RemoteAddr:  c.remoteAddr,
			ConnectedAt: time.Now(),
			Protocol:    c.version,
		})
		c.broker.pg.SetDeviceStatus(ctx, cid, true)
	}()

	metrics.ConnectTotal.Inc()
	metrics.ActiveConnections.Inc()
	metrics.ActiveSessions.Inc()

	c.logger.Info("client connected",
		zap.String("client", cid),
		zap.String("addr", c.remoteAddr),
		zap.Bool("clean", pkt.CleanSession),
		zap.Bool("present", present),
	)

	if c.version == protocol.V50 {
		c.enqueue(protocol.EncodeConnackV5(present, protocol.ConnAccepted, uint16(c.broker.cfg.Broker.TopicAliasMax)))
	} else {
		c.enqueue(protocol.EncodeConnack(present, protocol.ConnAccepted))
	}

	// Restore persistent session subscriptions
	if present && !pkt.CleanSession {
		go c.restoreSession()
	}

	// Deliver queued offline messages
	go c.drainOfflineQueue()

	return nil
}

func (c *Client) resolvePublishTopic(pkt *protocol.PublishPacket) (string, error) {
	if c.version != protocol.V50 {
		return pkt.Topic, nil
	}
	topic := pkt.Topic
	max := uint16(c.broker.cfg.Broker.TopicAliasMax)
	if pkt.TopicAlias > 0 && max > 0 && pkt.TopicAlias > max {
		return "", protocol.ErrProtocolViolation
	}
	if max == 0 && pkt.TopicAlias > 0 {
		return "", protocol.ErrProtocolViolation
	}
	if topic == "" {
		c.topicAliasMu.RLock()
		prev, ok := c.topicAliases[pkt.TopicAlias]
		c.topicAliasMu.RUnlock()
		if !ok {
			return "", protocol.ErrProtocolViolation
		}
		return prev, nil
	}
	if pkt.TopicAlias > 0 && max > 0 {
		c.topicAliasMu.Lock()
		if c.topicAliases == nil {
			c.topicAliases = make(map[uint16]string)
		}
		c.topicAliases[pkt.TopicAlias] = topic
		c.topicAliasMu.Unlock()
	}
	return topic, nil
}

// ─── PUBLISH ─────────────────────────────────────────────────────────────────

func (c *Client) handlePublish(fh protocol.FixedHeader, body []byte) error {
	start := time.Now()
	pkt, err := protocol.DecodePublish(c.version, fh, body)
	if err != nil {
		return err
	}
	topic, err := c.resolvePublishTopic(pkt)
	if err != nil {
		return err
	}
	if pkt.MessageExpiryInterval > 0 && time.Since(pkt.ReceivedAt) > time.Duration(pkt.MessageExpiryInterval)*time.Second {
		metrics.DroppedTotal.WithLabelValues("message_expired").Inc()
		return nil
	}

	if !c.broker.authn.CanPublish(c.clientID, topic) {
		if pkt.QoS == protocol.QoS1 {
			c.enqueuePubAck(protocol.PUBACK, pkt.PacketID)
		}
		metrics.DroppedTotal.WithLabelValues("acl_denied").Inc()
		return nil
	}

	metrics.PublishTotal.WithLabelValues(fmt.Sprintf("%d", pkt.QoS)).Inc()
	metrics.PublishBytes.Add(float64(len(pkt.Payload)))
	if err := c.broker.plugins.OnPublish(c.clientID, topic, pkt.Payload, pkt.QoS, pkt.Retain); err != nil {
		metrics.DroppedTotal.WithLabelValues("plugin_publish").Inc()
		return nil
	}

	switch pkt.QoS {
	case protocol.QoS0:
		c.broker.router.Publish(topic, pkt.Payload, pkt.QoS, pkt.Retain)

	case protocol.QoS1:
		c.broker.router.Publish(topic, pkt.Payload, pkt.QoS, pkt.Retain)
		c.enqueuePubAck(protocol.PUBACK, pkt.PacketID)

	case protocol.QoS2:
		// Phase 1: store if not already received
		if c.sess.GetInFlight(pkt.PacketID) == nil {
			c.sess.AddInFlight(&session.InFlight{
				PacketID: pkt.PacketID,
				Topic:    topic,
				Payload:  append([]byte(nil), pkt.Payload...),
				QoS:      pkt.QoS,
				Retain:   pkt.Retain,
				SentAt:   time.Now(),
			})
		}
		c.enqueuePubAck(protocol.PUBREC, pkt.PacketID)
	}

	metrics.PublishLatency.Observe(time.Since(start).Seconds())

	// Async logging and Redis fanout
	go func() {
		ctx := context.Background()
		c.broker.redis.IncrMsgRate(ctx)
		if pkt.Retain {
			c.broker.redis.SetRetained(ctx, topic, pkt.Payload, pkt.QoS)
		}
		c.broker.pg.LogMsg(ctx, topic, pkt.Payload, pkt.QoS, pkt.Retain, c.clientID)
		if c.broker.cfg.Cluster.Enabled {
			part := c.broker.partitions.PartitionForTopic(topic)
			c.broker.redis.PublishCluster(ctx, &storage.ClusterMsg{
				Ver:           1,
				SourceBroker:  c.broker.id,
				Partition:     part,
				Sequence:      c.broker.clusterSeq.Add(1),
				Topic:         topic,
				Payload:       pkt.Payload,
				QoS:           pkt.QoS,
				Retain:        pkt.Retain,
				TimestampUnix: time.Now().Unix(),
			})
		}
	}()

	return nil
}

// ─── QoS ACK Handlers ────────────────────────────────────────────────────────

func (c *Client) handlePuback(body []byte) error {
	id, err := protocol.DecodePacketID(body)
	if err != nil {
		return err
	}
	c.sess.AckOutFlight(id)
	return nil
}

func (c *Client) handlePubrec(body []byte) error {
	id, err := protocol.DecodePacketID(body)
	if err != nil {
		return err
	}
	c.sess.SetOutFlightPhase(id, session.QoS2Received)
	c.enqueuePubAck(protocol.PUBREL, id)
	return nil
}

func (c *Client) handlePubrel(body []byte) error {
	id, err := protocol.DecodePacketID(body)
	if err != nil {
		return err
	}
	if msg := c.sess.AckInFlight(id); msg != nil {
		c.broker.router.Publish(msg.Topic, msg.Payload, msg.QoS, msg.Retain)
	}
	c.enqueuePubAck(protocol.PUBCOMP, id)
	return nil
}

func (c *Client) handlePubcomp(body []byte) error {
	id, err := protocol.DecodePacketID(body)
	if err != nil {
		return err
	}
	c.sess.AckOutFlight(id)
	return nil
}

// ─── SUBSCRIBE ────────────────────────────────────────────────────────────────

func (c *Client) handleSubscribe(body []byte) error {
	packetID, subs, err := protocol.DecodeSubscribe(c.version, body)
	if err != nil {
		return err
	}

	codes := make([]byte, len(subs))
	for i, sub := range subs {
		qos := sub.QoS
		aclFilter := sub.Filter
		if _, inner, ok := protocol.ParseSharedTopicFilter(sub.Filter); ok {
			aclFilter = inner
		}
		if !protocol.ValidTopicFilter(sub.Filter) {
			codes[i] = protocol.SubackReasonByte(c.version, qos, protocol.SubackDeniedInvalidTopicFilter)
			continue
		}
		if !c.broker.authn.CanSubscribe(c.clientID, aclFilter) {
			codes[i] = protocol.SubackReasonByte(c.version, qos, protocol.SubackDeniedNotAuthorized)
			continue
		}
		if err := c.broker.plugins.OnSubscribe(c.clientID, sub.Filter, qos); err != nil {
			codes[i] = protocol.SubackReasonByte(c.version, qos, protocol.SubackDeniedPlugin)
			continue
		}
		c.broker.router.Subscribe(sub.Filter, &router.Subscriber{
			ClientID: c.clientID,
			QoS:      qos,
			Deliver:  c.makeDeliverFn(),
		})

		if !c.sess.CleanSession {
			go c.broker.pg.SaveSub(context.Background(), c.clientID, sub.Filter, qos)
		}
		codes[i] = protocol.SubackReasonByte(c.version, qos, protocol.SubackOK)

		// Send retained messages that match this filter
		go c.broker.router.SendRetained(sub.Filter, &router.Subscriber{
			ClientID: c.clientID,
			QoS:      qos,
			Deliver:  c.makeDeliverFn(),
		})
	}

	if c.version == protocol.V50 {
		c.enqueue(protocol.EncodeSubackV5(packetID, nil, codes))
	} else {
		c.enqueue(protocol.EncodeSuback(packetID, codes))
	}
	return nil
}

// ─── UNSUBSCRIBE ─────────────────────────────────────────────────────────────

func (c *Client) handleUnsubscribe(body []byte) error {
	packetID, filters, err := protocol.DecodeUnsubscribe(c.version, body)
	if err != nil {
		return err
	}

	if c.version == protocol.V50 {
		reasons := make([]byte, len(filters))
		for i, f := range filters {
			aclFilter := f
			if _, inner, ok := protocol.ParseSharedTopicFilter(f); ok {
				aclFilter = inner
			}
			var deny protocol.UnsubackDenyReason
			switch {
			case !protocol.ValidTopicFilter(f):
				deny = protocol.UnsubDeniedInvalidTopicFilter
			case !c.broker.authn.CanSubscribe(c.clientID, aclFilter):
				deny = protocol.UnsubDeniedNotAuthorized
			default:
				c.broker.router.Unsubscribe(f, c.clientID)
				if !c.sess.CleanSession {
					go c.broker.pg.DeleteSub(context.Background(), c.clientID, f)
				}
			}
			reasons[i] = protocol.UnsubackReasonByte(deny)
		}
		c.enqueue(protocol.EncodeUnsubackV5(packetID, nil, reasons))
		return nil
	}

	for _, f := range filters {
		aclFilter := f
		if _, inner, ok := protocol.ParseSharedTopicFilter(f); ok {
			aclFilter = inner
		}
		if !protocol.ValidTopicFilter(f) || !c.broker.authn.CanSubscribe(c.clientID, aclFilter) {
			continue
		}
		c.broker.router.Unsubscribe(f, c.clientID)
		if !c.sess.CleanSession {
			go c.broker.pg.DeleteSub(context.Background(), c.clientID, f)
		}
	}
	c.enqueue(protocol.EncodeUnsuback(packetID))
	return nil
}

// ─── Delivery ─────────────────────────────────────────────────────────────────

// makeDeliverFn returns a closure that delivers messages to this client.
// Delivery is non-blocking: offline clients queue for persistent sessions.
func (c *Client) makeDeliverFn() router.DeliverFn {
	return func(topic string, payload []byte, qos byte, retain bool, _ uint16) {
		if c.state.Load() != int32(stateConnected) {
			if c.sess != nil && !c.sess.CleanSession {
				c.sess.Enqueue(&session.Queued{
					Topic:   topic,
					Payload: append([]byte(nil), payload...),
					QoS:     qos,
					Retain:  retain,
				})
				metrics.OfflineQueuedMessages.Inc()
			}
			return
		}

		var pid uint16
		if qos > 0 {
			pid = c.sess.NextPacketID()
			c.sess.AddOutFlight(&session.InFlight{
				PacketID: pid,
				Topic:    topic,
				Payload:  payload,
				QoS:      qos,
				Retain:   retain,
				SentAt:   time.Now(),
			})
		}

		var enc []byte
		if c.version == protocol.V50 {
			enc = protocol.EncodePublishV5(topic, payload, qos, retain, pid, false, nil)
		} else {
			enc = protocol.EncodePublish(topic, payload, qos, retain, pid, false)
		}
		c.enqueue(enc)
	}
}

func (c *Client) restoreSession() {
	subs, err := c.broker.pg.GetSubs(context.Background(), c.clientID)
	if err != nil {
		c.logger.Error("session restore failed", zap.String("client", c.clientID), zap.Error(err))
		return
	}
	deliver := c.makeDeliverFn()
	for _, s := range subs {
		c.broker.router.Subscribe(s.Filter, &router.Subscriber{
			ClientID: c.clientID,
			QoS:      s.QoS,
			Deliver:  deliver,
		})
	}
	c.logger.Info("session restored", zap.String("client", c.clientID), zap.Int("subs", len(subs)))
}

func (c *Client) drainOfflineQueue() {
	if c.sess == nil {
		return
	}
	msgs := c.sess.DrainQueue()
	deliver := c.makeDeliverFn()
	for _, m := range msgs {
		deliver(m.Topic, m.Payload, m.QoS, m.Retain, 0)
		metrics.OfflineQueuedMessages.Dec()
	}
}

// ─── Write Loop ───────────────────────────────────────────────────────────────

func (c *Client) writeLoop(ctx context.Context) {
	for {
		select {
		case msg := <-c.writeQ:
			c.conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
			n, err := c.conn.Write(msg.data)
			if err != nil {
				return
			}
			c.txPackets.Add(1)
			c.txBytes.Add(uint64(n))

		case <-c.closeCh:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (c *Client) enqueuePubAck(t protocol.PacketType, packetID uint16) {
	if c.version == protocol.V50 {
		c.enqueue(protocol.EncodeAckV5(t, packetID, 0x00, nil))
		return
	}
	c.enqueue(protocol.EncodeAck(t, packetID))
}

// enqueue pushes data to the write queue. Non-blocking: drops if full.
func (c *Client) enqueue(data []byte) {
	if c.state.Load() == int32(stateClosed) {
		return
	}
	// Make a copy so the caller can reuse the underlying slice
	cp := make([]byte, len(data))
	copy(cp, data)
	select {
	case c.writeQ <- writeMsg{data: cp}:
	default:
		metrics.DroppedTotal.WithLabelValues("write_queue_full").Inc()
		c.logger.Warn("write queue full", zap.String("client", c.clientID))
	}
}

// ─── Cleanup ─────────────────────────────────────────────────────────────────

func (c *Client) cleanup() {
	c.closeOnce.Do(func() {
		c.state.Store(int32(stateClosed))
		close(c.closeCh)
		c.conn.Close()

		cid := c.clientID
		if cid == "" {
			return
		}

		c.broker.unregister(cid)

		go func() {
			ctx := context.Background()
			c.broker.redis.DelClient(ctx, cid)
			c.broker.pg.SetDeviceStatus(ctx, cid, false)
		}()

		if c.sess != nil {
			c.sess.SetConnected(false)
			if c.sess.CleanSession {
				c.broker.router.UnsubscribeAll(cid)
				c.broker.sessions.Delete(cid)
				metrics.ActiveSessions.Dec()
			}
			if c.sess.WillSet {
				c.broker.router.Publish(
					c.sess.WillTopic,
					c.sess.WillPayload,
					c.sess.WillQoS,
					c.sess.WillRetain,
				)
			}
		}

		metrics.ActiveConnections.Dec()
		metrics.DisconnectTotal.WithLabelValues("closed").Inc()
		_ = c.broker.plugins.OnClientDisconnect(c.clientID, c.username)

		c.logger.Info("client disconnected",
			zap.String("client", cid),
			zap.Uint64("rx", c.rxPackets.Load()),
			zap.Uint64("tx", c.txPackets.Load()),
		)
	})
}

func isClosedErr(err error) bool {
	if err == nil {
		return false
	}
	e := err.Error()
	return e == "use of closed network connection" ||
		e == "EOF" ||
		e == "io: read/write on closed pipe"
}
