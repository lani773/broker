// Package session manages MQTT client sessions: in-flight QoS 1/2 message
// tracking, persistent session state, and offline message queuing.
package session

import (
	"sync"
	"sync/atomic"
	"time"
)

const mqttProtoV50 byte = 5 // MQTT v5 protocol level byte

// ─── QoS 2 State ─────────────────────────────────────────────────────────────

type QoS2Phase int8

const (
	QoS2Published QoS2Phase = iota // PUBLISH sent, awaiting PUBREC
	QoS2Received                   // PUBREC received, sending PUBREL
	QoS2Released                   // PUBREL sent, awaiting PUBCOMP
)

// ─── In-Flight Message ────────────────────────────────────────────────────────

// InFlight tracks a single QoS 1 or 2 message awaiting acknowledgment.
type InFlight struct {
	PacketID  uint16
	Topic     string
	Payload   []byte
	QoS       byte
	Retain    bool
	SentAt    time.Time
	Retries   int
	QoS2Phase QoS2Phase // QoS 2 only
}

// ─── Queued Message ───────────────────────────────────────────────────────────

// Queued is a message stored for an offline persistent-session client.
type Queued struct {
	Topic   string
	Payload []byte
	QoS     byte
	Retain  bool
}

// ─── Session ─────────────────────────────────────────────────────────────────

const defaultMaxQueue = 1000

// Session holds the state for one MQTT client session.
type Session struct {
	ClientID     string
	CleanSession bool

	// Connection state
	mu          sync.RWMutex
	connected   bool
	connectedAt time.Time
	lastSeen    time.Time
	keepAlive   uint16

	// MQTT v5 persistent-session expiry (ignored for clean sessions / v3.1.1 scheduling).
	protocolLevel    byte
	sessionExpirySec uint32
	disconnectExpireAt time.Time // non-zero ⇒ sweep removes session after wall-clock deadline

	// Last Will
	WillTopic   string
	WillPayload []byte
	WillQoS     byte
	WillRetain  bool
	WillSet     bool

	// Packet ID counter (atomic, never 0)
	nextPktID uint32

	// Outbound in-flight (broker→client): QoS 1 waiting PUBACK, QoS 2 sequence
	outMu     sync.Mutex
	outFlight map[uint16]*InFlight

	// Inbound in-flight (client→broker): QoS 2 PUBLISH received, awaiting PUBREL
	inMu     sync.Mutex
	inFlight map[uint16]*InFlight

	// Offline queue for persistent sessions
	qMu      sync.Mutex
	queue    []*Queued
	maxQueue int
}

// New creates a fresh Session.
func New(clientID string, cleanSession bool) *Session {
	return &Session{
		ClientID:     clientID,
		CleanSession: cleanSession,
		outFlight:    make(map[uint16]*InFlight),
		inFlight:     make(map[uint16]*InFlight),
		maxQueue:     defaultMaxQueue,
		connectedAt:  time.Now(),
		lastSeen:     time.Now(),
	}
}

// ─── Connection State ─────────────────────────────────────────────────────────

func (s *Session) SetConnected(c bool) {
	s.mu.Lock()
	s.connected = c
	s.lastSeen = time.Now()
	if c {
		s.connectedAt = time.Now()
	}
	s.mu.Unlock()
}

func (s *Session) IsConnected() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.connected
}

func (s *Session) Touch() {
	s.mu.Lock()
	s.lastSeen = time.Now()
	s.mu.Unlock()
}

func (s *Session) SetKeepAlive(ka uint16) {
	s.mu.Lock()
	s.keepAlive = ka
	s.mu.Unlock()
}

// ClearDisconnectExpiry clears a pending disconnected-session expiry deadline (on reconnect).
func (s *Session) ClearDisconnectExpiry() {
	s.mu.Lock()
	s.disconnectExpireAt = time.Time{}
	s.mu.Unlock()
}

// SetProtocolAndExpiry stores the negotiated MQTT protocol level and v5 session expiry interval from CONNECT.
func (s *Session) SetProtocolAndExpiry(version byte, cleanSession bool, sessionExpirySec uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.protocolLevel = version
	if version == mqttProtoV50 && !cleanSession {
		s.sessionExpirySec = sessionExpirySec
		return
	}
	if cleanSession {
		s.sessionExpirySec = 0
		s.disconnectExpireAt = time.Time{}
	}
}

// UpdateSessionExpiryFromDisconnect applies MQTT v5 Session Expiry Interval from DISCONNECT (property present).
func (s *Session) UpdateSessionExpiryFromDisconnect(sec uint32) {
	s.mu.Lock()
	s.sessionExpirySec = sec
	s.mu.Unlock()
}

// PrepareDisconnectExpiry schedules expiry after disconnect for MQTT v5 persistent sessions.
// Returns true when the session must be purged immediately (interval 0).
func (s *Session) PrepareDisconnectExpiry(now time.Time) (purgeImmediate bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.CleanSession || s.protocolLevel != mqttProtoV50 {
		return false
	}
	switch s.sessionExpirySec {
	case 0:
		s.disconnectExpireAt = time.Time{}
		return true
	case ^uint32(0):
		s.disconnectExpireAt = time.Time{}
		return false
	default:
		s.disconnectExpireAt = now.Add(time.Duration(s.sessionExpirySec) * time.Second)
		return false
	}
}

// ShouldExpireDisconnected reports whether a disconnected persistent session passed its expiry deadline.
func (s *Session) ShouldExpireDisconnected(now time.Time) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.connected || s.CleanSession {
		return false
	}
	return !s.disconnectExpireAt.IsZero() && !now.Before(s.disconnectExpireAt)
}

func (s *Session) Info() (connectedAt, lastSeen time.Time, connected bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.connectedAt, s.lastSeen, s.connected
}

// ─── Packet ID ────────────────────────────────────────────────────────────────

// NextPacketID returns the next unused packet ID, cycling 1-65535.
func (s *Session) NextPacketID() uint16 {
	for {
		next := atomic.AddUint32(&s.nextPktID, 1)
		id := uint16(next)
		if id == 0 {
			id = 1
		}
		s.outMu.Lock()
		_, inUse := s.outFlight[id]
		s.outMu.Unlock()
		if !inUse {
			return id
		}
	}
}

// ─── Outbound In-Flight (broker → client) ────────────────────────────────────

func (s *Session) AddOutFlight(msg *InFlight) {
	s.outMu.Lock()
	s.outFlight[msg.PacketID] = msg
	s.outMu.Unlock()
}

// AddOutFlightLimited adds an outbound QoS>0 publish respecting the client's Receive Maximum (0 = unlimited).
// QoS 0 messages are not tracked in outFlight and always succeed.
func (s *Session) AddOutFlightLimited(msg *InFlight, receiveMax uint16) bool {
	if msg.QoS == 0 {
		return true
	}
	s.outMu.Lock()
	defer s.outMu.Unlock()
	if receiveMax > 0 {
		n := 0
		for _, m := range s.outFlight {
			if m.QoS > 0 {
				n++
			}
		}
		if n >= int(receiveMax) {
			return false
		}
	}
	s.outFlight[msg.PacketID] = msg
	return true
}

func (s *Session) GetOutFlight(id uint16) *InFlight {
	s.outMu.Lock()
	defer s.outMu.Unlock()
	return s.outFlight[id]
}

func (s *Session) AckOutFlight(id uint16) *InFlight {
	s.outMu.Lock()
	msg := s.outFlight[id]
	delete(s.outFlight, id)
	s.outMu.Unlock()
	return msg
}

func (s *Session) SetOutFlightPhase(id uint16, phase QoS2Phase) {
	s.outMu.Lock()
	if msg, ok := s.outFlight[id]; ok {
		msg.QoS2Phase = phase
	}
	s.outMu.Unlock()
}

func (s *Session) AllOutFlight() []*InFlight {
	s.outMu.Lock()
	defer s.outMu.Unlock()
	out := make([]*InFlight, 0, len(s.outFlight))
	for _, m := range s.outFlight {
		out = append(out, m)
	}
	return out
}

// ─── Inbound In-Flight (client → broker, QoS 2) ──────────────────────────────

func (s *Session) AddInFlight(msg *InFlight) {
	s.inMu.Lock()
	s.inFlight[msg.PacketID] = msg
	s.inMu.Unlock()
}

func (s *Session) GetInFlight(id uint16) *InFlight {
	s.inMu.Lock()
	defer s.inMu.Unlock()
	return s.inFlight[id]
}

func (s *Session) AckInFlight(id uint16) *InFlight {
	s.inMu.Lock()
	msg := s.inFlight[id]
	delete(s.inFlight, id)
	s.inMu.Unlock()
	return msg
}

// ─── Offline Queue ────────────────────────────────────────────────────────────

// Enqueue adds a message to the offline queue. Returns false if dropped.
func (s *Session) Enqueue(msg *Queued) bool {
	if s.CleanSession {
		return false
	}
	s.qMu.Lock()
	defer s.qMu.Unlock()
	if len(s.queue) >= s.maxQueue {
		// Drop oldest (head) to make room
		s.queue = s.queue[1:]
	}
	s.queue = append(s.queue, msg)
	return true
}

// DrainQueue returns and clears all queued messages.
func (s *Session) DrainQueue() []*Queued {
	s.qMu.Lock()
	defer s.qMu.Unlock()
	if len(s.queue) == 0 {
		return nil
	}
	out := make([]*Queued, len(s.queue))
	copy(out, s.queue)
	s.queue = s.queue[:0]
	return out
}

func (s *Session) QueueLen() int {
	s.qMu.Lock()
	defer s.qMu.Unlock()
	return len(s.queue)
}

// Clear resets transient state for a CleanSession reconnect.
func (s *Session) Clear() {
	s.outMu.Lock()
	s.outFlight = make(map[uint16]*InFlight)
	s.outMu.Unlock()

	s.inMu.Lock()
	s.inFlight = make(map[uint16]*InFlight)
	s.inMu.Unlock()

	s.qMu.Lock()
	s.queue = nil
	s.qMu.Unlock()

	s.WillSet = false
}

// ─── Store ────────────────────────────────────────────────────────────────────

// Store is a thread-safe map of clientID → *Session.
type Store struct {
	mu   sync.RWMutex
	data map[string]*Session
}

func NewStore() *Store {
	return &Store{data: make(map[string]*Session)}
}

// GetOrCreate returns an existing persistent session or creates a new one.
// Returns (session, alreadyExisted).
func (s *Store) GetOrCreate(clientID string, clean bool) (*Session, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if clean {
		// Always fresh for clean sessions
		sess := New(clientID, true)
		s.data[clientID] = sess
		return sess, false
	}

	if existing, ok := s.data[clientID]; ok && !existing.CleanSession {
		return existing, true // persistent session resumed
	}

	sess := New(clientID, false)
	s.data[clientID] = sess
	return sess, false
}

func (s *Store) Get(clientID string) (*Session, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sess, ok := s.data[clientID]
	return sess, ok
}

func (s *Store) Delete(clientID string) {
	s.mu.Lock()
	delete(s.data, clientID)
	s.mu.Unlock()
}

func (s *Store) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// Each iterates all sessions without holding the lock across the callback.
func (s *Store) Each(fn func(*Session)) {
	s.mu.RLock()
	sessions := make([]*Session, 0, len(s.data))
	for _, sess := range s.data {
		sessions = append(sessions, sess)
	}
	s.mu.RUnlock()
	for _, sess := range sessions {
		fn(sess)
	}
}
