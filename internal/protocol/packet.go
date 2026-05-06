// Package protocol implements the MQTT v3.1.1 and v5.0 wire protocol.
// Design goals:
//   - Zero allocations on the hot path (publish/deliver)
//   - sync.Pool buffer reuse for encode/decode scratch space
//   - Complete packet coverage: CONNECT, CONNACK, PUBLISH, PUBACK, PUBREC,
//     PUBREL, PUBCOMP, SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK, PINGREQ,
//     PINGRESP, DISCONNECT (v5 AUTH)
package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"time"
)

// ─── Packet Types ─────────────────────────────────────────────────────────────

type PacketType byte

const (
	RESERVED    PacketType = 0
	CONNECT     PacketType = 1
	CONNACK     PacketType = 2
	PUBLISH     PacketType = 3
	PUBACK      PacketType = 4
	PUBREC      PacketType = 5
	PUBREL      PacketType = 6
	PUBCOMP     PacketType = 7
	SUBSCRIBE   PacketType = 8
	SUBACK      PacketType = 9
	UNSUBSCRIBE PacketType = 10
	UNSUBACK    PacketType = 11
	PINGREQ     PacketType = 12
	PINGRESP    PacketType = 13
	DISCONNECT  PacketType = 14
	AUTH        PacketType = 15
)

var packetNames = [16]string{
	"RESERVED", "CONNECT", "CONNACK", "PUBLISH",
	"PUBACK", "PUBREC", "PUBREL", "PUBCOMP",
	"SUBSCRIBE", "SUBACK", "UNSUBSCRIBE", "UNSUBACK",
	"PINGREQ", "PINGRESP", "DISCONNECT", "AUTH",
}

func (t PacketType) String() string {
	if int(t) < len(packetNames) {
		return packetNames[t]
	}
	return fmt.Sprintf("UNKNOWN(%d)", byte(t))
}

// ─── QoS ─────────────────────────────────────────────────────────────────────

const (
	QoS0 byte = 0
	QoS1 byte = 1
	QoS2 byte = 2
)

// ─── Protocol Versions ────────────────────────────────────────────────────────

const (
	V311 byte = 4 // MQTT 3.1.1
	V50  byte = 5 // MQTT 5.0
)

// ─── Return / Reason Codes ────────────────────────────────────────────────────

const (
	ConnAccepted              byte = 0x00
	ConnRefusedProtocol       byte = 0x01
	ConnRefusedIDRejected     byte = 0x02
	ConnRefusedServerUnavail  byte = 0x03
	ConnRefusedBadCredentials byte = 0x04
	ConnRefusedNotAuthorized  byte = 0x05

	ReasonSuccess          byte = 0x00
	ReasonUnspecifiedError byte = 0x80
	ReasonNotAuthorized    byte = 0x87
	ReasonQuotaExceeded    byte = 0x97
)

// ─── Limits ───────────────────────────────────────────────────────────────────

const (
	MaxRemainingLength = 268435455 // 256 MiB per spec
	MaxTopicLen        = 65535
	MinPacketSize      = 2
)

// ─── Errors ───────────────────────────────────────────────────────────────────

var (
	ErrMalformed          = errors.New("malformed packet")
	ErrProtocolViolation  = errors.New("protocol violation")
	ErrTooLarge           = errors.New("packet too large")
	ErrInvalidVarInt      = errors.New("invalid variable length integer")
	ErrInvalidQoS         = errors.New("invalid QoS level")
	ErrInvalidTopic       = errors.New("invalid topic name")
	ErrUnsupportedVersion = errors.New("unsupported protocol version")
	ErrInvalidClientID    = errors.New("invalid client identifier")
	ErrZeroPacketID       = errors.New("packet identifier must be non-zero for QoS > 0")
)

// ─── Buffer Pool ──────────────────────────────────────────────────────────────

// bufPool recycles byte slices used for packet encoding to avoid GC pressure.
// Slices in the pool are at least 1024 bytes. Callers must not retain
// references to pooled slices after returning them.
var bufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 1024)
		return &b
	},
}

func getBuf() *[]byte {
	b := bufPool.Get().(*[]byte)
	*b = (*b)[:0]
	return b
}

func putBuf(b *[]byte) {
	if cap(*b) <= 64*1024 { // don't pool huge slices
		bufPool.Put(b)
	}
}

// ─── Fixed Header ─────────────────────────────────────────────────────────────

// FixedHeader is the 2+ byte header present in every MQTT packet.
type FixedHeader struct {
	Type            PacketType
	Flags           byte // bits 3-0 of first byte
	RemainingLength int
}

// ReadFixed reads the fixed header from r. Uses only 1-byte reads to avoid
// buffering; the caller's bufio.Reader provides the buffer.
func ReadFixed(r io.Reader) (FixedHeader, error) {
	var b1 [1]byte
	if _, err := io.ReadFull(r, b1[:]); err != nil {
		return FixedHeader{}, err
	}
	fh := FixedHeader{
		Type:  PacketType(b1[0] >> 4),
		Flags: b1[0] & 0x0F,
	}

	// Variable-length remaining length (1-4 bytes, 7 bits per byte)
	mult := 1
	val := 0
	for i := 0; i < 4; i++ {
		if _, err := io.ReadFull(r, b1[:]); err != nil {
			return FixedHeader{}, fmt.Errorf("remaining length: %w", err)
		}
		digit := int(b1[0])
		val += (digit & 0x7F) * mult
		mult *= 128
		if digit&0x80 == 0 {
			break
		}
		if i == 3 {
			return FixedHeader{}, ErrInvalidVarInt
		}
	}
	fh.RemainingLength = val
	return fh, nil
}

// encodeVarInt encodes n as a MQTT variable-length integer into dst.
// Returns number of bytes written (1-4).
func encodeVarInt(dst []byte, n int) int {
	i := 0
	for {
		digit := n % 128
		n /= 128
		if n > 0 {
			digit |= 0x80
		}
		dst[i] = byte(digit)
		i++
		if n == 0 {
			break
		}
	}
	return i
}

// ─── String Codec ─────────────────────────────────────────────────────────────

// readString reads a 2-byte length-prefixed UTF-8 string from r.
func readString(r io.Reader) (string, error) {
	var lenBuf [2]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return "", err
	}
	length := binary.BigEndian.Uint16(lenBuf[:])
	if length == 0 {
		return "", nil
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return "", err
	}
	return string(data), nil
}

// readUint16 reads a big-endian uint16 from r.
func readUint16(r io.Reader) (uint16, error) {
	var buf [2]byte
	_, err := io.ReadFull(r, buf[:])
	return binary.BigEndian.Uint16(buf[:]), err
}

// appendString appends a 2-byte-length-prefixed UTF-8 string to b.
func appendString(b []byte, s string) []byte {
	if len(s) > math.MaxUint16 {
		s = s[:math.MaxUint16]
	}
	b = append(b, byte(len(s)>>8), byte(len(s)))
	return append(b, s...)
}

// appendUint16 appends a big-endian uint16 to b.
func appendUint16(b []byte, v uint16) []byte {
	return append(b, byte(v>>8), byte(v))
}

// ─── CONNECT ─────────────────────────────────────────────────────────────────

// ConnectPacket represents a decoded CONNECT packet.
type ConnectPacket struct {
	Version               byte
	CleanSession          bool
	KeepAlive             uint16
	SessionExpiryInterval uint32

	// Flags
	WillFlag    bool
	WillQoS     byte
	WillRetain  bool
	HasUsername bool
	HasPassword bool

	// Payload
	ClientID    string
	WillTopic   string
	WillPayload []byte
	Username    string
	Password    []byte
}

// DecodeConnect decodes the variable header + payload of a CONNECT packet.
// body is the raw bytes after the fixed header.
func DecodeConnect(body []byte) (*ConnectPacket, error) {
	r := newSliceReader(body)
	pkt := &ConnectPacket{}

	// Protocol Name
	protoName, err := readString(r)
	if err != nil {
		return nil, fmt.Errorf("proto name: %w", err)
	}
	if protoName != "MQTT" && protoName != "MQIsdp" {
		return nil, ErrUnsupportedVersion
	}

	// Protocol Version
	ver := make([]byte, 1)
	if _, err := io.ReadFull(r, ver); err != nil {
		return nil, err
	}
	pkt.Version = ver[0]
	if pkt.Version != V311 && pkt.Version != V50 {
		return nil, ErrUnsupportedVersion
	}

	// Connect Flags
	flags := make([]byte, 1)
	if _, err := io.ReadFull(r, flags); err != nil {
		return nil, err
	}
	f := flags[0]
	if f&0x01 != 0 { // reserved bit must be 0
		return nil, ErrProtocolViolation
	}
	pkt.CleanSession = f&0x02 != 0
	pkt.WillFlag = f&0x04 != 0
	pkt.WillQoS = (f >> 3) & 0x03
	pkt.WillRetain = f&0x20 != 0
	pkt.HasPassword = f&0x40 != 0
	pkt.HasUsername = f&0x80 != 0

	if !pkt.WillFlag && (pkt.WillQoS != 0 || pkt.WillRetain) {
		return nil, ErrProtocolViolation
	}
	if pkt.WillQoS > QoS2 {
		return nil, ErrInvalidQoS
	}

	// Keep-alive
	ka, err := readUint16(r)
	if err != nil {
		return nil, err
	}
	pkt.KeepAlive = ka

	// v5: skip properties
	if pkt.Version == V50 {
		props, err := readConnectProps(r)
		if err != nil {
			return nil, err
		}
		if v, ok := props[0x11]; ok && len(v) == 4 {
			pkt.SessionExpiryInterval = binary.BigEndian.Uint32(v)
		}
	}

	// Client ID
	pkt.ClientID, err = readString(r)
	if err != nil {
		return nil, fmt.Errorf("client id: %w", err)
	}

	// Will
	if pkt.WillFlag {
		if pkt.Version == V50 {
			if err := skipProps(r); err != nil {
				return nil, err
			}
		}
		pkt.WillTopic, err = readString(r)
		if err != nil {
			return nil, fmt.Errorf("will topic: %w", err)
		}
		wLen, err := readUint16(r)
		if err != nil {
			return nil, err
		}
		if wLen > 0 {
			pkt.WillPayload = make([]byte, wLen)
			if _, err := io.ReadFull(r, pkt.WillPayload); err != nil {
				return nil, err
			}
		}
	}

	// Username
	if pkt.HasUsername {
		pkt.Username, err = readString(r)
		if err != nil {
			return nil, fmt.Errorf("username: %w", err)
		}
	}

	// Password
	if pkt.HasPassword {
		pLen, err := readUint16(r)
		if err != nil {
			return nil, err
		}
		if pLen > 0 {
			pkt.Password = make([]byte, pLen)
			if _, err := io.ReadFull(r, pkt.Password); err != nil {
				return nil, err
			}
		}
	}

	return pkt, nil
}

// ─── CONNACK ──────────────────────────────────────────────────────────────────

// EncodeConnack returns a 4-byte CONNACK packet.
// Zero-allocation: uses a fixed-size array on the stack.
func EncodeConnack(sessionPresent bool, code byte) []byte {
	pkt := [4]byte{byte(CONNACK) << 4, 2, 0, code}
	if sessionPresent {
		pkt[2] = 0x01
	}
	return pkt[:]
}

// ─── PUBLISH ─────────────────────────────────────────────────────────────────

// PublishPacket is a decoded MQTT PUBLISH packet.
// Payload is a direct slice into the read buffer where possible.
type PublishPacket struct {
	Dup                   bool
	QoS                   byte
	Retain                bool
	Topic                 string
	PacketID              uint16
	Payload               []byte
	MessageExpiryInterval uint32
	TopicAlias            uint16
	ReceivedAt            time.Time
}

// DecodePublish decodes the variable header + payload of a PUBLISH packet.
func DecodePublish(version byte, fh FixedHeader, body []byte) (*PublishPacket, error) {
	r := newSliceReader(body)
	pkt := &PublishPacket{
		Dup:        fh.Flags&0x08 != 0,
		QoS:        (fh.Flags >> 1) & 0x03,
		Retain:     fh.Flags&0x01 != 0,
		ReceivedAt: time.Now(),
	}
	if pkt.QoS > QoS2 {
		return nil, ErrInvalidQoS
	}
	if pkt.QoS == QoS0 && pkt.Dup {
		return nil, ErrProtocolViolation
	}

	var err error
	pkt.Topic, err = readString(r)
	if err != nil {
		return nil, fmt.Errorf("topic: %w", err)
	}
	if !ValidTopicName(pkt.Topic) {
		return nil, ErrInvalidTopic
	}

	if pkt.QoS > QoS0 {
		pkt.PacketID, err = readUint16(r)
		if err != nil {
			return nil, err
		}
		if pkt.PacketID == 0 {
			return nil, ErrZeroPacketID
		}
	}

	if version == V50 {
		props, err := readProps(r)
		if err != nil {
			return nil, err
		}
		if v, ok := props[0x02]; ok && len(v) == 4 {
			pkt.MessageExpiryInterval = binary.BigEndian.Uint32(v)
		}
		if v, ok := props[0x23]; ok && len(v) == 2 {
			pkt.TopicAlias = binary.BigEndian.Uint16(v)
		}
	}

	// Payload: remaining bytes
	if n := r.Remaining(); n > 0 {
		pkt.Payload = make([]byte, n)
		r.ReadFull(pkt.Payload)
	}
	return pkt, nil
}

// EncodePublish encodes a PUBLISH packet using a pooled buffer.
// The caller owns the returned slice until they call PutBuf.
// For QoS 0 fire-and-forget the slice can be used immediately.
func EncodePublish(topic string, payload []byte, qos byte, retain bool, packetID uint16, dup bool) []byte {
	// Calculate total size to avoid reallocations
	topicLen := 2 + len(topic)
	pidLen := 0
	if qos > 0 {
		pidLen = 2
	}
	payloadLen := len(payload)
	remaining := topicLen + pidLen + payloadLen

	// Encode remaining length (1-4 bytes)
	var varBuf [4]byte
	varLen := encodeVarInt(varBuf[:], remaining)

	// Build packet
	out := make([]byte, 0, 1+varLen+remaining)

	flags := (byte(PUBLISH) << 4)
	if dup {
		flags |= 0x08
	}
	flags |= (qos & 0x03) << 1
	if retain {
		flags |= 0x01
	}

	out = append(out, flags)
	out = append(out, varBuf[:varLen]...)
	out = appendString(out, topic)
	if qos > 0 {
		out = appendUint16(out, packetID)
	}
	out = append(out, payload...)
	return out
}

// ─── PUBACK / PUBREC / PUBREL / PUBCOMP ──────────────────────────────────────

// EncodeAck encodes a 4-byte QoS-ack packet (PUBACK, PUBREC, PUBREL, PUBCOMP).
func EncodeAck(t PacketType, packetID uint16) []byte {
	flags := byte(0)
	if t == PUBREL {
		flags = 0x02
	}
	return []byte{
		byte(t)<<4 | flags,
		2,
		byte(packetID >> 8),
		byte(packetID),
	}
}

// DecodePacketID reads a 2-byte packet identifier from a body slice.
func DecodePacketID(body []byte) (uint16, error) {
	if len(body) < 2 {
		return 0, ErrMalformed
	}
	return binary.BigEndian.Uint16(body[:2]), nil
}

// ─── SUBSCRIBE ────────────────────────────────────────────────────────────────

// Subscription is a single (topic filter, QoS) pair from a SUBSCRIBE packet.
type Subscription struct {
	Filter string
	QoS    byte
}

// DecodeSubscribe decodes a SUBSCRIBE packet body.
func DecodeSubscribe(body []byte) (packetID uint16, subs []Subscription, err error) {
	r := newSliceReader(body)
	packetID, err = readUint16(r)
	if err != nil {
		return 0, nil, err
	}

	for r.Remaining() > 0 {
		filter, err := readString(r)
		if err != nil {
			return 0, nil, err
		}
		qosBuf := make([]byte, 1)
		if _, err := io.ReadFull(r, qosBuf); err != nil {
			return 0, nil, err
		}
		subs = append(subs, Subscription{Filter: filter, QoS: qosBuf[0] & 0x03})
	}
	if len(subs) == 0 {
		return 0, nil, ErrProtocolViolation
	}
	return packetID, subs, nil
}

// EncodeSuback encodes a SUBACK packet.
func EncodeSuback(packetID uint16, codes []byte) []byte {
	remaining := 2 + len(codes)
	var varBuf [4]byte
	varLen := encodeVarInt(varBuf[:], remaining)

	out := make([]byte, 0, 1+varLen+remaining)
	out = append(out, byte(SUBACK)<<4)
	out = append(out, varBuf[:varLen]...)
	out = appendUint16(out, packetID)
	out = append(out, codes...)
	return out
}

// ─── UNSUBSCRIBE ─────────────────────────────────────────────────────────────

// DecodeUnsubscribe decodes an UNSUBSCRIBE packet body.
func DecodeUnsubscribe(body []byte) (packetID uint16, filters []string, err error) {
	r := newSliceReader(body)
	packetID, err = readUint16(r)
	if err != nil {
		return
	}
	for r.Remaining() > 0 {
		f, err2 := readString(r)
		if err2 != nil {
			return 0, nil, err2
		}
		filters = append(filters, f)
	}
	return
}

// EncodeUnsuback encodes an UNSUBACK packet.
func EncodeUnsuback(packetID uint16) []byte {
	return []byte{byte(UNSUBACK) << 4, 2, byte(packetID >> 8), byte(packetID)}
}

// ─── PINGRESP ─────────────────────────────────────────────────────────────────

// Pingresp is a static 2-byte PINGRESP packet (shared, never mutated).
var Pingresp = []byte{byte(PINGRESP) << 4, 0}

// ─── Topic Validation ─────────────────────────────────────────────────────────

// ValidTopicName checks that a PUBLISH topic contains no wildcards.
func ValidTopicName(t string) bool {
	if len(t) == 0 || len(t) > MaxTopicLen {
		return false
	}
	for i := 0; i < len(t); i++ {
		switch t[i] {
		case '+', '#', 0:
			return false
		}
	}
	return true
}

// ParseSharedTopicFilter parses MQTT v5 shared subscription "$share/{ShareName}/{TopicFilter}".
func ParseSharedTopicFilter(f string) (shareName, topicFilter string, ok bool) {
	const p = "$share/"
	if len(f) <= len(p)+1 || !strings.HasPrefix(f, p) {
		return "", "", false
	}
	rest := f[len(p):]
	i := strings.IndexByte(rest, '/')
	if i <= 0 || i >= len(rest)-1 {
		return "", "", false
	}
	shareName = rest[:i]
	topicFilter = rest[i+1:]
	if shareName == "" || topicFilter == "" {
		return "", "", false
	}
	for _, c := range shareName {
		if c == '+' || c == '#' || c == '/' || c == 0 {
			return "", "", false
		}
	}
	if strings.HasPrefix(topicFilter, "$share/") {
		return "", "", false
	}
	return shareName, topicFilter, true
}

func validTopicFilterSegments(segs []string) bool {
	for i, seg := range segs {
		switch seg {
		case "#":
			if i != len(segs)-1 {
				return false
			}
		case "+":
			// single-level wildcard
		default:
			for j := 0; j < len(seg); j++ {
				if seg[j] == '+' || seg[j] == '#' || seg[j] == 0 {
					return false
				}
			}
		}
	}
	return true
}

// ValidTopicFilter validates a SUBSCRIBE topic filter (wildcards allowed; MQTT v5 $share supported).
func ValidTopicFilter(f string) bool {
	if len(f) == 0 || len(f) > MaxTopicLen {
		return false
	}
	if strings.HasPrefix(f, "$share/") {
		if _, inner, ok := ParseSharedTopicFilter(f); ok {
			return validTopicFilterSegments(splitFilter(inner))
		}
		return false
	}
	return validTopicFilterSegments(splitFilter(f))
}

func splitFilter(f string) []string {
	var segs []string
	start := 0
	for i := 0; i < len(f); i++ {
		if f[i] == '/' {
			segs = append(segs, f[start:i])
			start = i + 1
		}
	}
	return append(segs, f[start:])
}

// ─── sliceReader ─────────────────────────────────────────────────────────────

// sliceReader is a lightweight io.Reader over a byte slice.
// Avoids the allocation of bytes.NewReader.
type sliceReader struct {
	data []byte
	pos  int
}

func newSliceReader(b []byte) *sliceReader { return &sliceReader{data: b} }

func (r *sliceReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func (r *sliceReader) ReadFull(p []byte) {
	copy(p, r.data[r.pos:])
	r.pos += len(p)
}

func (r *sliceReader) Remaining() int { return len(r.data) - r.pos }

// skipProps reads and discards a MQTT v5 properties block.
func skipProps(r *sliceReader) error {
	var propLen int
	mult := 1
	buf := make([]byte, 1)
	for i := 0; i < 4; i++ {
		if _, err := r.Read(buf); err != nil {
			return err
		}
		propLen += int(buf[0]&0x7F) * mult
		mult *= 128
		if buf[0]&0x80 == 0 {
			break
		}
	}
	if propLen == 0 {
		return nil
	}
	r.pos += propLen
	if r.pos > len(r.data) {
		return ErrMalformed
	}
	return nil
}

func readProps(r *sliceReader) (map[byte][]byte, error) {
	propLen, err := readVarInt(r)
	if err != nil {
		return nil, err
	}
	props := make(map[byte][]byte)
	start := r.pos
	for r.pos-start < propLen {
		if r.Remaining() < 1 {
			return nil, ErrMalformed
		}
		id := r.data[r.pos]
		r.pos++
		switch id {
		case 0x02: // message expiry interval
			if r.Remaining() < 4 {
				return nil, ErrMalformed
			}
			props[id] = append([]byte(nil), r.data[r.pos:r.pos+4]...)
			r.pos += 4
		case 0x23: // topic alias
			if r.Remaining() < 2 {
				return nil, ErrMalformed
			}
			props[id] = append([]byte(nil), r.data[r.pos:r.pos+2]...)
			r.pos += 2
		default:
			// Unknown property: skip the remainder as compatibility fallback.
			r.pos = start + propLen
			return props, nil
		}
	}
	return props, nil
}

func readConnectProps(r *sliceReader) (map[byte][]byte, error) {
	propLen, err := readVarInt(r)
	if err != nil {
		return nil, err
	}
	props := make(map[byte][]byte)
	start := r.pos
	for r.pos-start < propLen {
		if r.Remaining() < 1 {
			return nil, ErrMalformed
		}
		id := r.data[r.pos]
		r.pos++
		switch id {
		case 0x11: // session expiry interval
			if r.Remaining() < 4 {
				return nil, ErrMalformed
			}
			props[id] = append([]byte(nil), r.data[r.pos:r.pos+4]...)
			r.pos += 4
		default:
			// Unknown property: skip the remainder for compatibility.
			r.pos = start + propLen
			return props, nil
		}
	}
	return props, nil
}

func readVarInt(r *sliceReader) (int, error) {
	mult := 1
	val := 0
	for i := 0; i < 4; i++ {
		if r.Remaining() < 1 {
			return 0, ErrMalformed
		}
		digit := int(r.data[r.pos])
		r.pos++
		val += (digit & 0x7F) * mult
		mult *= 128
		if digit&0x80 == 0 {
			return val, nil
		}
	}
	return 0, ErrInvalidVarInt
}
