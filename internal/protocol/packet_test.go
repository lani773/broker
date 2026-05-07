package protocol

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestDecodePublishV5Properties(t *testing.T) {
	// topic "a/b", packet id 10, properties:
	// 0x02 message expiry=30, 0x23 topic alias=7, payload "ok"
	body := []byte{
		0x00, 0x03, 'a', '/', 'b',
		0x00, 0x0a,
		0x08, // properties length
		0x02, 0x00, 0x00, 0x00, 0x1e,
		0x23, 0x00, 0x07,
		'o', 'k',
	}
	fh := FixedHeader{Type: PUBLISH, Flags: 0x02, RemainingLength: len(body)}
	pkt, err := DecodePublish(V50, fh, body)
	if err != nil {
		t.Fatalf("DecodePublish error: %v", err)
	}
	if pkt.MessageExpiryInterval != 30 {
		t.Fatalf("expiry mismatch: %d", pkt.MessageExpiryInterval)
	}
	if pkt.TopicAlias != 7 {
		t.Fatalf("topic alias mismatch: %d", pkt.TopicAlias)
	}
	if string(pkt.Payload) != "ok" {
		t.Fatalf("payload mismatch: %q", string(pkt.Payload))
	}
}

func TestParseSharedTopicFilter(t *testing.T) {
	sn, inner, ok := ParseSharedTopicFilter("$share/grp/devices/+")
	if !ok || sn != "grp" || inner != "devices/+" {
		t.Fatalf("parse: ok=%v sn=%q inner=%q", ok, sn, inner)
	}
	if _, _, ok := ParseSharedTopicFilter("devices/+"); ok {
		t.Fatal("non-shared should fail")
	}
	if ValidTopicFilter("$share//a") {
		t.Fatal("empty share name invalid")
	}
	if !ValidTopicFilter("$share/g1/home/#") {
		t.Fatal("valid shared filter")
	}
}

func TestDecodePublishV5EmptyTopicUsesAlias(t *testing.T) {
	// QoS0: topic "", properties: topic alias = 7 (0x23), payload "x"
	body := []byte{
		0x00, 0x00, // empty topic
		0x03,       // props length
		0x23, 0x00, 0x07,
		'x',
	}
	fh := FixedHeader{Type: PUBLISH, Flags: 0, RemainingLength: len(body)}
	pkt, err := DecodePublish(V50, fh, body)
	if err != nil {
		t.Fatal(err)
	}
	if pkt.Topic != "" || pkt.TopicAlias != 7 || string(pkt.Payload) != "x" {
		t.Fatalf("got topic=%q alias=%d payload=%q", pkt.Topic, pkt.TopicAlias, pkt.Payload)
	}
}

func TestEncodeConnackV5TopicAliasMax(t *testing.T) {
	b := EncodeConnackV5(true, ConnAccepted, ConnackMQTT5Options{
		TopicAliasMax:                 64,
		ReceiveMaximum:                65535,
		MaxPacketSize:                 8192,
		RetainAvailable:               true,
		WildcardSubscriptionAvailable: true,
		SubscriptionIDAvailable:       true,
		SharedSubscriptionAvailable:   true,
	})
	if len(b) < 8 {
		t.Fatalf("CONNACK v5 too short: %d", len(b))
	}
	// Property id 0x22 Topic Alias Maximum must appear only on success.
	if bytes.Index(b, []byte{0x22, 0x00, 0x40}) < 0 {
		t.Fatal("missing Topic Alias Maximum property")
	}
	if bytes.Index(b, []byte{0x21}) < 0 {
		t.Fatal("missing Receive Maximum property")
	}
}

func TestEncodeConnackV5FailureOmitsTopicAliasAndSessionPresent(t *testing.T) {
	b := EncodeConnackV5(true, ConnackReasonV5BadUsernameOrPassword, ConnackMQTT5Options{TopicAliasMax: 99})
	if bytes.Contains(b, []byte{0x22}) {
		t.Fatal("Topic Alias property must not be sent on CONNACK failure")
	}
	// Session Present flag byte follows remaining length in variable header: skip fixed hdr + remlen then flags byte.
	r := bytes.NewReader(b)
	fh, err := ReadFixed(r)
	if err != nil {
		t.Fatal(err)
	}
	vh := make([]byte, fh.RemainingLength)
	if _, err := io.ReadFull(r, vh); err != nil {
		t.Fatal(err)
	}
	if len(vh) < 2 {
		t.Fatal("short CONNACK vh")
	}
	if vh[0]&0x01 != 0 {
		t.Fatal("Session Present must be 0 when Reason Code is not Success")
	}
	if vh[1] != ConnackReasonV5BadUsernameOrPassword {
		t.Fatalf("reason code: %#x", vh[1])
	}
}

func TestPeekConnectProtocolLevel(t *testing.T) {
	body := []byte{0x00, 0x04, 'M', 'Q', 'T', 'T', V50}
	lev, ok := PeekConnectProtocolLevel(body)
	if !ok || lev != V50 {
		t.Fatalf("peek MQTT v5: ok=%v lev=%d", ok, lev)
	}
	body311 := []byte{0x00, 0x04, 'M', 'Q', 'T', 'T', V311}
	lev, ok = PeekConnectProtocolLevel(body311)
	if !ok || lev != V311 {
		t.Fatalf("peek v311: ok=%v lev=%d", ok, lev)
	}
}

func TestUnsubackReasonByteDiagnostics(t *testing.T) {
	if got := UnsubackReasonByte(UnsubOK); got != UnsubackSuccessReason {
		t.Fatalf("success want %#x got %#x", UnsubackSuccessReason, got)
	}
	if got := UnsubackReasonByte(UnsubDeniedInvalidTopicFilter); got != SubackTopicFilterInvalid {
		t.Fatalf("invalid filter want %#x got %#x", SubackTopicFilterInvalid, got)
	}
	if got := UnsubackReasonByte(UnsubDeniedNotAuthorized); got != ReasonNotAuthorized {
		t.Fatalf("not auth want %#x got %#x", ReasonNotAuthorized, got)
	}
}

func TestEncodeUnsubackV5MatchesFilterOrder(t *testing.T) {
	codes := []byte{UnsubackSuccessReason, SubackTopicFilterInvalid, ReasonNotAuthorized}
	b := EncodeUnsubackV5(42, nil, codes)
	br := bytes.NewReader(b)
	fh, err := ReadFixed(br)
	if err != nil {
		t.Fatal(err)
	}
	rest := make([]byte, fh.RemainingLength)
	if _, err := io.ReadFull(br, rest); err != nil {
		t.Fatal(err)
	}
	if len(rest) < 2+1+len(codes) {
		t.Fatalf("short UNSUBACK body: %d", len(rest))
	}
	pid := uint16(rest[0])<<8 | uint16(rest[1])
	if pid != 42 {
		t.Fatalf("packet id %d", pid)
	}
	if rest[2] != 0 {
		t.Fatalf("property length expected 0, got %d", rest[2])
	}
	for i := range codes {
		if rest[3+i] != codes[i] {
			t.Fatalf("reason[%d] %#x != %#x", i, rest[3+i], codes[i])
		}
	}
}

func TestSubackReasonByteDiagnostics(t *testing.T) {
	tests := []struct {
		name   string
		ver    byte
		qos    byte
		deny   SubackDenyReason
		want   byte
	}{
		{"v5 qos1 ok", V50, 1, SubackOK, SubackGrantedQoS1},
		{"v5 invalid filter", V50, 1, SubackDeniedInvalidTopicFilter, SubackTopicFilterInvalid},
		{"v5 acl", V50, 2, SubackDeniedNotAuthorized, ReasonNotAuthorized},
		{"v5 plugin", V50, 0, SubackDeniedPlugin, SubackImplementationSpecific},
		{"v311 fail maps to 80", V311, 1, SubackDeniedNotAuthorized, Subv311SubackFailure},
		{"v311 ok qos2", V311, 2, SubackOK, SubackGrantedQoS2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SubackReasonByte(tt.ver, tt.qos, tt.deny); got != tt.want {
				t.Fatalf("want %#x got %#x", tt.want, got)
			}
		})
	}
}

func TestConnackReasonV5ForDecodeError(t *testing.T) {
	if got := ConnackReasonV5ForDecodeError(ErrUnsupportedVersion); got != ConnackReasonV5UnsupportedProtocolVersion {
		t.Fatalf("unsupported version -> %#x", got)
	}
	if got := ConnackReasonV5ForDecodeError(ErrProtocolViolation); got != ConnackReasonV5ProtocolError {
		t.Fatalf("protocol violation -> %#x", got)
	}
	if got := ConnackReasonV5ForDecodeError(errors.New("other")); got != ConnackReasonV5MalformedPacket {
		t.Fatalf("generic -> %#x", got)
	}
}

func TestEncodePublishV5RoundTrip(t *testing.T) {
	topic := "sensors/temp"
	payload := []byte("22.5")
	raw := EncodePublishV5(topic, payload, 1, false, 99, false, nil)
	r := bytes.NewReader(raw)
	fh, err := ReadFixed(r)
	if err != nil {
		t.Fatal(err)
	}
	body := make([]byte, fh.RemainingLength)
	if _, err := io.ReadFull(r, body); err != nil {
		t.Fatal(err)
	}
	pkt, err := DecodePublish(V50, fh, body)
	if err != nil {
		t.Fatal(err)
	}
	if pkt.Topic != topic || string(pkt.Payload) != string(payload) || pkt.QoS != 1 || pkt.PacketID != 99 {
		t.Fatalf("decoded mismatch: %+v", pkt)
	}
}

func TestDecodeSubscribeV5SkipsProperties(t *testing.T) {
	body := []byte{
		0x00, 0x05, // packet id 5
		0x00,             // properties length 0
		0x00, 0x03, 'a', '/', 'b',
		0x02, // v5 subscription options: max QoS 2
	}
	id, subs, err := DecodeSubscribe(V50, body)
	if err != nil {
		t.Fatal(err)
	}
	if id != 5 || len(subs) != 1 || subs[0].Filter != "a/b" || subs[0].QoS != 2 {
		t.Fatalf("got id=%d subs=%+v", id, subs)
	}
}

func TestDecodeSubscribeV311NoPropertySection(t *testing.T) {
	body := []byte{
		0x00, 0x03,
		0x00, 0x01, 'x',
		0x01,
	}
	id, subs, err := DecodeSubscribe(V311, body)
	if err != nil {
		t.Fatal(err)
	}
	if id != 3 || len(subs) != 1 || subs[0].Filter != "x" || subs[0].QoS != 1 {
		t.Fatalf("got %+v", subs)
	}
}

func TestEncodeDisconnectV311(t *testing.T) {
	b := EncodeDisconnectV311()
	if len(b) != 2 || b[0] != byte(DISCONNECT)<<4 || b[1] != 0 {
		t.Fatalf("v311 DISCONNECT: % x", b)
	}
}

func TestEncodeDisconnectV5QuotaReason(t *testing.T) {
	b := EncodeDisconnectV5(ReasonQuotaExceeded, nil)
	r := bytes.NewReader(b)
	fh, err := ReadFixed(r)
	if err != nil {
		t.Fatal(err)
	}
	body := make([]byte, fh.RemainingLength)
	if _, err := io.ReadFull(r, body); err != nil {
		t.Fatal(err)
	}
	if len(body) < 2 || body[0] != ReasonQuotaExceeded || body[1] != 0 {
		t.Fatalf("DISCONNECT v5 body: % x", body)
	}
}

func mqttPacketBody(t *testing.T, full []byte) []byte {
	t.Helper()
	r := bytes.NewReader(full)
	fh, err := ReadFixed(r)
	if err != nil {
		t.Fatal(err)
	}
	body := make([]byte, fh.RemainingLength)
	if _, err := io.ReadFull(r, body); err != nil {
		t.Fatal(err)
	}
	return body
}

func TestDecodeDisconnectV5RoundTrip(t *testing.T) {
	props := []byte{0x11, 0x00, 0x00, 0x00, 0x3c}
	full := EncodeDisconnectV5(ReasonSuccess, props)
	pkt, err := DecodeDisconnect(V50, mqttPacketBody(t, full))
	if err != nil {
		t.Fatal(err)
	}
	if pkt.ReasonCode != ReasonSuccess || pkt.SessionExpiryInterval != 60 {
		t.Fatalf("got %+v", pkt)
	}
}

func TestDecodeDisconnectV311RejectsPayload(t *testing.T) {
	if _, err := DecodeDisconnect(V311, []byte{0x00}); err == nil {
		t.Fatal("expected protocol violation")
	}
}

func TestDecodeDisconnectV50Truncated(t *testing.T) {
	if _, err := DecodeDisconnect(V50, []byte{0x04}); err == nil {
		t.Fatal("expected malformed (missing property length)")
	}
}

func TestEncodeSubackV5DecodeReasons(t *testing.T) {
	codes := []byte{0x01, 0x00}
	b := EncodeSubackV5(7, nil, codes)
	br := bytes.NewReader(b)
	fh, err := ReadFixed(br)
	if err != nil {
		t.Fatal(err)
	}
	rest := make([]byte, fh.RemainingLength)
	if _, err := io.ReadFull(br, rest); err != nil {
		t.Fatal(err)
	}
	if len(rest) < 2+1+len(codes) {
		t.Fatalf("short body: %d", len(rest))
	}
	if uint16(rest[0])<<8|uint16(rest[1]) != 7 {
		t.Fatalf("packet id")
	}
	if rest[2] != 0x00 {
		t.Fatalf("prop len should be 0, got %x", rest[2])
	}
	for i := range codes {
		if rest[3+i] != codes[i] {
			t.Fatalf("code %d", i)
		}
	}
}
