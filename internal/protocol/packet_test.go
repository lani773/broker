package protocol

import (
	"bytes"
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
	b := EncodeConnackV5(true, ConnAccepted, 64)
	if len(b) < 8 {
		t.Fatalf("CONNACK v5 too short: %d", len(b))
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
