package protocol

import "testing"

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
