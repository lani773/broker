package cluster

import "testing"

func TestPartitionOwnership(t *testing.T) {
	m := NewManager("broker-a", 16)
	p := m.PartitionForTopic("devices/site1/sensor")
	if p < 0 || p >= 16 {
		t.Fatalf("partition out of range: %d", p)
	}
	if !m.IsLocalOwner("devices/site1/sensor") {
		t.Fatalf("expected local owner by default")
	}
	m.SetOwner(p, "broker-b")
	if m.IsLocalOwner("devices/site1/sensor") {
		t.Fatalf("expected ownership transfer")
	}
}
