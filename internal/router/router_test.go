package router

import (
	"sync/atomic"
	"testing"

	"go.uber.org/zap"
)

func TestSharedSubscriptionRoundRobin(t *testing.T) {
	r := New(zap.NewNop())
	var aCount, bCount atomic.Int32
	r.Subscribe("$share/g/sensors/+", &Subscriber{
		ClientID: "a",
		QoS:      1,
		Deliver: func(string, []byte, byte, bool, uint16) {
			aCount.Add(1)
		},
	})
	r.Subscribe("$share/g/sensors/+", &Subscriber{
		ClientID: "b",
		QoS:      1,
		Deliver: func(string, []byte, byte, bool, uint16) {
			bCount.Add(1)
		},
	})
	const n = 100
	for i := 0; i < n; i++ {
		r.Publish("sensors/t1", []byte("x"), 1, false)
	}
	if got := aCount.Load() + bCount.Load(); int(got) != n {
		t.Fatalf("deliveries: want %d got %d", n, got)
	}
	if aCount.Load() == 0 || bCount.Load() == 0 {
		t.Fatalf("both subscribers should receive: a=%d b=%d", aCount.Load(), bCount.Load())
	}
}
