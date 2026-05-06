package plugin

import "testing"

type testHook struct {
	connects int
}

func (h *testHook) Name() string { return "test" }
func (h *testHook) OnClientConnect(string, string) error {
	h.connects++
	return nil
}
func (h *testHook) OnClientDisconnect(string, string) error            { return nil }
func (h *testHook) OnPublish(string, string, []byte, byte, bool) error { return nil }
func (h *testHook) OnSubscribe(string, string, byte) error             { return nil }

func TestManagerDispatchesHooks(t *testing.T) {
	m := NewManager()
	h := &testHook{}
	m.Register(h)
	if err := m.OnClientConnect("c1", "u1"); err != nil {
		t.Fatalf("OnClientConnect error: %v", err)
	}
	if h.connects != 1 {
		t.Fatalf("expected hook connect count to be 1, got %d", h.connects)
	}
}
