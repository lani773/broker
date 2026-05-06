package plugin

import "sync"

// Hook allows extensions to intercept key broker events.
type Hook interface {
	Name() string
	OnClientConnect(clientID, username string) error
	OnClientDisconnect(clientID, username string) error
	OnPublish(clientID, topic string, payload []byte, qos byte, retain bool) error
	OnSubscribe(clientID, filter string, qos byte) error
}

type noopHook struct{}

func (noopHook) Name() string { return "noop" }
func (noopHook) OnClientConnect(string, string) error {
	return nil
}
func (noopHook) OnClientDisconnect(string, string) error {
	return nil
}
func (noopHook) OnPublish(string, string, []byte, byte, bool) error {
	return nil
}
func (noopHook) OnSubscribe(string, string, byte) error {
	return nil
}

// Manager is the plugin registry and dispatcher.
type Manager struct {
	mu    sync.RWMutex
	hooks []Hook
}

func NewManager() *Manager {
	return &Manager{hooks: []Hook{noopHook{}}}
}

func (m *Manager) Register(h Hook) {
	if h == nil {
		return
	}
	m.mu.Lock()
	m.hooks = append(m.hooks, h)
	m.mu.Unlock()
}

func (m *Manager) call(fn func(Hook) error) error {
	m.mu.RLock()
	hooks := make([]Hook, len(m.hooks))
	copy(hooks, m.hooks)
	m.mu.RUnlock()
	for _, h := range hooks {
		if err := fn(h); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) OnClientConnect(clientID, username string) error {
	return m.call(func(h Hook) error { return h.OnClientConnect(clientID, username) })
}

func (m *Manager) OnClientDisconnect(clientID, username string) error {
	return m.call(func(h Hook) error { return h.OnClientDisconnect(clientID, username) })
}

func (m *Manager) OnPublish(clientID, topic string, payload []byte, qos byte, retain bool) error {
	return m.call(func(h Hook) error { return h.OnPublish(clientID, topic, payload, qos, retain) })
}

func (m *Manager) OnSubscribe(clientID, filter string, qos byte) error {
	return m.call(func(h Hook) error { return h.OnSubscribe(clientID, filter, qos) })
}
