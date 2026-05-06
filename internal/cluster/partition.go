package cluster

import (
	"hash/fnv"
	"sync"
)

// Manager tracks partition ownership and routing key placement.
type Manager struct {
	brokerID       string
	partitionCount int

	mu        sync.RWMutex
	ownership map[int]string
}

func NewManager(brokerID string, partitionCount int) *Manager {
	m := &Manager{
		brokerID:       brokerID,
		partitionCount: partitionCount,
		ownership:      make(map[int]string, partitionCount),
	}
	for i := 0; i < partitionCount; i++ {
		m.ownership[i] = brokerID
	}
	return m
}

func (m *Manager) PartitionForTopic(topic string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(topic))
	return int(h.Sum32() % uint32(m.partitionCount))
}

func (m *Manager) Owner(partition int) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if owner, ok := m.ownership[partition]; ok {
		return owner
	}
	return m.brokerID
}

func (m *Manager) SetOwner(partition int, brokerID string) {
	m.mu.Lock()
	m.ownership[partition] = brokerID
	m.mu.Unlock()
}

func (m *Manager) IsLocalOwner(topic string) bool {
	p := m.PartitionForTopic(topic)
	return m.Owner(p) == m.brokerID
}
