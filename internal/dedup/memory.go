package dedup

import "sync"

type Memory struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

func NewMemory() *Memory {
	return &Memory{seen: map[string]struct{}{}}
}

// SeenOrAdd returns true if id was already present.
func (m *Memory) SeenOrAdd(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.seen[id]; ok {
		return true
	}
	m.seen[id] = struct{}{}
	return false
}
