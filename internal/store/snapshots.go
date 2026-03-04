package store

import (
	"sync"
)

type SnapshotStore interface {
	Get(provider, entityType, entityID string) (map[string]any, bool)
	Put(provider, entityType, entityID string, snapshot map[string]any) error
}

type InMemorySnapshotStore struct {
	mu   sync.RWMutex
	data map[string]map[string]any
}

func NewInMemorySnapshotStore() *InMemorySnapshotStore {
	return &InMemorySnapshotStore{data: make(map[string]map[string]any)}
}

func (s *InMemorySnapshotStore) key(provider, entityType, entityID string) string {
	return provider + ":" + entityType + ":" + entityID
}

func (s *InMemorySnapshotStore) Get(provider, entityType, entityID string) (map[string]any, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[s.key(provider, entityType, entityID)]
	if !ok {
		return nil, false
	}
	cpy := make(map[string]any, len(v))
	for k, val := range v {
		cpy[k] = val
	}
	return cpy, true
}

func (s *InMemorySnapshotStore) Put(provider, entityType, entityID string, snapshot map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cpy := make(map[string]any, len(snapshot))
	for k, val := range snapshot {
		cpy[k] = val
	}
	s.data[s.key(provider, entityType, entityID)] = cpy
	return nil
}
