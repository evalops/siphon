package store

import "sync"

type CheckpointStore interface {
	Get(provider string) (string, bool)
	Set(provider, checkpoint string) error
}

type InMemoryCheckpointStore struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewInMemoryCheckpointStore() *InMemoryCheckpointStore {
	return &InMemoryCheckpointStore{data: make(map[string]string)}
}

func (s *InMemoryCheckpointStore) Get(provider string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[provider]
	return v, ok
}

func (s *InMemoryCheckpointStore) Set(provider, checkpoint string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[provider] = checkpoint
	return nil
}
