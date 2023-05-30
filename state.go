package falcon

import (
	"encoding/json"
	"sync"
)

type State[K comparable, V any] struct {
	mu   sync.RWMutex
	data map[K]V
}

func NewState[K comparable, V any]() *State[K, V] {
	return &State[K, V]{
		data: make(map[K]V),
	}
}

func (s *State[K, V]) Set(k K, v V) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[k] = v
}

func (s *State[K, V]) Get(k K) (V, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.data[k]
	return val, ok
}

func (s *State[K, V]) Delete(k K) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, k)
}

func (s *State[K, V]) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

func (s *State[K, V]) ForEach(f func(K, V)) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for key, val := range s.data {
		f(key, val)
	}
}

func (s *State[K, V]) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k := range s.data {
		delete(s.data, k)
	}
}

func (s *State[K, V]) MarshalJSON() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.data)
}

func (s *State[K, V]) String() string {
	b, _ := s.MarshalJSON()
	return string(b)
}
