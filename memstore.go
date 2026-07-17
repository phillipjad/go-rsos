package rsos

import (
	"bytes"
	"context"
	"sync"
)

// MemStore is an in-memory Store for tests and small deployments. It is safe for concurrent use.
// Point operations are O(1); Scan and DeleteRange are O(n log n) in the store size, which is fine
// because the forest's hot path (tree navigation) is point Gets and batched Puts — ranges are only
// touched by Build, Prune, and RebuildBucket.
type MemStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// NewMemStore returns an empty in-memory Store.
func NewMemStore() *MemStore {
	return &MemStore{data: make(map[string][]byte)}
}

func (m *MemStore) Get(_ context.Context, key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	v, ok := m.data[string(key)]
	if !ok {
		return nil, nil
	}

	return append([]byte(nil), v...), nil
}

func (m *MemStore) Put(_ context.Context, key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data[string(key)] = append([]byte(nil), value...)

	return nil
}

func (m *MemStore) Delete(_ context.Context, key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.data, string(key))

	return nil
}

func (m *MemStore) WriteBatch(_ context.Context, ops []Op) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := range ops {
		if ops[i].Delete {
			delete(m.data, string(ops[i].Key))

			continue
		}

		m.data[string(ops[i].Key)] = append([]byte(nil), ops[i].Value...)
	}

	return nil
}

func (m *MemStore) DeleteRange(_ context.Context, start, end []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for k := range m.data {
		if inByteRange([]byte(k), start, end) {
			delete(m.data, k)
		}
	}

	return nil
}

func inByteRange(key, start, end []byte) bool {
	if bytes.Compare(key, start) < 0 {
		return false
	}

	if end != nil && bytes.Compare(key, end) >= 0 {
		return false
	}

	return true
}
