package trail

import (
	"context"
	"sort"
	"sync"
	"time"
)

// MemoryStore is an in-memory Store implementation for tests and local use.
type MemoryStore struct {
	mu    sync.RWMutex
	flows map[FlowID]Snapshot
}

// NewMemoryStore creates an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		flows: make(map[FlowID]Snapshot),
	}
}

// Create inserts a new snapshot with revision 1.
func (s *MemoryStore) Create(_ context.Context, snapshot Snapshot) error {
	if snapshot.ID == "" || snapshot.Type == "" || snapshot.State == "" {
		return ErrInvalidSnapshot
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.flows[snapshot.ID]; exists {
		return ErrFlowConflict
	}
	snapshot.Revision = 1
	snapshot.Metadata = cloneStringMap(snapshot.Metadata)
	snapshot.Data = cloneBytes(snapshot.Data)
	s.flows[snapshot.ID] = snapshot
	return nil
}

// Get returns a snapshot by ID.
func (s *MemoryStore) Get(_ context.Context, id FlowID) (Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snapshot, exists := s.flows[id]
	if !exists {
		return Snapshot{}, ErrFlowNotFound
	}
	return cloneSnapshot(snapshot), nil
}

// Update persists a snapshot when its revision matches the current revision.
func (s *MemoryStore) Update(_ context.Context, snapshot Snapshot) error {
	if snapshot.ID == "" {
		return ErrInvalidSnapshot
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	current, exists := s.flows[snapshot.ID]
	if !exists {
		return ErrFlowNotFound
	}
	if snapshot.Revision != current.Revision {
		return ErrFlowConflict
	}
	snapshot.Revision = current.Revision + 1
	snapshot.CreatedAt = current.CreatedAt
	snapshot.Metadata = cloneStringMap(snapshot.Metadata)
	snapshot.Data = cloneBytes(snapshot.Data)
	s.flows[snapshot.ID] = snapshot
	return nil
}

// Delete removes a snapshot by ID.
func (s *MemoryStore) Delete(_ context.Context, id FlowID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.flows[id]; !exists {
		return ErrFlowNotFound
	}
	delete(s.flows, id)
	return nil
}

// List returns snapshots matching query in stable CreatedAt/ID order.
func (s *MemoryStore) List(_ context.Context, query Query) ([]Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]Snapshot, 0, len(s.flows))
	for _, snapshot := range s.flows {
		if !matchesQuery(snapshot, query) {
			continue
		}
		out = append(out, cloneSnapshot(snapshot))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	if query.Limit > 0 && len(out) > query.Limit {
		out = out[:query.Limit]
	}
	return out, nil
}

// DeleteExpired removes snapshots expiring at or before before.
func (s *MemoryStore) DeleteExpired(_ context.Context, before time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	deleted := 0
	for id, snapshot := range s.flows {
		if snapshot.ExpiresAt.IsZero() || snapshot.ExpiresAt.After(before) {
			continue
		}
		delete(s.flows, id)
		deleted++
	}
	return deleted, nil
}

func matchesQuery(snapshot Snapshot, query Query) bool {
	if query.Type != "" && snapshot.Type != query.Type {
		return false
	}
	if query.SubjectID != "" && snapshot.SubjectID != query.SubjectID {
		return false
	}
	if query.State != "" && snapshot.State != query.State {
		return false
	}
	if query.Completed != nil && snapshot.Completed != *query.Completed {
		return false
	}
	if query.Cancelled != nil && snapshot.Cancelled != *query.Cancelled {
		return false
	}
	if !query.ExpiresBefore.IsZero() {
		if snapshot.ExpiresAt.IsZero() || !snapshot.ExpiresAt.Before(query.ExpiresBefore) {
			return false
		}
	}
	if !query.ExpiresAfter.IsZero() {
		if snapshot.ExpiresAt.IsZero() || !snapshot.ExpiresAt.After(query.ExpiresAfter) {
			return false
		}
	}
	return true
}

// MemoryIdempotencyStore is an in-memory IdempotencyStore implementation.
type MemoryIdempotencyStore struct {
	mu      sync.RWMutex
	results map[IdempotencyKey]Result
}

// NewMemoryIdempotencyStore creates an empty in-memory idempotency store.
func NewMemoryIdempotencyStore() *MemoryIdempotencyStore {
	return &MemoryIdempotencyStore{
		results: make(map[IdempotencyKey]Result),
	}
}

func (s *MemoryIdempotencyStore) Get(_ context.Context, key IdempotencyKey) (Result, bool, error) {
	if key == "" {
		return Result{}, false, ErrInvalidIdempotencyKey
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	result, ok := s.results[key]
	return cloneResult(result), ok, nil
}

func (s *MemoryIdempotencyStore) Put(_ context.Context, key IdempotencyKey, result Result) error {
	if key == "" {
		return ErrInvalidIdempotencyKey
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.results[key] = cloneResult(result)
	return nil
}
