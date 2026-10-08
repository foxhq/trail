package trail

import (
	"context"
	"fmt"
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
func NewMemoryStore() *MemoryStore { return &MemoryStore{flows: make(map[FlowID]Snapshot)} }

// Create inserts a new snapshot with revision 1.
func (s *MemoryStore) Create(_ context.Context, snapshot Snapshot) (Snapshot, error) {
	if snapshot.ID == "" || snapshot.Type == "" || snapshot.State == "" {
		return Snapshot{}, ErrInvalidSnapshot
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.flows[snapshot.ID]; exists {
		return Snapshot{}, ErrFlowConflict
	}
	snapshot.Revision = 1
	snapshot = cloneSnapshot(snapshot)
	s.flows[snapshot.ID] = snapshot
	return cloneSnapshot(snapshot), nil
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

// Update persists a snapshot when its supplied revision matches the current
// revision and returns the committed snapshot at the next revision.
func (s *MemoryStore) Update(_ context.Context, snapshot Snapshot) (Snapshot, error) {
	if snapshot.ID == "" {
		return Snapshot{}, ErrInvalidSnapshot
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.flows[snapshot.ID]
	if !exists {
		return Snapshot{}, ErrFlowNotFound
	}
	if snapshot.Revision != current.Revision {
		return Snapshot{}, ErrFlowConflict
	}
	snapshot.Revision = current.Revision + 1
	snapshot.CreatedAt = current.CreatedAt
	snapshot = cloneSnapshot(snapshot)
	s.flows[snapshot.ID] = snapshot
	return cloneSnapshot(snapshot), nil
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
		if matchesQuery(snapshot, query) {
			out = append(out, cloneSnapshot(snapshot))
		}
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
	if !query.ExpiresBefore.IsZero() && (snapshot.ExpiresAt.IsZero() || !snapshot.ExpiresAt.Before(query.ExpiresBefore)) {
		return false
	}
	if !query.ExpiresAfter.IsZero() && (snapshot.ExpiresAt.IsZero() || !snapshot.ExpiresAt.After(query.ExpiresAfter)) {
		return false
	}
	return true
}

type memoryIdempotencyRecord struct {
	fingerprint string
	token       string
	result      Result
	completed   bool
}

// MemoryIdempotencyStore is an atomic in-memory IdempotencyStore for tests and
// local use. Production stores must reserve and complete in the same database
// transaction as the flow snapshot.
type MemoryIdempotencyStore struct {
	mu      sync.Mutex
	records map[IdempotencyKey]memoryIdempotencyRecord
	next    uint64
}

// NewMemoryIdempotencyStore creates an empty in-memory idempotency store.
func NewMemoryIdempotencyStore() *MemoryIdempotencyStore {
	return &MemoryIdempotencyStore{records: make(map[IdempotencyKey]memoryIdempotencyRecord)}
}

func (s *MemoryIdempotencyStore) Reserve(_ context.Context, key IdempotencyKey, fingerprint string) (*IdempotencyReservation, *Result, error) {
	if key == "" || fingerprint == "" {
		return nil, nil, ErrInvalidIdempotencyKey
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if record, exists := s.records[key]; exists {
		if record.fingerprint != fingerprint {
			return nil, nil, ErrIdempotencyKeyReuse
		}
		if !record.completed {
			return nil, nil, ErrIdempotencyInProgress
		}
		result := cloneResult(record.result)
		return nil, &result, nil
	}
	s.next++
	token := fmt.Sprintf("%d", s.next)
	s.records[key] = memoryIdempotencyRecord{fingerprint: fingerprint, token: token}
	return &IdempotencyReservation{Key: key, Fingerprint: fingerprint, token: token}, nil, nil
}

func (s *MemoryIdempotencyStore) Complete(_ context.Context, reservation *IdempotencyReservation, result Result) error {
	if reservation == nil || reservation.Key == "" || reservation.token == "" {
		return ErrInvalidIdempotencyKey
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, exists := s.records[reservation.Key]
	if !exists || record.token != reservation.token || record.fingerprint != reservation.Fingerprint {
		return ErrInvalidIdempotencyKey
	}
	record.result = cloneResult(result)
	record.completed = true
	s.records[reservation.Key] = record
	return nil
}

func (s *MemoryIdempotencyStore) Abort(_ context.Context, reservation *IdempotencyReservation) error {
	if reservation == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, exists := s.records[reservation.Key]
	if !exists || record.token != reservation.token || record.completed {
		return nil
	}
	delete(s.records, reservation.Key)
	return nil
}
