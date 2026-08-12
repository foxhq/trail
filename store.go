package trail

import (
	"context"
	"time"
)

// Store persists flow snapshots.
type Store interface {
	Create(ctx context.Context, snapshot Snapshot) error
	Get(ctx context.Context, id FlowID) (Snapshot, error)
	// Update persists a snapshot and MUST reject stale revisions with ErrFlowConflict.
	Update(ctx context.Context, snapshot Snapshot) error
	Delete(ctx context.Context, id FlowID) error
}

// Query filters snapshots in stores that support listing.
type Query struct {
	Type          FlowType
	SubjectID     SubjectID
	State         FlowState
	Completed     *bool
	Cancelled     *bool
	ExpiresBefore time.Time
	ExpiresAfter  time.Time
	Limit         int
}

// QueryStore is an optional store extension for listing snapshots.
type QueryStore interface {
	Store
	List(ctx context.Context, query Query) ([]Snapshot, error)
}

// CleanupStore is an optional store extension for retention cleanup.
type CleanupStore interface {
	Store
	DeleteExpired(ctx context.Context, before time.Time) (int, error)
}

// IdempotencyStore stores previously completed engine results for idempotent requests.
type IdempotencyStore interface {
	Get(ctx context.Context, key IdempotencyKey) (Result, bool, error)
	Put(ctx context.Context, key IdempotencyKey, result Result) error
}

// UnitOfWork runs engine writes inside an application-owned transaction or
// consistency boundary.
//
// The same context is passed to the Trail store, flow handlers, app
// repositories, and effect sink.
type UnitOfWork interface {
	Do(ctx context.Context, fn func(ctx context.Context) error) error
}

// NoopUnitOfWork runs fn directly without opening a transaction.
type NoopUnitOfWork struct{}

func (NoopUnitOfWork) Do(ctx context.Context, fn func(ctx context.Context) error) error {
	return fn(ctx)
}

// EffectSink publishes effects returned by specs.
//
// In production this is usually an application-owned transactional outbox,
// event bus, or job queue adapter. For simple applications, EffectRouter can be
// used directly as an in-process sink.
type EffectSink interface {
	Publish(ctx context.Context, flow Result, effects []Effect) error
}

// NoopEffectSink ignores effects.
type NoopEffectSink struct{}

func (NoopEffectSink) Publish(context.Context, Result, []Effect) error {
	return nil
}
