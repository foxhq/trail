package trail

import (
	"context"
	"time"
)

// Store persists flow snapshots. Create and Update return the committed
// snapshot, including its assigned revision. Update MUST reject stale supplied
// revisions with ErrFlowConflict.
type Store interface {
	Create(ctx context.Context, snapshot Snapshot) (Snapshot, error)
	Get(ctx context.Context, id FlowID) (Snapshot, error)
	Update(ctx context.Context, snapshot Snapshot) (Snapshot, error)
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

// IdempotencyReservation is the exclusive claim returned by Reserve. It is
// valid only for the operation that obtained it.
type IdempotencyReservation struct {
	Key         IdempotencyKey
	Fingerprint string
	token       string
}

// IdempotencyStore atomically reserves an idempotency key and stores the final
// client-safe Result. Its implementation must share the same transaction as
// Store when both are configured on an Engine.
//
// Reserve returns a replay result for an already completed matching request.
// It returns ErrIdempotencyInProgress while a matching request is active and
// ErrIdempotencyKeyReuse when the key is used with a different fingerprint.
type IdempotencyStore interface {
	Reserve(ctx context.Context, key IdempotencyKey, fingerprint string) (reservation *IdempotencyReservation, replay *Result, err error)
	Complete(ctx context.Context, reservation *IdempotencyReservation, result Result) error
	Abort(ctx context.Context, reservation *IdempotencyReservation) error
}

// UnitOfWork runs engine writes inside an application-owned transaction or
// consistency boundary. The same context is passed to stores, handlers, and
// effect recorders.
type UnitOfWork interface {
	Do(ctx context.Context, fn func(ctx context.Context) error) error
}

// NoopUnitOfWork runs fn directly without opening a transaction. It is useful
// only for flows with no effects, or for tests.
type NoopUnitOfWork struct{}

func (NoopUnitOfWork) Do(ctx context.Context, fn func(ctx context.Context) error) error {
	return fn(ctx)
}

// EffectRecorder durably records effects in the current unit of work. It must
// not call remote systems directly; an outbox worker or job runner dispatches
// recorded effects after the transaction commits.
type EffectRecorder interface {
	Record(ctx context.Context, flow EffectContext, effects []Effect) error
}

// EffectRecorderFunc adapts a function to EffectRecorder.
type EffectRecorderFunc func(ctx context.Context, flow EffectContext, effects []Effect) error

func (f EffectRecorderFunc) Record(ctx context.Context, flow EffectContext, effects []Effect) error {
	return f(ctx, flow, effects)
}
