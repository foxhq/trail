package trail

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"time"
)

// Engine coordinates specs, storage, idempotency, effects, and lifecycle events.
type Engine interface {
	Begin(ctx context.Context, req BeginRequest) (Result, error)
	Submit(ctx context.Context, req SubmitRequest) (Result, error)
	Get(ctx context.Context, id FlowID) (Result, error)
	Cancel(ctx context.Context, req CancelRequest) (Result, error)
}

// Clock supplies engine time.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now().UTC()
}

// IDGenerator supplies new flow IDs.
type IDGenerator interface {
	NextFlowID() (FlowID, error)
}

type randomIDGenerator struct{}

func (randomIDGenerator) NextFlowID() (FlowID, error) {
	buf := make([]byte, 18)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return FlowID(base64.RawURLEncoding.EncodeToString(buf)), nil
}

// EngineOption configures an Engine.
type EngineOption interface {
	apply(*engine)
}

type engineOptionFunc func(*engine)

func (f engineOptionFunc) apply(e *engine) {
	f(e)
}

// WithClock sets the engine clock.
func WithClock(clock Clock) EngineOption {
	return engineOptionFunc(func(e *engine) {
		if clock != nil {
			e.clock = clock
		}
	})
}

// WithIDGenerator sets the flow ID generator.
func WithIDGenerator(generator IDGenerator) EngineOption {
	return engineOptionFunc(func(e *engine) {
		if generator != nil {
			e.idgen = generator
		}
	})
}

// WithUnitOfWork sets the transaction or consistency boundary used by Begin,
// Submit, and Cancel.
func WithUnitOfWork(unitOfWork UnitOfWork) EngineOption {
	return engineOptionFunc(func(e *engine) {
		if unitOfWork != nil {
			e.unitOfWork = unitOfWork
		}
	})
}

// WithEffectSink sets the sink that publishes effects returned by specs.
func WithEffectSink(sink EffectSink) EngineOption {
	return engineOptionFunc(func(e *engine) {
		if sink != nil {
			e.effects = sink
		}
	})
}

// WithIdempotencyStore sets the idempotency result store.
func WithIdempotencyStore(store IdempotencyStore) EngineOption {
	return engineOptionFunc(func(e *engine) {
		e.idempotency = store
	})
}

// WithObserver registers a lifecycle observer.
func WithObserver(observer Observer) EngineOption {
	return engineOptionFunc(func(e *engine) {
		if observer != nil {
			e.observers = append(e.observers, observer)
		}
	})
}

type engine struct {
	store       Store
	registry    *SpecRegistry
	clock       Clock
	idgen       IDGenerator
	unitOfWork  UnitOfWork
	effects     EffectSink
	idempotency IdempotencyStore
	observers   []Observer
}

// NewEngine creates an engine for a snapshot store and spec registry.
func NewEngine(store Store, registry *SpecRegistry, opts ...EngineOption) Engine {
	e := &engine{
		store:      store,
		registry:   registry,
		clock:      realClock{},
		idgen:      randomIDGenerator{},
		unitOfWork: NoopUnitOfWork{},
		effects:    NoopEffectSink{},
	}
	for _, opt := range opts {
		if opt != nil {
			opt.apply(e)
		}
	}
	return e
}

func (e *engine) Begin(ctx context.Context, req BeginRequest) (result Result, err error) {
	if err := e.validate(); err != nil {
		return Result{}, err
	}
	if req.Type == "" {
		return Result{}, fmt.Errorf("%w: begin type is empty", ErrInvalidFlow)
	}

	startEvent := Event{
		Name: EventBeginStarted,
		Flow: Result{
			Type:      req.Type,
			SubjectID: req.SubjectID,
			ExpiresAt: req.ExpiresAt,
			Metadata:  cloneStringMap(req.Metadata),
		},
	}
	e.observe(ctx, startEvent)
	defer func() {
		if err != nil {
			e.observe(ctx, Event{Name: EventBeginFailed, Flow: result, Error: err})
		}
	}()

	var effects []Effect
	err = e.unitOfWork.Do(ctx, func(ctx context.Context) error {
		idemKey := scopedIdempotencyKey("begin:"+string(req.Type), req.IdempotencyKey)
		if e.idempotency != nil && idemKey != "" {
			stored, ok, err := e.idempotency.Get(ctx, idemKey)
			if err != nil || ok {
				result = stored
				return err
			}
		}

		spec, ok := e.registry.get(req.Type)
		if !ok {
			return fmt.Errorf("%w: %s", ErrSpecNotFound, req.Type)
		}

		id, err := e.idgen.NextFlowID()
		if err != nil {
			return err
		}
		now := e.clock.Now()

		snap, nextResult, nextEffects, err := spec.begin(ctx, id, req, now)
		if err != nil {
			return err
		}
		if err := e.store.Create(ctx, *snap); err != nil {
			return err
		}

		created, err := e.store.Get(ctx, snap.ID)
		if err != nil {
			return err
		}
		nextResult.Revision = created.Revision
		result = nextResult
		effects = nextEffects

		if len(nextEffects) > 0 {
			if err := e.effects.Publish(ctx, nextResult, nextEffects); err != nil {
				err = fmt.Errorf("%w: %v", ErrEffectPublishFailed, err)
				e.observe(ctx, Event{Name: EventEffectPublishFailed, Flow: nextResult, Error: err})
				return err
			}
		}
		if e.idempotency != nil && idemKey != "" {
			if err := e.idempotency.Put(ctx, idemKey, nextResult); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return result, err
	}
	if len(effects) > 0 {
		e.observe(ctx, Event{Name: EventEffectPublished, Flow: result})
	}
	e.observe(ctx, Event{Name: EventBeginCompleted, Flow: result})
	return result, nil
}

func (e *engine) Submit(ctx context.Context, req SubmitRequest) (result Result, err error) {
	if err := e.validate(); err != nil {
		return Result{}, err
	}
	if req.FlowID == "" {
		return Result{}, fmt.Errorf("%w: submit flow id is empty", ErrInvalidFlow)
	}
	if req.Action == nil {
		return Result{}, fmt.Errorf("%w: submit action is nil", ErrUnsupportedAction)
	}

	actionType := req.Action.Type()
	e.observe(ctx, Event{
		Name:   EventSubmitStarted,
		Flow:   Result{ID: req.FlowID},
		Action: actionType,
	})
	defer func() {
		if err != nil {
			e.observe(ctx, Event{Name: EventSubmitFailed, Flow: result, Action: actionType, Error: err})
		}
	}()

	var effects []Effect
	err = e.unitOfWork.Do(ctx, func(ctx context.Context) error {
		idemKey := scopedIdempotencyKey("submit:"+string(req.FlowID), req.IdempotencyKey)
		if e.idempotency != nil && idemKey != "" {
			stored, ok, err := e.idempotency.Get(ctx, idemKey)
			if err != nil || ok {
				result = stored
				return err
			}
		}

		snap, err := e.store.Get(ctx, req.FlowID)
		if err != nil {
			return err
		}
		now := e.clock.Now()
		if snap.Cancelled {
			return ErrFlowCancelled
		}
		if snap.Completed {
			return ErrFlowCompleted
		}
		if snap.IsExpiredAt(now) {
			return ErrFlowExpired
		}

		spec, ok := e.registry.get(snap.Type)
		if !ok {
			return fmt.Errorf("%w: %s", ErrSpecNotFound, snap.Type)
		}

		next, nextResult, nextEffects, err := spec.submit(ctx, snap, req.Action, now)
		if err != nil {
			return err
		}
		if err := e.store.Update(ctx, *next); err != nil {
			return err
		}

		updated, err := e.store.Get(ctx, next.ID)
		if err != nil {
			return err
		}
		nextResult.Revision = updated.Revision
		result = nextResult
		effects = nextEffects

		if len(nextEffects) > 0 {
			if err := e.effects.Publish(ctx, nextResult, nextEffects); err != nil {
				err = fmt.Errorf("%w: %v", ErrEffectPublishFailed, err)
				e.observe(ctx, Event{Name: EventEffectPublishFailed, Flow: nextResult, Action: actionType, Error: err})
				return err
			}
		}
		if e.idempotency != nil && idemKey != "" {
			if err := e.idempotency.Put(ctx, idemKey, nextResult); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return result, err
	}
	if len(effects) > 0 {
		e.observe(ctx, Event{Name: EventEffectPublished, Flow: result, Action: actionType})
	}
	e.observe(ctx, Event{Name: EventSubmitCompleted, Flow: result, Action: actionType})
	return result, nil
}

func (e *engine) Get(ctx context.Context, id FlowID) (Result, error) {
	if err := e.validate(); err != nil {
		return Result{}, err
	}
	if id == "" {
		return Result{}, fmt.Errorf("%w: get flow id is empty", ErrInvalidFlow)
	}
	snap, err := e.store.Get(ctx, id)
	if err != nil {
		return Result{}, err
	}
	return snap.View(), nil
}

func (e *engine) Cancel(ctx context.Context, req CancelRequest) (result Result, err error) {
	if err := e.validate(); err != nil {
		return Result{}, err
	}
	if req.FlowID == "" {
		return Result{}, fmt.Errorf("%w: cancel flow id is empty", ErrInvalidFlow)
	}
	e.observe(ctx, Event{Name: EventCancelStarted, Flow: Result{ID: req.FlowID}})
	defer func() {
		if err != nil {
			e.observe(ctx, Event{Name: EventCancelFailed, Flow: result, Error: err})
		}
	}()

	err = e.unitOfWork.Do(ctx, func(ctx context.Context) error {
		snap, err := e.store.Get(ctx, req.FlowID)
		if err != nil {
			return err
		}
		if snap.Cancelled {
			return ErrFlowCancelled
		}
		if snap.Completed {
			return ErrFlowCompleted
		}
		snap.Cancelled = true
		snap.Completed = true
		snap.CancelReason = req.Reason
		snap.UpdatedAt = e.clock.Now()
		if err := e.store.Update(ctx, snap); err != nil {
			return err
		}
		updated, err := e.store.Get(ctx, req.FlowID)
		if err != nil {
			return err
		}
		result = updated.View()
		return nil
	})
	if err != nil {
		return result, err
	}
	e.observe(ctx, Event{Name: EventCancelCompleted, Flow: result})
	return result, nil
}

func (e *engine) validate() error {
	if e.store == nil {
		return fmt.Errorf("%w: store is nil", ErrInvalidFlow)
	}
	if e.registry == nil {
		return fmt.Errorf("%w: registry is nil", ErrInvalidFlow)
	}
	return nil
}

func scopedIdempotencyKey(scope string, key IdempotencyKey) IdempotencyKey {
	if key == "" {
		return ""
	}
	return IdempotencyKey(scope + ":" + string(key))
}

func (e *engine) observe(ctx context.Context, event Event) {
	for _, observer := range e.observers {
		if observer == nil {
			continue
		}
		func() {
			defer func() {
				_ = recover()
			}()
			observer.Observe(ctx, event)
		}()
	}
}
