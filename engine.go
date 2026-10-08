package trail

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// Engine coordinates specs, storage, idempotency, durable effects, and
// lifecycle events.
type Engine interface {
	Begin(ctx context.Context, req BeginRequest) (Result, error)
	Submit(ctx context.Context, req SubmitRequest) (Result, error)
	Get(ctx context.Context, id FlowID) (Result, error)
	Cancel(ctx context.Context, req CancelRequest) (Result, error)
}

// Clock supplies engine time.
type Clock interface{ Now() time.Time }

type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }

// IDGenerator supplies new flow IDs.
type IDGenerator interface{ NextFlowID() (FlowID, error) }

type randomIDGenerator struct{}

func (randomIDGenerator) NextFlowID() (FlowID, error) {
	buf := make([]byte, 18)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return FlowID(base64.RawURLEncoding.EncodeToString(buf)), nil
}

// EngineOption configures an Engine.
type EngineOption interface{ apply(*engine) }
type engineOptionFunc func(*engine)

func (f engineOptionFunc) apply(e *engine) { f(e) }

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
// Submit, and Cancel. It is mandatory when transitions record effects.
func WithUnitOfWork(unitOfWork UnitOfWork) EngineOption {
	return engineOptionFunc(func(e *engine) {
		if unitOfWork != nil {
			e.unitOfWork = unitOfWork
			e.hasUnitOfWork = true
		}
	})
}

// WithEffectRecorder sets the transactional outbox or job-intent recorder.
// Trail never dispatches effects from the request path.
func WithEffectRecorder(recorder EffectRecorder) EngineOption {
	return engineOptionFunc(func(e *engine) { e.effectRecorder = recorder })
}

// WithIdempotencyStore sets the atomically reserving idempotency store.
func WithIdempotencyStore(store IdempotencyStore) EngineOption {
	return engineOptionFunc(func(e *engine) { e.idempotency = store })
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
	store          Store
	registry       *SpecRegistry
	clock          Clock
	idgen          IDGenerator
	unitOfWork     UnitOfWork
	hasUnitOfWork  bool
	effectRecorder EffectRecorder
	idempotency    IdempotencyStore
	observers      []Observer
}

// NewEngine creates an engine for a snapshot store and spec registry.
func NewEngine(store Store, registry *SpecRegistry, opts ...EngineOption) Engine {
	e := &engine{store: store, registry: registry, clock: realClock{}, idgen: randomIDGenerator{}, unitOfWork: NoopUnitOfWork{}}
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
	events := []Event{{Name: EventBeginStarted, Flow: Result{Type: req.Type, ExpiresAt: req.ExpiresAt}}}
	defer func() {
		if err != nil {
			events = append(events, Event{Name: EventBeginFailed, Flow: result, Error: err})
		}
		for _, event := range events {
			e.observe(ctx, event)
		}
	}()

	var recordedEffects bool
	err = e.unitOfWork.Do(ctx, func(ctx context.Context) error {
		reservation, replay, err := e.reserveBegin(ctx, req)
		if err != nil {
			return err
		}
		if replay != nil {
			result = *replay
			return nil
		}
		completed := false
		defer func() {
			if reservation != nil && !completed {
				_ = e.idempotency.Abort(ctx, reservation)
			}
		}()

		spec, ok := e.registry.get(req.Type)
		if !ok {
			return fmt.Errorf("%w: %s", ErrSpecNotFound, req.Type)
		}
		id, err := e.idgen.NextFlowID()
		if err != nil {
			return err
		}
		snapshot, effects, err := spec.begin(ctx, id, req, e.clock.Now())
		if err != nil {
			return err
		}
		if err := e.validateEffects(effects); err != nil {
			return err
		}
		committed, err := e.store.Create(ctx, *snapshot)
		if err != nil {
			return err
		}
		result = resultFromSnapshot(committed)
		if err := e.recordEffects(ctx, committed, effects); err != nil {
			events = append(events, Event{Name: EventEffectRecordFailed, Flow: result, Error: err})
			return err
		}
		recordedEffects = len(effects) > 0
		if reservation != nil {
			if err := e.idempotency.Complete(ctx, reservation, result); err != nil {
				return err
			}
			completed = true
		}
		return nil
	})
	if err != nil {
		return result, err
	}
	if recordedEffects {
		events = append(events, Event{Name: EventEffectRecorded, Flow: result})
	}
	events = append(events, Event{Name: EventBeginCompleted, Flow: result})
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
	events := []Event{{Name: EventSubmitStarted, Flow: Result{ID: req.FlowID}, Action: actionType}}
	defer func() {
		if err != nil {
			events = append(events, Event{Name: EventSubmitFailed, Flow: result, Action: actionType, Error: err})
		}
		for _, event := range events {
			e.observe(ctx, event)
		}
	}()

	var recordedEffects bool
	err = e.unitOfWork.Do(ctx, func(ctx context.Context) error {
		reservation, replay, err := e.reserveSubmit(ctx, req)
		if err != nil {
			return err
		}
		if replay != nil {
			result = *replay
			return nil
		}
		completed := false
		defer func() {
			if reservation != nil && !completed {
				_ = e.idempotency.Abort(ctx, reservation)
			}
		}()

		snapshot, err := e.store.Get(ctx, req.FlowID)
		if err != nil {
			return err
		}
		now := e.clock.Now()
		if snapshot.Cancelled {
			return ErrFlowCancelled
		}
		if snapshot.Completed {
			return ErrFlowCompleted
		}
		if snapshot.IsExpiredAt(now) {
			return ErrFlowExpired
		}
		spec, ok := e.registry.get(snapshot.Type)
		if !ok {
			return fmt.Errorf("%w: %s", ErrSpecNotFound, snapshot.Type)
		}
		next, effects, err := spec.submit(ctx, snapshot, req.Action, now)
		if err != nil {
			return err
		}
		if err := e.validateEffects(effects); err != nil {
			return err
		}
		committed, err := e.store.Update(ctx, *next)
		if err != nil {
			return err
		}
		result = resultFromSnapshot(committed)
		if err := e.recordEffects(ctx, committed, effects); err != nil {
			events = append(events, Event{Name: EventEffectRecordFailed, Flow: result, Action: actionType, Error: err})
			return err
		}
		recordedEffects = len(effects) > 0
		if reservation != nil {
			if err := e.idempotency.Complete(ctx, reservation, result); err != nil {
				return err
			}
			completed = true
		}
		return nil
	})
	if err != nil {
		return result, err
	}
	if recordedEffects {
		events = append(events, Event{Name: EventEffectRecorded, Flow: result, Action: actionType})
	}
	events = append(events, Event{Name: EventSubmitCompleted, Flow: result, Action: actionType})
	return result, nil
}

func (e *engine) Get(ctx context.Context, id FlowID) (Result, error) {
	if err := e.validate(); err != nil {
		return Result{}, err
	}
	if id == "" {
		return Result{}, fmt.Errorf("%w: get flow id is empty", ErrInvalidFlow)
	}
	snapshot, err := e.store.Get(ctx, id)
	if err != nil {
		return Result{}, err
	}
	return resultFromSnapshot(snapshot), nil
}

func (e *engine) Cancel(ctx context.Context, req CancelRequest) (result Result, err error) {
	if err := e.validate(); err != nil {
		return Result{}, err
	}
	if req.FlowID == "" {
		return Result{}, fmt.Errorf("%w: cancel flow id is empty", ErrInvalidFlow)
	}
	events := []Event{{Name: EventCancelStarted, Flow: Result{ID: req.FlowID}}}
	defer func() {
		if err != nil {
			events = append(events, Event{Name: EventCancelFailed, Flow: result, Error: err})
		}
		for _, event := range events {
			e.observe(ctx, event)
		}
	}()

	var recordedEffects bool
	err = e.unitOfWork.Do(ctx, func(ctx context.Context) error {
		reservation, replay, err := e.reserveCancel(ctx, req)
		if err != nil {
			return err
		}
		if replay != nil {
			result = *replay
			return nil
		}
		completed := false
		defer func() {
			if reservation != nil && !completed {
				_ = e.idempotency.Abort(ctx, reservation)
			}
		}()

		snapshot, err := e.store.Get(ctx, req.FlowID)
		if err != nil {
			return err
		}
		if snapshot.Cancelled {
			return ErrFlowCancelled
		}
		if snapshot.Completed {
			return ErrFlowCompleted
		}
		now := e.clock.Now()
		if snapshot.IsExpiredAt(now) {
			return ErrFlowExpired
		}
		spec, ok := e.registry.get(snapshot.Type)
		if !ok {
			return fmt.Errorf("%w: %s", ErrSpecNotFound, snapshot.Type)
		}
		next, effects, err := spec.cancel(ctx, snapshot, req, now)
		if err != nil {
			return err
		}
		if err := e.validateEffects(effects); err != nil {
			return err
		}
		committed, err := e.store.Update(ctx, *next)
		if err != nil {
			return err
		}
		result = resultFromSnapshot(committed)
		if err := e.recordEffects(ctx, committed, effects); err != nil {
			events = append(events, Event{Name: EventEffectRecordFailed, Flow: result, Error: err})
			return err
		}
		recordedEffects = len(effects) > 0
		if reservation != nil {
			if err := e.idempotency.Complete(ctx, reservation, result); err != nil {
				return err
			}
			completed = true
		}
		return nil
	})
	if err != nil {
		return result, err
	}
	if recordedEffects {
		events = append(events, Event{Name: EventEffectRecorded, Flow: result})
	}
	events = append(events, Event{Name: EventCancelCompleted, Flow: result})
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

func (e *engine) validateEffects(effects []Effect) error {
	if len(effects) == 0 {
		return nil
	}
	if !e.hasUnitOfWork {
		return ErrTransactionalEffectsRequired
	}
	if e.effectRecorder == nil {
		return ErrEffectRecorderRequired
	}
	for _, effect := range effects {
		if effect == nil || effect.Type() == "" {
			return ErrUnsupportedEffect
		}
	}
	return nil
}

func (e *engine) recordEffects(ctx context.Context, snapshot Snapshot, effects []Effect) error {
	if len(effects) == 0 {
		return nil
	}
	flow := EffectContext{FlowID: snapshot.ID, FlowType: snapshot.Type, SubjectID: snapshot.SubjectID, State: snapshot.State, Revision: snapshot.Revision}
	if err := e.effectRecorder.Record(ctx, flow, effects); err != nil {
		return fmt.Errorf("%w: %v", ErrEffectRecordFailed, err)
	}
	return nil
}

func (e *engine) reserveBegin(ctx context.Context, req BeginRequest) (*IdempotencyReservation, *Result, error) {
	if e.idempotency == nil || req.IdempotencyKey == "" {
		return nil, nil, nil
	}
	fingerprint, err := idempotencyFingerprint(req.IdempotencyFingerprint, req.Input)
	if err != nil {
		return nil, nil, err
	}
	return e.idempotency.Reserve(ctx, scopedIdempotencyKey("begin:"+string(req.Type)+":"+string(req.SubjectID), req.IdempotencyKey), fingerprint)
}

func (e *engine) reserveSubmit(ctx context.Context, req SubmitRequest) (*IdempotencyReservation, *Result, error) {
	if e.idempotency == nil || req.IdempotencyKey == "" {
		return nil, nil, nil
	}
	fingerprint, err := idempotencyFingerprint(req.IdempotencyFingerprint, req.Action)
	if err != nil {
		return nil, nil, err
	}
	return e.idempotency.Reserve(ctx, scopedIdempotencyKey("submit:"+string(req.FlowID), req.IdempotencyKey), fingerprint)
}

func (e *engine) reserveCancel(ctx context.Context, req CancelRequest) (*IdempotencyReservation, *Result, error) {
	if e.idempotency == nil || req.IdempotencyKey == "" {
		return nil, nil, nil
	}
	fingerprint, err := idempotencyFingerprint(req.IdempotencyFingerprint, struct {
		Reason string `json:"reason"`
	}{Reason: req.Reason})
	if err != nil {
		return nil, nil, err
	}
	return e.idempotency.Reserve(ctx, scopedIdempotencyKey("cancel:"+string(req.FlowID), req.IdempotencyKey), fingerprint)
}

func idempotencyFingerprint(explicit string, value any) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("%w: set IdempotencyFingerprint for non-JSON request data: %v", ErrInvalidIdempotencyKey, err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
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
			defer func() { _ = recover() }()
			observer.Observe(ctx, event)
		}()
	}
}
