package trail

import (
	"encoding/json"
	"time"
)

type FlowID string
type FlowType string
type FlowState string
type SubjectID string
type ActionType string
type EffectType string
type IdempotencyKey string

const (
	// BeginState is the synthetic source state used for begin transitions.
	BeginState FlowState = "$begin"
	// AnyState matches any source state in a transition graph or handler table.
	AnyState FlowState = "*"

	// BeginAction is the synthetic action used for begin transitions.
	BeginAction ActionType = "$begin"
	// CancelAction is the synthetic action used for cancellation transitions.
	CancelAction ActionType = "$cancel"
	// AnyAction matches any action in a transition graph.
	AnyAction ActionType = "*"
)

// Action is an input submitted to a running flow.
type Action interface {
	Type() ActionType
}

// Effect is a durable intent produced by a transition. It must be recorded
// atomically with the flow snapshot, then dispatched by application-owned
// infrastructure after commit.
type Effect interface {
	Type() EffectType
}

// BeginRequest starts a new flow.
type BeginRequest struct {
	Type                   FlowType
	SubjectID              SubjectID
	Input                  any
	ExpiresAt              time.Time
	ExpiresIn              time.Duration
	IdempotencyKey         IdempotencyKey
	IdempotencyFingerprint string
	Metadata               map[string]string
}

// SubmitRequest applies an action to an existing flow.
type SubmitRequest struct {
	FlowID                 FlowID
	Action                 Action
	IdempotencyKey         IdempotencyKey
	IdempotencyFingerprint string
}

// CancelRequest cancels an active flow.
type CancelRequest struct {
	FlowID                 FlowID
	Reason                 string
	IdempotencyKey         IdempotencyKey
	IdempotencyFingerprint string
}

// BeginContext contains engine-generated context for Spec.Begin.
type BeginContext struct {
	ID        FlowID
	Type      FlowType
	SubjectID SubjectID
	Now       time.Time
	ExpiresAt time.Time
	Metadata  map[string]string
}

// CancelContext contains a caller-supplied cancellation reason.
type CancelContext struct {
	Reason string
}

// Flow is the current persisted workflow instance passed to action handlers.
// Its data is private to the flow implementation; it is never returned in a
// Result or observer event.
type Flow[D any] struct {
	ID        FlowID
	Type      FlowType
	SubjectID SubjectID
	State     FlowState
	Data      D
	ExpiresAt time.Time
	Completed bool
	Cancelled bool
	Metadata  map[string]string
	CreatedAt time.Time
	UpdatedAt time.Time
	Revision  int64
}

// IsExpiredAt reports whether the flow has an expiration before t.
func (f *Flow[D]) IsExpiredAt(t time.Time) bool {
	return !f.ExpiresAt.IsZero() && f.ExpiresAt.Before(t)
}

// Transition is the next typed state returned by a spec.
type Transition[D any] struct {
	State     FlowState
	Data      D
	Completed bool
	ExpiresAt time.Time
	viewSet   bool
	view      any
	Effects   []Effect
}

// To creates a transition to state with updated typed data.
func To[D any](state FlowState, data D) *Transition[D] {
	return &Transition[D]{State: state, Data: data}
}

// Done creates a completed transition to state with updated typed data.
func Done[D any](state FlowState, data D) *Transition[D] {
	return &Transition[D]{State: state, Data: data, Completed: true}
}

// WithView replaces the persisted client-safe view for the flow.
//
// A transition that does not call WithView or ClearView keeps the previous
// view. A view is always replaced as a whole; Trail intentionally does not
// merge or patch it.
func (t *Transition[D]) WithView(view any) *Transition[D] {
	t.viewSet = true
	t.view = view
	return t
}

// ClearView clears the persisted client-safe view for the flow.
func (t *Transition[D]) ClearView() *Transition[D] {
	t.viewSet = true
	t.view = nil
	return t
}

func (t *Transition[D]) viewValue() (any, bool) {
	if t == nil || !t.viewSet {
		return nil, false
	}
	return t.view, true
}

// WithEffects adds durable effect intents to the transition. The engine
// requires a transactional EffectRecorder before accepting a transition with
// effects.
func (t *Transition[D]) WithEffects(effects ...Effect) *Transition[D] {
	t.Effects = append(t.Effects, effects...)
	return t
}

// WithExpiry changes the flow expiration on a transition.
func (t *Transition[D]) WithExpiry(expiresAt time.Time) *Transition[D] {
	t.ExpiresAt = expiresAt
	return t
}

// Transitions declares allowed source-state/action/target-state edges.
type Transitions map[FlowState]ActionTransitions

// ActionTransitions maps an action to the allowed target states.
type ActionTransitions map[ActionType][]FlowState

// Allows reports whether a transition edge is allowed.
func (t Transitions) Allows(from FlowState, action ActionType, to FlowState) bool {
	if len(t) == 0 {
		return true
	}
	return t.allowsExact(from, action, to) ||
		t.allowsExact(from, AnyAction, to) ||
		t.allowsExact(AnyState, action, to) ||
		t.allowsExact(AnyState, AnyAction, to)
}

func (t Transitions) allowsExact(from FlowState, action ActionType, to FlowState) bool {
	actions, ok := t[from]
	if !ok {
		return false
	}
	targets, ok := actions[action]
	if !ok {
		return false
	}
	for _, target := range targets {
		if target == to || target == AnyState {
			return true
		}
	}
	return false
}

// Result is the client-safe result returned by the engine. It deliberately
// excludes subject IDs, metadata, private data, and effect payloads.
//
// View is the exact JSON value persisted by WithView. Decode it once at the
// transport boundary with ViewAs instead of re-marshalling flow data at every
// state transition.
type Result struct {
	ID        FlowID
	Type      FlowType
	State     FlowState
	Completed bool
	Cancelled bool
	ExpiresAt time.Time
	View      json.RawMessage
	Revision  int64
}

// EffectContext identifies the committed flow revision that produced an
// effect. It is supplied only to trusted recorders and dispatchers; it is not
// part of Result or observer events.
type EffectContext struct {
	FlowID    FlowID
	FlowType  FlowType
	SubjectID SubjectID
	State     FlowState
	Revision  int64
}

// Snapshot is the serialized persistence representation of a flow.
// Snapshot is persistence-private and may contain sensitive data.
type Snapshot struct {
	ID           FlowID
	Type         FlowType
	SubjectID    SubjectID
	State        FlowState
	Data         []byte
	DataVersion  int
	ViewData     []byte
	ViewVersion  int
	Completed    bool
	Cancelled    bool
	CancelReason string
	ExpiresAt    time.Time
	CreatedAt    time.Time
	UpdatedAt    time.Time
	Revision     int64
	Metadata     map[string]string
}

// IsExpiredAt reports whether the snapshot has an expiration before t.
func (s Snapshot) IsExpiredAt(t time.Time) bool {
	return !s.ExpiresAt.IsZero() && s.ExpiresAt.Before(t)
}

// View returns a client-safe result without decoding or exposing private data.
func (s Snapshot) View() Result {
	return resultFromSnapshot(s)
}
