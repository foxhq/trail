package trail

import "time"

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
	// AnyState matches any source or target state in a transition graph.
	AnyState FlowState = "*"

	// BeginAction is the synthetic action used for begin transitions.
	BeginAction ActionType = "$begin"
	// AnyAction matches any action in a transition graph.
	AnyAction ActionType = "*"
)

// Action is an input submitted to a running flow.
type Action interface {
	Type() ActionType
}

// Effect describes work to perform after a transition commits.
type Effect interface {
	Type() EffectType
}

// BeginRequest starts a new flow.
type BeginRequest struct {
	Type           FlowType
	SubjectID      SubjectID
	Input          any
	ExpiresAt      time.Time
	ExpiresIn      time.Duration
	IdempotencyKey IdempotencyKey
	Metadata       map[string]string
}

// SubmitRequest applies an action to an existing flow.
type SubmitRequest struct {
	FlowID         FlowID
	Action         Action
	IdempotencyKey IdempotencyKey
}

// CancelRequest cancels an active flow.
type CancelRequest struct {
	FlowID FlowID
	Reason string
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

// Flow is the current persisted workflow instance passed to action handlers.
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
	Public    any
	Effects   []Effect
}

// To creates a transition to state with updated typed data.
func To[D any](state FlowState, data D) *Transition[D] {
	return &Transition[D]{
		State: state,
		Data:  data,
	}
}

// Done creates a completed transition to state with updated typed data.
func Done[D any](state FlowState, data D) *Transition[D] {
	return &Transition[D]{
		State:     state,
		Data:      data,
		Completed: true,
	}
}

// WithPublic attaches public response data to a transition.
func (t *Transition[D]) WithPublic(public any) *Transition[D] {
	t.Public = public
	return t
}

// WithEffects attaches effects to a transition.
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

// Result is the public result returned by the engine.
type Result struct {
	ID        FlowID
	Type      FlowType
	SubjectID SubjectID
	State     FlowState
	Completed bool
	Cancelled bool
	ExpiresAt time.Time
	Public    any
	Effects   []Effect
	Metadata  map[string]string
	Revision  int64
}

// Snapshot is the serialized persistence representation of a flow.
type Snapshot struct {
	ID           FlowID
	Type         FlowType
	SubjectID    SubjectID
	State        FlowState
	Data         []byte
	DataVersion  int
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

// View returns a public result without decoded typed data.
func (s Snapshot) View() Result {
	return Result{
		ID:        s.ID,
		Type:      s.Type,
		SubjectID: s.SubjectID,
		State:     s.State,
		Completed: s.Completed,
		Cancelled: s.Cancelled,
		ExpiresAt: s.ExpiresAt,
		Metadata:  cloneStringMap(s.Metadata),
		Revision:  s.Revision,
	}
}
