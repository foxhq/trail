package trail

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Spec defines one flow type and owns its typed private data.
type Spec[D any] interface {
	Type() FlowType
	DataVersion() int
	Begin(ctx context.Context, begin BeginContext, input any) (*Transition[D], error)
	Submit(ctx context.Context, flow *Flow[D], action Action) (*Transition[D], error)
	Encode(data D) ([]byte, error)
	Decode(version int, raw []byte) (D, error)
}

// CancellationSpec optionally supplies application-specific cancellation
// behavior. A cancellation handler can change private data, state, view, and
// emit durable effects. Returning ErrUnsupportedCancellation uses Trail's
// standard cancellation behavior instead.
type CancellationSpec[D any] interface {
	Cancel(ctx context.Context, flow *Flow[D], cancel CancelContext) (*Transition[D], error)
}

// TransitionSpec is an optional extension for specs that want engine-enforced
// state-transition validation.
type TransitionSpec interface {
	Transitions() Transitions
}

// JSONCodec encodes and decodes spec data as JSON. It accepts every version;
// use a custom Codec when data versions need migration rules.
type JSONCodec[D any] struct{}

func (JSONCodec[D]) Encode(data D) ([]byte, error) { return json.Marshal(data) }

func (JSONCodec[D]) Decode(_ int, raw []byte) (D, error) {
	var data D
	if len(raw) == 0 {
		return data, nil
	}
	err := json.Unmarshal(raw, &data)
	return data, err
}

// UnsupportedVersionCodec is the safe default JSON codec. It rejects any
// version other than one, forcing flows that evolve their data to opt into an
// explicit migration-aware codec.
type UnsupportedVersionCodec[D any] struct{}

func (UnsupportedVersionCodec[D]) Encode(data D) ([]byte, error) { return json.Marshal(data) }

func (UnsupportedVersionCodec[D]) Decode(version int, raw []byte) (D, error) {
	var data D
	if version != 1 {
		return data, fmt.Errorf("%w: %d", ErrUnsupportedDataVersion, version)
	}
	if len(raw) == 0 {
		return data, nil
	}
	err := json.Unmarshal(raw, &data)
	return data, err
}

// SupportsVersion implements VersionedCodec.
func (UnsupportedVersionCodec[D]) SupportsVersion(version int) bool { return version == 1 }

type erasedSpec interface {
	flowType() FlowType
	dataVersion() int
	transitions() Transitions
	begin(ctx context.Context, id FlowID, req BeginRequest, now time.Time) (*Snapshot, []Effect, error)
	submit(ctx context.Context, snap Snapshot, action Action, now time.Time) (*Snapshot, []Effect, error)
	cancel(ctx context.Context, snap Snapshot, request CancelRequest, now time.Time) (*Snapshot, []Effect, error)
}

type specAdapter[D any] struct{ spec Spec[D] }

func eraseSpec[D any](spec Spec[D]) erasedSpec { return specAdapter[D]{spec: spec} }

func (a specAdapter[D]) flowType() FlowType { return a.spec.Type() }
func (a specAdapter[D]) dataVersion() int   { return a.spec.DataVersion() }

func (a specAdapter[D]) transitions() Transitions {
	spec, ok := a.spec.(TransitionSpec)
	if !ok {
		return nil
	}
	return spec.Transitions()
}

func (a specAdapter[D]) begin(ctx context.Context, id FlowID, req BeginRequest, now time.Time) (*Snapshot, []Effect, error) {
	expiresAt := req.ExpiresAt
	if expiresAt.IsZero() && req.ExpiresIn > 0 {
		expiresAt = now.Add(req.ExpiresIn)
	}

	transition, err := a.spec.Begin(ctx, BeginContext{
		ID: id, Type: req.Type, SubjectID: req.SubjectID, Now: now,
		ExpiresAt: expiresAt, Metadata: cloneStringMap(req.Metadata),
	}, req.Input)
	if err != nil {
		return nil, nil, err
	}
	if transition == nil {
		return nil, nil, fmt.Errorf("%w: begin returned nil transition", ErrInvalidFlow)
	}
	if err := a.validateTransition(BeginState, BeginAction, transition.State); err != nil {
		return nil, nil, err
	}
	if !transition.ExpiresAt.IsZero() {
		expiresAt = transition.ExpiresAt
	}

	raw, err := a.spec.Encode(transition.Data)
	if err != nil {
		return nil, nil, err
	}
	viewRaw, err := viewForTransition(nil, transition)
	if err != nil {
		return nil, nil, err
	}

	return &Snapshot{
		ID: id, Type: req.Type, SubjectID: req.SubjectID, State: transition.State,
		Data: raw, DataVersion: a.spec.DataVersion(), ViewData: viewRaw,
		ViewVersion: viewVersionForTransition(0, transition), Completed: transition.Completed,
		ExpiresAt: expiresAt, CreatedAt: now, UpdatedAt: now,
		Metadata: cloneStringMap(req.Metadata),
	}, transition.Effects, nil
}

func (a specAdapter[D]) submit(ctx context.Context, snap Snapshot, action Action, now time.Time) (*Snapshot, []Effect, error) {
	flow, err := a.flowFromSnapshot(snap)
	if err != nil {
		return nil, nil, err
	}
	transition, err := a.spec.Submit(ctx, flow, action)
	if err != nil {
		return nil, nil, err
	}
	if transition == nil {
		return nil, nil, fmt.Errorf("%w: submit returned nil transition", ErrInvalidFlow)
	}
	if err := a.validateTransition(snap.State, action.Type(), transition.State); err != nil {
		return nil, nil, err
	}
	return a.nextSnapshot(snap, transition, now)
}

func (a specAdapter[D]) cancel(ctx context.Context, snap Snapshot, request CancelRequest, now time.Time) (*Snapshot, []Effect, error) {
	flow, err := a.flowFromSnapshot(snap)
	if err != nil {
		return nil, nil, err
	}

	var transition *Transition[D]
	handled := false
	if canceller, ok := a.spec.(CancellationSpec[D]); ok {
		transition, err = canceller.Cancel(ctx, flow, CancelContext{Reason: request.Reason})
		if err != nil && !errors.Is(err, ErrUnsupportedCancellation) {
			return nil, nil, err
		}
		handled = err == nil && transition != nil
	}
	if transition == nil {
		transition = To(snap.State, flow.Data)
	}
	if handled {
		if err := a.validateTransition(snap.State, CancelAction, transition.State); err != nil {
			return nil, nil, err
		}
	}
	next, effects, err := a.nextSnapshot(snap, transition, now)
	if err != nil {
		return nil, nil, err
	}
	next.Cancelled = true
	next.Completed = true
	next.CancelReason = request.Reason
	return next, effects, nil
}

func (a specAdapter[D]) flowFromSnapshot(snap Snapshot) (*Flow[D], error) {
	data, err := a.spec.Decode(snap.DataVersion, snap.Data)
	if err != nil {
		return nil, err
	}
	return &Flow[D]{
		ID: snap.ID, Type: snap.Type, SubjectID: snap.SubjectID, State: snap.State,
		Data: data, ExpiresAt: snap.ExpiresAt, Completed: snap.Completed,
		Cancelled: snap.Cancelled, Metadata: cloneStringMap(snap.Metadata),
		CreatedAt: snap.CreatedAt, UpdatedAt: snap.UpdatedAt, Revision: snap.Revision,
	}, nil
}

func (a specAdapter[D]) nextSnapshot(snap Snapshot, transition *Transition[D], now time.Time) (*Snapshot, []Effect, error) {
	raw, err := a.spec.Encode(transition.Data)
	if err != nil {
		return nil, nil, err
	}
	viewRaw, err := viewForTransition(snap.ViewData, transition)
	if err != nil {
		return nil, nil, err
	}
	next := cloneSnapshot(snap)
	next.State = transition.State
	next.Data = raw
	next.DataVersion = a.spec.DataVersion()
	next.ViewData = viewRaw
	next.ViewVersion = viewVersionForTransition(snap.ViewVersion, transition)
	next.Completed = transition.Completed
	if !transition.ExpiresAt.IsZero() {
		next.ExpiresAt = transition.ExpiresAt
	}
	next.UpdatedAt = now
	return &next, transition.Effects, nil
}

func (a specAdapter[D]) validateTransition(from FlowState, action ActionType, to FlowState) error {
	if to == "" {
		return fmt.Errorf("%w: target state is empty", ErrInvalidTransition)
	}
	if a.transitions().Allows(from, action, to) {
		return nil
	}
	return fmt.Errorf("%w: %s --%s--> %s", ErrInvalidTransition, from, action, to)
}
