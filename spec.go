package trail

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Spec defines one flow type and owns its typed private data.
type Spec[D any] interface {
	Type() FlowType
	DataVersion() int
	Begin(ctx context.Context, begin BeginContext, input any) (*Transition[D], error)
	Submit(ctx context.Context, instance *Flow[D], action Action) (*Transition[D], error)
	Encode(data D) ([]byte, error)
	Decode(version int, raw []byte) (D, error)
}

// TransitionSpec is an optional extension for specs that want engine-enforced
// state-transition validation.
type TransitionSpec interface {
	Transitions() Transitions
}

// JSONCodec encodes and decodes spec data as JSON.
type JSONCodec[D any] struct{}

func (JSONCodec[D]) Encode(data D) ([]byte, error) {
	return json.Marshal(data)
}

func (JSONCodec[D]) Decode(_ int, raw []byte) (D, error) {
	var data D
	if len(raw) == 0 {
		return data, nil
	}
	err := json.Unmarshal(raw, &data)
	return data, err
}

// UnsupportedVersionCodec is a JSON codec that only accepts data version 1.
type UnsupportedVersionCodec[D any] struct{}

func (UnsupportedVersionCodec[D]) Encode(data D) ([]byte, error) {
	return json.Marshal(data)
}

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

type erasedSpec interface {
	flowType() FlowType
	dataVersion() int
	transitions() Transitions
	begin(ctx context.Context, id FlowID, req BeginRequest, now time.Time) (*Snapshot, Result, []Effect, error)
	submit(ctx context.Context, snap Snapshot, action Action, now time.Time) (*Snapshot, Result, []Effect, error)
}

type specAdapter[D any] struct {
	spec Spec[D]
}

func eraseSpec[D any](spec Spec[D]) erasedSpec {
	return specAdapter[D]{spec: spec}
}

func (a specAdapter[D]) flowType() FlowType {
	return a.spec.Type()
}

func (a specAdapter[D]) dataVersion() int {
	return a.spec.DataVersion()
}

func (a specAdapter[D]) transitions() Transitions {
	spec, ok := a.spec.(TransitionSpec)
	if !ok {
		return nil
	}
	return spec.Transitions()
}

func (a specAdapter[D]) begin(ctx context.Context, id FlowID, req BeginRequest, now time.Time) (*Snapshot, Result, []Effect, error) {
	expiresAt := req.ExpiresAt
	if expiresAt.IsZero() && req.ExpiresIn > 0 {
		expiresAt = now.Add(req.ExpiresIn)
	}

	transition, err := a.spec.Begin(ctx, BeginContext{
		ID:        id,
		Type:      req.Type,
		SubjectID: req.SubjectID,
		Now:       now,
		ExpiresAt: expiresAt,
		Metadata:  cloneStringMap(req.Metadata),
	}, req.Input)
	if err != nil {
		return nil, Result{}, nil, err
	}
	if transition == nil {
		return nil, Result{}, nil, fmt.Errorf("%w: begin returned nil transition", ErrInvalidFlow)
	}
	if err := a.validateTransition(BeginState, BeginAction, transition.State); err != nil {
		return nil, Result{}, nil, err
	}
	if !transition.ExpiresAt.IsZero() {
		expiresAt = transition.ExpiresAt
	}

	raw, err := a.spec.Encode(transition.Data)
	if err != nil {
		return nil, Result{}, nil, err
	}

	snap := &Snapshot{
		ID:          id,
		Type:        req.Type,
		SubjectID:   req.SubjectID,
		State:       transition.State,
		Data:        raw,
		DataVersion: a.spec.DataVersion(),
		Completed:   transition.Completed,
		ExpiresAt:   expiresAt,
		CreatedAt:   now,
		UpdatedAt:   now,
		Metadata:    cloneStringMap(req.Metadata),
	}

	result := resultFromSnapshot(*snap, transition.Public, transition.Effects)
	return snap, result, transition.Effects, nil
}

func (a specAdapter[D]) submit(ctx context.Context, snap Snapshot, action Action, now time.Time) (*Snapshot, Result, []Effect, error) {
	data, err := a.spec.Decode(snap.DataVersion, snap.Data)
	if err != nil {
		return nil, Result{}, nil, err
	}

	instance := &Flow[D]{
		ID:        snap.ID,
		Type:      snap.Type,
		SubjectID: snap.SubjectID,
		State:     snap.State,
		Data:      data,
		ExpiresAt: snap.ExpiresAt,
		Completed: snap.Completed,
		Cancelled: snap.Cancelled,
		Metadata:  cloneStringMap(snap.Metadata),
		CreatedAt: snap.CreatedAt,
		UpdatedAt: snap.UpdatedAt,
		Revision:  snap.Revision,
	}

	transition, err := a.spec.Submit(ctx, instance, action)
	if err != nil {
		return nil, Result{}, nil, err
	}
	if transition == nil {
		return nil, Result{}, nil, fmt.Errorf("%w: submit returned nil transition", ErrInvalidFlow)
	}
	if err := a.validateTransition(snap.State, action.Type(), transition.State); err != nil {
		return nil, Result{}, nil, err
	}

	raw, err := a.spec.Encode(transition.Data)
	if err != nil {
		return nil, Result{}, nil, err
	}

	next := snap
	next.State = transition.State
	next.Data = raw
	next.DataVersion = a.spec.DataVersion()
	next.Completed = transition.Completed
	if !transition.ExpiresAt.IsZero() {
		next.ExpiresAt = transition.ExpiresAt
	}
	next.UpdatedAt = now

	result := resultFromSnapshot(next, transition.Public, transition.Effects)
	return &next, result, transition.Effects, nil
}

func (a specAdapter[D]) validateTransition(from FlowState, action ActionType, to FlowState) error {
	if to == "" {
		return fmt.Errorf("%w: target state is empty", ErrInvalidTransition)
	}
	transitionSpec, ok := any(a.spec).(TransitionSpec)
	if !ok {
		return nil
	}
	if transitionSpec.Transitions().Allows(from, action, to) {
		return nil
	}
	return fmt.Errorf("%w: %s --%s--> %s", ErrInvalidTransition, from, action, to)
}

func resultFromSnapshot(snap Snapshot, public any, effects []Effect) Result {
	return Result{
		ID:        snap.ID,
		Type:      snap.Type,
		SubjectID: snap.SubjectID,
		State:     snap.State,
		Completed: snap.Completed,
		Cancelled: snap.Cancelled,
		ExpiresAt: snap.ExpiresAt,
		Public:    public,
		Effects:   effects,
		Metadata:  cloneStringMap(snap.Metadata),
		Revision:  snap.Revision,
	}
}
