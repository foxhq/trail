package trail

import (
	"context"
	"fmt"
	"reflect"
	"sync/atomic"
)

// Codec encodes and decodes typed flow data. A codec is responsible for
// supporting every stored data version a flow may still contain.
type Codec[D any] interface {
	Encode(data D) ([]byte, error)
	Decode(version int, raw []byte) (D, error)
}

// VersionedCodec optionally declares the persisted versions it can decode.
// Trail checks this declaration while registering a Definition, so a flow
// cannot be configured to write a version its own codec rejects.
type VersionedCodec[D any] interface {
	Codec[D]
	SupportsVersion(version int) bool
}

// BeginFunc starts a typed flow.
type BeginFunc[D, I any] func(ctx context.Context, begin BeginContext, input I) (*Transition[D], error)

// SubmitFunc handles one typed action with the full flow instance.
type SubmitFunc[D any, A Action] func(ctx context.Context, flow *Flow[D], action A) (*Transition[D], error)

// DataFunc handles one typed action using only private flow data.
type DataFunc[D any, A Action] func(ctx context.Context, data D, action A) (*Transition[D], error)

// CancelFunc handles cancellation from one state. The engine always marks the
// returned transition completed and cancelled after validating it.
type CancelFunc[D any] func(ctx context.Context, flow *Flow[D], cancel CancelContext) (*Transition[D], error)

type submitHandler[D any] func(ctx context.Context, flow *Flow[D], action Action) (*Transition[D], error)

// Edge completes a transition declaration by listing the allowed next states.
type Edge interface {
	GoTo(to ...FlowState) error
	MustGoTo(to ...FlowState)
}

// Definition defines one workflow type with typed data, typed begin input, and
// compile-time checked action handlers.
//
// Configure it through Start, When, WhenFlow, and WhenCancel before registering
// it. Go 1.27 generic methods keep action-handler checking at compile time
// while allowing the definition to read fluently.
type Definition[D, I any] struct {
	flowType       FlowType
	dataVersion    int
	codec          Codec[D]
	begin          BeginFunc[D, I]
	handlers       map[FlowState]map[ActionType]submitHandler[D]
	cancelHandlers map[FlowState]CancelFunc[D]
	transitions    Transitions
	sealed         atomic.Bool
}

// Define creates a workflow definition with data version 1 and JSON encoding.
func Define[D, I any](flowType FlowType, begin BeginFunc[D, I]) *Definition[D, I] {
	return &Definition[D, I]{
		flowType:       flowType,
		dataVersion:    1,
		codec:          UnsupportedVersionCodec[D]{},
		begin:          begin,
		handlers:       make(map[FlowState]map[ActionType]submitHandler[D]),
		cancelHandlers: make(map[FlowState]CancelFunc[D]),
		transitions:    make(Transitions),
	}
}

// Version sets the encoded data version. Use WithCodec with a codec that can
// decode both this version and every historical version retained in storage.
func (s *Definition[D, I]) Version(version int) *Definition[D, I] {
	if err := s.assertMutable(); err != nil {
		panic(err)
	}
	s.dataVersion = version
	return s
}

// WithCodec sets the data codec and returns the definition.
func (s *Definition[D, I]) WithCodec(codec Codec[D]) *Definition[D, I] {
	if err := s.assertMutable(); err != nil {
		panic(err)
	}
	if codec != nil {
		s.codec = codec
	}
	return s
}

// Type returns the flow type.
func (s *Definition[D, I]) Type() FlowType { return s.flowType }

// DataVersion returns the encoded data version.
func (s *Definition[D, I]) DataVersion() int { return s.dataVersion }

// SupportsDataVersion reports whether the configured codec declares support
// for the version this definition writes. It is used by Register.
func (s *Definition[D, I]) SupportsDataVersion(version int) bool {
	codec, ok := s.codec.(interface{ SupportsVersion(int) bool })
	return !ok || codec.SupportsVersion(version)
}

// Begin casts input to I and calls the typed begin function.
func (s *Definition[D, I]) Begin(ctx context.Context, begin BeginContext, input any) (*Transition[D], error) {
	if s.begin == nil {
		return nil, fmt.Errorf("%w: begin handler is nil", ErrInvalidFlow)
	}
	typedInput, err := As[I](input)
	if err != nil {
		return nil, err
	}
	return s.begin(ctx, begin, typedInput)
}

// Submit routes an action by its current state and type. An exact state route
// wins over an AnyState fallback.
func (s *Definition[D, I]) Submit(ctx context.Context, flow *Flow[D], action Action) (*Transition[D], error) {
	if action == nil {
		return nil, ErrUnsupportedAction
	}
	if handler, ok := s.handlerFor(flow.State, action.Type()); ok {
		return handler(ctx, flow, action)
	}
	return nil, fmt.Errorf("%w: %s in %s", ErrUnsupportedAction, action.Type(), flow.State)
}

// Cancel runs a state-specific cancellation handler when declared. A flow
// without one receives the engine's standard cancellation behavior.
func (s *Definition[D, I]) Cancel(ctx context.Context, flow *Flow[D], cancel CancelContext) (*Transition[D], error) {
	handler, ok := s.cancelHandlerFor(flow.State)
	if !ok {
		return nil, ErrUnsupportedCancellation
	}
	return handler(ctx, flow, cancel)
}

func (s *Definition[D, I]) handlerFor(state FlowState, action ActionType) (submitHandler[D], bool) {
	for _, candidate := range []FlowState{state, AnyState} {
		handlers := s.handlers[candidate]
		if handler, ok := handlers[action]; ok {
			return handler, true
		}
		if handler, ok := handlers[AnyAction]; ok {
			return handler, true
		}
	}
	return nil, false
}

func (s *Definition[D, I]) cancelHandlerFor(state FlowState) (CancelFunc[D], bool) {
	if handler, ok := s.cancelHandlers[state]; ok {
		return handler, true
	}
	handler, ok := s.cancelHandlers[AnyState]
	return handler, ok
}

// Encode encodes typed flow data.
func (s *Definition[D, I]) Encode(data D) ([]byte, error) { return s.codec.Encode(data) }

// Decode decodes typed flow data.
func (s *Definition[D, I]) Decode(version int, raw []byte) (D, error) {
	return s.codec.Decode(version, raw)
}

// Transitions returns the allowed transition graph.
func (s *Definition[D, I]) Transitions() Transitions { return cloneTransitions(s.transitions) }

// Start starts the begin-transition declaration.
func (s *Definition[D, I]) Start() Edge { return startBuilder[D, I]{spec: s} }

// When declares a typed action handler that needs only the private flow data.
func (s *Definition[D, I]) When[A Action](from FlowState, handler DataFunc[D, A]) Edge {
	actionType, err := actionTypeFor[A]()
	if handler == nil && err == nil {
		err = fmt.Errorf("%w: submit handler is nil", ErrInvalidFlow)
	}
	var wrapped SubmitFunc[D, A]
	if handler != nil {
		wrapped = func(ctx context.Context, flow *Flow[D], action A) (*Transition[D], error) {
			return handler(ctx, flow.Data, action)
		}
	}
	return routeBuilder[D, I, A]{spec: s, from: from, actionType: actionType, typeErr: err, handler: wrapped}
}

// WhenFlow declares a typed action handler that needs the full flow instance.
func (s *Definition[D, I]) WhenFlow[A Action](from FlowState, handler SubmitFunc[D, A]) Edge {
	actionType, err := actionTypeFor[A]()
	return routeBuilder[D, I, A]{spec: s, from: from, actionType: actionType, typeErr: err, handler: handler}
}

// WhenCancel declares typed cancellation behavior for a source state. Use
// AnyState when all active states share the same behavior.
func (s *Definition[D, I]) WhenCancel(from FlowState, handler CancelFunc[D]) Edge {
	return cancelRouteBuilder[D, I]{spec: s, from: from, handler: handler}
}

func (s *Definition[D, I]) seal() {
	if s != nil {
		s.sealed.Store(true)
	}
}

func (s *Definition[D, I]) assertMutable() error {
	if s != nil && s.sealed.Load() {
		return fmt.Errorf("%w: definition %s is already registered", ErrInvalidFlow, s.flowType)
	}
	return nil
}

func registerHandler[D, I any, A Action](spec *Definition[D, I], from FlowState, actionType ActionType, handler SubmitFunc[D, A]) error {
	if spec == nil {
		return fmt.Errorf("%w: spec is nil", ErrInvalidFlow)
	}
	if err := spec.assertMutable(); err != nil {
		return err
	}
	if handler == nil {
		return fmt.Errorf("%w: submit handler is nil", ErrInvalidFlow)
	}
	if from == "" {
		return fmt.Errorf("%w: source state is empty", ErrInvalidTransition)
	}
	if actionType == "" {
		return fmt.Errorf("%w: action type is empty", ErrUnsupportedAction)
	}
	if spec.handlers[from] == nil {
		spec.handlers[from] = make(map[ActionType]submitHandler[D])
	}
	if _, exists := spec.handlers[from][actionType]; exists {
		return fmt.Errorf("%w: handler already registered for %s in %s", ErrInvalidFlow, actionType, from)
	}
	spec.handlers[from][actionType] = func(ctx context.Context, flow *Flow[D], action Action) (*Transition[D], error) {
		typedAction, err := ActionAs[A](action)
		if err != nil {
			return nil, err
		}
		return handler(ctx, flow, typedAction)
	}
	return nil
}

type routeBuilder[D, I any, A Action] struct {
	spec       *Definition[D, I]
	from       FlowState
	actionType ActionType
	typeErr    error
	handler    SubmitFunc[D, A]
}

// GoTo completes an action route declaration.
func (r routeBuilder[D, I, A]) GoTo(to ...FlowState) error {
	if r.typeErr != nil {
		return r.typeErr
	}
	if err := registerHandler(r.spec, r.from, r.actionType, r.handler); err != nil {
		return err
	}
	if err := allowTransition(r.spec, r.from, r.actionType, to...); err != nil {
		delete(r.spec.handlers[r.from], r.actionType)
		return err
	}
	return nil
}

// MustGoTo completes an action route declaration and panics on invalid setup.
func (r routeBuilder[D, I, A]) MustGoTo(to ...FlowState) {
	if err := r.GoTo(to...); err != nil {
		panic(err)
	}
}

type cancelRouteBuilder[D, I any] struct {
	spec    *Definition[D, I]
	from    FlowState
	handler CancelFunc[D]
}

func (r cancelRouteBuilder[D, I]) GoTo(to ...FlowState) error {
	if r.spec == nil {
		return fmt.Errorf("%w: spec is nil", ErrInvalidFlow)
	}
	if err := r.spec.assertMutable(); err != nil {
		return err
	}
	if r.handler == nil {
		return fmt.Errorf("%w: cancel handler is nil", ErrInvalidFlow)
	}
	if r.from == "" {
		return fmt.Errorf("%w: source state is empty", ErrInvalidTransition)
	}
	if _, exists := r.spec.cancelHandlers[r.from]; exists {
		return fmt.Errorf("%w: cancellation handler already registered for %s", ErrInvalidFlow, r.from)
	}
	r.spec.cancelHandlers[r.from] = r.handler
	if err := allowTransition(r.spec, r.from, CancelAction, to...); err != nil {
		delete(r.spec.cancelHandlers, r.from)
		return err
	}
	return nil
}

func (r cancelRouteBuilder[D, I]) MustGoTo(to ...FlowState) {
	if err := r.GoTo(to...); err != nil {
		panic(err)
	}
}

type startBuilder[D, I any] struct{ spec *Definition[D, I] }

func (s startBuilder[D, I]) GoTo(to ...FlowState) error {
	return allowTransition(s.spec, BeginState, BeginAction, to...)
}

func (s startBuilder[D, I]) MustGoTo(to ...FlowState) {
	if err := s.GoTo(to...); err != nil {
		panic(err)
	}
}

func allowTransition[D, I any](spec *Definition[D, I], from FlowState, action ActionType, to ...FlowState) error {
	if spec == nil {
		return fmt.Errorf("%w: spec is nil", ErrInvalidFlow)
	}
	if err := spec.assertMutable(); err != nil {
		return err
	}
	if from == "" {
		return fmt.Errorf("%w: source state is empty", ErrInvalidTransition)
	}
	if action == "" {
		return fmt.Errorf("%w: action is empty", ErrInvalidTransition)
	}
	if len(to) == 0 {
		return fmt.Errorf("%w: target states are empty", ErrInvalidTransition)
	}
	for _, target := range to {
		if target == "" {
			return fmt.Errorf("%w: target state is empty", ErrInvalidTransition)
		}
	}
	if spec.transitions[from] == nil {
		spec.transitions[from] = make(ActionTransitions)
	}
	spec.transitions[from][action] = append(spec.transitions[from][action], to...)
	return nil
}

func actionTypeFor[A Action]() (ActionType, error) {
	var action A
	if isNilAction(action) {
		actionValueType := reflect.TypeFor[A]()
		if actionValueType.Kind() != reflect.Pointer {
			return "", fmt.Errorf("%w: zero action is nil", ErrUnsupportedAction)
		}
		action = reflect.New(actionValueType.Elem()).Interface().(A)
	}
	actionType := action.Type()
	if actionType == "" {
		return "", fmt.Errorf("%w: action type is empty", ErrUnsupportedAction)
	}
	return actionType, nil
}

func isNilAction(action any) bool {
	if action == nil {
		return true
	}
	value := reflect.ValueOf(action)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
