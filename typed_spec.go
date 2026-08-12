package trail

import (
	"context"
	"fmt"
	"reflect"
)

// Codec encodes and decodes typed flow data.
type Codec[D any] interface {
	Encode(data D) ([]byte, error)
	Decode(version int, raw []byte) (D, error)
}

// BeginFunc starts a typed flow.
type BeginFunc[D, I any] func(ctx context.Context, begin BeginContext, input I) (*Transition[D], error)

// SubmitFunc handles one typed action for a typed flow.
type SubmitFunc[D any, A Action] func(ctx context.Context, instance *Flow[D], action A) (*Transition[D], error)

// DataFunc handles one typed action using only typed workflow data.
type DataFunc[D any, A Action] func(ctx context.Context, data D, action A) (*Transition[D], error)

type submitHandler[D any] func(ctx context.Context, instance *Flow[D], action Action) (*Transition[D], error)

// Edge completes a transition declaration by listing the allowed next states.
type Edge interface {
	GoTo(to ...FlowState) error
	MustGoTo(to ...FlowState)
}

// Definition defines one workflow type with typed data, typed begin input, and
// typed action handlers.
//
// Configure handlers and allowed transitions before registering the definition.
type Definition[D, I any] struct {
	flowType    FlowType
	dataVersion int
	codec       Codec[D]
	begin       BeginFunc[D, I]
	handlers    map[ActionType]submitHandler[D]
	transitions Transitions
}

// Define creates a workflow definition with data version 1 and JSON encoding.
//
// Define is the recommended constructor for application code because the call
// reads as a flow definition:
//
//	spec := trail.Define(FlowVerifyEmail, beginVerifyEmail)
func Define[D, I any](flowType FlowType, begin BeginFunc[D, I]) *Definition[D, I] {
	return &Definition[D, I]{
		flowType:    flowType,
		dataVersion: 1,
		codec:       JSONCodec[D]{},
		begin:       begin,
		handlers:    make(map[ActionType]submitHandler[D]),
		transitions: make(Transitions),
	}
}

// Version sets the encoded data version and returns the definition.
func (s *Definition[D, I]) Version(version int) *Definition[D, I] {
	s.dataVersion = version
	return s
}

// WithCodec sets the data codec and returns the definition.
func (s *Definition[D, I]) WithCodec(codec Codec[D]) *Definition[D, I] {
	if codec != nil {
		s.codec = codec
	}
	return s
}

// Type returns the flow type.
func (s *Definition[D, I]) Type() FlowType {
	return s.flowType
}

// DataVersion returns the encoded data version.
func (s *Definition[D, I]) DataVersion() int {
	return s.dataVersion
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

// Submit routes action to the typed handler registered for action.Type().
func (s *Definition[D, I]) Submit(ctx context.Context, instance *Flow[D], action Action) (*Transition[D], error) {
	if action == nil {
		return nil, ErrUnsupportedAction
	}
	handler, ok := s.handlers[action.Type()]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedAction, action.Type())
	}
	return handler(ctx, instance, action)
}

// Encode encodes typed flow data.
func (s *Definition[D, I]) Encode(data D) ([]byte, error) {
	return s.codec.Encode(data)
}

// Decode decodes typed flow data.
func (s *Definition[D, I]) Decode(version int, raw []byte) (D, error) {
	return s.codec.Decode(version, raw)
}

// Transitions returns the allowed transition graph.
func (s *Definition[D, I]) Transitions() Transitions {
	return cloneTransitions(s.transitions)
}

// Start starts the begin-transition definition.
func (s *Definition[D, I]) Start() Edge {
	return Start(s)
}

// When starts a route definition for handlers that only need typed data.
//
// The action argument is a zero-value prototype used to identify the action
// type. The handler must have this shape:
//
//	func(context.Context, D, MyAction) (*trail.Transition[D], error)
//
// Use the package-level When function when you want compile-time generic
// checking.
func (s *Definition[D, I]) When(from FlowState, action Action, handler any) Edge {
	return s.when(from, action, false, handler)
}

// WhenFlow starts a route definition for handlers that need the full persisted
// flow.
//
// The action argument is a zero-value prototype used to identify the action
// type. The handler must have this shape:
//
//	func(context.Context, *trail.Flow[D], MyAction) (*trail.Transition[D], error)
//
// Use the package-level WhenFlow function when you want compile-time generic
// checking.
func (s *Definition[D, I]) WhenFlow(from FlowState, action Action, handler any) Edge {
	return s.when(from, action, true, handler)
}

func (s *Definition[D, I]) when(from FlowState, action Action, withRun bool, handler any) Edge {
	actionType, actionValueType, err := actionPrototype(action)
	if err != nil {
		return methodRouteBuilder[D, I]{
			spec:    s,
			typeErr: err,
		}
	}

	wrapped, err := methodSubmitHandler[D](handler, actionValueType, withRun)
	return methodRouteBuilder[D, I]{
		spec:       s,
		from:       from,
		actionType: actionType,
		typeErr:    err,
		handler:    wrapped,
	}
}

func registerHandler[D, I any, A Action](spec *Definition[D, I], actionType ActionType, handler SubmitFunc[D, A]) error {
	if spec == nil {
		return fmt.Errorf("%w: spec is nil", ErrInvalidFlow)
	}
	if handler == nil {
		return fmt.Errorf("%w: submit handler is nil", ErrInvalidFlow)
	}
	if actionType == "" {
		return fmt.Errorf("%w: action type is empty", ErrUnsupportedAction)
	}
	if _, exists := spec.handlers[actionType]; exists {
		return fmt.Errorf("%w: handler already registered for %s", ErrInvalidFlow, actionType)
	}
	spec.handlers[actionType] = func(ctx context.Context, instance *Flow[D], action Action) (*Transition[D], error) {
		typedAction, err := ActionAs[A](action)
		if err != nil {
			return nil, err
		}
		return handler(ctx, instance, typedAction)
	}
	return nil
}

func registerErasedHandler[D, I any](spec *Definition[D, I], actionType ActionType, handler submitHandler[D]) error {
	if spec == nil {
		return fmt.Errorf("%w: spec is nil", ErrInvalidFlow)
	}
	if handler == nil {
		return fmt.Errorf("%w: submit handler is nil", ErrInvalidFlow)
	}
	if actionType == "" {
		return fmt.Errorf("%w: action type is empty", ErrUnsupportedAction)
	}
	if _, exists := spec.handlers[actionType]; exists {
		return fmt.Errorf("%w: handler already registered for %s", ErrInvalidFlow, actionType)
	}
	spec.handlers[actionType] = handler
	return nil
}

type methodRouteBuilder[D, I any] struct {
	spec       *Definition[D, I]
	from       FlowState
	actionType ActionType
	typeErr    error
	handler    submitHandler[D]
}

func (r methodRouteBuilder[D, I]) GoTo(to ...FlowState) error {
	if r.typeErr != nil {
		return r.typeErr
	}
	if err := registerErasedHandler(r.spec, r.actionType, r.handler); err != nil {
		return err
	}
	if err := allowTransition(r.spec, r.from, r.actionType, to...); err != nil {
		delete(r.spec.handlers, r.actionType)
		return err
	}
	return nil
}

func (r methodRouteBuilder[D, I]) MustGoTo(to ...FlowState) {
	if err := r.GoTo(to...); err != nil {
		panic(err)
	}
}

type routeBuilder[D, I any, A Action] struct {
	spec       *Definition[D, I]
	from       FlowState
	actionType ActionType
	typeErr    error
	handler    SubmitFunc[D, A]
}

// WhenFlow starts a route definition for handlers that need the full persisted
// flow.
func WhenFlow[D, I any, A Action](
	spec *Definition[D, I],
	from FlowState,
	handler SubmitFunc[D, A],
) Edge {
	actionType, err := actionTypeFor[A]()
	return routeBuilder[D, I, A]{
		spec:       spec,
		from:       from,
		actionType: actionType,
		typeErr:    err,
		handler:    handler,
	}
}

// When starts a route definition for handlers that only need typed data.
func When[D, I any, A Action](
	spec *Definition[D, I],
	from FlowState,
	handler DataFunc[D, A],
) Edge {
	actionType, err := actionTypeFor[A]()
	if handler == nil && err == nil {
		err = fmt.Errorf("%w: submit handler is nil", ErrInvalidFlow)
	}

	var wrapped SubmitFunc[D, A]
	if handler != nil {
		wrapped = func(ctx context.Context, instance *Flow[D], action A) (*Transition[D], error) {
			return handler(ctx, instance.Data, action)
		}
	}

	return routeBuilder[D, I, A]{
		spec:       spec,
		from:       from,
		actionType: actionType,
		typeErr:    err,
		handler:    wrapped,
	}
}

// GoTo completes a route definition by declaring its allowed target states.
func (r routeBuilder[D, I, A]) GoTo(to ...FlowState) error {
	if r.typeErr != nil {
		return r.typeErr
	}
	if err := registerHandler(r.spec, r.actionType, r.handler); err != nil {
		return err
	}
	if err := allowTransition(r.spec, r.from, r.actionType, to...); err != nil {
		delete(r.spec.handlers, r.actionType)
		return err
	}
	return nil
}

// MustGoTo completes a route definition and panics on error.
func (r routeBuilder[D, I, A]) MustGoTo(to ...FlowState) {
	if err := r.GoTo(to...); err != nil {
		panic(err)
	}
}

type startBuilder[D, I any] struct {
	spec *Definition[D, I]
}

// Start starts the begin-transition definition.
func Start[D, I any](spec *Definition[D, I]) Edge {
	return startBuilder[D, I]{spec: spec}
}

// GoTo completes the begin-transition definition.
func (s startBuilder[D, I]) GoTo(to ...FlowState) error {
	return allowTransition(s.spec, BeginState, BeginAction, to...)
}

// MustGoTo completes the begin-transition definition and panics on error.
func (s startBuilder[D, I]) MustGoTo(to ...FlowState) {
	if err := s.GoTo(to...); err != nil {
		panic(err)
	}
}

func allowTransition[D, I any](spec *Definition[D, I], from FlowState, action ActionType, to ...FlowState) error {
	if spec == nil {
		return fmt.Errorf("%w: spec is nil", ErrInvalidFlow)
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
	if spec.transitions == nil {
		spec.transitions = make(Transitions)
	}
	if spec.transitions[from] == nil {
		spec.transitions[from] = make(ActionTransitions)
	}
	for _, target := range to {
		if target == "" {
			return fmt.Errorf("%w: target state is empty", ErrInvalidTransition)
		}
		spec.transitions[from][action] = append(spec.transitions[from][action], target)
	}
	return nil
}

func actionPrototype(action Action) (ActionType, reflect.Type, error) {
	if action == nil {
		return "", nil, fmt.Errorf("%w: action prototype is nil", ErrUnsupportedAction)
	}

	actionValueType := reflect.TypeOf(action)
	if isNilAction(action) {
		if actionValueType.Kind() != reflect.Pointer {
			return "", nil, fmt.Errorf("%w: zero action is nil", ErrUnsupportedAction)
		}
		action = reflect.New(actionValueType.Elem()).Interface().(Action)
	}

	actionType := action.Type()
	if actionType == "" {
		return "", nil, fmt.Errorf("%w: action type is empty", ErrUnsupportedAction)
	}
	return actionType, actionValueType, nil
}

func methodSubmitHandler[D any](handler any, actionType reflect.Type, withRun bool) (submitHandler[D], error) {
	if isNilHandler(handler) {
		return nil, fmt.Errorf("%w: submit handler is nil", ErrInvalidFlow)
	}

	handlerValue := reflect.ValueOf(handler)
	if err := validateSubmitHandler[D](handlerValue.Type(), actionType, withRun); err != nil {
		return nil, err
	}

	return func(ctx context.Context, run *Flow[D], action Action) (*Transition[D], error) {
		if action == nil {
			return nil, ErrUnsupportedAction
		}
		if actualType := reflect.TypeOf(action); actualType != actionType {
			return nil, fmt.Errorf("%w: expected action %s, got %s", ErrUnsupportedAction, actionType, actualType)
		}

		args := []reflect.Value{reflect.ValueOf(ctx)}
		if withRun {
			args = append(args, reflect.ValueOf(run))
		} else {
			args = append(args, reflect.ValueOf(run.Data))
		}
		args = append(args, reflect.ValueOf(action))

		out := handlerValue.Call(args)
		if !out[1].IsNil() {
			return nil, out[1].Interface().(error)
		}
		if out[0].IsNil() {
			return nil, nil
		}
		return out[0].Interface().(*Transition[D]), nil
	}, nil
}

func validateSubmitHandler[D any](handlerType reflect.Type, actionType reflect.Type, withRun bool) error {
	if handlerType.Kind() != reflect.Func {
		return fmt.Errorf("%w: submit handler must be a function", ErrInvalidFlow)
	}
	if handlerType.NumIn() != 3 {
		return fmt.Errorf("%w: submit handler must have 3 inputs", ErrInvalidFlow)
	}
	if handlerType.In(0) != contextType {
		return fmt.Errorf("%w: submit handler first input must be context.Context", ErrInvalidFlow)
	}

	dataType := reflect.TypeFor[D]()
	runType := reflect.TypeFor[*Flow[D]]()
	if withRun {
		if handlerType.In(1) != runType {
			return fmt.Errorf("%w: submit handler second input must be *trail.Flow[%s]", ErrInvalidFlow, dataType)
		}
	} else if handlerType.In(1) != dataType {
		return fmt.Errorf("%w: submit handler second input must be %s", ErrInvalidFlow, dataType)
	}
	if handlerType.In(2) != actionType {
		return fmt.Errorf("%w: submit handler action input must be %s", ErrInvalidFlow, actionType)
	}

	transitionType := reflect.TypeFor[*Transition[D]]()
	if handlerType.NumOut() != 2 || handlerType.Out(0) != transitionType || handlerType.Out(1) != errorType {
		return fmt.Errorf("%w: submit handler must return (*trail.Transition[%s], error)", ErrInvalidFlow, dataType)
	}
	return nil
}

func actionTypeFor[A Action]() (ActionType, error) {
	var action A
	if isNilAction(action) {
		actionType := reflect.TypeFor[A]()
		if actionType.Kind() != reflect.Pointer {
			return "", fmt.Errorf("%w: zero action is nil", ErrUnsupportedAction)
		}
		action = reflect.New(actionType.Elem()).Interface().(A)
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
