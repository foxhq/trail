package trail

import (
	"context"
	"errors"
	"fmt"
	"reflect"
)

var (
	contextType         = reflect.TypeFor[context.Context]()
	effectInterfaceType = reflect.TypeFor[Effect]()
	errorType           = reflect.TypeFor[error]()
	resultType          = reflect.TypeFor[Result]()
)

type effectHandler func(ctx context.Context, result Result, effect Effect) error

// EffectFunc handles one typed effect.
type EffectFunc[E Effect] func(ctx context.Context, effect E) error

// EffectResultFunc handles one typed effect with the committed engine result.
type EffectResultFunc[E Effect] func(ctx context.Context, result Result, effect E) error

// EffectRoute configures an EffectRouter.
type EffectRoute func(*EffectRouter)

// EffectRouter routes effects to registered typed handlers.
type EffectRouter struct {
	handlers map[EffectType]effectHandler
	errs     []error
}

// NewEffectRouter creates an effect executor that routes effects by Effect.Type().
func NewEffectRouter(routes ...EffectRoute) *EffectRouter {
	router := &EffectRouter{
		handlers: make(map[EffectType]effectHandler),
	}
	for _, route := range routes {
		if route == nil {
			router.addError(fmt.Errorf("%w: effect route is nil", ErrInvalidFlow))
			continue
		}
		route(router)
	}
	return router
}

// Handle registers a typed effect handler.
//
// The effect argument is a zero-value prototype used to identify the effect
// type. The handler must have this shape:
//
//	func(context.Context, MyEffect) error
func (r *EffectRouter) Handle(effect Effect, handler any) error {
	return r.register(effect, false, handler)
}

// HandleWithResult registers a typed effect handler that also receives the
// committed engine result.
//
// The effect argument is a zero-value prototype used to identify the effect
// type. The handler must have this shape:
//
//	func(context.Context, trail.Result, MyEffect) error
func (r *EffectRouter) HandleWithResult(effect Effect, handler any) error {
	return r.register(effect, true, handler)
}

// OnEffect registers a typed effect handler and records setup errors for
// Validate.
//
// This fluent method is intentionally non-generic because Go methods cannot
// declare their own type parameters. Use the package-level OnEffect function
// when you want compile-time generic checking.
func (r *EffectRouter) OnEffect(handler any) *EffectRouter {
	if r == nil {
		return r
	}
	r.addError(r.registerInferred(false, handler))
	return r
}

// OnEffectWithResult registers a typed effect handler that also receives the
// committed engine result, and records setup errors for Validate.
//
// This fluent method is intentionally non-generic because Go methods cannot
// declare their own type parameters. Use the package-level OnEffectWithResult
// function when you want compile-time generic checking.
func (r *EffectRouter) OnEffectWithResult(handler any) *EffectRouter {
	if r == nil {
		return r
	}
	r.addError(r.registerInferred(true, handler))
	return r
}

// Validate returns all effect registration errors collected by fluent or
// constructor-style registration.
func (r *EffectRouter) Validate() error {
	if r == nil {
		return fmt.Errorf("%w: effect router is nil", ErrInvalidFlow)
	}
	return errors.Join(r.errs...)
}

// MustValidate panics if Validate fails.
func (r *EffectRouter) MustValidate() {
	if err := r.Validate(); err != nil {
		panic(err)
	}
}

// OnEffect returns a route that registers a compile-time checked typed effect
// handler.
func OnEffect[E Effect](handler EffectFunc[E]) EffectRoute {
	return func(router *EffectRouter) {
		if router == nil {
			return
		}
		router.addError(HandleEffect(router, handler))
	}
}

// OnEffectWithResult returns a route that registers a compile-time checked
// typed effect handler that also receives the committed engine result.
func OnEffectWithResult[E Effect](handler EffectResultFunc[E]) EffectRoute {
	return func(router *EffectRouter) {
		if router == nil {
			return
		}
		router.addError(HandleEffectWithResult(router, handler))
	}
}

func (r *EffectRouter) register(effect Effect, withResult bool, handler any) error {
	if r == nil {
		return fmt.Errorf("%w: effect router is nil", ErrInvalidFlow)
	}
	if isNilHandler(handler) {
		return fmt.Errorf("%w: effect handler is nil", ErrInvalidFlow)
	}

	effectType, effectValueType, err := effectPrototype(effect)
	if err != nil {
		return err
	}
	if _, exists := r.handlers[effectType]; exists {
		return fmt.Errorf("%w: handler already registered for %s", ErrInvalidFlow, effectType)
	}

	handlerValue := reflect.ValueOf(handler)
	if err := validateEffectHandler(handlerValue.Type(), effectValueType, withResult); err != nil {
		return err
	}

	r.handlers[effectType] = func(ctx context.Context, result Result, effect Effect) error {
		if effect == nil {
			return ErrUnsupportedEffect
		}
		if actualType := reflect.TypeOf(effect); actualType != effectValueType {
			return fmt.Errorf("%w: expected effect %s, got %s", ErrUnsupportedEffect, effectValueType, actualType)
		}

		args := []reflect.Value{reflect.ValueOf(ctx)}
		if withResult {
			args = append(args, reflect.ValueOf(result))
		}
		args = append(args, reflect.ValueOf(effect))

		out := handlerValue.Call(args)
		if out[0].IsNil() {
			return nil
		}
		return out[0].Interface().(error)
	}
	return nil
}

func (r *EffectRouter) registerInferred(withResult bool, handler any) error {
	if r == nil {
		return fmt.Errorf("%w: effect router is nil", ErrInvalidFlow)
	}
	if isNilHandler(handler) {
		return fmt.Errorf("%w: effect handler is nil", ErrInvalidFlow)
	}

	handlerValue := reflect.ValueOf(handler)
	handlerType := handlerValue.Type()
	effectValueType, err := inferEffectHandlerType(handlerType, withResult)
	if err != nil {
		return err
	}
	effectType, err := effectTypeFromType(effectValueType)
	if err != nil {
		return err
	}
	if err := validateEffectHandler(handlerType, effectValueType, withResult); err != nil {
		return err
	}
	return r.registerReflectedHandler(effectType, effectValueType, withResult, handlerValue)
}

func (r *EffectRouter) registerHandler(effectType EffectType, handler effectHandler) error {
	if r == nil {
		return fmt.Errorf("%w: effect router is nil", ErrInvalidFlow)
	}
	if handler == nil {
		return fmt.Errorf("%w: effect handler is nil", ErrInvalidFlow)
	}
	if effectType == "" {
		return fmt.Errorf("%w: effect type is empty", ErrUnsupportedEffect)
	}
	if _, exists := r.handlers[effectType]; exists {
		return fmt.Errorf("%w: handler already registered for %s", ErrInvalidFlow, effectType)
	}
	r.handlers[effectType] = handler
	return nil
}

func (r *EffectRouter) registerReflectedHandler(effectType EffectType, effectValueType reflect.Type, withResult bool, handlerValue reflect.Value) error {
	if r == nil {
		return fmt.Errorf("%w: effect router is nil", ErrInvalidFlow)
	}
	if effectType == "" {
		return fmt.Errorf("%w: effect type is empty", ErrUnsupportedEffect)
	}
	if _, exists := r.handlers[effectType]; exists {
		return fmt.Errorf("%w: handler already registered for %s", ErrInvalidFlow, effectType)
	}

	r.handlers[effectType] = func(ctx context.Context, result Result, effect Effect) error {
		if effect == nil {
			return ErrUnsupportedEffect
		}
		if actualType := reflect.TypeOf(effect); actualType != effectValueType {
			return fmt.Errorf("%w: expected effect %s, got %s", ErrUnsupportedEffect, effectValueType, actualType)
		}

		args := []reflect.Value{reflect.ValueOf(ctx)}
		if withResult {
			args = append(args, reflect.ValueOf(result))
		}
		args = append(args, reflect.ValueOf(effect))

		out := handlerValue.Call(args)
		if out[0].IsNil() {
			return nil
		}
		return out[0].Interface().(error)
	}
	return nil
}

func (r *EffectRouter) addError(err error) {
	if err != nil {
		r.errs = append(r.errs, err)
	}
}

// HandleEffect registers a compile-time checked typed effect handler.
func HandleEffect[E Effect](router *EffectRouter, handler EffectFunc[E]) error {
	if handler == nil {
		return fmt.Errorf("%w: effect handler is nil", ErrInvalidFlow)
	}

	effectType, err := effectTypeFor[E]()
	if err != nil {
		return err
	}
	return router.registerHandler(effectType, func(ctx context.Context, _ Result, effect Effect) error {
		typedEffect, err := EffectAs[E](effect)
		if err != nil {
			return err
		}
		return handler(ctx, typedEffect)
	})
}

// HandleEffectWithResult registers a compile-time checked typed effect handler
// that also receives the committed engine result.
func HandleEffectWithResult[E Effect](router *EffectRouter, handler EffectResultFunc[E]) error {
	if handler == nil {
		return fmt.Errorf("%w: effect handler is nil", ErrInvalidFlow)
	}

	effectType, err := effectTypeFor[E]()
	if err != nil {
		return err
	}
	return router.registerHandler(effectType, func(ctx context.Context, result Result, effect Effect) error {
		typedEffect, err := EffectAs[E](effect)
		if err != nil {
			return err
		}
		return handler(ctx, result, typedEffect)
	})
}

// Apply routes each effect to the registered typed handler.
func (r *EffectRouter) Apply(ctx context.Context, result Result, effects []Effect) error {
	if r == nil {
		return fmt.Errorf("%w: effect router is nil", ErrInvalidFlow)
	}
	if err := r.Validate(); err != nil {
		return err
	}
	for _, effect := range effects {
		if effect == nil {
			return ErrUnsupportedEffect
		}
		handler, ok := r.handlers[effect.Type()]
		if !ok {
			return fmt.Errorf("%w: %s", ErrUnsupportedEffect, effect.Type())
		}
		if err := handler(ctx, result, effect); err != nil {
			return err
		}
	}
	return nil
}

// Publish implements EffectSink by applying effects in-process.
func (r *EffectRouter) Publish(ctx context.Context, result Result, effects []Effect) error {
	return r.Apply(ctx, result, effects)
}

func effectPrototype(effect Effect) (EffectType, reflect.Type, error) {
	if effect == nil {
		return "", nil, fmt.Errorf("%w: effect prototype is nil", ErrUnsupportedEffect)
	}

	effectValueType := reflect.TypeOf(effect)
	if isNilEffect(effect) {
		if effectValueType.Kind() != reflect.Pointer {
			return "", nil, fmt.Errorf("%w: zero effect is nil", ErrUnsupportedEffect)
		}
		effect = reflect.New(effectValueType.Elem()).Interface().(Effect)
	}

	effectType := effect.Type()
	if effectType == "" {
		return "", nil, fmt.Errorf("%w: effect type is empty", ErrUnsupportedEffect)
	}
	return effectType, effectValueType, nil
}

func effectTypeFor[E Effect]() (EffectType, error) {
	var effect E
	if isNilEffect(effect) {
		effectType := reflect.TypeFor[E]()
		if effectType.Kind() != reflect.Pointer {
			return "", fmt.Errorf("%w: zero effect is nil", ErrUnsupportedEffect)
		}
		effect = reflect.New(effectType.Elem()).Interface().(E)
	}
	effectType := effect.Type()
	if effectType == "" {
		return "", fmt.Errorf("%w: effect type is empty", ErrUnsupportedEffect)
	}
	return effectType, nil
}

func inferEffectHandlerType(handlerType reflect.Type, withResult bool) (reflect.Type, error) {
	if handlerType.Kind() != reflect.Func {
		return nil, fmt.Errorf("%w: effect handler must be a function", ErrInvalidFlow)
	}

	wantInputs := 2
	if withResult {
		wantInputs = 3
	}
	if handlerType.NumIn() != wantInputs {
		return nil, fmt.Errorf("%w: effect handler must have %d inputs", ErrInvalidFlow, wantInputs)
	}

	effectInput := 1
	if withResult {
		effectInput = 2
	}
	effectValueType := handlerType.In(effectInput)
	if !effectValueType.Implements(effectInterfaceType) {
		return nil, fmt.Errorf("%w: effect handler effect input must implement trail.Effect", ErrInvalidFlow)
	}
	return effectValueType, nil
}

func effectTypeFromType(effectValueType reflect.Type) (EffectType, error) {
	var effect Effect
	if effectValueType.Kind() == reflect.Pointer {
		effect = reflect.New(effectValueType.Elem()).Interface().(Effect)
	} else {
		effect = reflect.Zero(effectValueType).Interface().(Effect)
	}
	if isNilEffect(effect) {
		return "", fmt.Errorf("%w: zero effect is nil", ErrUnsupportedEffect)
	}
	effectType := effect.Type()
	if effectType == "" {
		return "", fmt.Errorf("%w: effect type is empty", ErrUnsupportedEffect)
	}
	return effectType, nil
}

func validateEffectHandler(handlerType reflect.Type, effectType reflect.Type, withResult bool) error {
	if handlerType.Kind() != reflect.Func {
		return fmt.Errorf("%w: effect handler must be a function", ErrInvalidFlow)
	}

	wantInputs := 2
	if withResult {
		wantInputs = 3
	}
	if handlerType.NumIn() != wantInputs {
		return fmt.Errorf("%w: effect handler must have %d inputs", ErrInvalidFlow, wantInputs)
	}
	if handlerType.In(0) != contextType {
		return fmt.Errorf("%w: effect handler first input must be context.Context", ErrInvalidFlow)
	}

	effectInput := 1
	if withResult {
		if handlerType.In(1) != resultType {
			return fmt.Errorf("%w: effect handler second input must be trail.Result", ErrInvalidFlow)
		}
		effectInput = 2
	}
	if handlerType.In(effectInput) != effectType {
		return fmt.Errorf("%w: effect handler effect input must be %s", ErrInvalidFlow, effectType)
	}

	if handlerType.NumOut() != 1 || handlerType.Out(0) != errorType {
		return fmt.Errorf("%w: effect handler must return error", ErrInvalidFlow)
	}
	return nil
}

func isNilEffect(effect any) bool {
	if effect == nil {
		return true
	}
	value := reflect.ValueOf(effect)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func isNilHandler(handler any) bool {
	if handler == nil {
		return true
	}
	value := reflect.ValueOf(handler)
	return value.Kind() == reflect.Func && value.IsNil()
}
