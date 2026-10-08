package trail

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
)

type effectHandler func(ctx context.Context, flow EffectContext, effect Effect) error

// EffectFunc handles one typed effect.
type EffectFunc[E Effect] func(ctx context.Context, effect E) error

// EffectContextFunc handles one typed effect with the trusted context of the
// committed flow revision that produced it.
type EffectContextFunc[E Effect] func(ctx context.Context, flow EffectContext, effect E) error

// EffectRoute configures an EffectRouter. Use OnEffect or
// OnEffectWithContext; both preserve compile-time checking for the effect type.
type EffectRoute func(*EffectRouter)

// EffectRouter routes effects to typed handlers. It is not an Engine option:
// the engine only records durable intents through EffectRecorder. Use it from
// a worker to perform post-commit delivery, or from a recorder only when every
// handler itself persists a durable job or event rather than calling a remote
// system.
type EffectRouter struct {
	mu       sync.RWMutex
	handlers map[EffectType]effectHandler
	errs     []error
	sealed   bool
}

// NewEffectRouter creates an effect dispatcher. Routes are compile-time typed;
// Validate is optional and reports duplicate or invalid registrations.
func NewEffectRouter(routes ...EffectRoute) *EffectRouter {
	router := &EffectRouter{handlers: make(map[EffectType]effectHandler)}
	for _, route := range routes {
		if route == nil {
			router.addError(fmt.Errorf("%w: effect route is nil", ErrInvalidFlow))
			continue
		}
		route(router)
	}
	return router
}

// OnEffect registers a compile-time checked typed effect handler and returns
// the router, enabling fluent composition.
func (r *EffectRouter) OnEffect[E Effect](handler EffectFunc[E]) *EffectRouter {
	if r == nil {
		return r
	}
	if handler == nil {
		r.addError(fmt.Errorf("%w: effect handler is nil", ErrInvalidFlow))
		return r
	}
	r.addError(registerEffect(r, func(ctx context.Context, _ EffectContext, effect E) error {
		return handler(ctx, effect)
	}))
	return r
}

// OnEffectWithContext registers a compile-time checked typed effect handler
// that also receives the producing flow's trusted context.
func (r *EffectRouter) OnEffectWithContext[E Effect](handler EffectContextFunc[E]) *EffectRouter {
	if r == nil {
		return r
	}
	if handler == nil {
		r.addError(fmt.Errorf("%w: effect handler is nil", ErrInvalidFlow))
		return r
	}
	r.addError(registerEffect(r, handler))
	return r
}

// Validate returns collected router configuration errors. It is safe to call
// concurrently with registration and dispatch, and does not seal the router.
func (r *EffectRouter) Validate() error {
	if r == nil {
		return fmt.Errorf("%w: effect router is nil", ErrInvalidFlow)
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return errors.Join(r.errs...)
}

// MustValidate panics if router configuration is invalid. It is convenient for
// composition-root setup, where invalid registrations are programmer errors.
func (r *EffectRouter) MustValidate() {
	if err := r.Validate(); err != nil {
		panic(err)
	}
}

// Seal validates the router and prevents further handler registration. Use it
// after startup configuration when an immutable routing table is desired.
// Dispatch remains safe for concurrent use and uses a consistent handler
// snapshot for each call.
func (r *EffectRouter) Seal() error {
	if r == nil {
		return fmt.Errorf("%w: effect router is nil", ErrInvalidFlow)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sealed = true
	return errors.Join(r.errs...)
}

// MustSeal panics when sealing finds an invalid router configuration.
func (r *EffectRouter) MustSeal() {
	if err := r.Seal(); err != nil {
		panic(err)
	}
}

// OnEffect returns a compile-time checked route for a typed effect handler.
func OnEffect[E Effect](handler EffectFunc[E]) EffectRoute {
	return func(router *EffectRouter) {
		router.OnEffect(handler)
	}
}

// OnEffectWithContext returns a compile-time checked route for a typed effect
// handler that needs the producing flow's trusted context.
func OnEffectWithContext[E Effect](handler EffectContextFunc[E]) EffectRoute {
	return func(router *EffectRouter) {
		router.OnEffectWithContext(handler)
	}
}

func registerEffect[E Effect](router *EffectRouter, handler EffectContextFunc[E]) error {
	if router == nil {
		return fmt.Errorf("%w: effect router is nil", ErrInvalidFlow)
	}
	if handler == nil {
		return fmt.Errorf("%w: effect handler is nil", ErrInvalidFlow)
	}
	effectType, err := effectTypeFor[E]()
	if err != nil {
		return err
	}

	router.mu.Lock()
	defer router.mu.Unlock()
	if router.sealed {
		return fmt.Errorf("%w: effect router is sealed", ErrInvalidFlow)
	}
	if _, exists := router.handlers[effectType]; exists {
		return fmt.Errorf("%w: handler already registered for %s", ErrInvalidFlow, effectType)
	}
	router.handlers[effectType] = func(ctx context.Context, flow EffectContext, effect Effect) error {
		typedEffect, err := EffectAs[E](effect)
		if err != nil {
			return err
		}
		return handler(ctx, flow, typedEffect)
	}
	return nil
}

func (r *EffectRouter) addError(err error) {
	if err == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errs = append(r.errs, err)
}

// Dispatch routes already-recorded effects to their typed handlers. Call it
// from an outbox worker or job runner after the committing transaction ends.
// It is safe to call concurrently with registration; each dispatch uses one
// consistent snapshot of the router's validated handler table.
func (r *EffectRouter) Dispatch(ctx context.Context, flow EffectContext, effects []Effect) error {
	if r == nil {
		return fmt.Errorf("%w: effect router is nil", ErrInvalidFlow)
	}

	r.mu.RLock()
	err := errors.Join(r.errs...)
	handlers := make(map[EffectType]effectHandler, len(r.handlers))
	for effectType, handler := range r.handlers {
		handlers[effectType] = handler
	}
	r.mu.RUnlock()
	if err != nil {
		return err
	}

	for _, effect := range effects {
		if effect == nil {
			return ErrUnsupportedEffect
		}
		handler, ok := handlers[effect.Type()]
		if !ok {
			return fmt.Errorf("%w: %s", ErrUnsupportedEffect, effect.Type())
		}
		if err := handler(ctx, flow, effect); err != nil {
			return err
		}
	}
	return nil
}

func effectTypeFor[E Effect]() (EffectType, error) {
	var effect E
	if isNilEffect(effect) {
		effectValueType := reflect.TypeFor[E]()
		if effectValueType.Kind() != reflect.Pointer {
			return "", fmt.Errorf("%w: zero effect is nil", ErrUnsupportedEffect)
		}
		effect = reflect.New(effectValueType.Elem()).Interface().(E)
	}
	effectType := effect.Type()
	if effectType == "" {
		return "", fmt.Errorf("%w: effect type is empty", ErrUnsupportedEffect)
	}
	return effectType, nil
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
