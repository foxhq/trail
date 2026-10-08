package trail

import (
	"fmt"
	"reflect"
	"sync"
)

// SpecRegistry maps flow types to specs.
type SpecRegistry struct {
	mu    sync.RWMutex
	specs map[FlowType]erasedSpec
}

type definitionSealer interface {
	seal()
}

type dataVersionSupport interface {
	SupportsDataVersion(version int) bool
}

// NewRegistry creates an empty spec registry.
func NewRegistry() *SpecRegistry {
	return &SpecRegistry{
		specs: make(map[FlowType]erasedSpec),
	}
}

// Register adds a typed spec to a registry.
func Register[D any](r *SpecRegistry, spec Spec[D]) error {
	if r == nil {
		return fmt.Errorf("%w: registry is nil", ErrInvalidFlow)
	}
	if spec == nil || isNilSpec(spec) {
		return fmt.Errorf("%w: spec is nil", ErrInvalidFlow)
	}
	flowType := spec.Type()
	if flowType == "" {
		return fmt.Errorf("%w: spec type is empty", ErrInvalidFlow)
	}
	if spec.DataVersion() <= 0 {
		return fmt.Errorf("%w: data version must be positive", ErrInvalidFlow)
	}
	if versioned, ok := any(spec).(dataVersionSupport); ok && !versioned.SupportsDataVersion(spec.DataVersion()) {
		return fmt.Errorf("%w: codec does not support data version %d", ErrUnsupportedDataVersion, spec.DataVersion())
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.specs[flowType]; exists {
		return fmt.Errorf("%w: %s", ErrSpecAlreadyRegistered, flowType)
	}
	if sealable, ok := any(spec).(definitionSealer); ok {
		sealable.seal()
	}
	r.specs[flowType] = eraseSpec(spec)
	return nil
}

func isNilSpec(spec any) bool {
	value := reflect.ValueOf(spec)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// Has reports whether a flow type has a registered spec.
func (r *SpecRegistry) Has(flowType FlowType) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.specs[flowType]
	return ok
}

func (r *SpecRegistry) get(flowType FlowType) (erasedSpec, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	spec, ok := r.specs[flowType]
	return spec, ok
}

// Types returns the registered flow types.
func (r *SpecRegistry) Types() []FlowType {
	r.mu.RLock()
	defer r.mu.RUnlock()
	types := make([]FlowType, 0, len(r.specs))
	for flowType := range r.specs {
		types = append(types, flowType)
	}
	return types
}
