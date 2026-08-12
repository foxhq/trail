package trail

import "fmt"

// As casts input to T and returns ErrInvalidFlow on mismatch.
func As[T any](value any) (T, error) {
	cast, ok := value.(T)
	if ok {
		return cast, nil
	}
	var zero T
	return zero, fmt.Errorf("%w: expected %T, got %T", ErrInvalidFlow, zero, value)
}

// ActionAs casts an Action to T and returns ErrUnsupportedAction on mismatch.
func ActionAs[T Action](action Action) (T, error) {
	cast, ok := action.(T)
	if ok {
		return cast, nil
	}
	var zero T
	return zero, fmt.Errorf("%w: expected action %T, got %T", ErrUnsupportedAction, zero, action)
}

// EffectAs casts an Effect to T and returns ErrUnsupportedEffect on mismatch.
func EffectAs[T Effect](effect Effect) (T, error) {
	cast, ok := effect.(T)
	if ok {
		return cast, nil
	}
	var zero T
	return zero, fmt.Errorf("%w: expected effect %T, got %T", ErrUnsupportedEffect, zero, effect)
}
