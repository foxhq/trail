package trail

import "errors"

var (
	ErrFlowNotFound           = errors.New("trail: flow not found")
	ErrFlowExpired            = errors.New("trail: flow expired")
	ErrFlowCompleted          = errors.New("trail: flow completed")
	ErrFlowCancelled          = errors.New("trail: flow cancelled")
	ErrFlowConflict           = errors.New("trail: flow conflict")
	ErrSpecNotFound           = errors.New("trail: spec not found")
	ErrSpecAlreadyRegistered  = errors.New("trail: spec already registered")
	ErrUnsupportedAction      = errors.New("trail: unsupported action")
	ErrUnsupportedEffect      = errors.New("trail: unsupported effect")
	ErrEffectPublishFailed    = errors.New("trail: effect publish failed")
	ErrInvalidTransition      = errors.New("trail: invalid transition")
	ErrInvalidFlow            = errors.New("trail: invalid flow")
	ErrInvalidSnapshot        = errors.New("trail: invalid snapshot")
	ErrInvalidIdempotencyKey  = errors.New("trail: invalid idempotency key")
	ErrUnsupportedDataVersion = errors.New("trail: unsupported data version")
)
