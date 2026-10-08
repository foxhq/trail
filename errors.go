package trail

import "errors"

var (
	ErrFlowNotFound                 = errors.New("trail: flow not found")
	ErrFlowExpired                  = errors.New("trail: flow expired")
	ErrFlowCompleted                = errors.New("trail: flow completed")
	ErrFlowCancelled                = errors.New("trail: flow cancelled")
	ErrFlowConflict                 = errors.New("trail: flow conflict")
	ErrSpecNotFound                 = errors.New("trail: spec not found")
	ErrSpecAlreadyRegistered        = errors.New("trail: spec already registered")
	ErrUnsupportedAction            = errors.New("trail: unsupported action")
	ErrUnsupportedEffect            = errors.New("trail: unsupported effect")
	ErrUnsupportedCancellation      = errors.New("trail: unsupported cancellation")
	ErrEffectRecorderRequired       = errors.New("trail: effect recorder required")
	ErrTransactionalEffectsRequired = errors.New("trail: transactional unit of work required for effects")
	ErrEffectRecordFailed           = errors.New("trail: effect record failed")
	ErrInvalidTransition            = errors.New("trail: invalid transition")
	ErrInvalidFlow                  = errors.New("trail: invalid flow")
	ErrInvalidSnapshot              = errors.New("trail: invalid snapshot")
	ErrInvalidIdempotencyKey        = errors.New("trail: invalid idempotency key")
	ErrIdempotencyInProgress        = errors.New("trail: idempotency request in progress")
	ErrIdempotencyKeyReuse          = errors.New("trail: idempotency key reused with a different request")
	ErrUnsupportedDataVersion       = errors.New("trail: unsupported data version")
)
