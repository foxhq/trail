package trail

import "context"

// EventName identifies an engine lifecycle event.
type EventName string

const (
	EventBeginStarted        EventName = "begin_started"
	EventBeginCompleted      EventName = "begin_completed"
	EventBeginFailed         EventName = "begin_failed"
	EventSubmitStarted       EventName = "submit_started"
	EventSubmitCompleted     EventName = "submit_completed"
	EventSubmitFailed        EventName = "submit_failed"
	EventCancelStarted       EventName = "cancel_started"
	EventCancelCompleted     EventName = "cancel_completed"
	EventCancelFailed        EventName = "cancel_failed"
	EventEffectPublished     EventName = "effect_published"
	EventEffectPublishFailed EventName = "effect_publish_failed"
)

// Event is sent to observers during engine operations.
type Event struct {
	Name   EventName
	Flow   Result
	Action ActionType
	Error  error
}

// Observer receives lifecycle events. Observer failures must be handled inside
// the observer; panics are recovered by the engine.
type Observer interface {
	Observe(ctx context.Context, event Event)
}

// ObserverFunc adapts a function to Observer.
type ObserverFunc func(ctx context.Context, event Event)

func (f ObserverFunc) Observe(ctx context.Context, event Event) {
	f(ctx, event)
}
