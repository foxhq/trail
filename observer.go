package trail

import "context"

// EventName identifies an engine lifecycle event.
type EventName string

const (
	EventBeginStarted       EventName = "begin_started"
	EventBeginCompleted     EventName = "begin_completed"
	EventBeginFailed        EventName = "begin_failed"
	EventSubmitStarted      EventName = "submit_started"
	EventSubmitCompleted    EventName = "submit_completed"
	EventSubmitFailed       EventName = "submit_failed"
	EventCancelStarted      EventName = "cancel_started"
	EventCancelCompleted    EventName = "cancel_completed"
	EventCancelFailed       EventName = "cancel_failed"
	EventEffectRecorded     EventName = "effect_recorded"
	EventEffectRecordFailed EventName = "effect_record_failed"
)

// Event is queued during an engine operation and delivered to observers after
// its UnitOfWork has returned.
type Event struct {
	Name   EventName
	Flow   Result
	Action ActionType
	Error  error
}

// Observer receives lifecycle events synchronously after the engine's
// UnitOfWork has returned. Observers do not return errors and must handle their
// own failures; the engine recovers panics. Slow observers add request latency
// but never prolong the UnitOfWork or alter its outcome.
type Observer interface {
	Observe(ctx context.Context, event Event)
}

// ObserverFunc adapts a function to Observer.
type ObserverFunc func(ctx context.Context, event Event)

func (f ObserverFunc) Observe(ctx context.Context, event Event) {
	f(ctx, event)
}
