// Package trail provides a typed persistent workflow state-machine engine.
//
// A Definition owns one workflow type, its typed private data, typed begin
// input, and typed action handlers. The engine owns persistence,
// serialization, expiration checks, idempotency, and durable effect recording.
//
// Definitions declare allowed transitions with their Start, When, WhenFlow, and
// WhenCancel methods. Stores must preserve optimistic concurrency through
// Snapshot.Revision and return ErrFlowConflict for stale updates.
package trail
