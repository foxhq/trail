// Package trail provides a typed persistent workflow state-machine engine.
//
// A Definition owns one workflow type, its typed private data, typed begin
// input, and typed action handlers. The engine owns persistence,
// serialization, expiration checks, idempotency, and effect execution.
//
// Definitions declare allowed transitions with Start, When, and WhenFlow, either
// as package-level functions or as Definition methods.
// Stores must preserve optimistic concurrency through Snapshot.Revision and return
// ErrFlowConflict for stale updates.
package trail
