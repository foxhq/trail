# Changelog

## v0.2.0

Breaking hardening pass before the next release.

- Routes now dispatch by `(state, action type)`, so an action type can have distinct behavior in distinct states.
- Replaced reflection-based route/effect registration with Go 1.27 generic fluent methods: `Definition.When`, `Definition.WhenFlow`, `Definition.WhenCancel`, and `EffectRouter.OnEffect`.
- Removed compatibility public fields and in-request `EffectSink` dispatch.
- `Result` is now client-safe by construction and exposes a persisted `json.RawMessage` view decoded with `ViewAs`.
- Effects require an explicit transactional `UnitOfWork` and `EffectRecorder`; `EffectRouter` is a separate typed routing adapter.
- Stores return committed snapshots from `Create` and `Update`, eliminating post-write reads.
- Idempotency uses atomic reserve/complete/abort semantics and detects key reuse with a different request fingerprint.
- Added typed cancellation routes, definition sealing, version-support validation, and the reusable `trail/storetest` contract suite.
- Effect routers are safe for concurrent registration and dispatch; `Seal` optionally freezes a validated routing table.
- Lifecycle observers now receive queued events after the unit of work returns, preserving flow outcomes and transaction latency.

## v0.1.0

Initial release.
