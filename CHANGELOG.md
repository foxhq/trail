# Changelog

## v0.1.0

Initial release.

- Typed workflow definitions with `Define`.
- Route registration with package-level `Start`, `When`, and `WhenFlow`, plus method-style `Definition.Start`, `Definition.When`, and `Definition.WhenFlow`.
- Typed workflow data, typed begin input, and typed actions.
- Typed effect routing with `NewEffectRouter`, `OnEffect`, and `OnEffectWithResult`.
- Snapshot-based persistence contract with optimistic concurrency.
- Optional idempotency store, effect sink, lifecycle observers, query store, and cleanup store.
- In-memory store implementations for tests and local development.
