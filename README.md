# trail

Typed, durable workflows for Go.

Trail is for the part of your product that cannot be finished in one request: a sign-in challenge, onboarding, approval, checkout, provisioning, recovery, review, or long-running setup. Define its behavior as ordinary Go, save its progress, and resume it safely later.

It gives a workflow a real shape: typed private data, explicit state transitions, a client-safe view, optimistic concurrency, and durable work that survives a commit. The application still owns its database, transactions, jobs, and delivery infrastructure.

## Contents

- [Install](#install)
- [A workflow in 30 seconds](#a-workflow-in-30-seconds)
- [The model](#the-model)
- [Defining behavior](#defining-behavior)
- [States and transitions](#states-and-transitions)
- [Client views](#client-views)
- [Durable effects](#durable-effects)
- [Transactions](#transactions)
- [Persistence](#persistence)
- [Idempotency](#idempotency)
- [Cancellation](#cancellation)
- [Visualizing workflows](#visualizing-workflows)
- [Observability](#observability)
- [Production checklist](#production-checklist)
- [License](#license)

## Install

```sh
go get github.com/foxhq/trail
```

Trail requires Go 1.27 or later.

## A workflow in 30 seconds

```go
type VerifyData struct {
	CodeHash string
}

type VerifyInput struct{ CodeHash string }
type SubmitCode struct{ Code string }

func (SubmitCode) Type() trail.ActionType { return "submit_code" }

spec := trail.Define(
	trail.FlowType("verify_email"),
	func(ctx context.Context, begin trail.BeginContext, input VerifyInput) (*trail.Transition[VerifyData], error) {
		return trail.To("waiting_for_code", VerifyData{CodeHash: input.CodeHash}).
			WithView(map[string]string{"screen": "enter_code"}), nil
	},
)

spec.Start().MustGoTo("waiting_for_code")

spec.When("waiting_for_code", func(
	ctx context.Context,
	data VerifyData,
	action SubmitCode,
) (*trail.Transition[VerifyData], error) {
	if !codes.Match(data.CodeHash, action.Code) {
		return trail.To("waiting_for_code", data), nil
	}
	return trail.Done("verified", data), nil
}).MustGoTo("waiting_for_code", "verified")

registry := trail.NewRegistry()
if err := trail.Register(registry, spec); err != nil {
	return err
}

engine := trail.NewEngine(store, registry)

started, err := engine.Begin(ctx, trail.BeginRequest{
	Type:      "verify_email",
	SubjectID: "user_123",
	Input:     VerifyInput{CodeHash: hash},
})

done, err := engine.Submit(ctx, trail.SubmitRequest{
	FlowID: started.ID,
	Action: SubmitCode{Code: "123456"},
})
```

## The model

Trail separates three things that are often accidentally mixed together:

- `Flow[D]` is private, typed state for the workflow implementation. It can hold hashes, counters, internal IDs, and decision data.
- `Result.View` is a persisted, client-safe JSON document. It is the current screen or public representation of the flow.
- `Effect` is a durable intent for work outside the transaction, such as an email, webhook, job, or integration event.

`Result` deliberately does not include the subject, metadata, private data, or effect payloads. Those values stay inside trusted application code.

## Defining behavior

For a small flow, an inline begin function is ideal. For a larger flow, put behavior on a struct with its dependencies:

```go
type VerifyEmailFlow struct {
	codes CodeService
}

func (f VerifyEmailFlow) Begin(
	ctx context.Context,
	begin trail.BeginContext,
	input VerifyInput,
) (*trail.Transition[VerifyData], error) {
	return trail.To("waiting_for_code", VerifyData{CodeHash: input.CodeHash}), nil
}

func (f VerifyEmailFlow) SubmitCode(
	ctx context.Context,
	data VerifyData,
	action SubmitCode,
) (*trail.Transition[VerifyData], error) {
	if !f.codes.Match(data.CodeHash, action.Code) {
		return trail.To("waiting_for_code", data), nil
	}
	return trail.Done("verified", data), nil
}

flow := VerifyEmailFlow{codes: codes}
spec := trail.Define("verify_email", flow.Begin)

spec.Start().MustGoTo("waiting_for_code")
spec.When("waiting_for_code", flow.SubmitCode).
	MustGoTo("waiting_for_code", "verified")
```

`When` is for handlers that only need `D`. `WhenFlow` is for the occasional handler that genuinely needs flow metadata such as `ID`, `SubjectID`, `Revision`, or `ExpiresAt`:

```go
spec.WhenFlow("waiting_for_code", func(
	ctx context.Context,
	flow *trail.Flow[VerifyData],
	action SubmitCode,
) (*trail.Transition[VerifyData], error) {
	return trail.To(flow.State, flow.Data), nil
}).MustGoTo("waiting_for_code")
```

The action type is inferred from the handler. Trail does not use reflection-based registration, so an incompatible handler fails to compile.

Definitions are sealed when registered. Finish configuring a `Definition` before calling `Register`.

## States and transitions

Each declaration does two jobs: it registers a typed handler and declares exactly where that handler may lead.

```go
spec.When("waiting_for_code", flow.SubmitCode).
	MustGoTo("waiting_for_code", "verified", "locked")
```

The same action type may be used in multiple states. Trail dispatches by `(current state, action type)`, so a `continue` action can correctly mean one thing in a setup step and another in a review step. `AnyState` is available for an intentional fallback.

Use `trail.To(state, data)` for an active flow and `trail.Done(state, data)` for a completed one. The engine validates returned state edges against the declared graph.

## Client views

Attach a complete public view when a transition changes what a client should render:

```go
type VerifyView struct {
	Screen        string    `json:"screen"`
	CanResendAt   time.Time `json:"canResendAt"`
	AllowedAction []string  `json:"allowedActions"`
}

return trail.To("waiting_for_code", data).WithView(VerifyView{
	Screen:        "enter_code",
	CanResendAt:   data.ResendAfter,
	AllowedAction: []string{"submit_code", "resend_code"},
}), nil
```

Views are full replacements:

- no `WithView` call retains the prior view;
- `WithView(next)` replaces it atomically;
- `ClearView()` removes it.

Trail never patches or merges a view. A transport boundary can decode it once into the response type it owns:

```go
view, err := trail.ViewAs[VerifyView](result)
```

This keeps data serialization at the persistence/API boundary, not inside every state handler.

## Durable effects

An effect is an intent to do work after a successful commit.

```go
type SendVerificationEmail struct {
	Address string
	Code    string
}

func (SendVerificationEmail) Type() trail.EffectType {
	return "send_verification_email"
}

return trail.To("waiting_for_code", data).
	WithEffects(SendVerificationEmail{Address: address, Code: code}), nil
```

The engine never sends mail, calls a webhook, or publishes a message itself. If a transition emits effects, it requires both a configured `UnitOfWork` and `EffectRecorder`; otherwise it fails before persisting the transition.

The recorder writes an outbox row, job, or event record using the same transaction context as the flow snapshot:

```go
engine := trail.NewEngine(
	store,
	registry,
	trail.WithUnitOfWork(appTx),
	trail.WithEffectRecorder(trail.EffectRecorderFunc(func(
		ctx context.Context,
		flow trail.EffectContext,
		effects []trail.Effect,
	) error {
		return outbox.Record(ctx, flow, effects)
	})),
)
```

After commit, a worker dispatches a recorded item with an `EffectRouter`:

```go
router := trail.NewEffectRouter().
	OnEffect(func(ctx context.Context, effect SendVerificationEmail) error {
		return mailer.Send(ctx, effect.Address, effect.Code)
	})
if err := router.Validate(); err != nil {
	return err
}

// In the worker, after loading an outbox record:
err := router.Dispatch(ctx, record.Flow, record.Effects)
```

`OnEffect` and `OnEffectWithContext` are compile-time typed. The constructor form—`trail.NewEffectRouter(trail.OnEffect(...))`—remains available when it reads better. `EffectContext` identifies the flow revision that produced an effect and is provided only to trusted recorder/worker code.

Registration, validation, and dispatch are concurrency-safe. Each `Dispatch` call uses one validated snapshot of the handler table, so a concurrently added route applies only to later dispatches. Call `router.Seal()` after startup configuration to freeze the table; it validates the configuration and rejects later registration. Because fluent registration cannot return an error, a registration attempted after sealing is reported by `Validate` and `Dispatch` as a configuration error.

An `EffectRouter` can also sit behind an `EffectRecorder` when every registered handler only creates another transaction-bound durable record (for example, a job table). It must never be used there for SMTP, HTTP, or broker delivery.

## Transactions

The engine invokes the store, flow handlers, domain repositories, idempotency store, and effect recorder inside one application-owned boundary:

```go
type UnitOfWork interface {
	Do(ctx context.Context, fn func(context.Context) error) error
}
```

Use it to mutate domain entities immediately while returning effects only for external work:

```text
request
  └─ transaction
       ├─ handler changes domain entities
       ├─ Trail writes the next flow snapshot
       ├─ recorder writes outbox/job intent
       └─ commit
             └─ worker dispatches email/webhook/event
```

This is compatible with an existing outbox or message platform. Trail owns neither; it only enforces that it receives a transaction-bound recorder for effectful transitions.

## Persistence

Trail stores `Snapshot` values. A store assigns the committed revision and returns the committed snapshot, avoiding a second read after every write:

```go
type Store interface {
	Create(context.Context, trail.Snapshot) (trail.Snapshot, error)
	Get(context.Context, trail.FlowID) (trail.Snapshot, error)
	Update(context.Context, trail.Snapshot) (trail.Snapshot, error)
	Delete(context.Context, trail.FlowID) error
}
```

`Update` must compare the supplied `Snapshot.Revision` and return `trail.ErrFlowConflict` for a stale write. `MemoryStore` is useful for tests and local development; production stores normally implement the same compare-and-swap condition in SQL.

`QueryStore` and `CleanupStore` are optional interfaces for operational listing and retention cleanup.

The `trail/storetest` package provides a reusable contract suite for adapters:

```go
func TestPostgresStoreContract(t *testing.T) {
	storetest.Contract(t, func(t testing.TB) trail.Store {
		return newIsolatedPostgresStore(t)
	})
}
```

Data versioning belongs to the codec. `Define` starts with a safe version-one JSON codec that rejects another stored version. When data evolves, supply a `Codec[D]` that explicitly decodes or migrates every historical version you retain.

## Idempotency

For retryable public requests, configure an `IdempotencyStore`:

```go
engine := trail.NewEngine(
	store,
	registry,
	trail.WithIdempotencyStore(idempotencyStore),
)

result, err := engine.Submit(ctx, trail.SubmitRequest{
	FlowID:         flowID,
	Action:         SubmitCode{Code: "123456"},
	IdempotencyKey: "request_abc",
})
```

Trail atomically reserves the key. A completed matching request replays its safe `Result`; a concurrent matching request returns `ErrIdempotencyInProgress`; a different request under the same key returns `ErrIdempotencyKeyReuse`.

By default Trail fingerprints JSON-serializable request input. Set `IdempotencyFingerprint` yourself when the input/action is not JSON-serializable or when your API already has a canonical request hash. Your production idempotency implementation must use the same transaction as the flow store.

## Cancellation

Every active flow can use standard cancellation:

```go
result, err := engine.Cancel(ctx, trail.CancelRequest{
	FlowID: flowID,
	Reason: "user_cancelled",
})
```

For flows that must release a reservation, clear a challenge, change the view, or emit an effect during cancellation, declare typed state-specific behavior:

```go
spec.WhenCancel("awaiting_approval", func(
	ctx context.Context,
	flow *trail.Flow[ApprovalData],
	cancel trail.CancelContext,
) (*trail.Transition[ApprovalData], error) {
	return trail.To("cancelled", release(flow.Data)).
		WithView(ApprovalView{Status: "cancelled"}), nil
}).MustGoTo("cancelled")
```

Trail validates the declared cancellation edge and marks the resulting flow `Completed` and `Cancelled`.

## Visualizing workflows

Trail can export the transitions already declared in source code. Keep that as developer tooling, not a production endpoint:

```go
graph, err := trail.GraphOfRegistry(registry)
if err != nil {
	return err
}
fmt.Print(trail.Mermaid(graph))
```

For example, a small command can build the app registry and write a Mermaid artifact during CI:

```sh
go run ./cmd/workflowviz > docs/workflows.mmd
```

The diagram comes from the same `Start`, `When`, and `WhenCancel` declarations that the engine enforces. There is no separate visualization configuration to drift.

## Observability

Observers receive lifecycle events for metrics, logs, and tracing:

```go
observer := trail.ObserverFunc(func(ctx context.Context, event trail.Event) {
	logger.Info("trail event",
		"name", event.Name,
		"flow_id", event.Flow.ID,
		"type", event.Flow.Type,
		"state", event.Flow.State,
		"action", event.Action,
		"error", event.Error,
	)
})
```

Observer events carry the same client-safe `Result` shape as the engine. They never contain private data, subject IDs, metadata, or effect payloads. Events are delivered synchronously, in lifecycle order, after the unit of work has returned. Observers must handle their own errors; Trail recovers observer panics. They cannot affect the flow or hold its transaction open, although slow observers still add request latency.

## Production checklist

- Finish and register definitions before serving requests.
- Back `Store.Update` with optimistic compare-and-swap revisions.
- Keep private flow data serializable and create an explicit codec migration before changing its version.
- Use `WithUnitOfWork` and a transactional `EffectRecorder` for any flow that emits effects.
- Dispatch effects from an outbox/job worker after commit, never from a request handler.
- Use idempotency for retryable public writes and store it in the same transaction as the flow.
- Return complete client-safe views; decode them with `ViewAs` at the API boundary.
- Call `router.Seal()` during startup when the effect routes should remain immutable, or `router.Validate()` when runtime route registration is intentional.
- Generate graphs in development or CI to review real transition declarations.

## License

MIT © 2026 Phuc Tran, Manifox Technology Solutions Co., Ltd.
