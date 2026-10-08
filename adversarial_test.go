package trail

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type clearViewAction struct{}
type setViewAction struct{}
type fallbackAction struct{}
type lateEffect struct{}

func (clearViewAction) Type() ActionType { return "clear_view" }
func (setViewAction) Type() ActionType   { return "set_view" }
func (fallbackAction) Type() ActionType  { return "fallback" }
func (lateEffect) Type() EffectType      { return "late" }

type adversarialClock struct{ now time.Time }

func (c *adversarialClock) Now() time.Time { return c.now }

func TestInvalidRoutesAreAtomicAndUndeclaredTargetsAreRejected(t *testing.T) {
	spec := Define(FlowType("invalid_routes"), func(_ context.Context, _ BeginContext, _ testInput) (*Transition[testData], error) {
		return To("one", testData{}), nil
	})

	if err := spec.Start().GoTo("one", ""); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("invalid start route error = %v, want ErrInvalidTransition", err)
	}
	graph, err := GraphOf(spec)
	if err != nil {
		t.Fatal(err)
	}
	if len(graph.Edges) != 0 {
		t.Fatalf("invalid start route left graph edges: %+v", graph.Edges)
	}

	spec.Start().MustGoTo("one")
	err = spec.When("one", func(_ context.Context, data testData, _ advance) (*Transition[testData], error) {
		return To("two", data), nil
	}).GoTo("two", "")
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("invalid action route error = %v, want ErrInvalidTransition", err)
	}
	graph, err = GraphOf(spec)
	if err != nil {
		t.Fatal(err)
	}
	if len(graph.Edges) != 1 {
		t.Fatalf("invalid action route left graph edges: %+v", graph.Edges)
	}

	runtimeSpec := Define(FlowType("undeclared_target"), func(_ context.Context, _ BeginContext, _ testInput) (*Transition[testData], error) {
		return To("one", testData{}), nil
	})
	runtimeSpec.Start().MustGoTo("one")
	runtimeSpec.When("one", func(_ context.Context, data testData, _ advance) (*Transition[testData], error) {
		return To("not_declared", data), nil
	}).MustGoTo("two")
	engine := newTestEngine(t, runtimeSpec)
	started, err := engine.Begin(context.Background(), BeginRequest{Type: "undeclared_target", Input: testInput{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Submit(context.Background(), SubmitRequest{FlowID: started.ID, Action: advance{}}); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("undeclared target error = %v, want ErrInvalidTransition", err)
	}
	current, err := engine.Get(context.Background(), started.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != "one" || current.Revision != started.Revision {
		t.Fatalf("invalid transition persisted a change: %+v", current)
	}
}

func TestAnyStateFallbackDefersToExactRoute(t *testing.T) {
	spec := Define(FlowType("any_state"), func(_ context.Context, _ BeginContext, _ testInput) (*Transition[testData], error) {
		return To("one", testData{}), nil
	})
	spec.Start().MustGoTo("one")
	spec.When(AnyState, func(_ context.Context, data testData, _ fallbackAction) (*Transition[testData], error) {
		return To("fallback", data), nil
	}).MustGoTo("fallback")
	spec.When("one", func(_ context.Context, data testData, _ fallbackAction) (*Transition[testData], error) {
		return To("two", data), nil
	}).MustGoTo("two")

	engine := newTestEngine(t, spec)
	started, err := engine.Begin(context.Background(), BeginRequest{Type: "any_state", Input: testInput{}})
	if err != nil {
		t.Fatal(err)
	}
	exact, err := engine.Submit(context.Background(), SubmitRequest{FlowID: started.ID, Action: fallbackAction{}})
	if err != nil {
		t.Fatal(err)
	}
	if exact.State != "two" {
		t.Fatalf("exact route did not win: %+v", exact)
	}
	fallback, err := engine.Submit(context.Background(), SubmitRequest{FlowID: started.ID, Action: fallbackAction{}})
	if err != nil {
		t.Fatal(err)
	}
	if fallback.State != "fallback" {
		t.Fatalf("AnyState route did not handle the unmatched state: %+v", fallback)
	}
}

func TestTransitionViewsRetainReplaceAndClear(t *testing.T) {
	spec := Define(FlowType("view_lifecycle"), func(_ context.Context, _ BeginContext, _ testInput) (*Transition[testData], error) {
		return To("one", testData{}).WithView(map[string]string{"screen": "one"}), nil
	})
	spec.Start().MustGoTo("one")
	spec.When("one", func(_ context.Context, data testData, _ advance) (*Transition[testData], error) {
		return To("two", data), nil
	}).MustGoTo("two")
	spec.When("two", func(_ context.Context, data testData, _ clearViewAction) (*Transition[testData], error) {
		return To("three", data).ClearView(), nil
	}).MustGoTo("three")
	spec.When("three", func(_ context.Context, data testData, _ setViewAction) (*Transition[testData], error) {
		return To("four", data).WithView(map[string]string{"screen": "four"}), nil
	}).MustGoTo("four")

	engine := newTestEngine(t, spec)
	started, err := engine.Begin(context.Background(), BeginRequest{Type: "view_lifecycle", Input: testInput{}})
	if err != nil {
		t.Fatal(err)
	}
	retained, err := engine.Submit(context.Background(), SubmitRequest{FlowID: started.ID, Action: advance{}})
	if err != nil {
		t.Fatal(err)
	}
	if string(retained.View) != `{"screen":"one"}` {
		t.Fatalf("view was not retained: %s", retained.View)
	}
	cleared, err := engine.Submit(context.Background(), SubmitRequest{FlowID: started.ID, Action: clearViewAction{}})
	if err != nil {
		t.Fatal(err)
	}
	if len(cleared.View) != 0 {
		t.Fatalf("view was not cleared: %s", cleared.View)
	}
	replaced, err := engine.Submit(context.Background(), SubmitRequest{FlowID: started.ID, Action: setViewAction{}})
	if err != nil {
		t.Fatal(err)
	}
	if string(replaced.View) != `{"screen":"four"}` {
		t.Fatalf("view was not replaced: %s", replaced.View)
	}
}

func TestTerminalAndExpiredFlowsRejectFurtherWrites(t *testing.T) {
	t.Run("completed", func(t *testing.T) {
		engine := newTestEngine(t, testDefinition(false))
		started, err := engine.Begin(context.Background(), BeginRequest{Type: "test", Input: testInput{}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := engine.Submit(context.Background(), SubmitRequest{FlowID: started.ID, Action: advance{}}); err != nil {
			t.Fatal(err)
		}
		if _, err := engine.Submit(context.Background(), SubmitRequest{FlowID: started.ID, Action: commonAction{}}); err != nil {
			t.Fatal(err)
		}
		if _, err := engine.Submit(context.Background(), SubmitRequest{FlowID: started.ID, Action: advance{}}); !errors.Is(err, ErrFlowCompleted) {
			t.Fatalf("completed submit error = %v, want ErrFlowCompleted", err)
		}
		if _, err := engine.Cancel(context.Background(), CancelRequest{FlowID: started.ID}); !errors.Is(err, ErrFlowCompleted) {
			t.Fatalf("completed cancel error = %v, want ErrFlowCompleted", err)
		}
	})

	t.Run("cancelled", func(t *testing.T) {
		engine := newTestEngine(t, testDefinition(false))
		started, err := engine.Begin(context.Background(), BeginRequest{Type: "test", Input: testInput{}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := engine.Cancel(context.Background(), CancelRequest{FlowID: started.ID}); err != nil {
			t.Fatal(err)
		}
		if _, err := engine.Submit(context.Background(), SubmitRequest{FlowID: started.ID, Action: advance{}}); !errors.Is(err, ErrFlowCancelled) {
			t.Fatalf("cancelled submit error = %v, want ErrFlowCancelled", err)
		}
		if _, err := engine.Cancel(context.Background(), CancelRequest{FlowID: started.ID}); !errors.Is(err, ErrFlowCancelled) {
			t.Fatalf("cancelled cancel error = %v, want ErrFlowCancelled", err)
		}
	})

	t.Run("expired", func(t *testing.T) {
		clock := &adversarialClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
		engine := newTestEngine(t, testDefinition(false), WithClock(clock))
		started, err := engine.Begin(context.Background(), BeginRequest{Type: "test", Input: testInput{}, ExpiresIn: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		clock.now = clock.now.Add(2 * time.Minute)
		if _, err := engine.Submit(context.Background(), SubmitRequest{FlowID: started.ID, Action: advance{}}); !errors.Is(err, ErrFlowExpired) {
			t.Fatalf("expired submit error = %v, want ErrFlowExpired", err)
		}
		if _, err := engine.Cancel(context.Background(), CancelRequest{FlowID: started.ID}); !errors.Is(err, ErrFlowExpired) {
			t.Fatalf("expired cancel error = %v, want ErrFlowExpired", err)
		}
	})
}

func TestIdempotencyContentionAndRetry(t *testing.T) {
	t.Run("contention_replays_only_after_completion", func(t *testing.T) {
		entered := make(chan struct{})
		release := make(chan struct{})
		var calls atomic.Int32
		spec := Define(FlowType("idempotency_contention"), func(_ context.Context, _ BeginContext, _ testInput) (*Transition[testData], error) {
			calls.Add(1)
			close(entered)
			<-release
			return To("one", testData{}), nil
		})
		spec.Start().MustGoTo("one")
		engine := newTestEngine(t, spec, WithIdempotencyStore(NewMemoryIdempotencyStore()))
		request := BeginRequest{
			Type: "idempotency_contention", SubjectID: "subject", Input: testInput{},
			IdempotencyKey: "key",
		}

		var first Result
		firstDone := make(chan error, 1)
		go func() {
			var err error
			first, err = engine.Begin(context.Background(), request)
			firstDone <- err
		}()
		<-entered
		if _, err := engine.Begin(context.Background(), request); !errors.Is(err, ErrIdempotencyInProgress) {
			t.Fatalf("concurrent request error = %v, want ErrIdempotencyInProgress", err)
		}
		close(release)
		if err := <-firstDone; err != nil {
			t.Fatal(err)
		}
		replay, err := engine.Begin(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		if replay.ID != first.ID || calls.Load() != 1 {
			t.Fatalf("unexpected replay: first=%+v replay=%+v calls=%d", first, replay, calls.Load())
		}
	})

	t.Run("failed_attempt_is_aborted_for_retry", func(t *testing.T) {
		attemptErr := errors.New("begin failed")
		fail := true
		spec := Define(FlowType("idempotency_retry"), func(_ context.Context, _ BeginContext, _ testInput) (*Transition[testData], error) {
			if fail {
				return nil, attemptErr
			}
			return To("one", testData{}), nil
		})
		spec.Start().MustGoTo("one")
		engine := newTestEngine(t, spec, WithIdempotencyStore(NewMemoryIdempotencyStore()))
		request := BeginRequest{
			Type: "idempotency_retry", SubjectID: "subject", Input: testInput{},
			IdempotencyKey: "key",
		}
		if _, err := engine.Begin(context.Background(), request); !errors.Is(err, attemptErr) {
			t.Fatalf("first request error = %v, want %v", err, attemptErr)
		}
		fail = false
		if _, err := engine.Begin(context.Background(), request); err != nil {
			t.Fatalf("retry did not reserve the aborted key: %v", err)
		}
	})
}

type migrationFailureCodec struct{ err error }

func (c migrationFailureCodec) Encode(testData) ([]byte, error) { return []byte(`{}`), nil }

func (c migrationFailureCodec) Decode(version int, _ []byte) (testData, error) {
	if version == 1 {
		return testData{}, c.err
	}
	return testData{}, nil
}

func (migrationFailureCodec) SupportsVersion(version int) bool { return version == 2 }

func TestCodecMigrationFailureDoesNotWrite(t *testing.T) {
	migrationErr := errors.New("legacy data cannot migrate")
	spec := Define(FlowType("migration_failure"), func(_ context.Context, _ BeginContext, _ testInput) (*Transition[testData], error) {
		return To("one", testData{}), nil
	}).Version(2).WithCodec(migrationFailureCodec{err: migrationErr})
	spec.Start().MustGoTo("one")
	spec.When("one", func(_ context.Context, data testData, _ advance) (*Transition[testData], error) {
		return To("two", data), nil
	}).MustGoTo("two")

	registry := NewRegistry()
	if err := Register(registry, spec); err != nil {
		t.Fatal(err)
	}
	store := NewMemoryStore()
	created, err := store.Create(context.Background(), Snapshot{
		ID: "legacy", Type: "migration_failure", State: "one",
		Data: []byte(`{"Count":1}`), DataVersion: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	engine := NewEngine(store, registry)
	if _, err := engine.Submit(context.Background(), SubmitRequest{FlowID: "legacy", Action: advance{}}); !errors.Is(err, migrationErr) {
		t.Fatalf("migration failure = %v, want %v", err, migrationErr)
	}
	current, err := store.Get(context.Background(), "legacy")
	if err != nil {
		t.Fatal(err)
	}
	if current.Revision != created.Revision || current.State != created.State {
		t.Fatalf("migration failure wrote the flow: %+v", current)
	}
}

type commitTrackingUnitOfWork struct {
	commitErr error
	commits   int
	rollbacks int
}

func (u *commitTrackingUnitOfWork) Do(ctx context.Context, fn func(context.Context) error) error {
	if err := fn(ctx); err != nil {
		u.rollbacks++
		return err
	}
	if u.commitErr != nil {
		u.rollbacks++
		return u.commitErr
	}
	u.commits++
	return nil
}

func TestEffectFailuresAndFailedCommitsAbortTheUnitOfWork(t *testing.T) {
	t.Run("recorder_failure", func(t *testing.T) {
		uow := &commitTrackingUnitOfWork{}
		engine := newTestEngine(
			t, testDefinition(true),
			WithUnitOfWork(uow),
			WithEffectRecorder(EffectRecorderFunc(func(context.Context, EffectContext, []Effect) error {
				return errors.New("outbox unavailable")
			})),
		)
		if _, err := engine.Begin(context.Background(), BeginRequest{Type: "test", Input: testInput{}}); !errors.Is(err, ErrEffectRecordFailed) {
			t.Fatalf("recorder failure = %v, want ErrEffectRecordFailed", err)
		}
		if uow.commits != 0 || uow.rollbacks != 1 {
			t.Fatalf("recorder failure did not abort the unit of work: %+v", uow)
		}
	})

	t.Run("commit_failure", func(t *testing.T) {
		commitErr := errors.New("commit failed")
		uow := &commitTrackingUnitOfWork{commitErr: commitErr}
		engine := newTestEngine(
			t, testDefinition(true),
			WithUnitOfWork(uow),
			WithEffectRecorder(EffectRecorderFunc(func(context.Context, EffectContext, []Effect) error {
				return nil
			})),
		)
		if _, err := engine.Begin(context.Background(), BeginRequest{Type: "test", Input: testInput{}}); !errors.Is(err, commitErr) {
			t.Fatalf("commit failure = %v, want %v", err, commitErr)
		}
		if uow.commits != 0 || uow.rollbacks != 1 {
			t.Fatalf("commit failure did not abort the unit of work: %+v", uow)
		}
	})
}

type observerTimingUnitOfWork struct{ active atomic.Bool }

func (u *observerTimingUnitOfWork) Do(ctx context.Context, fn func(context.Context) error) error {
	u.active.Store(true)
	defer u.active.Store(false)
	return fn(ctx)
}

func TestObserversRecoverPanicsAndRunAfterTheUnitOfWork(t *testing.T) {
	uow := &observerTimingUnitOfWork{}
	var mu sync.Mutex
	var events []EventName
	var observedInsideUnitOfWork bool
	engine := newTestEngine(
		t, testDefinition(true),
		WithUnitOfWork(uow),
		WithEffectRecorder(EffectRecorderFunc(func(context.Context, EffectContext, []Effect) error {
			return errors.New("outbox unavailable")
		})),
		WithObserver(ObserverFunc(func(context.Context, Event) {
			panic("observer panic")
		})),
		WithObserver(ObserverFunc(func(_ context.Context, event Event) {
			mu.Lock()
			defer mu.Unlock()
			observedInsideUnitOfWork = observedInsideUnitOfWork || uow.active.Load()
			events = append(events, event.Name)
		})),
	)

	if _, err := engine.Begin(context.Background(), BeginRequest{Type: "test", Input: testInput{}}); !errors.Is(err, ErrEffectRecordFailed) {
		t.Fatalf("begin error = %v, want ErrEffectRecordFailed", err)
	}
	if observedInsideUnitOfWork {
		t.Fatal("observer ran before the unit of work returned")
	}
	want := []EventName{EventBeginStarted, EventEffectRecordFailed, EventBeginFailed}
	if len(events) != len(want) {
		t.Fatalf("observer events = %v, want %v", events, want)
	}
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("observer event %d = %q, want %q", i, events[i], want[i])
		}
	}
}

func TestEffectRouterLifecycleAndConcurrentDispatch(t *testing.T) {
	t.Run("duplicate_and_sealed_routes_are_invalid", func(t *testing.T) {
		router := NewEffectRouter().
			OnEffect(func(context.Context, emittedEffect) error { return nil }).
			OnEffect(func(context.Context, emittedEffect) error { return nil })
		if err := router.Validate(); !errors.Is(err, ErrInvalidFlow) {
			t.Fatalf("duplicate route validation = %v, want ErrInvalidFlow", err)
		}

		sealed := NewEffectRouter().OnEffect(func(context.Context, emittedEffect) error { return nil })
		if err := sealed.Seal(); err != nil {
			t.Fatal(err)
		}
		sealed.OnEffect(func(context.Context, lateEffect) error { return nil })
		if err := sealed.Validate(); !errors.Is(err, ErrInvalidFlow) {
			t.Fatalf("late route validation = %v, want ErrInvalidFlow", err)
		}
	})

	t.Run("dispatch_uses_a_handler_snapshot", func(t *testing.T) {
		var router *EffectRouter
		router = NewEffectRouter().OnEffect(func(context.Context, emittedEffect) error {
			router.OnEffect(func(context.Context, lateEffect) error { return nil })
			return nil
		})
		err := router.Dispatch(context.Background(), EffectContext{}, []Effect{emittedEffect{}, lateEffect{}})
		if !errors.Is(err, ErrUnsupportedEffect) {
			t.Fatalf("same dispatch unexpectedly saw a new route: %v", err)
		}
		if err := router.Dispatch(context.Background(), EffectContext{}, []Effect{lateEffect{}}); err != nil {
			t.Fatalf("later dispatch did not see the new route: %v", err)
		}
	})

	t.Run("registration_and_dispatch_are_safe_concurrently", func(t *testing.T) {
		router := NewEffectRouter().OnEffect(func(context.Context, emittedEffect) error { return nil })
		start := make(chan struct{})
		errs := make(chan error, 100)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			router.OnEffect(func(context.Context, lateEffect) error { return nil })
		}()
		go func() {
			defer wg.Done()
			<-start
			for range 100 {
				if err := router.Dispatch(context.Background(), EffectContext{}, []Effect{emittedEffect{}}); err != nil {
					errs <- err
				}
			}
		}()
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Errorf("concurrent dispatch failed: %v", err)
		}
		if err := router.Validate(); err != nil {
			t.Fatal(err)
		}
		if err := router.Dispatch(context.Background(), EffectContext{}, []Effect{lateEffect{}}); err != nil {
			t.Fatalf("concurrent registration was not retained: %v", err)
		}
	})
}
