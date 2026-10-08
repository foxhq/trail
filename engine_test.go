package trail

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type testData struct{ Count int }
type testInput struct{ Count int }
type advance struct{}
type commonAction struct{}
type emittedEffect struct{ Message string }

func (advance) Type() ActionType       { return "advance" }
func (commonAction) Type() ActionType  { return "common" }
func (emittedEffect) Type() EffectType { return "emitted" }

func testDefinition(withEffects bool) *Definition[testData, testInput] {
	spec := Define(FlowType("test"), func(_ context.Context, _ BeginContext, input testInput) (*Transition[testData], error) {
		transition := To(FlowState("one"), testData{Count: input.Count}).WithView(struct {
			State string `json:"state"`
			Count int    `json:"count"`
		}{State: "one", Count: input.Count})
		if withEffects {
			transition.WithEffects(emittedEffect{Message: "started"})
		}
		return transition, nil
	})
	spec.Start().MustGoTo("one")
	spec.When("one", func(_ context.Context, data testData, _ advance) (*Transition[testData], error) {
		return To("two", testData{Count: data.Count + 1}), nil
	}).MustGoTo("two")
	spec.When("one", func(_ context.Context, data testData, _ commonAction) (*Transition[testData], error) {
		return To("two", testData{Count: data.Count + 10}), nil
	}).MustGoTo("two")
	spec.When("two", func(_ context.Context, data testData, _ commonAction) (*Transition[testData], error) {
		return Done("done", testData{Count: data.Count + 100}), nil
	}).MustGoTo("done")
	return spec
}

func newTestEngine(t *testing.T, spec *Definition[testData, testInput], opts ...EngineOption) Engine {
	t.Helper()
	registry := NewRegistry()
	if err := Register(registry, spec); err != nil {
		t.Fatal(err)
	}
	return NewEngine(NewMemoryStore(), registry, opts...)
}

func TestStateAwareRoutesMayReuseActionType(t *testing.T) {
	engine := newTestEngine(t, testDefinition(false))
	started, err := engine.Begin(context.Background(), BeginRequest{Type: "test", SubjectID: "subject", Input: testInput{Count: 1}})
	if err != nil {
		t.Fatal(err)
	}
	second, err := engine.Submit(context.Background(), SubmitRequest{FlowID: started.ID, Action: commonAction{}})
	if err != nil {
		t.Fatal(err)
	}
	if second.State != "two" || second.Completed {
		t.Fatalf("unexpected second result: %+v", second)
	}
	done, err := engine.Submit(context.Background(), SubmitRequest{FlowID: started.ID, Action: commonAction{}})
	if err != nil {
		t.Fatal(err)
	}
	if done.State != "done" || !done.Completed {
		t.Fatalf("unexpected done result: %+v", done)
	}
}

func TestViewIsJSONAndDecodesAtBoundary(t *testing.T) {
	engine := newTestEngine(t, testDefinition(false))
	result, err := engine.Begin(context.Background(), BeginRequest{Type: "test", Input: testInput{Count: 7}})
	if err != nil {
		t.Fatal(err)
	}
	if string(result.View) != `{"state":"one","count":7}` {
		t.Fatalf("unexpected raw view: %s", result.View)
	}
	view, err := ViewAs[struct {
		State string `json:"state"`
		Count int    `json:"count"`
	}](result)
	if err != nil {
		t.Fatal(err)
	}
	if view.State != "one" || view.Count != 7 {
		t.Fatalf("unexpected view: %+v", view)
	}
}

type txKey struct{}
type testUnitOfWork struct{}

func (testUnitOfWork) Do(ctx context.Context, fn func(context.Context) error) error {
	return fn(context.WithValue(ctx, txKey{}, "transaction"))
}

type recordingEffects struct {
	mu       sync.Mutex
	contexts []EffectContext
	effects  [][]Effect
	seenTx   bool
}

func (r *recordingEffects) Record(ctx context.Context, flow EffectContext, effects []Effect) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seenTx = ctx.Value(txKey{}) == "transaction"
	r.contexts = append(r.contexts, flow)
	r.effects = append(r.effects, append([]Effect(nil), effects...))
	return nil
}

func TestEffectsRequireTransactionalRecorder(t *testing.T) {
	plain := newTestEngine(t, testDefinition(true))
	_, err := plain.Begin(context.Background(), BeginRequest{Type: "test", Input: testInput{}})
	if !errors.Is(err, ErrTransactionalEffectsRequired) {
		t.Fatalf("expected transactional effect error, got %v", err)
	}

	withoutRecorder := newTestEngine(t, testDefinition(true), WithUnitOfWork(testUnitOfWork{}))
	_, err = withoutRecorder.Begin(context.Background(), BeginRequest{Type: "test", Input: testInput{}})
	if !errors.Is(err, ErrEffectRecorderRequired) {
		t.Fatalf("expected recorder error, got %v", err)
	}

	recorder := &recordingEffects{}
	engine := newTestEngine(t, testDefinition(true), WithUnitOfWork(testUnitOfWork{}), WithEffectRecorder(recorder))
	result, err := engine.Begin(context.Background(), BeginRequest{Type: "test", SubjectID: "private-subject", Input: testInput{}})
	if err != nil {
		t.Fatal(err)
	}
	if !recorder.seenTx || len(recorder.effects) != 1 || recorder.contexts[0].FlowID != result.ID {
		t.Fatalf("effect was not durably recorded in the unit of work: %+v", recorder)
	}
	if recorder.contexts[0].SubjectID != "private-subject" {
		t.Fatalf("trusted recorder lost flow context: %+v", recorder.contexts[0])
	}
}

func TestEffectRouterDispatchesRecordedEffects(t *testing.T) {
	called := ""
	router := NewEffectRouter().OnEffect(func(_ context.Context, effect emittedEffect) error {
		called = effect.Message
		return nil
	})
	if err := router.Dispatch(context.Background(), EffectContext{FlowID: "flow"}, []Effect{emittedEffect{Message: "recorded"}}); err != nil {
		t.Fatal(err)
	}
	if called != "recorded" {
		t.Fatalf("effect handler was not called: %q", called)
	}
}

func TestIdempotencyReservesAtomicallyAndChecksFingerprint(t *testing.T) {
	engine := newTestEngine(t, testDefinition(false), WithIdempotencyStore(NewMemoryIdempotencyStore()))
	first, err := engine.Begin(context.Background(), BeginRequest{Type: "test", SubjectID: "subject", Input: testInput{Count: 1}, IdempotencyKey: "one"})
	if err != nil {
		t.Fatal(err)
	}
	replay, err := engine.Begin(context.Background(), BeginRequest{Type: "test", SubjectID: "subject", Input: testInput{Count: 1}, IdempotencyKey: "one"})
	if err != nil {
		t.Fatal(err)
	}
	if replay.ID != first.ID || replay.Revision != first.Revision {
		t.Fatalf("expected replay, got first=%+v replay=%+v", first, replay)
	}
	_, err = engine.Begin(context.Background(), BeginRequest{Type: "test", SubjectID: "subject", Input: testInput{Count: 2}, IdempotencyKey: "one"})
	if !errors.Is(err, ErrIdempotencyKeyReuse) {
		t.Fatalf("expected fingerprint mismatch, got %v", err)
	}
}

func TestCancellationSupportsDefaultAndTypedBehavior(t *testing.T) {
	defaultEngine := newTestEngine(t, testDefinition(false))
	started, err := defaultEngine.Begin(context.Background(), BeginRequest{Type: "test", Input: testInput{}})
	if err != nil {
		t.Fatal(err)
	}
	cancelled, err := defaultEngine.Cancel(context.Background(), CancelRequest{FlowID: started.ID, Reason: "user"})
	if err != nil {
		t.Fatal(err)
	}
	if !cancelled.Cancelled || !cancelled.Completed || cancelled.State != "one" {
		t.Fatalf("unexpected standard cancellation: %+v", cancelled)
	}

	spec := testDefinition(false)
	spec.WhenCancel("one", func(_ context.Context, flow *Flow[testData], cancel CancelContext) (*Transition[testData], error) {
		if cancel.Reason != "expired by user" {
			return nil, errors.New("unexpected reason")
		}
		return To("cancelled", testData{Count: flow.Data.Count + 1}).WithView(map[string]string{"state": "cancelled"}), nil
	}).MustGoTo("cancelled")
	engine := newTestEngine(t, spec)
	started, err = engine.Begin(context.Background(), BeginRequest{Type: "test", Input: testInput{}})
	if err != nil {
		t.Fatal(err)
	}
	cancelled, err = engine.Cancel(context.Background(), CancelRequest{FlowID: started.ID, Reason: "expired by user"})
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.State != "cancelled" || !cancelled.Cancelled || string(cancelled.View) != `{"state":"cancelled"}` {
		t.Fatalf("unexpected custom cancellation: %+v", cancelled)
	}
}

type countReadsStore struct {
	Store
	gets int
}

func (s *countReadsStore) Get(ctx context.Context, id FlowID) (Snapshot, error) {
	s.gets++
	return s.Store.Get(ctx, id)
}

func TestWritesUseCommittedStoreSnapshotWithoutReread(t *testing.T) {
	registry := NewRegistry()
	spec := testDefinition(false)
	if err := Register(registry, spec); err != nil {
		t.Fatal(err)
	}
	store := &countReadsStore{Store: NewMemoryStore()}
	engine := NewEngine(store, registry)
	started, err := engine.Begin(context.Background(), BeginRequest{Type: "test", Input: testInput{}})
	if err != nil {
		t.Fatal(err)
	}
	if store.gets != 0 {
		t.Fatalf("begin re-read its committed snapshot %d times", store.gets)
	}
	_, err = engine.Submit(context.Background(), SubmitRequest{FlowID: started.ID, Action: advance{}})
	if err != nil {
		t.Fatal(err)
	}
	if store.gets != 1 {
		t.Fatalf("submit should read once before its update, got %d", store.gets)
	}
}

func TestMemoryStoreUsesOptimisticRevision(t *testing.T) {
	store := NewMemoryStore()
	created, err := store.Create(context.Background(), Snapshot{ID: "one", Type: "test", State: "new", CreatedAt: time.Now()})
	if err != nil || created.Revision != 1 {
		t.Fatalf("create = %+v, %v", created, err)
	}
	updated, err := store.Update(context.Background(), created)
	if err != nil || updated.Revision != 2 {
		t.Fatalf("update = %+v, %v", updated, err)
	}
	_, err = store.Update(context.Background(), created)
	if !errors.Is(err, ErrFlowConflict) {
		t.Fatalf("expected stale update conflict, got %v", err)
	}
}

func TestDefinitionRejectsUnsupportedWriteVersionAtRegistration(t *testing.T) {
	spec := testDefinition(false).Version(2)
	registry := NewRegistry()
	if err := Register(registry, spec); !errors.Is(err, ErrUnsupportedDataVersion) {
		t.Fatalf("register error = %v, want ErrUnsupportedDataVersion", err)
	}
}
