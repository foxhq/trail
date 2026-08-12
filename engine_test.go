package trail

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

const testFlowType FlowType = "test"

type testData struct {
	Count int `json:"count"`
}

type testSpec struct {
	JSONCodec[testData]
	beginCalls  int
	submitCalls int
}

func (s *testSpec) Type() FlowType   { return testFlowType }
func (s *testSpec) DataVersion() int { return 1 }
func (s *testSpec) Begin(_ context.Context, begin BeginContext, input any) (*Transition[testData], error) {
	s.beginCalls++
	count, _ := input.(int)
	return &Transition[testData]{
		State: "begun",
		Data:  testData{Count: count},
		Public: map[string]any{
			"count": count,
		},
		Effects: []Effect{testEffect("begun")},
	}, nil
}
func (s *testSpec) Submit(_ context.Context, instance *Flow[testData], action Action) (*Transition[testData], error) {
	s.submitCalls++
	switch action.Type() {
	case "inc":
		instance.Data.Count++
		return &Transition[testData]{
			State: "incremented",
			Data:  instance.Data,
			Public: map[string]any{
				"count": instance.Data.Count,
			},
		}, nil
	case "finish":
		return &Transition[testData]{
			State:     "done",
			Data:      instance.Data,
			Completed: true,
		}, nil
	default:
		return nil, ErrUnsupportedAction
	}
}

type testAction string

func (a testAction) Type() ActionType { return ActionType(a) }

type incrementAction struct{}

func (incrementAction) Type() ActionType { return "inc" }

type testEffect string

func (e testEffect) Type() EffectType { return EffectType(e) }

type emailEffect struct {
	Address string
	Code    string
}

func (emailEffect) Type() EffectType { return "email" }

type recordingEffects struct {
	applied []Effect
}

func (r *recordingEffects) Publish(_ context.Context, _ Result, effects []Effect) error {
	r.applied = append(r.applied, effects...)
	return nil
}

type failingEffects struct {
	err error
}

func (f failingEffects) Publish(context.Context, Result, []Effect) error {
	return f.err
}

type effectSinkFunc func(context.Context, Result, []Effect) error

func (f effectSinkFunc) Publish(ctx context.Context, result Result, effects []Effect) error {
	return f(ctx, result, effects)
}

type testTxContextKey struct{}

type recordingUnitOfWork struct {
	calls int
}

func (u *recordingUnitOfWork) Do(ctx context.Context, fn func(context.Context) error) error {
	u.calls++
	return fn(context.WithValue(ctx, testTxContextKey{}, "tx"))
}

type txAwareSpec struct {
	JSONCodec[testData]
	sawBeginTx bool
}

func (s *txAwareSpec) Type() FlowType   { return "tx_test" }
func (s *txAwareSpec) DataVersion() int { return 1 }
func (s *txAwareSpec) Begin(ctx context.Context, _ BeginContext, _ any) (*Transition[testData], error) {
	s.sawBeginTx = ctx.Value(testTxContextKey{}) == "tx"
	return &Transition[testData]{
		State:   "begun",
		Data:    testData{},
		Effects: []Effect{testEffect("begun")},
	}, nil
}
func (s *txAwareSpec) Submit(context.Context, *Flow[testData], Action) (*Transition[testData], error) {
	return nil, ErrUnsupportedAction
}

type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time { return c.now }

type fixedID struct {
	id FlowID
}

func (g fixedID) NextFlowID() (FlowID, error) { return g.id, nil }

func TestEngineRunsWritesInUnitOfWork(t *testing.T) {
	ctx := context.Background()
	registry := NewRegistry()
	spec := &txAwareSpec{}
	if err := Register(registry, spec); err != nil {
		t.Fatal(err)
	}
	unitOfWork := &recordingUnitOfWork{}
	sawEffectSinkTx := false
	engine := NewEngine(
		NewMemoryStore(),
		registry,
		WithIDGenerator(fixedID{id: "flow_1"}),
		WithUnitOfWork(unitOfWork),
		WithEffectSink(effectSinkFunc(func(ctx context.Context, _ Result, _ []Effect) error {
			sawEffectSinkTx = ctx.Value(testTxContextKey{}) == "tx"
			return nil
		})),
	)

	if _, err := engine.Begin(ctx, BeginRequest{Type: "tx_test"}); err != nil {
		t.Fatal(err)
	}
	if unitOfWork.calls != 1 {
		t.Fatalf("expected one unit of work call, got %d", unitOfWork.calls)
	}
	if !spec.sawBeginTx {
		t.Fatal("expected begin handler to receive unit of work context")
	}
	if !sawEffectSinkTx {
		t.Fatal("expected effect sink to receive unit of work context")
	}
}

func TestEngineBeginSubmitGet(t *testing.T) {
	ctx := context.Background()
	registry := NewRegistry()
	spec := &testSpec{}
	if err := Register(registry, spec); err != nil {
		t.Fatal(err)
	}
	effects := &recordingEffects{}
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	engine := NewEngine(
		NewMemoryStore(),
		registry,
		WithClock(fixedClock{now: now}),
		WithIDGenerator(fixedID{id: "flow_1"}),
		WithEffectSink(effects),
	)

	started, err := engine.Begin(ctx, BeginRequest{
		Type:      testFlowType,
		SubjectID: "user_1",
		Input:     41,
		ExpiresIn: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if started.ID != "flow_1" || started.State != "begun" || started.Revision != 1 {
		t.Fatalf("unexpected begin result: %+v", started)
	}
	if len(effects.applied) != 1 || effects.applied[0].Type() != "begun" {
		t.Fatalf("effects not applied: %+v", effects.applied)
	}

	next, err := engine.Submit(ctx, SubmitRequest{
		FlowID: "flow_1",
		Action: testAction("inc"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if next.State != "incremented" || next.Revision != 2 {
		t.Fatalf("unexpected submit result: %+v", next)
	}

	view, err := engine.Get(ctx, "flow_1")
	if err != nil {
		t.Fatal(err)
	}
	if view.State != "incremented" || view.Revision != 2 {
		t.Fatalf("unexpected get result: %+v", view)
	}
}

func TestEngineRejectsCompletedFlow(t *testing.T) {
	ctx := context.Background()
	registry := NewRegistry()
	if err := Register(registry, &testSpec{}); err != nil {
		t.Fatal(err)
	}
	engine := NewEngine(NewMemoryStore(), registry, WithIDGenerator(fixedID{id: "flow_1"}))

	if _, err := engine.Begin(ctx, BeginRequest{Type: testFlowType}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Submit(ctx, SubmitRequest{FlowID: "flow_1", Action: testAction("finish")}); err != nil {
		t.Fatal(err)
	}
	_, err := engine.Submit(ctx, SubmitRequest{FlowID: "flow_1", Action: testAction("inc")})
	if !errors.Is(err, ErrFlowCompleted) {
		t.Fatalf("expected completed error, got %v", err)
	}
}

func TestEngineRejectsExpiredFlow(t *testing.T) {
	ctx := context.Background()
	registry := NewRegistry()
	if err := Register(registry, &testSpec{}); err != nil {
		t.Fatal(err)
	}
	store := NewMemoryStore()
	startTime := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	engine := NewEngine(store, registry, WithClock(fixedClock{now: startTime}), WithIDGenerator(fixedID{id: "flow_1"}))

	if _, err := engine.Begin(ctx, BeginRequest{
		Type:      testFlowType,
		ExpiresAt: startTime.Add(-time.Minute),
	}); err != nil {
		t.Fatal(err)
	}

	_, err := engine.Submit(ctx, SubmitRequest{FlowID: "flow_1", Action: testAction("inc")})
	if !errors.Is(err, ErrFlowExpired) {
		t.Fatalf("expected expired error, got %v", err)
	}
}

func TestBeginIdempotency(t *testing.T) {
	ctx := context.Background()
	registry := NewRegistry()
	spec := &testSpec{}
	if err := Register(registry, spec); err != nil {
		t.Fatal(err)
	}
	idem := NewMemoryIdempotencyStore()
	engine := NewEngine(
		NewMemoryStore(),
		registry,
		WithIDGenerator(fixedID{id: "flow_1"}),
		WithIdempotencyStore(idem),
	)

	first, err := engine.Begin(ctx, BeginRequest{
		Type:           testFlowType,
		Input:          1,
		IdempotencyKey: "k1",
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := engine.Begin(ctx, BeginRequest{
		Type:           testFlowType,
		Input:          2,
		IdempotencyKey: "k1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID {
		t.Fatalf("expected idempotent result, got %s and %s", first.ID, second.ID)
	}
	if spec.beginCalls != 1 {
		t.Fatalf("expected one begin call, got %d", spec.beginCalls)
	}
}

func TestSubmitIdempotency(t *testing.T) {
	ctx := context.Background()
	registry := NewRegistry()
	spec := &testSpec{}
	if err := Register(registry, spec); err != nil {
		t.Fatal(err)
	}
	engine := NewEngine(
		NewMemoryStore(),
		registry,
		WithIDGenerator(fixedID{id: "flow_1"}),
		WithIdempotencyStore(NewMemoryIdempotencyStore()),
	)

	if _, err := engine.Begin(ctx, BeginRequest{Type: testFlowType, Input: 1}); err != nil {
		t.Fatal(err)
	}
	first, err := engine.Submit(ctx, SubmitRequest{
		FlowID:         "flow_1",
		Action:         testAction("inc"),
		IdempotencyKey: "submit_1",
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := engine.Submit(ctx, SubmitRequest{
		FlowID:         "flow_1",
		Action:         testAction("inc"),
		IdempotencyKey: "submit_1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.Revision != second.Revision || first.State != second.State {
		t.Fatalf("expected idempotent submit result, got %+v and %+v", first, second)
	}
	if spec.submitCalls != 1 {
		t.Fatalf("expected one submit call, got %d", spec.submitCalls)
	}
}

func TestEngineCancel(t *testing.T) {
	ctx := context.Background()
	registry := NewRegistry()
	if err := Register(registry, &testSpec{}); err != nil {
		t.Fatal(err)
	}
	engine := NewEngine(NewMemoryStore(), registry, WithIDGenerator(fixedID{id: "flow_1"}))

	if _, err := engine.Begin(ctx, BeginRequest{Type: testFlowType}); err != nil {
		t.Fatal(err)
	}
	cancelled, err := engine.Cancel(ctx, CancelRequest{FlowID: "flow_1", Reason: "user_request"})
	if err != nil {
		t.Fatal(err)
	}
	if !cancelled.Cancelled || !cancelled.Completed {
		t.Fatalf("expected cancelled completed flow, got %+v", cancelled)
	}
	_, err = engine.Cancel(ctx, CancelRequest{FlowID: "flow_1"})
	if !errors.Is(err, ErrFlowCancelled) {
		t.Fatalf("expected cancelled error, got %v", err)
	}
	_, err = engine.Submit(ctx, SubmitRequest{FlowID: "flow_1", Action: testAction("inc")})
	if !errors.Is(err, ErrFlowCancelled) {
		t.Fatalf("expected cancelled error, got %v", err)
	}
}

type strictSpec struct {
	testSpec
}

func (s *strictSpec) Transitions() Transitions {
	return Transitions{
		BeginState: {
			BeginAction: {"begun"},
		},
		"begun": {
			"inc": {"wrong_state"},
		},
	}
}

func TestEngineRejectsInvalidTransition(t *testing.T) {
	ctx := context.Background()
	registry := NewRegistry()
	if err := Register[testData](registry, &strictSpec{}); err != nil {
		t.Fatal(err)
	}
	engine := NewEngine(NewMemoryStore(), registry, WithIDGenerator(fixedID{id: "flow_1"}))

	if _, err := engine.Begin(ctx, BeginRequest{Type: testFlowType}); err != nil {
		t.Fatal(err)
	}
	_, err := engine.Submit(ctx, SubmitRequest{FlowID: "flow_1", Action: testAction("inc")})
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("expected invalid transition error, got %v", err)
	}
}

func TestMemoryStoreListAndDeleteExpired(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)

	snapshots := []Snapshot{
		{ID: "a", Type: testFlowType, SubjectID: "user_1", State: "open", ExpiresAt: now.Add(-time.Hour), CreatedAt: now.Add(-3 * time.Hour)},
		{ID: "b", Type: testFlowType, SubjectID: "user_1", State: "done", Completed: true, ExpiresAt: now.Add(time.Hour), CreatedAt: now.Add(-2 * time.Hour)},
		{ID: "c", Type: "other", SubjectID: "user_2", State: "open", ExpiresAt: now.Add(-time.Minute), CreatedAt: now.Add(-time.Hour)},
	}
	for _, snapshot := range snapshots {
		snapshot.DataVersion = 1
		if err := store.Create(ctx, snapshot); err != nil {
			t.Fatal(err)
		}
	}

	completed := true
	listed, err := store.List(ctx, Query{Type: testFlowType, SubjectID: "user_1", Completed: &completed})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].ID != "b" {
		t.Fatalf("unexpected query result: %+v", listed)
	}

	deleted, err := store.DeleteExpired(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 2 {
		t.Fatalf("expected two deleted flows, got %d", deleted)
	}
	if _, err := store.Get(ctx, "a"); !errors.Is(err, ErrFlowNotFound) {
		t.Fatalf("expected expired flow to be removed, got %v", err)
	}
	if _, err := store.Get(ctx, "b"); err != nil {
		t.Fatalf("expected unexpired flow to remain, got %v", err)
	}
}

func TestObserversReceiveLifecycleEvents(t *testing.T) {
	ctx := context.Background()
	registry := NewRegistry()
	if err := Register(registry, &testSpec{}); err != nil {
		t.Fatal(err)
	}
	var events []EventName
	engine := NewEngine(
		NewMemoryStore(),
		registry,
		WithIDGenerator(fixedID{id: "flow_1"}),
		WithObserver(ObserverFunc(func(_ context.Context, event Event) {
			events = append(events, event.Name)
		})),
	)

	if _, err := engine.Begin(ctx, BeginRequest{Type: testFlowType}); err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 {
		t.Fatalf("expected three events, got %v", events)
	}
	want := []EventName{EventBeginStarted, EventEffectPublished, EventBeginCompleted}
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("unexpected events: got %v want %v", events, want)
		}
	}
}

func TestTypedHelpers(t *testing.T) {
	value, err := As[int](42)
	if err != nil {
		t.Fatal(err)
	}
	if value != 42 {
		t.Fatalf("unexpected cast result: %d", value)
	}

	action, err := ActionAs[testAction](testAction("inc"))
	if err != nil {
		t.Fatal(err)
	}
	if action.Type() != "inc" {
		t.Fatalf("unexpected action: %s", action.Type())
	}

	if _, err := As[string](42); !errors.Is(err, ErrInvalidFlow) {
		t.Fatalf("expected invalid flow error, got %v", err)
	}

	effect, err := EffectAs[emailEffect](emailEffect{Address: "user@example.com"})
	if err != nil {
		t.Fatal(err)
	}
	if effect.Type() != "email" {
		t.Fatalf("unexpected effect: %s", effect.Type())
	}
}

func TestEffectFailureReturnsCommittedResult(t *testing.T) {
	ctx := context.Background()
	registry := NewRegistry()
	if err := Register(registry, &testSpec{}); err != nil {
		t.Fatal(err)
	}
	engine := NewEngine(
		NewMemoryStore(),
		registry,
		WithIDGenerator(fixedID{id: "flow_1"}),
		WithEffectSink(failingEffects{err: errors.New("downstream unavailable")}),
	)

	result, err := engine.Begin(ctx, BeginRequest{Type: testFlowType})
	if !errors.Is(err, ErrEffectPublishFailed) {
		t.Fatalf("expected effect failure, got %v", err)
	}
	if result.ID != "flow_1" || result.Revision != 1 || result.State != "begun" {
		t.Fatalf("expected committed result with effect error, got %+v", result)
	}
}

func TestEffectSinkIsSkippedWhenNoEffects(t *testing.T) {
	ctx := context.Background()
	registry := NewRegistry()
	spec := Define(testFlowType, func(context.Context, BeginContext, int) (*Transition[testData], error) {
		return To("begun", testData{}), nil
	})
	Start(spec).MustGoTo("begun")
	When(spec, "begun", func(context.Context, testData, incrementAction) (*Transition[testData], error) {
		return To("incremented", testData{}), nil
	}).MustGoTo("incremented")
	if err := Register(registry, spec); err != nil {
		t.Fatal(err)
	}

	engine := NewEngine(
		NewMemoryStore(),
		registry,
		WithIDGenerator(fixedID{id: "flow_1"}),
		WithEffectSink(failingEffects{err: errors.New("should not be called")}),
	)

	result, err := engine.Begin(ctx, BeginRequest{Type: testFlowType, Input: 1})
	if err != nil {
		t.Fatal(err)
	}
	if result.State != "begun" {
		t.Fatalf("unexpected result: %+v", result)
	}

	result, err = engine.Submit(ctx, SubmitRequest{FlowID: "flow_1", Action: incrementAction{}})
	if err != nil {
		t.Fatal(err)
	}
	if result.State != "incremented" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestEffectRouterDispatchesTypedEffects(t *testing.T) {
	ctx := context.Background()
	router := NewEffectRouter()
	var sentTo string
	var seenResult Result

	if err := router.HandleWithResult(emailEffect{}, func(_ context.Context, result Result, effect emailEffect) error {
		seenResult = result
		sentTo = effect.Address
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	err := router.Publish(ctx, Result{ID: "flow_1"}, []Effect{
		emailEffect{Address: "user@example.com", Code: "123456"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if sentTo != "user@example.com" || seenResult.ID != "flow_1" {
		t.Fatalf("effect was not dispatched correctly: sentTo=%s result=%+v", sentTo, seenResult)
	}
}

func TestHandleEffectRegistersTypedEffects(t *testing.T) {
	ctx := context.Background()
	router := NewEffectRouter()
	var sentTo string

	if err := HandleEffect(router, func(_ context.Context, effect emailEffect) error {
		sentTo = effect.Address
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	err := router.Publish(ctx, Result{ID: "flow_1"}, []Effect{
		emailEffect{Address: "user@example.com", Code: "123456"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if sentTo != "user@example.com" {
		t.Fatalf("effect was not dispatched correctly: sentTo=%s", sentTo)
	}
}

func TestNewEffectRouterRegistersEffectRoutes(t *testing.T) {
	ctx := context.Background()
	var sentTo string

	router := NewEffectRouter(
		OnEffect(func(_ context.Context, effect emailEffect) error {
			sentTo = effect.Address
			return nil
		}),
	)
	if err := router.Validate(); err != nil {
		t.Fatal(err)
	}

	err := router.Publish(ctx, Result{ID: "flow_1"}, []Effect{
		emailEffect{Address: "user@example.com", Code: "123456"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if sentTo != "user@example.com" {
		t.Fatalf("effect was not dispatched correctly: sentTo=%s", sentTo)
	}
}

func TestEffectRouterFluentOnEffectRegistersHandlers(t *testing.T) {
	ctx := context.Background()
	var sentTo string

	router := NewEffectRouter().
		OnEffect(func(_ context.Context, effect emailEffect) error {
			sentTo = effect.Address
			return nil
		})
	if err := router.Validate(); err != nil {
		t.Fatal(err)
	}

	err := router.Publish(ctx, Result{ID: "flow_1"}, []Effect{
		emailEffect{Address: "user@example.com", Code: "123456"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if sentTo != "user@example.com" {
		t.Fatalf("effect was not dispatched correctly: sentTo=%s", sentTo)
	}
}

func TestEffectRouterValidateReportsRegistrationErrors(t *testing.T) {
	router := NewEffectRouter(
		OnEffect(func(context.Context, emailEffect) error {
			return nil
		}),
		OnEffect(func(context.Context, emailEffect) error {
			return nil
		}),
	)

	if err := router.Validate(); !errors.Is(err, ErrInvalidFlow) {
		t.Fatalf("expected duplicate handler validation error, got %v", err)
	}
	if err := router.Publish(context.Background(), Result{}, nil); !errors.Is(err, ErrInvalidFlow) {
		t.Fatalf("expected apply to reject invalid router, got %v", err)
	}
}

func TestEffectRouterRejectsInvalidHandlers(t *testing.T) {
	router := NewEffectRouter()

	if err := router.Handle(emailEffect{}, nil); !errors.Is(err, ErrInvalidFlow) {
		t.Fatalf("expected invalid flow error, got %v", err)
	}

	if err := router.Handle(emailEffect{}, func(context.Context, Result, emailEffect) error {
		return nil
	}); !errors.Is(err, ErrInvalidFlow) {
		t.Fatalf("expected invalid signature error, got %v", err)
	}

	if err := router.Handle(emailEffect{}, func(context.Context, emailEffect) error {
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := router.Handle(emailEffect{}, func(context.Context, emailEffect) error {
		return nil
	}); !errors.Is(err, ErrInvalidFlow) {
		t.Fatalf("expected duplicate handler error, got %v", err)
	}
}

func TestEffectRouterRejectsUnsupportedEffect(t *testing.T) {
	err := NewEffectRouter().Publish(context.Background(), Result{}, []Effect{emailEffect{}})
	if !errors.Is(err, ErrUnsupportedEffect) {
		t.Fatalf("expected unsupported effect error, got %v", err)
	}
}

func TestGraphOfReturnsDeclaredTransitions(t *testing.T) {
	spec := Define(testFlowType, func(_ context.Context, _ BeginContext, input int) (*Transition[testData], error) {
		return To("begun", testData{Count: input}), nil
	})
	Start(spec).MustGoTo("begun")
	When(spec, "begun", func(_ context.Context, data testData, _ incrementAction) (*Transition[testData], error) {
		return Done("done", data), nil
	}).MustGoTo("done")

	graph, err := GraphOf(spec)
	if err != nil {
		t.Fatal(err)
	}

	if graph.Type != testFlowType {
		t.Fatalf("expected flow type %s, got %s", testFlowType, graph.Type)
	}
	if len(graph.States) != 2 {
		t.Fatalf("expected 2 states, got %+v", graph.States)
	}
	if len(graph.Edges) != 2 {
		t.Fatalf("expected 2 edges, got %+v", graph.Edges)
	}
}

func TestGraphOfRegistryRendersMermaid(t *testing.T) {
	registry := NewRegistry()
	spec := Define(testFlowType, func(_ context.Context, _ BeginContext, input int) (*Transition[testData], error) {
		return To("begun", testData{Count: input}), nil
	})
	Start(spec).MustGoTo("begun")
	When(spec, "begun", func(_ context.Context, data testData, _ incrementAction) (*Transition[testData], error) {
		return Done("done", data), nil
	}).MustGoTo("done")

	if err := Register(registry, spec); err != nil {
		t.Fatal(err)
	}

	graph, err := GraphOfRegistry(registry)
	if err != nil {
		t.Fatal(err)
	}

	diagram := Mermaid(graph)
	if !strings.Contains(diagram, "stateDiagram-v2") {
		t.Fatalf("expected mermaid state diagram, got:\n%s", diagram)
	}
	if !strings.Contains(diagram, "[*] --> begun") {
		t.Fatalf("expected begin edge, got:\n%s", diagram)
	}
	if !strings.Contains(diagram, "begun --> done: inc") {
		t.Fatalf("expected action edge, got:\n%s", diagram)
	}
}

func TestDefinitionMethodsRouteTypedActions(t *testing.T) {
	ctx := context.Background()
	registry := NewRegistry()

	spec := Define(testFlowType, func(_ context.Context, _ BeginContext, input int) (*Transition[testData], error) {
		return To("begun", testData{Count: input}), nil
	})
	spec.Start().MustGoTo("begun")
	spec.When("begun", incrementAction{}, func(_ context.Context, data testData, _ incrementAction) (*Transition[testData], error) {
		data.Count++
		return To("incremented", data).WithPublic(map[string]any{
			"count": data.Count,
		}), nil
	}).MustGoTo("incremented")

	if err := Register(registry, spec); err != nil {
		t.Fatal(err)
	}
	engine := NewEngine(NewMemoryStore(), registry, WithIDGenerator(fixedID{id: "flow_1"}))

	started, err := engine.Begin(ctx, BeginRequest{Type: testFlowType, Input: 41})
	if err != nil {
		t.Fatal(err)
	}
	next, err := engine.Submit(ctx, SubmitRequest{FlowID: started.ID, Action: incrementAction{}})
	if err != nil {
		t.Fatal(err)
	}
	public, ok := next.Public.(map[string]any)
	if !ok || public["count"] != 42 {
		t.Fatalf("unexpected method spec result: %+v", next)
	}
}

func TestDefinitionMethodsRejectInvalidHandlers(t *testing.T) {
	spec := Define(testFlowType, func(context.Context, BeginContext, int) (*Transition[testData], error) {
		return To("begun", testData{}), nil
	})

	var handler DataFunc[testData, incrementAction]
	if err := spec.When("begun", incrementAction{}, handler).GoTo("incremented"); !errors.Is(err, ErrInvalidFlow) {
		t.Fatalf("expected invalid flow error for nil handler, got %v", err)
	}

	if err := spec.When("begun", incrementAction{}, func(context.Context, Result, incrementAction) (*Transition[testData], error) {
		return To("incremented", testData{}), nil
	}).GoTo("incremented"); !errors.Is(err, ErrInvalidFlow) {
		t.Fatalf("expected invalid signature error, got %v", err)
	}
}

func TestDefinitionRoutesTypedInputAndActions(t *testing.T) {
	ctx := context.Background()
	registry := NewRegistry()

	spec := Define(
		testFlowType,
		func(_ context.Context, _ BeginContext, input int) (*Transition[testData], error) {
			return &Transition[testData]{
				State: "begun",
				Data:  testData{Count: input},
			}, nil
		},
	)
	Start(spec).MustGoTo("begun")
	WhenFlow(spec,
		"begun",
		func(_ context.Context, instance *Flow[testData], _ incrementAction) (*Transition[testData], error) {
			instance.Data.Count++
			return &Transition[testData]{
				State: "incremented",
				Data:  instance.Data,
				Public: map[string]any{
					"count": instance.Data.Count,
				},
			}, nil
		},
	).MustGoTo("incremented")

	if err := Register(registry, spec); err != nil {
		t.Fatal(err)
	}
	engine := NewEngine(NewMemoryStore(), registry, WithIDGenerator(fixedID{id: "flow_1"}))

	started, err := engine.Begin(ctx, BeginRequest{Type: testFlowType, Input: 41})
	if err != nil {
		t.Fatal(err)
	}
	next, err := engine.Submit(ctx, SubmitRequest{FlowID: started.ID, Action: incrementAction{}})
	if err != nil {
		t.Fatal(err)
	}
	public, ok := next.Public.(map[string]any)
	if !ok || public["count"] != 42 {
		t.Fatalf("unexpected typed spec result: %+v", next)
	}
}

func TestDefinitionRejectsWrongInputType(t *testing.T) {
	ctx := context.Background()
	registry := NewRegistry()
	spec := Define(
		testFlowType,
		func(context.Context, BeginContext, int) (*Transition[testData], error) {
			return &Transition[testData]{State: "begun"}, nil
		},
	)
	if err := Register(registry, spec); err != nil {
		t.Fatal(err)
	}
	engine := NewEngine(NewMemoryStore(), registry)

	_, err := engine.Begin(ctx, BeginRequest{Type: testFlowType, Input: "wrong"})
	if !errors.Is(err, ErrInvalidFlow) {
		t.Fatalf("expected invalid flow error, got %v", err)
	}
}

func TestRegisterRejectsTypedNilSpec(t *testing.T) {
	registry := NewRegistry()
	var spec *Definition[testData, int]

	err := Register[testData](registry, spec)
	if !errors.Is(err, ErrInvalidFlow) {
		t.Fatalf("expected invalid flow error, got %v", err)
	}
}

func TestSimplifiedDefinitionAPI(t *testing.T) {
	ctx := context.Background()
	registry := NewRegistry()

	spec := Define(testFlowType, func(_ context.Context, _ BeginContext, input int) (*Transition[testData], error) {
		return &Transition[testData]{
			State: "begun",
			Data:  testData{Count: input},
		}, nil
	}).Version(2)

	Start(spec).MustGoTo("begun")
	WhenFlow(spec,
		"begun",
		func(_ context.Context, instance *Flow[testData], _ incrementAction) (*Transition[testData], error) {
			instance.Data.Count++
			return &Transition[testData]{
				State: "incremented",
				Data:  instance.Data,
			}, nil
		},
	).MustGoTo("incremented")

	if err := Register(registry, spec); err != nil {
		t.Fatal(err)
	}
	engine := NewEngine(NewMemoryStore(), registry, WithIDGenerator(fixedID{id: "flow_1"}))

	started, err := engine.Begin(ctx, BeginRequest{Type: testFlowType, Input: 1})
	if err != nil {
		t.Fatal(err)
	}
	next, err := engine.Submit(ctx, SubmitRequest{FlowID: started.ID, Action: incrementAction{}})
	if err != nil {
		t.Fatal(err)
	}
	if next.State != "incremented" {
		t.Fatalf("unexpected state: %+v", next)
	}
}

func TestDefinitionRejectsNilHandlersDuringSetup(t *testing.T) {
	spec := Define(testFlowType, func(context.Context, BeginContext, int) (*Transition[testData], error) {
		return To("begun", testData{}), nil
	})

	var dataHandler DataFunc[testData, incrementAction]
	if err := When(spec, "begun", dataHandler).GoTo("incremented"); !errors.Is(err, ErrInvalidFlow) {
		t.Fatalf("expected invalid flow error for nil data handler, got %v", err)
	}

	var runHandler SubmitFunc[testData, incrementAction]
	if err := WhenFlow(spec, "begun", runHandler).GoTo("incremented"); !errors.Is(err, ErrInvalidFlow) {
		t.Fatalf("expected invalid flow error for nil run handler, got %v", err)
	}
}

type structFlow struct {
	beginState FlowState
	nextState  FlowState
}

func (f structFlow) Begin(_ context.Context, _ BeginContext, input int) (*Transition[testData], error) {
	return &Transition[testData]{
		State: f.beginState,
		Data:  testData{Count: input},
	}, nil
}

func (f structFlow) Increment(_ context.Context, instance *Flow[testData], _ incrementAction) (*Transition[testData], error) {
	instance.Data.Count++
	return &Transition[testData]{
		State: f.nextState,
		Data:  instance.Data,
		Public: map[string]any{
			"count": instance.Data.Count,
		},
	}, nil
}

func TestDefineUsesStructMethods(t *testing.T) {
	ctx := context.Background()
	registry := NewRegistry()
	flow := structFlow{
		beginState: "begun",
		nextState:  "incremented",
	}

	spec := Define(testFlowType, flow.Begin)
	Start(spec).MustGoTo("begun")
	WhenFlow(spec, "begun", flow.Increment).MustGoTo("incremented")

	if err := Register(registry, spec); err != nil {
		t.Fatal(err)
	}
	engine := NewEngine(NewMemoryStore(), registry, WithIDGenerator(fixedID{id: "flow_1"}))

	started, err := engine.Begin(ctx, BeginRequest{Type: testFlowType, Input: 41})
	if err != nil {
		t.Fatal(err)
	}
	next, err := engine.Submit(ctx, SubmitRequest{FlowID: started.ID, Action: incrementAction{}})
	if err != nil {
		t.Fatal(err)
	}
	public, ok := next.Public.(map[string]any)
	if !ok || public["count"] != 42 {
		t.Fatalf("unexpected struct flow result: %+v", next)
	}
}

func TestWhenUsesOnlyTypedData(t *testing.T) {
	ctx := context.Background()
	registry := NewRegistry()
	flow := structFlow{
		beginState: "begun",
		nextState:  "incremented",
	}

	spec := Define(testFlowType, flow.Begin)
	Start(spec).MustGoTo("begun")
	When(spec, "begun", func(_ context.Context, data testData, _ incrementAction) (*Transition[testData], error) {
		data.Count++
		return To("incremented", data).WithPublic(map[string]any{
			"count": data.Count,
		}), nil
	}).MustGoTo("incremented")

	if err := Register(registry, spec); err != nil {
		t.Fatal(err)
	}
	engine := NewEngine(NewMemoryStore(), registry, WithIDGenerator(fixedID{id: "flow_1"}))

	started, err := engine.Begin(ctx, BeginRequest{Type: testFlowType, Input: 41})
	if err != nil {
		t.Fatal(err)
	}
	next, err := engine.Submit(ctx, SubmitRequest{FlowID: started.ID, Action: incrementAction{}})
	if err != nil {
		t.Fatal(err)
	}
	public, ok := next.Public.(map[string]any)
	if !ok || public["count"] != 42 {
		t.Fatalf("unexpected data handler result: %+v", next)
	}
}

func TestTransitionHelpers(t *testing.T) {
	data := testData{Count: 1}
	transition := Done("done", data).
		WithPublic("ok").
		WithEffects(testEffect("sent")).
		WithExpiry(time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC))

	if transition.State != "done" || !transition.Completed || transition.Data.Count != 1 {
		t.Fatalf("unexpected transition: %+v", transition)
	}
	if transition.Public != "ok" || len(transition.Effects) != 1 || transition.Effects[0].Type() != "sent" {
		t.Fatalf("unexpected transition extras: %+v", transition)
	}
	if transition.ExpiresAt.IsZero() {
		t.Fatalf("expected expiry")
	}
}
