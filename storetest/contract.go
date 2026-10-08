// Package storetest provides a reusable contract suite for Trail Store
// implementations.
package storetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/foxhq/trail"
)

// Factory creates an isolated empty Store for one contract invocation.
type Factory func(t testing.TB) trail.Store

// Contract verifies the persistence guarantees Trail relies on: committed
// revisions, snapshot isolation, optimistic conflict detection, and deletion.
// Call it from an adapter's test package with a factory backed by an isolated
// database or transaction.
func Contract(t *testing.T, newStore Factory) {
	t.Helper()
	if newStore == nil {
		t.Fatal("trail store factory is nil")
	}

	t.Run("create_get_and_snapshot_isolation", func(t *testing.T) {
		store := newStore(t)
		createdAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
		input := trail.Snapshot{
			ID: "flow_1", Type: "contract", State: "waiting", Data: []byte(`{"count":1}`),
			ViewData: []byte(`{"screen":"waiting"}`), DataVersion: 1, ViewVersion: 1,
			CreatedAt: createdAt, UpdatedAt: createdAt, Metadata: map[string]string{"key": "value"},
		}
		created, err := store.Create(context.Background(), input)
		if err != nil {
			t.Fatal(err)
		}
		if created.Revision != 1 {
			t.Fatalf("create revision = %d, want 1", created.Revision)
		}
		input.Data[0] = 'i'
		input.ViewData[0] = 'i'
		input.Metadata["key"] = "input-mutated"
		created.Data[0] = 'c'
		created.ViewData[0] = 'c'
		created.Metadata["key"] = "created-mutated"

		stored, err := store.Get(context.Background(), "flow_1")
		if err != nil {
			t.Fatal(err)
		}
		if string(stored.Data) != `{"count":1}` || string(stored.ViewData) != `{"screen":"waiting"}` || stored.Metadata["key"] != "value" {
			t.Fatalf("store did not isolate create ingress and egress: %+v", stored)
		}
		stored.Data[0] = 'g'
		stored.ViewData[0] = 'g'
		stored.Metadata["key"] = "get-mutated"
		storedAgain, err := store.Get(context.Background(), "flow_1")
		if err != nil {
			t.Fatal(err)
		}
		if string(storedAgain.Data) != `{"count":1}` || string(storedAgain.ViewData) != `{"screen":"waiting"}` || storedAgain.Metadata["key"] != "value" {
			t.Fatalf("store did not isolate get result: %+v", storedAgain)
		}
	})

	t.Run("update_assigns_next_revision_and_rejects_stale_write", func(t *testing.T) {
		store := newStore(t)
		created, err := store.Create(context.Background(), trail.Snapshot{
			ID: "flow_1", Type: "contract", State: "waiting",
			Data: []byte(`{"count":1}`), ViewData: []byte(`{"screen":"waiting"}`),
			Metadata: map[string]string{"key": "value"},
		})
		if err != nil {
			t.Fatal(err)
		}
		candidate := created
		candidate.State = "done"
		candidate.Data = []byte(`{"count":2}`)
		candidate.ViewData = []byte(`{"screen":"done"}`)
		candidate.Metadata = map[string]string{"key": "updated"}
		updated, err := store.Update(context.Background(), candidate)
		if err != nil {
			t.Fatal(err)
		}
		if updated.Revision != created.Revision+1 || updated.State != "done" {
			t.Fatalf("unexpected committed update: %+v", updated)
		}
		candidate.Data[0] = 'c'
		candidate.ViewData[0] = 'c'
		candidate.Metadata["key"] = "candidate-mutated"
		updated.Data[0] = 'u'
		updated.ViewData[0] = 'u'
		updated.Metadata["key"] = "updated-mutated"
		stored, err := store.Get(context.Background(), "flow_1")
		if err != nil {
			t.Fatal(err)
		}
		if string(stored.Data) != `{"count":2}` || string(stored.ViewData) != `{"screen":"done"}` || stored.Metadata["key"] != "updated" {
			t.Fatalf("store did not isolate update ingress and egress: %+v", stored)
		}
		if _, err := store.Update(context.Background(), created); !errors.Is(err, trail.ErrFlowConflict) {
			t.Fatalf("stale update error = %v, want ErrFlowConflict", err)
		}
	})

	t.Run("list_returns_isolated_snapshots", func(t *testing.T) {
		store := newStore(t)
		queryStore, ok := store.(trail.QueryStore)
		if !ok {
			t.Skip("store does not implement QueryStore")
		}
		if _, err := store.Create(context.Background(), trail.Snapshot{
			ID: "flow_1", Type: "contract", State: "waiting",
			Data: []byte(`{"count":1}`), ViewData: []byte(`{"screen":"waiting"}`),
			Metadata: map[string]string{"key": "value"},
		}); err != nil {
			t.Fatal(err)
		}
		first, err := queryStore.List(context.Background(), trail.Query{})
		if err != nil || len(first) != 1 {
			t.Fatalf("list = %+v, %v", first, err)
		}
		first[0].Data[0] = 'l'
		first[0].ViewData[0] = 'l'
		first[0].Metadata["key"] = "list-mutated"
		second, err := queryStore.List(context.Background(), trail.Query{})
		if err != nil || len(second) != 1 {
			t.Fatalf("second list = %+v, %v", second, err)
		}
		if string(second[0].Data) != `{"count":1}` || string(second[0].ViewData) != `{"screen":"waiting"}` || second[0].Metadata["key"] != "value" {
			t.Fatalf("store did not isolate list result: %+v", second[0])
		}
	})

	t.Run("delete", func(t *testing.T) {
		store := newStore(t)
		if _, err := store.Create(context.Background(), trail.Snapshot{ID: "flow_1", Type: "contract", State: "waiting"}); err != nil {
			t.Fatal(err)
		}
		if err := store.Delete(context.Background(), "flow_1"); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Get(context.Background(), "flow_1"); !errors.Is(err, trail.ErrFlowNotFound) {
			t.Fatalf("get after delete error = %v, want ErrFlowNotFound", err)
		}
	})
}
