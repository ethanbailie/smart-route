package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethan/smart-route/internal/httpapi"
	"github.com/ethan/smart-route/internal/store/sqlite"
)

func TestClientAgainstAPI(t *testing.T) {
	db, err := sqlite.Open(filepath.Join(t.TempDir(), "api.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	server := httptest.NewServer(httpapi.New(db, httpapi.Config{}).Handler())
	defer server.Close()
	client, err := New(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	upstream := "model-a"
	request := SubmitJob{IdempotencyKey: "stable-key", Kind: "generic", Payload: json.RawMessage(`{"task":"run"}`), Constraints: Constraints{Capabilities: []string{"gpu"}, Labels: map[string]string{"pool": "main"}, Upstream: &upstream}, TimeoutSeconds: 60, Retry: Retry{MaxAttempts: 2}}
	first, err := client.SubmitJob(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := client.SubmitJob(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	conflicting := request
	conflicting.Kind = "different"
	_, conflictErr := client.SubmitJob(ctx, conflicting)
	var apiErr *APIError
	if !errors.As(conflictErr, &apiErr) || apiErr.Code != CodeDuplicateIdempotencyConflict {
		t.Fatalf("duplicate conflict = %v", conflictErr)
	}
	if first.ID == "" || duplicate.ID != first.ID || string(first.Payload) != `{"task":"run"}` || first.TimeoutSeconds != 60 || len(first.Constraints.Capabilities) != 1 {
		t.Fatalf("jobs = %#v, %#v", first, duplicate)
	}
	got, err := client.GetJob(ctx, first.ID)
	if err != nil || got.Kind != "generic" {
		t.Fatalf("get = %#v, %v", got, err)
	}
	if attempts, err := client.ListAttempts(ctx, first.ID); err != nil || len(attempts) != 0 {
		t.Fatalf("attempts = %#v, %v", attempts, err)
	}
	if events, err := client.ListEvents(ctx, first.ID); err != nil || len(events) != 0 {
		t.Fatalf("events = %#v, %v", events, err)
	}
	if workers, err := client.ListWorkers(ctx); err != nil || len(workers) != 0 {
		t.Fatalf("workers = %#v, %v", workers, err)
	}
	if sandboxes, err := client.ListSandboxes(ctx); err != nil || len(sandboxes) != 0 {
		t.Fatalf("sandboxes = %#v, %v", sandboxes, err)
	}
	if err := client.Health(ctx); err != nil {
		t.Fatal(err)
	}
	if err := client.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	canceled, err := client.CancelJob(ctx, first.ID)
	if err != nil || canceled.State != "canceled" {
		t.Fatalf("cancel = %#v, %v", canceled, err)
	}
}

func TestWaitTerminalHonorsContext(t *testing.T) {
	db, err := sqlite.Open(filepath.Join(t.TempDir(), "poll.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	server := httptest.NewServer(httpapi.New(db, httpapi.Config{}).Handler())
	defer server.Close()
	client, _ := New(server.URL, server.Client())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = client.WaitTerminal(ctx, "job", time.Hour)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("wait error = %v", err)
	}
}
