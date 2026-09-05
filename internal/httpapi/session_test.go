package httpapi_test

import (
	"context"
	"io"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethan/smart-route/internal/checkpoint"
	"github.com/ethan/smart-route/internal/domain"
	"github.com/ethan/smart-route/internal/httpapi"
	"github.com/ethan/smart-route/internal/sandbox"
	"github.com/ethan/smart-route/internal/sandbox/fake"
	"github.com/ethan/smart-route/internal/store/sqlite"
	"github.com/ethan/smart-route/pkg/client"
)

func TestSessionClientLifecycle(t *testing.T) {
	db, err := sqlite.Open(filepath.Join(t.TempDir(), "api.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	server := httptest.NewServer(httpapi.New(db, httpapi.Config{CheckpointAdapter: checkpoint.Filesystem{Root: t.TempDir()}, CheckpointTTL: time.Hour}).Handler())
	defer server.Close()
	c, err := client.New(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	session, err := c.CreateSession(ctx, client.CreateSession{Pool: "agent", Capabilities: []string{"git"}, IdleTTL: client.Duration(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	job, err := c.SubmitJob(ctx, client.SubmitJob{IdempotencyKey: "step", Kind: "command", Payload: []byte(`{"command":"true"}`), SessionID: session.ID})
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := c.ListSessionJobs(ctx, session.ID)
	if err != nil || len(jobs) != 1 || jobs[0].ID != job.ID || jobs[0].SessionID != session.ID {
		t.Fatalf("jobs=%#v %v", jobs, err)
	}
	closed, err := c.CloseSession(ctx, session.ID)
	if err != nil || closed.State != "closed" {
		t.Fatalf("close=%#v %v", closed, err)
	}
	recoverable, err := c.CreateSession(ctx, client.CreateSession{Pool: "agent", RecoveryPolicy: domain.RecoveryCheckpoint, CheckpointMode: domain.CheckpointExplicit})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err = db.UpsertWorker(ctx, domain.Worker{ID: "rw", SandboxID: "rb", LastSeenAt: now}); err != nil {
		t.Fatal(err)
	}
	if err = db.UpsertSandbox(ctx, domain.Sandbox{ID: "rb", WorkerID: "rw", Provider: "fake", State: "ready", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err = db.BindSession(ctx, domain.SessionID(recoverable.ID), "rw", "rb", now); err != nil {
		t.Fatal(err)
	}
	cp, err := c.CreateCheckpoint(ctx, recoverable.ID, []byte("portable-state"))
	if err != nil || cp.State != "ready" {
		t.Fatalf("checkpoint=%#v %v", cp, err)
	}
	items, err := c.ListCheckpoints(ctx, recoverable.ID)
	if err != nil || len(items) != 1 {
		t.Fatalf("checkpoints=%#v %v", items, err)
	}
	status, err := c.RecoverSession(ctx, recoverable.ID)
	if err != nil || status.State != "recovering" || status.Epoch != 2 {
		t.Fatalf("recover=%#v %v", status, err)
	}
}

func TestExplicitProviderCheckpointUsesNativeSnapshot(t *testing.T) {
	ctx := context.Background()
	db, err := sqlite.Open(filepath.Join(t.TempDir(), "native.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	provider := fake.New()
	registry, err := sandbox.NewRegistry(map[string]sandbox.ProviderConfig{"configured": {Type: "fake"}}, map[string]sandbox.Factory{"fake": func(map[string]string) (sandbox.Provider, error) { return provider, nil }})
	if err != nil {
		t.Fatal(err)
	}
	adapter := checkpoint.ProviderSnapshot{Backing: checkpoint.Filesystem{Root: t.TempDir()}}
	server := httptest.NewServer(httpapi.New(db, httpapi.Config{CheckpointAdapter: adapter, Providers: registry}).Handler())
	defer server.Close()
	apiClient, err := client.New(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	session, err := apiClient.CreateSession(ctx, client.CreateSession{Pool: "agent", RecoveryPolicy: domain.RecoveryCheckpoint, CheckpointMode: domain.CheckpointExplicit})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err = db.UpsertWorker(ctx, domain.Worker{ID: "worker", SandboxID: "box", LastSeenAt: now}); err != nil {
		t.Fatal(err)
	}
	if err = db.UpsertSandbox(ctx, domain.Sandbox{ID: "box", WorkerID: "worker", Provider: "configured", State: "ready", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err = db.BindSession(ctx, domain.SessionID(session.ID), "worker", "box", now); err != nil {
		t.Fatal(err)
	}
	provider.SetSnapshot("box", []byte("provider-native"))
	cp, err := apiClient.CreateCheckpoint(ctx, session.ID, []byte("caller-archive"))
	if err != nil {
		t.Fatal(err)
	}
	stream, err := adapter.Open(ctx, cp)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	data, err := io.ReadAll(stream)
	if err != nil || string(data) != "provider-native" {
		t.Fatalf("stored snapshot=%q err=%v", data, err)
	}
}
