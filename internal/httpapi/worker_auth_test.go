package httpapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethan/smart-route/internal/domain"
	"github.com/ethan/smart-route/internal/store/sqlite"
)

func TestBootstrapCredentialBindingsAndExpiry(t *testing.T) {
	db, err := sqlite.Open(filepath.Join(t.TempDir(), "auth.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	api := New(db, Config{BootstrapTokenTTL: time.Millisecond})
	server := httptest.NewServer(api.Handler())
	defer server.Close()
	caps := domain.Capabilities{Capabilities: []string{"build"}, Labels: map[string]string{"pool": "main"}, Architecture: domain.ArchitectureAMD64}
	body := func(token, sandboxID, provider, capability string) string {
		return fmt.Sprintf(`{"bootstrap_token":%q,"instance_id":"550e8400-e29b-41d4-a716-446655440000","sandbox_id":%q,"sandbox_provider":%q,"worker_version":"1","protocol_version":"1","max_concurrency":1,"capabilities":{"capabilities":[%q],"labels":{"pool":"main"},"architecture":"amd64"}}`, token, sandboxID, provider, capability)
	}
	for _, tc := range []struct{ name, sandbox, provider, capability string }{
		{"cross sandbox", "other", "localdocker", "build"},
		{"wrong provider audience", "sandbox-1", "other", "build"},
		{"capability mismatch", "sandbox-1", "localdocker", "admin"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			token, err := api.MintBootstrapToken(context.Background(), "sandbox-1", "localdocker", "main", caps)
			if err != nil {
				t.Fatal(err)
			}
			res := workerRequest(t, server.Client(), http.MethodPost, server.URL+"/v1/worker/register", "", "", body(token, tc.sandbox, tc.provider, tc.capability))
			defer res.Body.Close()
			if res.StatusCode != http.StatusUnauthorized {
				t.Fatalf("status = %d", res.StatusCode)
			}
		})
	}
	token, err := api.MintBootstrapToken(context.Background(), "sandbox-1", "localdocker", "main", caps)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * time.Millisecond)
	res := workerRequest(t, server.Client(), http.MethodPost, server.URL+"/v1/worker/register", "", "", body(token, "sandbox-1", "localdocker", "build"))
	defer res.Body.Close()
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expired status = %d", res.StatusCode)
	}
}

func TestWorkerSessionEpochFence(t *testing.T) {
	db, err := sqlite.Open(filepath.Join(t.TempDir(), "epoch.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	now := time.Now().UTC()
	token := "token"
	sum := sha256.Sum256([]byte(token))
	s, err := db.CreateSession(ctx, domain.Session{ID: "s", Pool: "p", RecoveryPolicy: domain.RecoveryRebuild, RebuildPlan: []domain.RebuildStep{{Kind: "command", IdempotencyKey: "safe"}}, CreatedAt: now, LastActivity: now})
	if err != nil {
		t.Fatal(err)
	}
	w := domain.Worker{ID: "w", SandboxID: "b", SessionTokenHash: hex.EncodeToString(sum[:]), SessionExpiresAt: now.Add(time.Hour), ProtocolVersion: "1", MaxConcurrency: 1, AvailableSlots: 1, LastSeenAt: now}
	if err = db.UpsertWorker(ctx, w); err != nil {
		t.Fatal(err)
	}
	if err = db.UpsertSandbox(ctx, domain.Sandbox{ID: "b", WorkerID: "w", Provider: "fake", State: "ready", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err = db.BindSession(ctx, s.ID, w.ID, w.SandboxID, now); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(New(db, Config{}).Handler())
	defer server.Close()
	if err = db.RequestRecovery(ctx, s.ID, now); err != nil {
		t.Fatal(err)
	}
	res := workerRequest(t, server.Client(), http.MethodPost, server.URL+"/v1/worker/heartbeat", "w", token, `{"available_slots":1}`)
	defer res.Body.Close()
	if res.StatusCode != http.StatusConflict {
		t.Fatalf("stale epoch status=%d", res.StatusCode)
	}
}
