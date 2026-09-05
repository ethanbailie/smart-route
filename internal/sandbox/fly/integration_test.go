//go:build integration

package fly

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethan/smart-route/internal/controller"
	"github.com/ethan/smart-route/internal/domain"
	"github.com/ethan/smart-route/internal/httpapi"
	"github.com/ethan/smart-route/internal/sandbox"
	"github.com/ethan/smart-route/internal/store/sqlite"
	"github.com/ethan/smart-route/pkg/client"
)

// TestLiveWorkerE2E is excluded from default tests. The caller must expose the
// listener at CONTROL_PLANE_URL (for example through an authenticated tunnel).
// The test owns every Fly Machine it creates and always schedules cleanup.
func TestLiveWorkerE2E(t *testing.T) {
	app := os.Getenv("FLY_INTEGRATION_APP")
	image := os.Getenv("FLY_INTEGRATION_IMAGE")
	listen := os.Getenv("FLY_INTEGRATION_LISTEN")
	publicURL := os.Getenv("FLY_INTEGRATION_CONTROL_PLANE_URL")
	if os.Getenv("FLY_API_TOKEN") == "" || app == "" || image == "" || listen == "" || publicURL == "" {
		t.Skip("see README: set all Fly integration environment variables")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	db, err := sqlite.Open(filepath.Join(t.TempDir(), "fly-e2e.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	api := httpapi.New(db, httpapi.Config{RequestTimeout: 30 * time.Second, MaxClaimWait: 2 * time.Second, HeartbeatInterval: time.Second, LeaseDuration: 10 * time.Second, BootstrapTokenTTL: 5 * time.Minute, WorkerSessionTTL: 5 * time.Minute, InlineResultBytes: 64 << 10, MaxResultBytes: 1 << 20, MaxEvents: 100})
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: api.Handler()}
	go func() { _ = server.Serve(listener) }()
	defer server.Shutdown(context.Background())
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	local, err := client.New("http://127.0.0.1:"+port, &http.Client{Timeout: 35 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err = local.Health(ctx); err != nil {
		t.Fatal(err)
	}

	p, err := New(Config{App: app, Image: image, StartupTimeout: 2 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	id := domain.SandboxID(fmt.Sprintf("fly-e2e-%d", time.Now().UnixNano()))
	caps := domain.Capabilities{Labels: map[string]string{"pool": "fly-e2e"}, Architecture: domain.ArchitectureAMD64, ExecutorKinds: []domain.ExecutorKind{domain.ExecutorProcess, domain.ExecutorRemote}}
	bootstrap, err := api.MintBootstrapToken(ctx, id, ProviderName, "fly-e2e", caps)
	if err != nil {
		t.Fatal(err)
	}
	created, err := p.Create(ctx, sandbox.CreateSpec{SandboxID: id, WorkerID: "pending-" + domain.WorkerID(id), ControlPlaneURL: publicURL, BootstrapToken: bootstrap, Image: image, WorkerMaxConcurrency: 1, MaxLifetime: 10 * time.Minute, Capabilities: caps, Labels: map[string]string{"test": "fly-live"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, done := context.WithTimeout(context.Background(), time.Minute)
		defer done()
		_ = p.Terminate(cleanupCtx, created.ID)
	})

	var registered client.Worker
	poll(t, ctx, "worker registration", func() bool {
		workers, e := local.ListWorkers(ctx)
		if e != nil {
			return false
		}
		for _, w := range workers {
			if w.Capabilities.Labels["pool"] == "fly-e2e" {
				registered = w
				return true
			}
		}
		return false
	})
	payload, _ := json.Marshal(map[string]any{"command": "/bin/echo", "args": []string{"fly-e2e-ok"}, "timeout_seconds": 15})
	job, err := local.SubmitJob(ctx, client.SubmitJob{IdempotencyKey: string(id), Kind: "command", Payload: payload, Constraints: client.Constraints{Labels: map[string]string{"pool": "fly-e2e"}, ExecutorKind: "process", PreferredSandbox: string(id)}, TimeoutSeconds: 60, Retry: client.Retry{MaxAttempts: 1}})
	if err != nil {
		t.Fatal(err)
	}
	job, err = local.WaitTerminal(ctx, job.ID, time.Second)
	if err != nil || job.State != "succeeded" {
		t.Fatalf("job state=%q err=%v worker=%s", job.State, err, registered.ID)
	}
	result, err := local.GetResult(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(result.Data, []byte("fly-e2e-ok\n")) {
		t.Fatalf("result=%q", result.Data)
	}

	registry, err := sandbox.NewRegistry(map[string]sandbox.ProviderConfig{ProviderName: {Type: "fly-instance"}}, map[string]sandbox.Factory{"fly-instance": func(map[string]string) (sandbox.Provider, error) { return p, nil }})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Add(time.Minute)
	reaper := controller.NewSandboxReaper(db, registry, controller.ReaperConfig{IdleAfter: time.Second, DrainGrace: time.Second}, func() time.Time { return now })
	if err = reaper.Run(ctx); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	if err = reaper.Run(ctx); err != nil {
		t.Fatal(err)
	}
	poll(t, ctx, "Machine removal", func() bool {
		items, e := p.List(ctx, sandbox.Filter{Labels: map[string]string{"test": "fly-live"}})
		if e != nil {
			return false
		}
		for _, item := range items {
			if item.ID == id && item.State != sandbox.StateTerminated {
				return false
			}
		}
		return true
	})
}

func poll(t *testing.T, ctx context.Context, name string, ready func() bool) {
	t.Helper()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if ready() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s: %v", name, ctx.Err())
		case <-ticker.C:
		}
	}
}
