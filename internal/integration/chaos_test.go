//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/ethan/smart-route/internal/controller"
	"github.com/ethan/smart-route/internal/domain"
	"github.com/ethan/smart-route/internal/sandbox"
	"github.com/ethan/smart-route/internal/sandbox/fake"
	"github.com/ethan/smart-route/internal/store"
	"github.com/ethan/smart-route/internal/store/sqlite"
	"github.com/ethan/smart-route/pkg/client"
)

// TestDockerSQLiteChaos exercises the control plane as a child process and
// workers through the production localdocker provider. It is intentionally one
// scenario-driven test so CI builds one image and launches one cluster.
func TestDockerSQLiteChaos(t *testing.T) {
	requireDocker(t)
	root := repositoryRoot(t)
	tmp := t.TempDir()
	stamp := fmt.Sprintf("%d", time.Now().UnixNano())
	image := "smart-route-worker:bai-21-" + stamp
	build(t, root, filepath.Join(tmp, "smart-route"), image)
	defer removeImage(t, image)

	port := freePort(t)
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	database := filepath.Join(tmp, "chaos.db")
	config := filepath.Join(tmp, "chaos.yaml")
	credential := "chaos-credential-" + stamp
	t.Setenv("BAI21_CHAOS_CANARY", credential)
	writeConfig(t, config, database, base, image, credential)

	var logs lockedBuffer
	service := startService(t, filepath.Join(tmp, "smart-route"), config, &logs)
	defer func() {
		if t.Failed() {
			t.Logf("control-plane logs:\n%s", logs.String())
		}
		service.stop(t)
		removeContainers(t, image)
	}()
	api, err := client.New(base, &httpClient)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, "control plane readiness", func() bool { return api.Ready(context.Background()) == nil })

	command := func(key, shell string, attempts int, preferred ...string) client.Job {
		payload, _ := json.Marshal(map[string]any{"command": "/bin/sh", "args": []string{"-c", shell}})
		constraints := client.Constraints{ExecutorKind: "process", Labels: map[string]string{"smart-route.pool": "chaos"}}
		if len(preferred) > 0 {
			constraints.PreferredSandbox = preferred[0]
		}
		job, err := api.SubmitJob(context.Background(), client.SubmitJob{IdempotencyKey: key, Kind: "command", Payload: payload, Constraints: constraints, TimeoutSeconds: 40, Retry: client.Retry{MaxAttempts: attempts, Backoff: client.Duration(100 * time.Millisecond)}})
		if err != nil {
			t.Fatalf("submit %s: %v", key, err)
		}
		return job
	}

	// Parallel execution and burst scale-up.
	waitFor(t, 20*time.Second, "four local Docker workers", func() bool {
		workers, e := api.ListWorkers(context.Background())
		return e == nil && len(workers) >= 4
	})
	boxes, err := api.ListSandboxes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	preferred := make([]string, 0, 4)
	for _, box := range boxes {
		if box.State == "ready" || box.State == "running" {
			preferred = append(preferred, box.ID)
		}
	}
	if len(preferred) < 4 {
		t.Fatalf("routable sandboxes=%d, want 4", len(preferred))
	}
	jobs := make([]client.Job, 50)
	var submit sync.WaitGroup
	for i := range jobs {
		submit.Add(1)
		go func(i int) {
			defer submit.Done()
			jobs[i] = command(fmt.Sprintf("parallel-%s-%02d", stamp, i), "sleep 0.25; hostname", 2, preferred[i%len(preferred)])
		}(i)
	}
	submit.Wait()
	used := map[string]bool{}
	for _, job := range jobs {
		terminal := terminal(t, api, job.ID, 30*time.Second)
		attempts, e := api.ListAttempts(context.Background(), job.ID)
		if terminal.State != "succeeded" {
			t.Fatalf("parallel job %s = %s, attempts=%#v", job.ID, terminal.State, attempts)
		}
		if e != nil || len(attempts) == 0 {
			t.Fatalf("parallel attempts %s = %#v, %v", job.ID, attempts, e)
		}
		used[attempts[len(attempts)-1].SandboxID] = true
	}
	if len(used) < 2 {
		t.Fatalf("50 jobs used %d sandbox, want multiple", len(used))
	}
	assertInvariants(t, api, jobs, 4)

	// Duplicate client submission is one logical job, even concurrently.
	duplicate := make([]client.Job, 8)
	var duplicateWG sync.WaitGroup
	for i := range duplicate {
		duplicateWG.Add(1)
		go func(i int) { defer duplicateWG.Done(); duplicate[i] = command("duplicate-"+stamp, "echo duplicate", 1) }(i)
	}
	duplicateWG.Wait()
	for i := 1; i < len(duplicate); i++ {
		if duplicate[i].ID != duplicate[0].ID {
			t.Fatalf("duplicate IDs %q and %q", duplicate[0].ID, duplicate[i].ID)
		}
	}
	terminal(t, api, duplicate[0].ID, 10*time.Second)

	// Heartbeats and renewals retain a legitimate lease past its original TTL.
	long := command("long-"+stamp, "sleep 8; echo renewed", 1)
	waitRunning(t, api, long.ID, 10*time.Second)
	if got := terminal(t, api, long.ID, 15*time.Second); got.State != "succeeded" {
		attempts, _ := api.ListAttempts(context.Background(), long.ID)
		t.Fatalf("long job = %s, attempts=%#v", got.State, attempts)
	}
	if attempts, _ := api.ListAttempts(context.Background(), long.ID); len(attempts) != 1 {
		t.Fatalf("long job attempts = %d, want 1", len(attempts))
	}

	// Running cancellation is delivered on the next heartbeat and releases the worker.
	cancelJob := command("cancel-running-"+stamp, "sleep 30", 1)
	cancelAttempt := waitRunning(t, api, cancelJob.ID, 10*time.Second)
	started := time.Now()
	if got, e := api.CancelJob(context.Background(), cancelJob.ID); e != nil || got.State != "canceled" {
		t.Fatalf("cancel running = %#v, %v", got, e)
	}
	waitFor(t, 2500*time.Millisecond, "worker cancellation acknowledgement", func() bool {
		workers, e := api.ListWorkers(context.Background())
		if e != nil {
			return false
		}
		for _, worker := range workers {
			if worker.ID == cancelAttempt.WorkerID && worker.LastSeenAt.After(started) {
				return true
			}
		}
		return false
	})
	if time.Since(started) > 2500*time.Millisecond {
		t.Fatal("running cancellation exceeded heartbeat latency bound")
	}
	queuedPayload, _ := json.Marshal(map[string]any{"command": "/bin/sh", "args": []string{"-c", "echo should-not-run"}})
	queued, e := api.SubmitJob(context.Background(), client.SubmitJob{IdempotencyKey: "cancel-queued-" + stamp, Kind: "command", Payload: queuedPayload, Constraints: client.Constraints{Labels: map[string]string{"unavailable": "true"}}, Retry: client.Retry{MaxAttempts: 1}})
	if e != nil {
		t.Fatal(e)
	}
	if got, e := api.CancelJob(context.Background(), queued.ID); e != nil || got.State != "canceled" {
		t.Fatalf("cancel queued = %#v, %v", got, e)
	}

	// Kill a real worker process while its command is running. The lease reaper
	// retries the job and another live sandbox completes the second attempt.
	crash := command("crash-"+stamp, "sleep 8; echo recovered", 2)
	first := waitRunning(t, api, crash.ID, 10*time.Second)
	if out, e := exec.Command("docker", "kill", "smart-route-worker-"+first.SandboxID).CombinedOutput(); e != nil {
		t.Fatalf("kill worker: %v: %s", e, out)
	}
	if got := terminal(t, api, crash.ID, 25*time.Second); got.State != "succeeded" {
		attempts, _ := api.ListAttempts(context.Background(), crash.ID)
		t.Fatalf("crash recovery job = %s, attempts=%#v", got.State, attempts)
	}
	crashAttempts, _ := api.ListAttempts(context.Background(), crash.ID)
	if len(crashAttempts) != 2 || crashAttempts[0].State != "lease_expired" || crashAttempts[0].SandboxID == crashAttempts[1].SandboxID {
		t.Fatalf("crash attempts = %#v", crashAttempts)
	}

	// Workers remain alive across a short control-plane restart and the durable
	// in-flight job completes after the service reopens the same SQLite file.
	restartJob := command("restart-"+stamp, "sleep 5; echo restart-ok", 1)
	waitRunning(t, api, restartJob.ID, 10*time.Second)
	before, _ := api.ListWorkers(context.Background())
	service.stop(t)
	time.Sleep(500 * time.Millisecond)
	service = startService(t, filepath.Join(tmp, "smart-route"), config, &logs)
	waitFor(t, 10*time.Second, "restarted control plane", func() bool { return api.Ready(context.Background()) == nil })
	if got := terminal(t, api, restartJob.ID, 12*time.Second); got.State != "succeeded" {
		t.Fatalf("restart job = %s", got.State)
	}
	after, _ := api.ListWorkers(context.Background())
	if len(after) < len(before) {
		t.Fatalf("workers after restart = %d, before = %d", len(after), len(before))
	}

	all := append(append(jobs, duplicate[0]), long, cancelJob, queued, crash, restartJob)
	assertInvariants(t, api, all, 4)
	service.stop(t)
	dump, e := os.ReadFile(database)
	if e != nil {
		t.Fatal(e)
	}
	if bytes.Contains(dump, []byte(credential)) || strings.Contains(logs.String(), credential) {
		t.Fatal("credential leaked to captured logs or SQLite database")
	}
}

func TestDeterministicRecoveryPolicies(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	db, err := sqlite.Open(filepath.Join(t.TempDir(), "policies.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	worker := domain.Worker{ID: "worker-old", InstanceID: "00000000-0000-0000-0000-000000000001", SandboxID: "box-old", MaxConcurrency: 1, AvailableSlots: 1, Capabilities: domain.Capabilities{Architecture: domain.ArchitectureAMD64}, Health: map[string]string{"status": "healthy"}, LastSeenAt: now}
	if err = db.UpsertWorker(ctx, worker); err != nil {
		t.Fatal(err)
	}
	if err = db.UpsertSandbox(ctx, domain.Sandbox{ID: worker.SandboxID, WorkerID: worker.ID, Provider: "fake", State: "ready", Capabilities: worker.Capabilities, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	worker2 := worker
	worker2.ID, worker2.InstanceID, worker2.SandboxID = "worker-new", "00000000-0000-0000-0000-000000000002", "box-new"
	if err = db.UpsertWorker(ctx, worker2); err != nil {
		t.Fatal(err)
	}
	if err = db.UpsertSandbox(ctx, domain.Sandbox{ID: worker2.SandboxID, WorkerID: worker2.ID, Provider: "fake", State: "ready", Capabilities: worker2.Capabilities, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	job, err := db.CreateJob(ctx, domain.Job{ID: "late", IdempotencyKey: "late-key", State: domain.JobQueued, RetryPolicy: domain.RetryPolicy{MaxAttempts: 2}, CreatedAt: now, UpdatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	old, _, err := db.ClaimNextJob(ctx, store.ClaimRequest{Worker: worker, SandboxID: worker.SandboxID, Now: now, LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExpireWorkerLeases(ctx, worker.ID, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	newAttempt, _, err := db.ClaimNextJob(ctx, store.ClaimRequest{Worker: worker2, SandboxID: worker2.SandboxID, Now: now.Add(3 * time.Second), LeaseDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	err = db.CompleteAttempt(ctx, store.Completion{AttemptID: old.ID, WorkerID: worker.ID, AttemptState: domain.AttemptSucceeded, JobState: domain.JobSucceeded, At: now.Add(3 * time.Second), Event: domain.Event{Type: domain.EventAttemptTransition}})
	if !errors.Is(err, store.ErrConflict) {
		t.Fatalf("late completion = %v", err)
	}
	if newAttempt.Number != 2 || newAttempt.JobID != job.ID || newAttempt.WorkerID != worker2.ID {
		t.Fatalf("retry attempt = %#v", newAttempt)
	}
	if err = db.CompleteAttempt(ctx, store.Completion{AttemptID: newAttempt.ID, WorkerID: worker2.ID, AttemptState: domain.AttemptSucceeded, JobState: domain.JobSucceeded, At: now.Add(4 * time.Second), Event: domain.Event{Type: domain.EventAttemptTransition}}); err != nil {
		t.Fatal(err)
	}
	if err = db.CancelJob(ctx, job.ID, now.Add(5*time.Second)); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("terminal mutation = %v", err)
	}
	immutable, err := db.GetJob(ctx, job.ID)
	if err != nil || immutable.State != domain.JobSucceeded {
		t.Fatalf("immutable terminal job = %s, %v", immutable.State, err)
	}

	provider := fake.New()
	provider.SetCreateError(sandbox.CodeCapacity)
	registry, err := sandbox.NewRegistry(map[string]sandbox.ProviderConfig{"fake": {Type: "fake"}}, map[string]sandbox.Factory{"fake": func(map[string]string) (sandbox.Provider, error) { return provider, nil }})
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.CreateJob(ctx, domain.Job{ID: "capacity", IdempotencyKey: "capacity-key", State: domain.JobQueued, Constraints: domain.RoutingConstraints{Architecture: domain.ArchitectureAMD64}, RetryPolicy: domain.RetryPolicy{MaxAttempts: 1}, CreatedAt: now, UpdatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	var decisions []controller.ScaleDecision
	auto := controller.NewQueueAutoscaler(db, registry, []controller.SandboxPool{{Name: "failed", Provider: "fake", Create: sandbox.CreateSpec{ControlPlaneURL: "http://control", BootstrapToken: "token"}, Capabilities: domain.Capabilities{Architecture: domain.ArchitectureAMD64}, MaxReplicas: 1, WorkerConcurrency: 1}}, func(d controller.ScaleDecision) { decisions = append(decisions, d) }, func() time.Time { return now })
	auto.Limits = controller.AutoscalerLimits{ProviderBackoffBase: time.Minute}
	if err = auto.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err = auto.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if provider.CreateCalls() != 1 || len(decisions) != 2 || decisions[1].Reason != "provider backoff circuit is open" {
		t.Fatalf("provider backoff: calls=%d decisions=%#v", provider.CreateCalls(), decisions)
	}
	queued, _ := db.GetJob(ctx, "capacity")
	if queued.State != domain.JobQueued {
		t.Fatalf("capacity job = %s", queued.State)
	}
	provider.SetCreateError("")
	orphan, err := provider.Create(ctx, sandbox.CreateSpec{SandboxID: "orphan", WorkerID: "orphan-worker", ControlPlaneURL: "http://control", BootstrapToken: "token", Labels: map[string]string{"owner": "route"}})
	if err != nil {
		t.Fatal(err)
	}
	reconciler := controller.NewSandboxReconciler(db, registry, controller.ReconcileConfig{OwnerLabel: "owner", OwnerValue: "route", Orphans: controller.OrphanTerminate}, func() time.Time { return now })
	if err = reconciler.Run(ctx); err != nil {
		t.Fatal(err)
	}
	gotOrphan, err := provider.Get(ctx, orphan.ID)
	if err != nil || gotOrphan.State != sandbox.StateTerminated {
		t.Fatalf("orphan state=%s err=%v", gotOrphan.State, err)
	}

	scaleDB, err := sqlite.Open(filepath.Join(t.TempDir(), "autoscale.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer scaleDB.Close()
	for i := range 4 {
		_, err = scaleDB.CreateJob(ctx, domain.Job{ID: domain.JobID(fmt.Sprintf("scale-%d", i)), IdempotencyKey: fmt.Sprintf("scale-key-%d", i), State: domain.JobQueued, RetryPolicy: domain.RetryPolicy{MaxAttempts: 1}, CreatedAt: now, UpdatedAt: now})
		if err != nil {
			t.Fatal(err)
		}
	}
	scaleProvider := fake.New()
	scaleRegistry, err := sandbox.NewRegistry(map[string]sandbox.ProviderConfig{"scale": {Type: "fake"}}, map[string]sandbox.Factory{"fake": func(map[string]string) (sandbox.Provider, error) { return scaleProvider, nil }})
	if err != nil {
		t.Fatal(err)
	}
	scaleNow := now
	scaler := controller.NewQueueAutoscaler(scaleDB, scaleRegistry, []controller.SandboxPool{{Name: "scale", Provider: "scale", Create: sandbox.CreateSpec{ControlPlaneURL: "http://control", BootstrapToken: "token"}, MaxReplicas: 4, WorkerConcurrency: 1}}, nil, func() time.Time { return scaleNow })
	scaler.Limits.MaxTotalSandboxes = 4
	if err = scaler.Run(ctx); err != nil {
		t.Fatal(err)
	}
	boxes, _ := scaleDB.ListSandboxes(ctx)
	if len(boxes) != 4 {
		t.Fatalf("scale-up sandboxes=%d, want 4", len(boxes))
	}
	for i := range 4 {
		if err = scaleDB.CancelJob(ctx, domain.JobID(fmt.Sprintf("scale-%d", i)), now); err != nil {
			t.Fatal(err)
		}
	}
	scaleNow = now.Add(time.Second)
	if err = scaler.Run(ctx); err != nil {
		t.Fatal(err)
	}
	boxes, _ = scaleDB.ListSandboxes(ctx)
	draining := 0
	for _, box := range boxes {
		if box.State == "draining" {
			draining++
		}
	}
	if draining != 4 {
		t.Fatalf("scale-down draining=%d, want 4", draining)
	}
}

var httpClient = *clientHTTP()

func clientHTTP() *http.Client { return &http.Client{Timeout: 5 * time.Second} }

type process struct{ cmd *exec.Cmd }

func startService(t *testing.T, binary, config string, logs *lockedBuffer) *process {
	t.Helper()
	cmd := exec.Command(binary, "--config", config, "serve")
	cmd.Stdout, cmd.Stderr = logs, logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	return &process{cmd: cmd}
}
func (p *process) stop(t *testing.T) {
	t.Helper()
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return
	}
	if p.cmd.ProcessState != nil && p.cmd.ProcessState.Exited() {
		return
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan error, 1)
	go func() { done <- p.cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("stop service: %v", err)
		}
	case <-time.After(5 * time.Second):
		_ = p.cmd.Process.Kill()
		<-done
		t.Fatal("service did not stop gracefully")
	}
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}
func (b *lockedBuffer) String() string { b.mu.Lock(); defer b.mu.Unlock(); return b.buf.String() }

func repositoryRoot(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("go", "env", "GOMOD").Output()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Dir(strings.TrimSpace(string(out)))
}
func build(t *testing.T, root, binary, image string) {
	t.Helper()
	run(t, root, "go", "build", "-o", binary, "./cmd/smart-route")
	run(t, root, "docker", "build", "--build-arg", "VERSION=bai-21", "-t", image, "-f", "Dockerfile.worker", ".")
}
func requireDocker(t *testing.T) {
	t.Helper()
	if os.Getenv("SMART_ROUTE_INTEGRATION") != "1" {
		t.Skip("set SMART_ROUTE_INTEGRATION=1")
	}
	if out, err := exec.Command("docker", "info").CombinedOutput(); err != nil {
		t.Fatalf("docker unavailable: %v: %s", err, out)
	}
}
func removeImage(t *testing.T, image string) {
	t.Helper()
	_ = exec.Command("docker", "image", "rm", "--force", image).Run()
}
func removeContainers(t *testing.T, image string) {
	t.Helper()
	out, _ := exec.Command("docker", "ps", "--all", "--quiet", "--filter", "ancestor="+image).Output()
	ids := strings.Fields(string(out))
	if len(ids) > 0 {
		_ = exec.Command("docker", append([]string{"rm", "--force"}, ids...)...).Run()
	}
}
func run(t *testing.T, dir, name string, args ...string) {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%s %v: %v\n%s", name, args, err, out)
	}
}
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
func writeConfig(t *testing.T, path, database, base, image, credential string) {
	t.Helper()
	data := fmt.Sprintf(`http:
  listen: %s
  public_url: %s
  request_timeout: 5s
  shutdown_timeout: 3s
database:
  dsn: %s
jobs:
  heartbeat_interval: 1s
  lease_duration: 6s
  worker_timeout: 3s
  max_claim_wait: 200ms
  max_events: 100
  inline_result_bytes: 65536
  max_result_bytes: 8388608
  max_attempts: 2
auth:
  insecure_local: true
  bootstrap_token_ttl: 1m
  worker_session_ttl: 1m
providers:
  docker:
    type: localdocker
    config:
      image: %s
      network: host
pools:
  - name: chaos
    provider: docker
    image: %s
    capabilities: [shell]
    executor_kinds: [process, remote]
    architecture: amd64
    labels: {chaos-owner: "1"}
    min_replicas: 4
    max_replicas: 4
    worker_concurrency: 2
    idle_ttl: 500ms
    startup_timeout: 10s
    scale_down_stabilize: 500ms
controllers:
  lease_reaper: 200ms
  job_timeouts: 1h
  worker_health: 1h
  reconciler: 1h
  reaper: 1h
  autoscaler: 200ms
  worker_suspect_after: 2s
  worker_dead_after: 3s
  idle_after: 1m
  drain_grace: 200ms
  orphans: terminate
  owner_label: chaos-owner
  owner_value: "1"
  minimum_warm: 0
  max_scale_up_per_run: 4
  provisioning_concurrency: 4
  max_total_sandboxes: 4
secrets:
  environment:
    canary:
      TOKEN: BAI21_CHAOS_CANARY
`, strings.TrimPrefix(base, "http://"), base, database, image, image)
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
}
func waitFor(t *testing.T, timeout time.Duration, what string, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}
func terminal(t *testing.T, api *client.Client, id string, timeout time.Duration) client.Job {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	job, err := api.WaitTerminal(ctx, id, 50*time.Millisecond)
	if err != nil {
		current, _ := api.GetJob(context.Background(), id)
		attempts, _ := api.ListAttempts(context.Background(), id)
		containers, _ := exec.Command("docker", "ps", "--all", "--filter", "label=smart-route.managed=true", "--format", "{{.Names}} {{.Status}}").CombinedOutput()
		t.Fatalf("wait terminal %s: %v; job=%#v attempts=%#v containers=%s", id, err, current, attempts, containers)
	}
	return job
}
func waitRunning(t *testing.T, api *client.Client, id string, timeout time.Duration) client.Attempt {
	t.Helper()
	var found client.Attempt
	waitFor(t, timeout, "running attempt "+id, func() bool {
		attempts, err := api.ListAttempts(context.Background(), id)
		if err != nil || len(attempts) == 0 {
			return false
		}
		found = attempts[len(attempts)-1]
		return found.State == "leased" || found.State == "running"
	})
	return found
}
func assertInvariants(t *testing.T, api *client.Client, jobs []client.Job, maxSandboxes int) {
	t.Helper()
	boxes, err := api.ListSandboxes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	activeBoxes := 0
	for _, box := range boxes {
		if box.State != "terminated" && box.State != "missing" {
			activeBoxes++
		}
	}
	if activeBoxes > maxSandboxes {
		t.Fatalf("active sandboxes = %d, max %d", activeBoxes, maxSandboxes)
	}
	seen := map[string]bool{}
	for _, submitted := range jobs {
		if seen[submitted.ID] {
			continue
		}
		seen[submitted.ID] = true
		job, err := api.GetJob(context.Background(), submitted.ID)
		if err != nil {
			t.Fatal(err)
		}
		attempts, err := api.ListAttempts(context.Background(), job.ID)
		if err != nil {
			t.Fatal(err)
		}
		active := 0
		last := 0
		for _, attempt := range attempts {
			if attempt.Number <= last {
				t.Fatalf("job %s attempt sequence %#v", job.ID, attempts)
			}
			last = attempt.Number
			if attempt.State == "leased" || attempt.State == "running" {
				active++
			}
		}
		if active > 1 {
			t.Fatalf("job %s has %d active attempts", job.ID, active)
		}
		if job.State == "succeeded" || job.State == "failed" || job.State == "canceled" || job.State == "timed_out" {
			events, e := api.ListEvents(context.Background(), job.ID)
			if e != nil {
				t.Fatal(e)
			}
			terminalEvent := false
			for _, event := range events {
				if event.Type == "attempt_transition" || event.Type == "job_transition" {
					terminalEvent = true
				}
			}
			if !terminalEvent {
				t.Fatalf("terminal job %s has no terminal event", job.ID)
			}
		}
	}
}
