package controller_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ethan/smart-route/internal/controller"
	"github.com/ethan/smart-route/internal/domain"
	"github.com/ethan/smart-route/internal/sandbox"
	"github.com/ethan/smart-route/internal/sandbox/fake"
	"github.com/ethan/smart-route/internal/store"
	"github.com/ethan/smart-route/internal/store/sqlite"
)

func TestLeaseReaperIsOverlapSafeAndRejectsStaleCompletion(t *testing.T) {
	db, now, worker, box, job := fixture(t)
	defer db.Close()
	attempt, _, err := db.ClaimNextJob(context.Background(), store.ClaimRequest{Worker: worker, SandboxID: box.ID, Now: now, LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	clock := func() time.Time { return now.Add(2 * time.Second) }
	reaper := controller.NewLeaseReaper(db, clock)
	start := make(chan struct{})
	errs := make(chan error, 8)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() { defer wg.Done(); <-start; errs <- reaper.Run(context.Background()) }()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	got, err := db.GetJob(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != domain.JobQueued || len(got.Events) != 1 || got.Attempts[0].State != domain.AttemptExpired {
		t.Fatalf("reaped job = %#v", got)
	}
	err = db.CompleteAttempt(context.Background(), store.Completion{AttemptID: attempt.ID, WorkerID: worker.ID, AttemptState: domain.AttemptSucceeded, JobState: domain.JobSucceeded, At: clock(), Event: domain.Event{Type: domain.EventAttemptTransition}})
	if !errors.Is(err, store.ErrConflict) {
		t.Fatalf("stale completion error = %v", err)
	}
	if _, _, err = db.ClaimNextJob(context.Background(), store.ClaimRequest{Worker: worker, SandboxID: box.ID, Now: clock(), LeaseDuration: time.Second}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("retry claim before backoff = %v", err)
	}
}

func TestWorkerHealthAndJobTimeoutControllers(t *testing.T) {
	db, now, worker, box, job := fixture(t)
	defer db.Close()
	worker.LastSeenAt = now.Add(-time.Minute)
	if err := db.UpsertWorker(context.Background(), worker); err != nil {
		t.Fatal(err)
	}
	health := controller.NewWorkerHealth(db, controller.WorkerHealthConfig{SuspectAfter: 10 * time.Second, DeadAfter: 30 * time.Second}, func() time.Time { return now })
	if err := health.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	gotWorker, err := db.GetWorker(context.Background(), worker.ID)
	if err != nil {
		t.Fatal(err)
	}
	if gotWorker.Health["status"] != "dead" {
		t.Fatalf("worker health = %q", gotWorker.Health["status"])
	}
	job.TimeoutAt = now.Add(time.Second)
	if _, err = db.CreateJob(context.Background(), domain.Job{ID: "timeout-job", IdempotencyKey: "timeout-key", State: domain.JobQueued, CreatedAt: now, UpdatedAt: now, TimeoutAt: job.TimeoutAt, RetryPolicy: domain.RetryPolicy{MaxAttempts: 1}}); err != nil {
		t.Fatal(err)
	}
	timeouts := controller.NewJobTimeouts(db, func() time.Time { return now.Add(2 * time.Second) })
	if err = timeouts.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	timed, err := db.GetJob(context.Background(), "timeout-job")
	if err != nil {
		t.Fatal(err)
	}
	if timed.State != domain.JobTimedOut || len(timed.Events) != 1 {
		t.Fatalf("timed out job = %#v", timed)
	}
	_ = box
}

func TestSandboxReconcilerAdoptsOwnedProviderOrphan(t *testing.T) {
	db, now, worker, _, _ := fixture(t)
	defer db.Close()
	var provider *fake.Provider
	registry, err := sandbox.NewRegistry(map[string]sandbox.ProviderConfig{"fake": {Type: "fake"}}, map[string]sandbox.Factory{"fake": func(map[string]string) (sandbox.Provider, error) { provider = fake.New(); return provider, nil }})
	if err != nil {
		t.Fatal(err)
	}
	item, err := provider.Create(context.Background(), sandbox.CreateSpec{WorkerID: worker.ID, ControlPlaneURL: "http://control", BootstrapToken: "token", Labels: map[string]string{"owner": "route"}})
	if err != nil {
		t.Fatal(err)
	}
	reconciler := controller.NewSandboxReconciler(db, registry, controller.ReconcileConfig{OwnerLabel: "owner", OwnerValue: "route", Orphans: controller.OrphanAdopt}, func() time.Time { return now })
	if err = reconciler.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := db.GetSandbox(context.Background(), item.ID)
	if err != nil || got.Provider != "fake" || got.ExternalID != item.ExternalID {
		t.Fatalf("adopted sandbox = %#v, %v", got, err)
	}
}

func TestQueueAutoscalerScalesCompatibleDemandWithoutOverlapDuplicates(t *testing.T) {
	db, err := sqlite.Open(filepath.Join(t.TempDir(), "autoscaler.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for i := range 5 {
		_, err = db.CreateJob(context.Background(), domain.Job{
			ID:             domain.JobID(fmt.Sprintf("job-%d", i)),
			IdempotencyKey: fmt.Sprintf("autoscale-%d", i),
			State:          domain.JobQueued,
			Constraints:    domain.RoutingConstraints{Architecture: domain.ArchitectureAMD64},
			RetryPolicy:    domain.RetryPolicy{MaxAttempts: 1},
			CreatedAt:      now,
			UpdatedAt:      now,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	provider := fake.New()
	registry, err := sandbox.NewRegistry(
		map[string]sandbox.ProviderConfig{"cheap-us": {Type: "fake"}},
		map[string]sandbox.Factory{"fake": func(map[string]string) (sandbox.Provider, error) { return provider, nil }},
	)
	if err != nil {
		t.Fatal(err)
	}
	var decisions []controller.ScaleDecision
	autoscaler := controller.NewQueueAutoscaler(db, registry, []controller.SandboxPool{{
		Name: "linux", Provider: "cheap-us",
		Create:            sandbox.CreateSpec{ControlPlaneURL: "http://control", BootstrapToken: "token"},
		Capabilities:      domain.Capabilities{Architecture: domain.ArchitectureAMD64},
		MinReplicas:       1,
		MaxReplicas:       2,
		WorkerConcurrency: 2,
	}}, func(decision controller.ScaleDecision) { decisions = append(decisions, decision) }, func() time.Time { return now })

	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- autoscaler.Run(context.Background())
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for runErr := range errs {
		if runErr != nil {
			t.Fatal(runErr)
		}
	}
	boxes, err := db.ListSandboxes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(boxes) != 2 {
		t.Fatalf("sandboxes = %d, want max-capped 2", len(boxes))
	}
	for _, box := range boxes {
		if box.Provider != "cheap-us" || box.Capabilities.Labels["smart-route.pool"] != "linux" {
			t.Fatalf("sandbox = %#v", box)
		}
	}
	if len(decisions) == 0 || decisions[0].Action != controller.ScaleProvision || decisions[0].Desired != 2 || decisions[0].Queued != 5 {
		t.Fatalf("decisions = %#v", decisions)
	}
}

func TestQueueAutoscalerCountsPendingSessionDemand(t *testing.T) {
	db, err := sqlite.Open(filepath.Join(t.TempDir(), "session-demand.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	if _, err = db.CreateSession(context.Background(), domain.Session{ID: "pending", Pool: "sessions", State: domain.SessionPending, CreatedAt: now, LastActivity: now}); err != nil {
		t.Fatal(err)
	}
	provider := fake.New()
	registry, err := sandbox.NewRegistry(map[string]sandbox.ProviderConfig{"fake": {Type: "fake"}}, map[string]sandbox.Factory{"fake": func(map[string]string) (sandbox.Provider, error) { return provider, nil }})
	if err != nil {
		t.Fatal(err)
	}
	var decision controller.ScaleDecision
	a := controller.NewQueueAutoscaler(db, registry, []controller.SandboxPool{{Name: "sessions", Provider: "fake", Create: sandbox.CreateSpec{ControlPlaneURL: "http://control", BootstrapToken: "token"}, MaxReplicas: 1, WorkerConcurrency: 1}}, func(v controller.ScaleDecision) { decision = v }, func() time.Time { return now })
	if err = a.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if decision.Queued != 1 || decision.Desired != 1 || decision.Action != controller.ScaleProvision {
		t.Fatalf("decision=%#v", decision)
	}
}

func TestQueueAutoscalerBacksOffOnProviderCapacityError(t *testing.T) {
	db, err := sqlite.Open(filepath.Join(t.TempDir(), "capacity.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for i := range 4 {
		_, err = db.CreateJob(context.Background(), domain.Job{ID: domain.JobID(fmt.Sprintf("capacity-%d", i)), IdempotencyKey: fmt.Sprintf("capacity-key-%d", i), State: domain.JobQueued, Constraints: domain.RoutingConstraints{Architecture: domain.ArchitectureAMD64}, RetryPolicy: domain.RetryPolicy{MaxAttempts: 1}, CreatedAt: at, UpdatedAt: at})
		if err != nil {
			t.Fatal(err)
		}
	}
	provider := fake.New()
	provider.SetCapacity(1)
	registry, err := sandbox.NewRegistry(map[string]sandbox.ProviderConfig{"limited": {Type: "fake"}}, map[string]sandbox.Factory{"fake": func(map[string]string) (sandbox.Provider, error) { return provider, nil }})
	if err != nil {
		t.Fatal(err)
	}
	var decisions []controller.ScaleDecision
	autoscaler := controller.NewQueueAutoscaler(db, registry, []controller.SandboxPool{{Name: "limited", Provider: "limited", Create: sandbox.CreateSpec{ControlPlaneURL: "http://control", BootstrapToken: "token"}, Capabilities: domain.Capabilities{Architecture: domain.ArchitectureAMD64}, MaxReplicas: 4, WorkerConcurrency: 1}}, func(d controller.ScaleDecision) { decisions = append(decisions, d) }, func() time.Time { return at })
	autoscaler.Limits = controller.AutoscalerLimits{MaxScaleUpPerRun: 2, ProvisioningConcurrency: 2, ProviderBackoffBase: time.Minute}
	if err = autoscaler.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = autoscaler.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if provider.CreateCalls() != 2 {
		t.Fatalf("create calls during backoff = %d, want 2", provider.CreateCalls())
	}
	_, capacityErr := provider.Create(context.Background(), sandbox.CreateSpec{WorkerID: "probe", ControlPlaneURL: "http://control", BootstrapToken: "token"})
	if !errors.Is(capacityErr, sandbox.ErrCapacity) {
		t.Fatalf("capacity error = %v", capacityErr)
	}
	if len(decisions) != 2 || decisions[1].Reason != "provider backoff circuit is open" {
		t.Fatalf("decisions = %#v", decisions)
	}
}

func TestQueueAutoscalerCountsSlowStartsAndCoalescesRuns(t *testing.T) {
	db, err := sqlite.Open(filepath.Join(t.TempDir(), "slow.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for i := range 2 {
		_, err = db.CreateJob(context.Background(), domain.Job{ID: domain.JobID(fmt.Sprintf("slow-%d", i)), IdempotencyKey: fmt.Sprintf("slow-key-%d", i), State: domain.JobQueued, Constraints: domain.RoutingConstraints{Architecture: domain.ArchitectureAMD64}, RetryPolicy: domain.RetryPolicy{MaxAttempts: 1}, CreatedAt: at, UpdatedAt: at})
		if err != nil {
			t.Fatal(err)
		}
	}
	provider := fake.New()
	provider.SetCreateDelay(50 * time.Millisecond)
	registry, err := sandbox.NewRegistry(map[string]sandbox.ProviderConfig{"slow": {Type: "fake"}}, map[string]sandbox.Factory{"fake": func(map[string]string) (sandbox.Provider, error) { return provider, nil }})
	if err != nil {
		t.Fatal(err)
	}
	autoscaler := controller.NewQueueAutoscaler(db, registry, []controller.SandboxPool{{Name: "slow", Provider: "slow", Create: sandbox.CreateSpec{ControlPlaneURL: "http://control", BootstrapToken: "token"}, Capabilities: domain.Capabilities{Architecture: domain.ArchitectureAMD64}, MaxReplicas: 2, WorkerConcurrency: 1}}, nil, func() time.Time { return at })
	autoscaler.Limits.ProvisioningConcurrency = 2
	done := make(chan error, 1)
	go func() { done <- autoscaler.Run(context.Background()) }()
	deadline := time.Now().Add(time.Second)
	for provider.CreateCalls() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if err = autoscaler.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if provider.CreateCalls() != 2 {
		t.Fatalf("slow create calls = %d, want 2", provider.CreateCalls())
	}
	boxes, err := db.ListSandboxes(context.Background())
	if err != nil || len(boxes) != 2 {
		t.Fatalf("sandboxes = %d, err = %v", len(boxes), err)
	}
}

func fixture(t *testing.T) (*sqlite.DB, time.Time, domain.Worker, domain.Sandbox, domain.Job) {
	t.Helper()
	db, err := sqlite.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	caps := domain.Capabilities{Architecture: domain.ArchitectureAMD64}
	worker := domain.Worker{ID: "worker", InstanceID: "instance", SandboxID: "sandbox", SandboxProvider: "fake", Capabilities: caps, MaxConcurrency: 1, AvailableSlots: 1, LastSeenAt: now}
	box := domain.Sandbox{ID: "sandbox", WorkerID: worker.ID, Provider: "fake", State: "ready", Capabilities: caps, CreatedAt: now, UpdatedAt: now}
	job := domain.Job{ID: "job", IdempotencyKey: "key", State: domain.JobQueued, Constraints: domain.RoutingConstraints{Architecture: domain.ArchitectureAMD64}, RetryPolicy: domain.RetryPolicy{MaxAttempts: 2, Backoff: time.Minute}, CreatedAt: now, UpdatedAt: now}
	if err = db.UpsertWorker(context.Background(), worker); err != nil {
		t.Fatal(err)
	}
	if err = db.UpsertSandbox(context.Background(), box); err != nil {
		t.Fatal(err)
	}
	if _, err = db.CreateJob(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	return db, now, worker, box, job
}
