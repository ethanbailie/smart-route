package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethan/smart-route/internal/domain"
	"github.com/ethan/smart-route/internal/store"
)

func TestSessionAffinityDependenciesAndLoss(t *testing.T) {
	ctx := context.Background()
	db, err := Open(filepath.Join(t.TempDir(), "sessions.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Now().UTC().Truncate(time.Second)
	caps := domain.Capabilities{Capabilities: []string{"git"}, Labels: map[string]string{"smart-route.pool": "agent"}}
	w1 := domain.Worker{ID: "w1", SandboxID: "s1", SandboxProvider: "fake", Capabilities: caps, MaxConcurrency: 1, AvailableSlots: 1, LastSeenAt: now}
	w2 := domain.Worker{ID: "w2", SandboxID: "s2", SandboxProvider: "fake", Capabilities: caps, MaxConcurrency: 1, AvailableSlots: 1, LastSeenAt: now}
	for _, w := range []domain.Worker{w1, w2} {
		if err = db.UpsertWorker(ctx, w); err != nil {
			t.Fatal(err)
		}
		if err = db.UpsertSandbox(ctx, domain.Sandbox{ID: w.SandboxID, WorkerID: w.ID, Provider: "fake", State: "ready", Capabilities: caps, CreatedAt: now, UpdatedAt: now}); err != nil {
			t.Fatal(err)
		}
	}
	session, err := db.CreateSession(ctx, domain.Session{ID: "session", Pool: "agent", Capabilities: caps, IdleTTL: time.Minute, MaxLifetime: time.Hour, CreatedAt: now, LastActivity: now})
	if err != nil {
		t.Fatal(err)
	}
	job := func(id string, deps ...domain.JobID) domain.Job {
		return domain.Job{ID: domain.JobID(id), IdempotencyKey: id, State: domain.JobQueued, SessionID: session.ID, DependsOn: deps, Constraints: domain.RoutingConstraints{Capabilities: []string{"git"}, Labels: caps.Labels}, RetryPolicy: domain.RetryPolicy{MaxAttempts: 1}, CreatedAt: now, UpdatedAt: now}
	}
	if _, err = db.CreateJob(ctx, job("cycle", "cycle")); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("self-cycle error=%v", err)
	}
	if _, err = db.CreateJob(ctx, job("a")); err != nil {
		t.Fatal(err)
	}
	if _, err = db.CreateJob(ctx, job("b", "a")); err != nil {
		t.Fatal(err)
	}
	if _, err = db.CreateJob(ctx, job("c", "b")); err != nil {
		t.Fatal(err)
	}
	a, got, err := db.ClaimNextJob(ctx, store.ClaimRequest{Worker: w1, SandboxID: "s1", Now: now, LeaseDuration: time.Minute, Capacity: 1})
	if err != nil || got.ID != "a" {
		t.Fatalf("first claim=%s %v", got.ID, err)
	}
	if ids, err := db.ExpireSessions(ctx, now.Add(2*time.Minute)); err != nil || len(ids) != 0 {
		t.Fatalf("active session expired: %v %v", ids, err)
	}
	if _, _, err = db.ClaimNextJob(ctx, store.ClaimRequest{Worker: w2, SandboxID: "s2", Now: now, LeaseDuration: time.Minute, Capacity: 1}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("wrong-worker claim: %v", err)
	}
	if err = db.CompleteAttempt(ctx, store.Completion{AttemptID: a.ID, WorkerID: w1.ID, AttemptState: domain.AttemptSucceeded, JobState: domain.JobSucceeded, At: now.Add(time.Second), Event: domain.Event{Type: domain.EventAttemptTransition}}); err != nil {
		t.Fatal(err)
	}
	w1.AvailableSlots = 1
	b, got, err := db.ClaimNextJob(ctx, store.ClaimRequest{Worker: w1, SandboxID: "s1", Now: now.Add(2 * time.Second), LeaseDuration: time.Minute, Capacity: 1})
	if err != nil || got.ID != "b" {
		t.Fatalf("second claim=%s %v", got.ID, err)
	}
	if err = db.CompleteAttempt(ctx, store.Completion{AttemptID: b.ID, WorkerID: w1.ID, AttemptState: domain.AttemptFailed, JobState: domain.JobFailed, Failure: &domain.Failure{Code: "boom", Class: domain.FailureNonRetryable}, At: now.Add(3 * time.Second), Event: domain.Event{Type: domain.EventAttemptTransition}}); err != nil {
		t.Fatal(err)
	}
	c, err := db.GetJob(ctx, "c")
	if err != nil || c.State != domain.JobDependencyFailed {
		t.Fatalf("downstream=%s %v", c.State, err)
	}
	if len(c.Events) != 1 || c.Events[0].Data["reason"] != "dependency_failed" {
		t.Fatalf("dependency terminal events=%#v", c.Events)
	}
	if err = db.CancelJob(ctx, c.ID, now.Add(4*time.Second)); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("terminal cancel=%v", err)
	}
	if _, err = db.db.Exec(`UPDATE jobs SET timeout_at=? WHERE id=?`, now, c.ID); err != nil {
		t.Fatal(err)
	}
	if ids, err := db.TimeoutJobs(ctx, now.Add(time.Hour)); err != nil || len(ids) != 0 {
		t.Fatalf("terminal timeout mutation=%v %v", ids, err)
	}
	if _, err = db.CreateJob(ctx, job("d")); err != nil {
		t.Fatal(err)
	}
	if err = db.SetWorkerHealth(ctx, w1.ID, domain.WorkerDead, now.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	session, err = db.GetSession(ctx, session.ID)
	if err != nil || session.State != domain.SessionLost || session.Failure == nil || session.Failure.Code != "session_lost" {
		t.Fatalf("lost session=%#v %v", session, err)
	}
	after, err := db.GetJob(ctx, "d")
	if err != nil {
		t.Fatal(err)
	}
	if after.State != domain.JobSessionLost || len(after.Events) != 1 || after.Events[0].Data["reason"] != "session_lost" {
		t.Fatalf("session lost job=%#v", after)
	}
}

func TestSessionExpiryReleasesReservation(t *testing.T) {
	ctx := context.Background()
	db, err := Open(filepath.Join(t.TempDir(), "expiry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Now().UTC().Truncate(time.Second)
	w := domain.Worker{ID: "w", SandboxID: "s", Capabilities: domain.Capabilities{}, MaxConcurrency: 1, AvailableSlots: 1, LastSeenAt: now}
	_ = db.UpsertWorker(ctx, w)
	_ = db.UpsertSandbox(ctx, domain.Sandbox{ID: "s", WorkerID: "w", State: "ready", CreatedAt: now, UpdatedAt: now})
	v, _ := db.CreateSession(ctx, domain.Session{ID: "x", Pool: "p", IdleTTL: time.Second, CreatedAt: now, LastActivity: now})
	if err = db.BindSession(ctx, v.ID, w.ID, w.SandboxID, now); err != nil {
		t.Fatal(err)
	}
	ids, err := db.ExpireSessions(ctx, now.Add(2*time.Second))
	if err != nil || len(ids) != 1 {
		t.Fatalf("expired=%v %v", ids, err)
	}
	got, _ := db.GetWorker(ctx, w.ID)
	box, _ := db.GetSandbox(ctx, w.SandboxID)
	if got.ReservedSessionID != "" || box.ReservedSessionID != "" || box.State != "draining" {
		t.Fatalf("reservation not released: %#v %#v", got, box)
	}
}
