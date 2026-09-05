package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethan/smart-route/internal/domain"
	"github.com/ethan/smart-route/internal/store"
)

func fixture(t *testing.T) (*DB, string, domain.Worker, domain.Sandbox, domain.Job) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "state.db")
	db, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	w := domain.Worker{ID: "worker-1", LastSeenAt: now, Capabilities: domain.Capabilities{Architecture: domain.ArchitectureAMD64, Labels: map[string]string{"pool": "main"}}}
	s := domain.Sandbox{ID: "sandbox-1", WorkerID: w.ID, State: "ready", CreatedAt: now, Capabilities: w.Capabilities}
	j := domain.Job{ID: "job-1", IdempotencyKey: "request-1", State: domain.JobQueued, CreatedAt: now, UpdatedAt: now, Constraints: domain.RoutingConstraints{Architecture: domain.ArchitectureAMD64, Labels: map[string]string{"pool": "main"}}, RetryPolicy: domain.RetryPolicy{MaxAttempts: 2}}
	if err = db.UpsertWorker(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	if err = db.UpsertSandbox(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if _, err = db.CreateJob(context.Background(), j); err != nil {
		t.Fatal(err)
	}
	return db, p, w, s, j
}

func TestPersistenceIdempotencyAndPrimaryRecords(t *testing.T) {
	db, p, w, s, j := fixture(t)
	ctx := context.Background()
	now := j.CreatedAt
	a, _, err := db.ClaimNextJob(ctx, store.ClaimRequest{Worker: w, SandboxID: s.ID, Now: now, LeaseDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	ev := domain.Event{ID: "event-1", Sequence: 1, Type: domain.EventAttemptTransition, OccurredAt: now.Add(time.Second)}
	if err = db.CompleteAttempt(ctx, store.Completion{AttemptID: a.ID, WorkerID: w.ID, AttemptState: domain.AttemptFailed, JobState: domain.JobFailed, At: ev.OccurredAt, Event: ev}); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	got, err := db.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != domain.JobFailed || len(got.Attempts) != 1 || len(got.Events) != 1 {
		t.Fatalf("incomplete reopened job: %#v", got)
	}
	if _, err = db.GetWorker(ctx, w.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = db.GetSandbox(ctx, s.ID); err != nil {
		t.Fatal(err)
	}
	duplicate := j
	duplicate.ID = "different"
	same, err := db.CreateJob(ctx, duplicate)
	if err != nil || same.ID != j.ID {
		t.Fatalf("idempotent create = %#v, %v", same, err)
	}
}

func TestDurableEventOrderDedupeAndResult(t *testing.T) {
	db, p, w, s, j := fixture(t)
	ctx := context.Background()
	a, _, err := db.ClaimNextJob(ctx, store.ClaimRequest{Worker: w, SandboxID: s.ID, Now: j.CreatedAt, LeaseDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	first := domain.Event{Type: domain.EventProgress, Data: map[string]string{"percent": "10"}, WorkerSequence: 1, IdempotencyKey: "a:1"}
	got, err := db.AppendAttemptEvent(ctx, a.ID, w.ID, first)
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := db.AppendAttemptEvent(ctx, a.ID, w.ID, first)
	if err != nil || duplicate.ID != got.ID || duplicate.Sequence != got.Sequence {
		t.Fatalf("dedupe = %#v, %v", duplicate, err)
	}
	if _, err = db.AppendAttemptEvent(ctx, a.ID, w.ID, domain.Event{Type: domain.EventLog, WorkerSequence: 2, IdempotencyKey: "a:2"}); err != nil {
		t.Fatal(err)
	}
	if err = db.SaveResult(ctx, a.ID, w.ID, domain.JobResult{StatusCode: 201, Data: []byte("done"), Metadata: map[string]string{"kind": "text"}, CreatedAt: j.CreatedAt}); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	events, err := db.ListEvents(ctx, j.ID, got.Sequence, 1)
	if err != nil || len(events) != 1 || events[0].Sequence != got.Sequence+1 {
		t.Fatalf("events = %#v, %v", events, err)
	}
	result, err := db.GetResult(ctx, j.ID)
	if err != nil || string(result.Data) != "done" || result.StatusCode != 201 {
		t.Fatalf("result = %#v, %v", result, err)
	}
}

func TestConcurrentClaimHasSingleWinner(t *testing.T) {
	db, _, w, s, j := fixture(t)
	defer db.Close()
	ctx := context.Background()
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _, err := db.ClaimNextJob(ctx, store.ClaimRequest{Worker: w, SandboxID: s.ID, Now: j.CreatedAt, LeaseDuration: time.Minute})
			results <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	wins := 0
	for err := range results {
		if err == nil {
			wins++
		} else if !errors.Is(err, store.ErrNotFound) {
			t.Fatal(err)
		}
	}
	if wins != 1 {
		t.Fatalf("claims succeeded = %d", wins)
	}
}

func TestClaimUsesSchedulerRanking(t *testing.T) {
	db, _, w, s, first := fixture(t)
	defer db.Close()
	first.Constraints.PreferredProvider = "other"
	if _, err := db.db.Exec(`UPDATE jobs SET constraints_json=? WHERE id=?`, mustJSON(t, first.Constraints), first.ID); err != nil {
		t.Fatal(err)
	}
	second := first
	second.ID, second.IdempotencyKey = "job-2", "request-2"
	second.Constraints.PreferredProvider = ""
	if _, err := db.CreateJob(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	w.SandboxProvider = "other"
	_, got, err := db.ClaimNextJob(context.Background(), store.ClaimRequest{Worker: w, SandboxID: s.ID, Now: first.CreatedAt, LeaseDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != first.ID {
		t.Fatalf("claimed %s, want scheduler-preferred %s", got.ID, first.ID)
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	s, err := enc(v)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestLeaseExpirationUsesIndex(t *testing.T) {
	db, _, w, s, j := fixture(t)
	defer db.Close()
	ctx := context.Background()
	a, _, err := db.ClaimNextJob(ctx, store.ClaimRequest{Worker: w, SandboxID: s.ID, Now: j.CreatedAt, LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	var plan string
	rows, err := db.db.QueryContext(ctx, `EXPLAIN QUERY PLAN SELECT attempt_id FROM leases WHERE expires_at<=? ORDER BY expires_at`, j.CreatedAt.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err = rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan += detail
	}
	if !strings.Contains(plan, "leases_expiration_idx") {
		t.Fatalf("expiration index not used: %s", plan)
	}
	rows.Close()
	ids, err := db.ExpireLeases(ctx, j.CreatedAt.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != a.ID {
		t.Fatalf("expired = %v", ids)
	}
}
