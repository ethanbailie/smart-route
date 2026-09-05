package controller_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethan/smart-route/internal/checkpoint"
	"github.com/ethan/smart-route/internal/controller"
	"github.com/ethan/smart-route/internal/domain"
	"github.com/ethan/smart-route/internal/sandbox"
	"github.com/ethan/smart-route/internal/store"
	"github.com/ethan/smart-route/internal/store/sqlite"
)

type recoveryProvider struct {
	restored string
	fail     bool
	items    map[domain.SandboxID]sandbox.Sandbox
	calls    int
}

func (p *recoveryProvider) Create(ctx context.Context, s sandbox.CreateSpec) (sandbox.Sandbox, error) {
	p.calls++
	if p.fail {
		return sandbox.Sandbox{}, sandbox.NewError("test", "create", sandbox.CodeUnavailable, io.ErrUnexpectedEOF)
	}
	return p.make(s), nil
}
func (p *recoveryProvider) RestoreSnapshot(ctx context.Context, s sandbox.CreateSpec, r io.Reader) (sandbox.Sandbox, error) {
	p.calls++
	if p.fail {
		return sandbox.Sandbox{}, sandbox.NewError("test", "restore", sandbox.CodeUnavailable, io.ErrUnexpectedEOF)
	}
	b, e := io.ReadAll(r)
	p.restored = string(b)
	return p.make(s), e
}
func (p *recoveryProvider) CreateSnapshot(context.Context, domain.SandboxID, io.Writer) error {
	return nil
}
func (p *recoveryProvider) make(s sandbox.CreateSpec) sandbox.Sandbox {
	v := sandbox.Sandbox{ID: s.SandboxID, Provider: "test", ExternalID: string(s.SandboxID), State: sandbox.StateRunning, Capabilities: s.Capabilities, Labels: s.Labels, CreatedAt: time.Now().UTC()}
	p.items[v.ID] = v
	return v
}
func (p *recoveryProvider) Get(_ context.Context, id domain.SandboxID) (sandbox.Sandbox, error) {
	return p.items[id], nil
}
func (p *recoveryProvider) List(context.Context, sandbox.Filter) ([]sandbox.Sandbox, error) {
	return nil, nil
}
func (p *recoveryProvider) Terminate(context.Context, domain.SandboxID) error { return nil }

type resolver struct{ p sandbox.Provider }

func (r resolver) Get(string) (sandbox.Provider, error) { return r.p, nil }
func (resolver) Names() []string                        { return []string{"test"} }

type tokens struct{}

func (tokens) MintBootstrapToken(context.Context, domain.SandboxID, string, string, domain.Capabilities) (string, error) {
	return "token", nil
}

func TestRecoveryControllerRestoresFallbackAndAdoptsOnce(t *testing.T) {
	ctx := context.Background()
	at := time.Now().UTC()
	db, e := sqlite.Open(filepath.Join(t.TempDir(), "r.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	fs := checkpoint.Filesystem{Root: t.TempDir()}
	ps := checkpoint.ProviderSnapshot{Backing: fs}
	s, e := db.CreateSession(ctx, domain.Session{ID: "s", Pool: "p", RecoveryPolicy: domain.RecoveryCheckpoint, CheckpointMode: domain.CheckpointExplicit, CreatedAt: at, LastActivity: at})
	if e != nil {
		t.Fatal(e)
	}
	if e = db.UpsertWorker(ctx, domain.Worker{ID: "old", SandboxID: "oldbox", LastSeenAt: at}); e != nil {
		t.Fatal(e)
	}
	if e = db.UpsertSandbox(ctx, domain.Sandbox{ID: "oldbox", WorkerID: "old", Provider: "test", State: "ready", CreatedAt: at, UpdatedAt: at}); e != nil {
		t.Fatal(e)
	}
	if e = db.BindSession(ctx, s.ID, "old", "oldbox", at); e != nil {
		t.Fatal(e)
	}
	for _, v := range []struct{ id, data string }{{"good", "state"}, {"newer", "bad"}} {
		cp := domain.Checkpoint{ID: v.id, SessionID: s.ID, Epoch: s.Epoch, Adapter: ps.Name(), CreatedAt: at}
		if e = db.CreateCheckpoint(ctx, cp); e != nil {
			t.Fatal(e)
		}
		loc, sum, n, e := ps.Save(ctx, cp, io.NopCloser(&reader{b: []byte(v.data)}))
		if e != nil {
			t.Fatal(e)
		}
		if e = db.CompleteCheckpoint(ctx, v.id, s.ID, s.Epoch, loc, sum, n, at); e != nil {
			t.Fatal(e)
		}
		if v.id == "newer" {
			if e = corrupt(loc); e != nil {
				t.Fatal(e)
			}
		}
	}
	if e = db.RequestRecovery(ctx, s.ID, at); e != nil {
		t.Fatal(e)
	}
	p := &recoveryProvider{items: map[domain.SandboxID]sandbox.Sandbox{}}
	c := controller.NewRecoveryController(db, resolver{p}, []controller.SandboxPool{{Name: "p", Provider: "test", Create: sandbox.CreateSpec{ControlPlaneURL: "http://control"}, WorkerConcurrency: 1, MaxReplicas: 1}}, controller.RecoveryConfig{BackoffBase: time.Second, BackoffMax: time.Minute, MaxAttempts: 3}, func() time.Time { return at })
	c.Checkpoints = map[string]checkpoint.Adapter{ps.Name(): ps}
	c.Tokens = tokens{}
	if e = c.Run(ctx); e != nil || p.restored != "state" {
		t.Fatalf("restore=%q %v", p.restored, e)
	}
	current, _ := db.GetSession(ctx, s.ID)
	box := domain.SandboxID("recovery-s-2")
	if e = db.UpsertWorker(ctx, domain.Worker{ID: "new", SandboxID: box, ReservedSessionID: s.ID, SessionEpoch: current.Epoch, LastSeenAt: at}); e != nil {
		t.Fatal(e)
	}
	if e = c.Run(ctx); e != nil {
		t.Fatal(e)
	}
	current, _ = db.GetSession(ctx, s.ID)
	if current.State != domain.SessionRecovering || current.RestoreAcknowledged {
		t.Fatalf("activated before restore acknowledgement: %#v", current)
	}
	if e = db.AcknowledgeRecovery(ctx, s.ID, current.Epoch, "new", at); e != nil {
		t.Fatal(e)
	}
	// A fresh controller instance adopts persisted provisioning work after restart.
	c = controller.NewRecoveryController(db, resolver{p}, c.Pools, c.Config, func() time.Time { return at })
	c.Checkpoints = map[string]checkpoint.Adapter{ps.Name(): ps}
	c.Tokens = tokens{}
	if e = c.Run(ctx); e != nil {
		t.Fatal(e)
	}
	current, _ = db.GetSession(ctx, s.ID)
	if current.State != domain.SessionActive || current.WorkerID != "new" {
		t.Fatalf("adopted=%#v", current)
	}
	// A normalized transient provider failure is durable and delayed by backoff.
	f, e := db.CreateSession(ctx, domain.Session{ID: "f", Pool: "p", RecoveryPolicy: domain.RecoveryRebuild, RebuildPlan: []domain.RebuildStep{{Kind: "command", Payload: []byte(`{"command":"true"}`), IdempotencyKey: "safe"}}, CreatedAt: at, LastActivity: at})
	if e != nil {
		t.Fatal(e)
	}
	if e = db.UpsertWorker(ctx, domain.Worker{ID: "fw", SandboxID: "fb", LastSeenAt: at}); e != nil {
		t.Fatal(e)
	}
	if e = db.UpsertSandbox(ctx, domain.Sandbox{ID: "fb", WorkerID: "fw", Provider: "test", State: "ready", CreatedAt: at, UpdatedAt: at}); e != nil {
		t.Fatal(e)
	}
	if e = db.BindSession(ctx, f.ID, "fw", "fb", at); e != nil {
		t.Fatal(e)
	}
	if e = db.RequestRecovery(ctx, f.ID, at); e != nil {
		t.Fatal(e)
	}
	p.fail = true
	if e = c.Run(ctx); e != nil {
		t.Fatal(e)
	}
	f, _ = db.GetSession(ctx, f.ID)
	if f.RecoveryState != domain.RecoveryFailed || !f.RecoveryAfter.After(at) {
		t.Fatalf("backoff=%#v", f)
	}
	calls := p.calls
	if e = c.Run(ctx); e != nil || p.calls != calls {
		t.Fatalf("backoff duplicated provision: calls=%d err=%v", p.calls, e)
	}
	p.fail = false
	c.Clock = func() time.Time { return at.Add(2 * time.Second) }
	if e = c.Run(ctx); e != nil {
		t.Fatal(e)
	}
	f, _ = db.GetSession(ctx, f.ID)
	replacement := domain.SandboxID("recovery-f-2")
	if e = db.UpsertWorker(ctx, domain.Worker{ID: "fresh", SandboxID: replacement, ReservedSessionID: f.ID, SessionEpoch: f.Epoch, MaxConcurrency: 1, AvailableSlots: 1, LastSeenAt: at}); e != nil {
		t.Fatal(e)
	}
	registeredBox, _ := db.GetSandbox(ctx, replacement)
	registeredBox.WorkerID = "fresh"
	registeredBox.State = "ready"
	if e = db.UpsertSandbox(ctx, registeredBox); e != nil {
		t.Fatal(e)
	}
	if e = c.Run(ctx); e != nil {
		t.Fatal(e)
	}
	queued, _ := db.ListSessionJobs(ctx, f.ID)
	if len(queued) != 1 || queued[0].State != domain.JobQueued {
		t.Fatalf("queued rebuild=%#v", queued)
	}
	fresh, _ := db.GetWorker(ctx, "fresh")
	attempt, _, e := db.ClaimNextJob(ctx, store.ClaimRequest{Worker: fresh, SandboxID: replacement, Now: at.Add(3 * time.Second), LeaseDuration: time.Minute})
	if e != nil {
		t.Fatal(e)
	}
	if e = db.CompleteAttempt(ctx, store.Completion{AttemptID: attempt.ID, WorkerID: fresh.ID, AttemptState: domain.AttemptSucceeded, JobState: domain.JobSucceeded, At: at.Add(4 * time.Second), Event: domain.Event{Type: domain.EventAttemptTransition}}); e != nil {
		t.Fatal(e)
	}
	if e = c.Run(ctx); e != nil {
		t.Fatal(e)
	}
	jobs, e := db.ListSessionJobs(ctx, f.ID)
	if e != nil || len(jobs) != 1 || jobs[0].IdempotencyKey != "rebuild:f:2:safe" {
		t.Fatalf("rebuild jobs=%#v %v", jobs, e)
	}
	// A failed rebuild validation is terminal and never activates the session.
	h, e := db.CreateSession(ctx, domain.Session{ID: "h", Pool: "p", RecoveryPolicy: domain.RecoveryRebuild, RebuildPlan: []domain.RebuildStep{{Kind: "command", Payload: []byte(`{"command":"false"}`), IdempotencyKey: "fails"}}, CreatedAt: at, LastActivity: at})
	if e != nil {
		t.Fatal(e)
	}
	if e = db.UpsertWorker(ctx, domain.Worker{ID: "hw", SandboxID: "hb", LastSeenAt: at}); e != nil {
		t.Fatal(e)
	}
	if e = db.UpsertSandbox(ctx, domain.Sandbox{ID: "hb", WorkerID: "hw", Provider: "test", State: "ready", CreatedAt: at, UpdatedAt: at}); e != nil {
		t.Fatal(e)
	}
	if e = db.BindSession(ctx, h.ID, "hw", "hb", at); e != nil {
		t.Fatal(e)
	}
	if e = db.RequestRecovery(ctx, h.ID, at); e != nil {
		t.Fatal(e)
	}
	if e = c.Run(ctx); e != nil {
		t.Fatal(e)
	}
	h, _ = db.GetSession(ctx, h.ID)
	hbox := domain.SandboxID("recovery-h-2")
	if e = db.UpsertWorker(ctx, domain.Worker{ID: "hnew", SandboxID: hbox, ReservedSessionID: h.ID, SessionEpoch: h.Epoch, MaxConcurrency: 1, AvailableSlots: 1, LastSeenAt: at}); e != nil {
		t.Fatal(e)
	}
	sandboxRecord, _ := db.GetSandbox(ctx, hbox)
	sandboxRecord.WorkerID, sandboxRecord.State = "hnew", "ready"
	if e = db.UpsertSandbox(ctx, sandboxRecord); e != nil {
		t.Fatal(e)
	}
	if e = c.Run(ctx); e != nil {
		t.Fatal(e)
	}
	hworker, _ := db.GetWorker(ctx, "hnew")
	hattempt, _, e := db.ClaimNextJob(ctx, store.ClaimRequest{Worker: hworker, SandboxID: hbox, Now: at.Add(3 * time.Second), LeaseDuration: time.Minute})
	if e != nil {
		t.Fatal(e)
	}
	if e = db.CompleteAttempt(ctx, store.Completion{AttemptID: hattempt.ID, WorkerID: hworker.ID, AttemptState: domain.AttemptFailed, JobState: domain.JobFailed, Failure: &domain.Failure{Code: "validation", Class: domain.FailureNonRetryable}, At: at.Add(4 * time.Second), Event: domain.Event{Type: domain.EventAttemptTransition}}); e != nil {
		t.Fatal(e)
	}
	if e = c.Run(ctx); e != nil {
		t.Fatal(e)
	}
	h, _ = db.GetSession(ctx, h.ID)
	if h.State != domain.SessionLost || h.Failure == nil || h.Failure.Code != "recovery_failed" {
		t.Fatalf("failed rebuild activated: %#v", h)
	}
	// Restore failure never activates the session and is retried durably.
	g, e := db.CreateSession(ctx, domain.Session{ID: "g", Pool: "p", RecoveryPolicy: domain.RecoveryCheckpoint, CreatedAt: at, LastActivity: at})
	if e != nil {
		t.Fatal(e)
	}
	if e = db.UpsertWorker(ctx, domain.Worker{ID: "gw", SandboxID: "gb", LastSeenAt: at}); e != nil {
		t.Fatal(e)
	}
	if e = db.UpsertSandbox(ctx, domain.Sandbox{ID: "gb", WorkerID: "gw", Provider: "test", State: "ready", CreatedAt: at, UpdatedAt: at}); e != nil {
		t.Fatal(e)
	}
	if e = db.BindSession(ctx, g.ID, "gw", "gb", at); e != nil {
		t.Fatal(e)
	}
	cp := domain.Checkpoint{ID: "gcp", SessionID: g.ID, Epoch: g.Epoch, Adapter: ps.Name(), CreatedAt: at}
	if e = db.CreateCheckpoint(ctx, cp); e != nil {
		t.Fatal(e)
	}
	loc, sum, n, e := ps.Save(ctx, cp, &reader{b: []byte("state")})
	if e != nil {
		t.Fatal(e)
	}
	if e = db.CompleteCheckpoint(ctx, cp.ID, g.ID, g.Epoch, loc, sum, n, at); e != nil {
		t.Fatal(e)
	}
	if e = db.RequestRecovery(ctx, g.ID, at); e != nil {
		t.Fatal(e)
	}
	p.fail = true
	c.Clock = func() time.Time { return at.Add(3 * time.Second) }
	if e = c.Run(ctx); e != nil {
		t.Fatal(e)
	}
	g, _ = db.GetSession(ctx, g.ID)
	if g.State != domain.SessionRecovering || g.RecoveryState != domain.RecoveryFailed {
		t.Fatalf("restore failure activated=%#v calls=%d", g, p.calls)
	}
	// Exhaustion is terminal rather than an indefinitely delayed retry.
	c.Config.MaxAttempts = 1
	c.Clock = func() time.Time { return at.Add(10 * time.Second) }
	if e = c.Run(ctx); e != nil {
		t.Fatal(e)
	}
	g, _ = db.GetSession(ctx, g.ID)
	if g.State != domain.SessionLost || g.Failure == nil || g.Failure.Code != "recovery_failed" {
		t.Fatalf("exhaustion was not terminal: %#v", g)
	}
	events, e := db.ListRecoveryEvents(ctx, g.ID)
	if e != nil {
		t.Fatal(e)
	}
	wantStages := map[string]bool{"requested": false, "provisioning": false, "retry_wait": false, "terminal_failed": false}
	for _, event := range events {
		if _, ok := wantStages[event.Stage]; ok {
			wantStages[event.Stage] = true
		}
	}
	for stage, seen := range wantStages {
		if !seen {
			t.Fatalf("missing recovery event %q in %#v", stage, events)
		}
	}
}

type reader struct{ b []byte }

func (r *reader) Read(p []byte) (int, error) {
	if len(r.b) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.b)
	r.b = r.b[n:]
	return n, nil
}
func corrupt(path string) error { return os.WriteFile(path, []byte("corrupt"), 0600) }
