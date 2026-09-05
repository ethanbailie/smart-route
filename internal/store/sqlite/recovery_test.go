package sqlite

import (
	"context"
	"errors"
	"github.com/ethan/smart-route/internal/domain"
	"github.com/ethan/smart-route/internal/store"
	"path/filepath"
	"testing"
	"time"
)

func TestRecoveryEpochCheckpointFallbackAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recovery.db")
	db, e := Open(path)
	if e != nil {
		t.Fatal(e)
	}
	ctx := context.Background()
	at := time.Now().UTC()
	s, e := db.CreateSession(ctx, domain.Session{ID: "s", Pool: "p", RecoveryPolicy: domain.RecoveryCheckpoint, CheckpointMode: domain.CheckpointExplicit, CreatedAt: at, LastActivity: at})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = db.CreateSession(ctx, domain.Session{ID: "unsafe", Pool: "p", RecoveryPolicy: domain.RecoveryRebuild, RebuildPlan: []domain.RebuildStep{{Kind: "command"}}, CreatedAt: at, LastActivity: at}); !errors.Is(e, store.ErrConflict) {
		t.Fatalf("unsafe rebuild=%v", e)
	}
	if e = db.UpsertWorker(ctx, domain.Worker{ID: "old", SandboxID: "oldbox", ReservedSessionID: s.ID, SessionEpoch: s.Epoch, LastSeenAt: at}); e != nil {
		t.Fatal(e)
	}
	if e = db.UpsertSandbox(ctx, domain.Sandbox{ID: "oldbox", WorkerID: "old", Provider: "fake", State: "ready", CreatedAt: at, UpdatedAt: at, ReservedSessionID: s.ID}); e != nil {
		t.Fatal(e)
	}
	if e = db.BindSession(ctx, s.ID, "old", "oldbox", at); e != nil {
		t.Fatal(e)
	}
	for _, cp := range []domain.Checkpoint{{ID: "good", SessionID: s.ID, Epoch: s.Epoch, Adapter: "filesystem", State: "creating", CreatedAt: at}, {ID: "newer-corrupt", SessionID: s.ID, Epoch: s.Epoch, Adapter: "filesystem", State: "corrupt", CreatedAt: at.Add(time.Second)}} {
		if e = db.CreateCheckpoint(ctx, cp); e != nil {
			t.Fatal(e)
		}
	}
	if e = db.CompleteCheckpoint(ctx, "good", s.ID, s.Epoch, "/checkpoint", "sum", 5, at); e != nil {
		t.Fatal(e)
	}
	if _, e = db.db.ExecContext(ctx, `CREATE TRIGGER reject_recovery_event BEFORE INSERT ON session_recovery_events BEGIN SELECT RAISE(ABORT,'event failure'); END`); e != nil {
		t.Fatal(e)
	}
	if e = db.RequestRecovery(ctx, s.ID, at); e == nil {
		t.Fatal("expected recovery event failure")
	}
	unchanged, e := db.GetSession(ctx, s.ID)
	if e != nil || unchanged.State != domain.SessionActive || unchanged.Epoch != 1 {
		t.Fatalf("event failure did not roll back recovery: %#v %v", unchanged, e)
	}
	if _, e = db.db.ExecContext(ctx, `DROP TRIGGER reject_recovery_event`); e != nil {
		t.Fatal(e)
	}
	if e = db.RequestRecovery(ctx, s.ID, at); e != nil {
		t.Fatal(e)
	}
	got, e := db.GetSession(ctx, s.ID)
	if e != nil || got.Epoch != 2 || got.State != domain.SessionRecovering {
		t.Fatalf("session=%#v %v", got, e)
	}
	if e = db.CompleteRecovery(ctx, s.ID, 1, "stale", "box", at); !errors.Is(e, store.ErrConflict) {
		t.Fatalf("stale completion=%v", e)
	}
	db.Close()
	db, e = Open(path)
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	ready, e := db.ListRecoveringSessions(ctx, at)
	if e != nil || len(ready) != 1 || ready[0].Epoch != 2 {
		t.Fatalf("restart=%#v %v", ready, e)
	}
	for _, cp := range []domain.Checkpoint{{ID: "partial", SessionID: s.ID, Epoch: 2, Adapter: "filesystem", State: "partial", CreatedAt: at, ExpiresAt: at.Add(time.Hour)}, {ID: "expired", SessionID: s.ID, Epoch: 2, Adapter: "filesystem", State: "creating", CreatedAt: at, ExpiresAt: at.Add(-time.Second)}} {
		if e = db.CreateCheckpoint(ctx, cp); e != nil {
			t.Fatal(e)
		}
	}
	removed, e := db.GarbageCollectCheckpoints(ctx, at, 1, false)
	if e != nil {
		t.Fatal(e)
	}
	ids := map[string]bool{}
	for _, cp := range removed {
		ids[cp.ID] = true
	}
	if !ids["partial"] || !ids["expired"] || !ids["newer-corrupt"] {
		t.Fatalf("gc=%#v", removed)
	}
	remaining, e := db.ListAllCheckpoints(ctx)
	if e != nil || len(remaining) != 1 || remaining[0].ID != "good" {
		t.Fatalf("retention=%#v %v", remaining, e)
	}
}

func TestCheckpointGCRetainsLatestAndDeletesOnClose(t *testing.T) {
	ctx := context.Background()
	at := time.Now().UTC()
	db, err := Open(filepath.Join(t.TempDir(), "retention.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	session, err := db.CreateSession(ctx, domain.Session{ID: "retained", Pool: "p", RecoveryPolicy: domain.RecoveryCheckpoint, CreatedAt: at, LastActivity: at})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"one", "two", "three"} {
		cp := domain.Checkpoint{ID: id, SessionID: session.ID, Epoch: session.Epoch, Adapter: "filesystem", CreatedAt: at, ExpiresAt: at.Add(-time.Second)}
		if err = db.CreateCheckpoint(ctx, cp); err != nil {
			t.Fatal(err)
		}
		if err = db.CompleteCheckpoint(ctx, id, session.ID, session.Epoch, "/"+id, id, 1, at); err != nil {
			t.Fatal(err)
		}
	}
	removed, err := db.GarbageCollectCheckpoints(ctx, at, 2, false)
	if err != nil || len(removed) != 1 || removed[0].ID != "one" {
		t.Fatalf("latest-N removal=%#v err=%v", removed, err)
	}
	current, err := db.GetSession(ctx, session.ID)
	if err != nil || current.LatestCheckpointID != "three" {
		t.Fatalf("latest pointer=%#v err=%v", current, err)
	}
	if err = db.CloseSession(ctx, session.ID, at); err != nil {
		t.Fatal(err)
	}
	removed, err = db.GarbageCollectCheckpoints(ctx, at, 2, true)
	if err != nil || len(removed) != 2 {
		t.Fatalf("close removal=%#v err=%v", removed, err)
	}
	current, err = db.GetSession(ctx, session.ID)
	if err != nil || current.LatestCheckpointID != "" {
		t.Fatalf("closed pointer=%#v err=%v", current, err)
	}
}
