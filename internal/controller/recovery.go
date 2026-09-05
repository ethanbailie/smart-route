package controller

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"time"

	"github.com/ethan/smart-route/internal/checkpoint"
	"github.com/ethan/smart-route/internal/domain"
	"github.com/ethan/smart-route/internal/sandbox"
	"github.com/ethan/smart-route/internal/store"
)

type RecoveryStore interface {
	ListRecoveringSessions(context.Context, time.Time) ([]domain.Session, error)
	ClaimRecovery(context.Context, domain.SessionID, time.Time) (domain.Session, error)
	CompleteRecovery(context.Context, domain.SessionID, uint64, domain.WorkerID, domain.SandboxID, time.Time) error
	FailRecovery(context.Context, domain.SessionID, uint64, string, time.Time, time.Time) error
	ListCheckpoints(context.Context, domain.SessionID) ([]domain.Checkpoint, error)
	MarkCheckpoint(context.Context, string, string, string) error
	ListSandboxes(context.Context) ([]domain.Sandbox, error)
	ListWorkers(context.Context) ([]domain.Worker, error)
	UpsertSandbox(context.Context, domain.Sandbox) error
	DeleteSandbox(context.Context, domain.SandboxID) error
	CreateJob(context.Context, domain.Job) (domain.Job, error)
	ListSessionJobs(context.Context, domain.SessionID) ([]domain.Job, error)
	AcknowledgeRecovery(context.Context, domain.SessionID, uint64, domain.WorkerID, time.Time) error
	TerminalRecovery(context.Context, domain.SessionID, uint64, string, time.Time) error
}
type RecoveryConfig struct {
	BackoffBase, BackoffMax time.Duration
	MaxAttempts             int
}
type CheckpointGCStore interface {
	GarbageCollectCheckpoints(context.Context, time.Time, int, bool) ([]domain.Checkpoint, error)
	ListAllCheckpoints(context.Context) ([]domain.Checkpoint, error)
}
type CheckpointGC struct {
	Store         CheckpointGCStore
	Adapters      map[string]checkpoint.Adapter
	Clock         Clock
	RetainLatest  int
	DeleteOnClose bool
	gate          gate
}

func (c *CheckpointGC) Run(ctx context.Context) error {
	return c.gate.run(ctx, func(ctx context.Context) error {
		items, e := c.Store.GarbageCollectCheckpoints(ctx, now(c.Clock), c.RetainLatest, c.DeleteOnClose)
		if e != nil {
			return e
		}
		for _, cp := range items {
			if a := c.Adapters[cp.Adapter]; a != nil {
				if e = a.Delete(ctx, cp); e != nil {
					return e
				}
			}
		}
		all, e := c.Store.ListAllCheckpoints(ctx)
		if e != nil {
			return e
		}
		live := map[string]bool{}
		for _, cp := range all {
			if cp.Location != "" {
				live[cp.Location] = true
			}
		}
		for _, a := range c.Adapters {
			if sweeper, ok := a.(interface {
				SweepOrphans(context.Context, map[string]bool, time.Time) error
			}); ok {
				if e = sweeper.SweepOrphans(ctx, live, now(c.Clock).Add(-time.Hour)); e != nil {
					return e
				}
			}
		}
		return nil
	})
}
func (c *CheckpointGC) Start(ctx context.Context, d time.Duration) error { return start(ctx, d, c.Run) }

type RecoveryController struct {
	Store       RecoveryStore
	Providers   ProviderResolver
	Pools       []SandboxPool
	Checkpoints map[string]checkpoint.Adapter
	Tokens      BootstrapTokenMinter
	Config      RecoveryConfig
	Clock       Clock
	gate        gate
}

func NewRecoveryController(s RecoveryStore, p ProviderResolver, pools []SandboxPool, c RecoveryConfig, clock Clock) *RecoveryController {
	return &RecoveryController{Store: s, Providers: p, Pools: pools, Config: c, Clock: clock, Checkpoints: map[string]checkpoint.Adapter{}}
}
func (c *RecoveryController) Start(ctx context.Context, d time.Duration) error {
	return start(ctx, d, c.Run)
}
func (c *RecoveryController) Run(ctx context.Context) error { return c.gate.run(ctx, c.run) }
func (c *RecoveryController) run(ctx context.Context) error {
	at := now(c.Clock)
	sessions, err := c.Store.ListRecoveringSessions(ctx, at)
	if err != nil {
		return err
	}
	boxes, err := c.Store.ListSandboxes(ctx)
	if err != nil {
		return err
	}
	workers, err := c.Store.ListWorkers(ctx)
	if err != nil {
		return err
	}
	for _, s := range sessions {
		// A restarted controller first adopts an already registered replacement.
		adopted := false
		for _, w := range workers {
			if w.ReservedSessionID == s.ID && w.SessionEpoch == s.Epoch {
				ready := s.RestoreAcknowledged
				if s.RecoveryPolicy == domain.RecoveryRebuild {
					ready, err = c.rebuildValidated(ctx, s, w, at)
					if err != nil {
						return err
					}
				}
				if !ready {
					adopted = true
					break
				}
				if err = c.Store.CompleteRecovery(ctx, s.ID, s.Epoch, w.ID, w.SandboxID, at); err != nil && !errors.Is(err, store.ErrConflict) {
					return err
				}
				adopted = true
				break
			}
		}
		if adopted {
			continue
		}
		if s.RecoveryState == domain.RecoveryProvisioning || s.RecoveryState == domain.RecoveryRestoring {
			var pending *domain.Sandbox
			for i := range boxes {
				if boxes[i].ReservedSessionID == s.ID {
					pending = &boxes[i]
					break
				}
			}
			pool, _ := recoveryPool(c.Pools, s.Pool)
			if pending == nil {
				if err := c.Store.FailRecovery(ctx, s.ID, s.Epoch, "replacement record missing", at.Add(c.Config.BackoffBase), at); err != nil {
					return err
				}
			} else if pool.StartupTimeout > 0 && at.Sub(pending.CreatedAt) >= pool.StartupTimeout {
				if p, e := c.Providers.Get(pending.Provider); e == nil {
					_ = p.Terminate(ctx, pending.ID)
				}
				if err := c.Store.DeleteSandbox(ctx, pending.ID); err != nil {
					return err
				}
				if err := c.fail(ctx, s, fmt.Errorf("replacement startup timeout"), at); err != nil {
					return err
				}
			}
			continue
		}
		claimed, e := c.Store.ClaimRecovery(ctx, s.ID, at)
		if e != nil {
			if errors.Is(e, store.ErrConflict) {
				continue
			}
			return e
		}
		s = claimed
		if c.Config.MaxAttempts > 0 && s.RecoveryAttempts > c.Config.MaxAttempts {
			for _, box := range boxes {
				if box.ReservedSessionID == s.ID {
					if p, x := c.Providers.Get(box.Provider); x == nil {
						_ = p.Terminate(ctx, box.ID)
					}
					_ = c.Store.DeleteSandbox(ctx, box.ID)
				}
			}
			if e = c.Store.TerminalRecovery(ctx, s.ID, s.Epoch, "recovery attempts exhausted", at); e != nil {
				return e
			}
			continue
		}
		pool, ok := recoveryPool(c.Pools, s.Pool)
		if !ok {
			return fmt.Errorf("recovery pool %q not configured", s.Pool)
		}
		provider, e := c.Providers.Get(pool.Provider)
		if e != nil {
			return e
		}
		spec := pool.Create
		spec.SandboxID = domain.SandboxID(fmt.Sprintf("recovery-%s-%d", s.ID, s.Epoch))
		spec.WorkerID = domain.WorkerID(fmt.Sprintf("recovery-worker-%s-%d", s.ID, s.Epoch))
		spec.SandboxProvider = pool.Provider
		spec.Capabilities = s.Capabilities
		if spec.Labels == nil {
			spec.Labels = map[string]string{}
		}
		spec.Labels[poolLabel] = pool.Name
		spec.Labels["smart-route.session"] = string(s.ID)
		spec.Labels["smart-route.epoch"] = fmt.Sprint(s.Epoch)
		if c.Tokens != nil {
			spec.BootstrapToken, e = c.Tokens.MintBootstrapToken(ctx, spec.SandboxID, pool.Provider, pool.Name, s.Capabilities)
			if e != nil {
				if failErr := c.fail(ctx, s, e, at); failErr != nil {
					return failErr
				}
				continue
			}
		}
		var created sandbox.Sandbox
		if s.RecoveryPolicy == domain.RecoveryCheckpoint {
			var cp domain.Checkpoint
			var adapter checkpoint.Adapter
			cp, adapter, e = c.usableCheckpoint(ctx, s)
			if e != nil {
				if failErr := c.fail(ctx, s, e, at); failErr != nil {
					return failErr
				}
				continue
			}
			var r io.ReadCloser
			r, e = adapter.Open(ctx, cp)
			if e != nil {
				if failErr := c.fail(ctx, s, e, at); failErr != nil {
					return failErr
				}
				continue
			}
			strategy := checkpoint.StrategyApplication
			if strategic, ok := adapter.(checkpoint.Strategic); ok {
				strategy = strategic.Strategy()
			}
			if snap, ok := provider.(sandbox.Snapshotter); ok && strategy == checkpoint.StrategyProviderSnapshot {
				created, e = snap.RestoreSnapshot(ctx, spec, r)
			} else {
				created, e = provider.Create(ctx, spec)
			}
			r.Close()
		} else if s.RecoveryPolicy == domain.RecoveryRebuild {
			created, e = provider.Create(ctx, spec)
		} else {
			e = fmt.Errorf("recovery disabled")
		}
		if e != nil {
			if failErr := c.fail(ctx, s, e, at); failErr != nil {
				return failErr
			}
			continue
		}
		if e = c.Store.UpsertSandbox(ctx, domain.Sandbox{ID: created.ID, Provider: created.Provider, ExternalID: created.ExternalID, Capabilities: created.Capabilities, State: string(created.State), CreatedAt: created.CreatedAt, UpdatedAt: at, ReservedSessionID: s.ID}); e != nil {
			return e
		}
		_ = boxes
	}
	return nil
}
func (c *RecoveryController) rebuildValidated(ctx context.Context, s domain.Session, w domain.Worker, at time.Time) (bool, error) {
	if err := c.enqueueRebuild(ctx, s, at); err != nil {
		return false, err
	}
	jobs, err := c.Store.ListSessionJobs(ctx, s.ID)
	if err != nil {
		return false, err
	}
	complete := true
	for _, j := range jobs {
		if !strings.HasPrefix(string(j.ID), fmt.Sprintf("rebuild-%s-%d-", s.ID, s.Epoch)) {
			continue
		}
		if j.State.Terminal() && j.State != domain.JobSucceeded {
			if err = c.Store.TerminalRecovery(ctx, s.ID, s.Epoch, "rebuild validation failed", at); err != nil {
				return false, err
			}
			return false, nil
		}
		if j.State != domain.JobSucceeded {
			complete = false
		}
	}
	if !complete {
		return false, nil
	}
	if !s.RestoreAcknowledged {
		if err = c.Store.AcknowledgeRecovery(ctx, s.ID, s.Epoch, w.ID, at); err != nil {
			return false, err
		}
	}
	return true, nil
}
func (c *RecoveryController) enqueueRebuild(ctx context.Context, s domain.Session, at time.Time) error {
	var previous domain.JobID
	for i, step := range s.RebuildPlan {
		id := domain.JobID(fmt.Sprintf("rebuild-%s-%d-%d", s.ID, s.Epoch, i))
		deps := []domain.JobID{}
		if previous != "" {
			deps = []domain.JobID{previous}
		}
		_, e := c.Store.CreateJob(ctx, domain.Job{ID: id, IdempotencyKey: fmt.Sprintf("rebuild:%s:%d:%s", s.ID, s.Epoch, step.IdempotencyKey), Kind: step.Kind, Payload: step.Payload, State: domain.JobQueued, SessionID: s.ID, DependsOn: deps, RetryPolicy: domain.RetryPolicy{MaxAttempts: 1}, CreatedAt: at, UpdatedAt: at})
		if e != nil && !errors.Is(e, store.ErrConflict) {
			return e
		}
		previous = id
	}
	return nil
}
func (c *RecoveryController) fail(ctx context.Context, s domain.Session, e error, at time.Time) error {
	base := c.Config.BackoffBase
	if base <= 0 {
		base = time.Second
	}
	ceiling := c.Config.BackoffMax
	if ceiling <= 0 {
		ceiling = time.Minute
	}
	d := time.Duration(math.Pow(2, float64(max(0, s.RecoveryAttempts-1)))) * base
	if d > ceiling {
		d = ceiling
	}
	return c.Store.FailRecovery(ctx, s.ID, s.Epoch, e.Error(), at.Add(d), at)
}
func (c *RecoveryController) usableCheckpoint(ctx context.Context, s domain.Session) (domain.Checkpoint, checkpoint.Adapter, error) {
	items, e := c.Store.ListCheckpoints(ctx, s.ID)
	if e != nil {
		return domain.Checkpoint{}, nil, e
	}
	for _, cp := range items {
		if cp.State != "ready" {
			continue
		}
		a := c.Checkpoints[cp.Adapter]
		if a == nil {
			continue
		}
		r, e := a.Open(ctx, cp)
		if e == nil {
			r.Close()
			return cp, a, nil
		}
		if !errors.Is(e, checkpoint.ErrCorrupt) {
			return domain.Checkpoint{}, nil, e
		}
		_ = c.Store.MarkCheckpoint(ctx, cp.ID, "corrupt", e.Error())
	}
	return domain.Checkpoint{}, nil, fmt.Errorf("no usable checkpoint")
}
func recoveryPool(pools []SandboxPool, name string) (SandboxPool, bool) {
	for _, p := range pools {
		if p.Name == name {
			return p, true
		}
	}
	return SandboxPool{}, false
}
