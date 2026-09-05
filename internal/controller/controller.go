// Package controller contains the control plane's periodic, provider-neutral
// maintenance loops. Each controller may be run on demand or started until its
// context is cancelled; concurrent runs of the same instance are coalesced.
package controller

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/ethan/smart-route/internal/domain"
	"github.com/ethan/smart-route/internal/sandbox"
)

type Clock func() time.Time

func now(clock Clock) time.Time {
	if clock == nil {
		return time.Now().UTC()
	}
	return clock().UTC()
}

type gate struct{ running atomic.Bool }

func (g *gate) run(ctx context.Context, fn func(context.Context) error) error {
	if !g.running.CompareAndSwap(false, true) {
		return nil
	}
	defer g.running.Store(false)
	if err := ctx.Err(); err != nil {
		return err
	}
	return fn(ctx)
}

// start runs immediately and then at interval until cancellation.
func start(ctx context.Context, interval time.Duration, run func(context.Context) error) error {
	if interval <= 0 {
		return fmt.Errorf("controller interval must be positive")
	}
	if err := run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if err := run(ctx); err != nil {
				return err
			}
		}
	}
}

type LeaseStore interface {
	ExpireLeases(context.Context, time.Time) ([]domain.AttemptID, error)
}
type LeaseReaper struct {
	Store   LeaseStore
	Clock   Clock
	Observe func(int)
	gate    gate
}

func NewLeaseReaper(s LeaseStore, clock Clock) *LeaseReaper {
	return &LeaseReaper{Store: s, Clock: clock}
}
func (c *LeaseReaper) Run(ctx context.Context) error {
	return c.gate.run(ctx, func(ctx context.Context) error {
		ids, err := c.Store.ExpireLeases(ctx, now(c.Clock))
		if err == nil && c.Observe != nil {
			c.Observe(len(ids))
		}
		return err
	})
}
func (c *LeaseReaper) Start(ctx context.Context, interval time.Duration) error {
	return start(ctx, interval, c.Run)
}

type TimeoutStore interface {
	TimeoutJobs(context.Context, time.Time) ([]domain.JobID, error)
}
type JobTimeouts struct {
	Store TimeoutStore
	Clock Clock
	gate  gate
}
type SessionExpiryStore interface {
	ExpireSessions(context.Context, time.Time) ([]domain.SessionID, error)
}
type SessionExpiry struct {
	Store SessionExpiryStore
	Clock Clock
	gate  gate
}

func NewSessionExpiry(s SessionExpiryStore, clock Clock) *SessionExpiry {
	return &SessionExpiry{Store: s, Clock: clock}
}
func (c *SessionExpiry) Run(ctx context.Context) error {
	return c.gate.run(ctx, func(ctx context.Context) error { _, err := c.Store.ExpireSessions(ctx, now(c.Clock)); return err })
}
func (c *SessionExpiry) Start(ctx context.Context, interval time.Duration) error {
	return start(ctx, interval, c.Run)
}

func NewJobTimeouts(s TimeoutStore, clock Clock) *JobTimeouts {
	return &JobTimeouts{Store: s, Clock: clock}
}
func (c *JobTimeouts) Run(ctx context.Context) error {
	return c.gate.run(ctx, func(ctx context.Context) error { _, err := c.Store.TimeoutJobs(ctx, now(c.Clock)); return err })
}
func (c *JobTimeouts) Start(ctx context.Context, interval time.Duration) error {
	return start(ctx, interval, c.Run)
}

type WorkerHealthStore interface {
	ListWorkers(context.Context) ([]domain.Worker, error)
	SetWorkerHealth(context.Context, domain.WorkerID, domain.WorkerHealth, time.Time) error
	ExpireWorkerLeases(context.Context, domain.WorkerID, time.Time) ([]domain.AttemptID, error)
}
type WorkerHealthConfig struct{ SuspectAfter, DeadAfter time.Duration }
type WorkerHealth struct {
	Store  WorkerHealthStore
	Config WorkerHealthConfig
	Clock  Clock
	gate   gate
}

func NewWorkerHealth(s WorkerHealthStore, c WorkerHealthConfig, clock Clock) *WorkerHealth {
	return &WorkerHealth{Store: s, Config: c, Clock: clock}
}
func (c *WorkerHealth) Run(ctx context.Context) error { return c.gate.run(ctx, c.run) }
func (c *WorkerHealth) run(ctx context.Context) error {
	if c.Config.SuspectAfter <= 0 || c.Config.DeadAfter < c.Config.SuspectAfter {
		return fmt.Errorf("invalid worker health thresholds")
	}
	at := now(c.Clock)
	workers, err := c.Store.ListWorkers(ctx)
	if err != nil {
		return err
	}
	for _, w := range workers {
		age := at.Sub(w.LastSeenAt)
		health := domain.WorkerHealthy
		if age >= c.Config.DeadAfter {
			health = domain.WorkerDead
		} else if age >= c.Config.SuspectAfter {
			health = domain.WorkerSuspect
		}
		if w.Health["status"] == string(health) {
			continue
		}
		if err = c.Store.SetWorkerHealth(ctx, w.ID, health, at); err != nil {
			return err
		}
		if health == domain.WorkerDead {
			if _, err = c.Store.ExpireWorkerLeases(ctx, w.ID, at); err != nil {
				return err
			}
		}
	}
	return nil
}
func (c *WorkerHealth) Start(ctx context.Context, interval time.Duration) error {
	return start(ctx, interval, c.Run)
}

type ProviderResolver interface {
	Get(string) (sandbox.Provider, error)
	Names() []string
}
type SandboxStore interface {
	ListSandboxes(context.Context) ([]domain.Sandbox, error)
	ListWorkers(context.Context) ([]domain.Worker, error)
	UpsertSandbox(context.Context, domain.Sandbox) error
	SetSandboxState(context.Context, domain.SandboxID, string, time.Time) error
	DeleteSandbox(context.Context, domain.SandboxID) error
}

type OrphanPolicy string

const (
	OrphanTerminate OrphanPolicy = "terminate"
	OrphanAdopt     OrphanPolicy = "adopt"
)

type ReconcileConfig struct {
	OwnerLabel, OwnerValue  string
	Orphans                 OrphanPolicy
	MaxLifetime, DrainGrace time.Duration
}
type SandboxReconciler struct {
	Store     SandboxStore
	Providers ProviderResolver
	Config    ReconcileConfig
	Clock     Clock
	gate      gate
}

func NewSandboxReconciler(s SandboxStore, p ProviderResolver, c ReconcileConfig, clock Clock) *SandboxReconciler {
	return &SandboxReconciler{Store: s, Providers: p, Config: c, Clock: clock}
}
func (c *SandboxReconciler) Run(ctx context.Context) error { return c.gate.run(ctx, c.run) }
func (c *SandboxReconciler) run(ctx context.Context) error {
	at := now(c.Clock)
	records, err := c.Store.ListSandboxes(ctx)
	if err != nil {
		return err
	}
	db := make(map[string]domain.Sandbox, len(records))
	for _, v := range records {
		db[v.Provider+"\x00"+string(v.ID)] = v
	}
	for _, name := range c.Providers.Names() {
		provider, err := c.Providers.Get(name)
		if err != nil {
			return err
		}
		items, err := provider.List(ctx, sandbox.Filter{})
		if err != nil {
			return err
		}
		seen := map[domain.SandboxID]bool{}
		for _, item := range items {
			seen[item.ID] = true
			key := name + "\x00" + string(item.ID)
			record, known := db[key]
			owned := c.Config.OwnerLabel == "" || item.Labels[c.Config.OwnerLabel] == c.Config.OwnerValue
			if !known {
				if !owned {
					continue
				}
				if c.Config.Orphans == OrphanAdopt {
					err = c.Store.UpsertSandbox(ctx, fromProvider(name, item, at))
				} else {
					err = provider.Terminate(ctx, item.ID)
				}
				if err != nil {
					return err
				}
				continue
			}
			if record.State == "terminating" {
				if err = provider.Terminate(ctx, item.ID); err != nil {
					return err
				}
				continue
			}
			if c.Config.MaxLifetime > 0 && at.Sub(item.CreatedAt) >= c.Config.MaxLifetime {
				if record.State != "draining" {
					if err = c.Store.SetSandboxState(ctx, item.ID, "draining", at); err != nil {
						return err
					}
				}
				if !record.DrainAt.IsZero() && at.Sub(record.DrainAt) >= c.Config.DrainGrace {
					if err = c.Store.SetSandboxState(ctx, item.ID, "terminating", at); err != nil {
						return err
					}
					if err = provider.Terminate(ctx, item.ID); err != nil {
						return err
					}
				}
				continue
			}
			record.State = string(item.State)
			record.UpdatedAt = at
			if err = c.Store.UpsertSandbox(ctx, record); err != nil {
				return err
			}
		}
		for _, record := range records {
			if record.Provider == name && !seen[record.ID] {
				if record.State == "terminating" || record.State == "terminated" {
					_ = c.Store.DeleteSandbox(ctx, record.ID)
				} else if err = c.Store.SetSandboxState(ctx, record.ID, "missing", at); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
func (c *SandboxReconciler) Start(ctx context.Context, interval time.Duration) error {
	return start(ctx, interval, c.Run)
}

func fromProvider(name string, v sandbox.Sandbox, at time.Time) domain.Sandbox {
	return domain.Sandbox{ID: v.ID, WorkerID: v.WorkerID, Provider: name, ExternalID: v.ExternalID, Capabilities: v.Capabilities, State: string(v.State), CreatedAt: v.CreatedAt, UpdatedAt: at}
}

type ReaperConfig struct {
	IdleAfter, DrainGrace time.Duration
	MinimumWarm           int
}
type SandboxReaper struct {
	Store     SandboxStore
	Providers ProviderResolver
	Config    ReaperConfig
	Clock     Clock
	gate      gate
}

func NewSandboxReaper(s SandboxStore, p ProviderResolver, c ReaperConfig, clock Clock) *SandboxReaper {
	return &SandboxReaper{Store: s, Providers: p, Config: c, Clock: clock}
}
func (c *SandboxReaper) Run(ctx context.Context) error { return c.gate.run(ctx, c.run) }
func (c *SandboxReaper) run(ctx context.Context) error {
	at := now(c.Clock)
	items, err := c.Store.ListSandboxes(ctx)
	if err != nil {
		return err
	}
	workers, err := c.Store.ListWorkers(ctx)
	if err != nil {
		return err
	}
	busy := map[domain.WorkerID]bool{}
	for _, worker := range workers {
		busy[worker.ID] = worker.ReservedSessionID != "" || len(worker.ActiveAttempts) > 0 || worker.AvailableSlots < worker.MaxConcurrency
	}
	sort.Slice(items, func(i, j int) bool { return items[i].CreatedAt.Before(items[j].CreatedAt) })
	warm := map[string]int{}
	for _, v := range items {
		if v.State == "running" || v.State == "ready" {
			warm[v.Provider]++
		}
	}
	for _, v := range items {
		provider, err := c.Providers.Get(v.Provider)
		if err != nil {
			return err
		}
		if v.State == "draining" && !v.DrainAt.IsZero() && at.Sub(v.DrainAt) >= c.Config.DrainGrace {
			if err = c.Store.SetSandboxState(ctx, v.ID, "terminating", at); err != nil {
				return err
			}
			if err = provider.Terminate(ctx, v.ID); err != nil {
				return err
			}
			continue
		}
		if busy[v.WorkerID] || (v.State != "running" && v.State != "ready") || c.Config.IdleAfter <= 0 || at.Sub(v.UpdatedAt) < c.Config.IdleAfter || warm[v.Provider] <= c.Config.MinimumWarm {
			continue
		}
		if err = c.Store.SetSandboxState(ctx, v.ID, "draining", at); err != nil {
			return err
		}
		warm[v.Provider]--
	}
	return nil
}
func (c *SandboxReaper) Start(ctx context.Context, interval time.Duration) error {
	return start(ctx, interval, c.Run)
}
