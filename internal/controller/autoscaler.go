package controller

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethan/smart-route/internal/domain"
	"github.com/ethan/smart-route/internal/sandbox"
	"github.com/ethan/smart-route/internal/store"
)

const poolLabel = "smart-route.pool"

// SandboxPool describes a homogeneous group of workers. Create is copied for
// every provision and is never persisted by the controller (in particular, its
// bootstrap token remains write-only).
type SandboxPool struct {
	Name               string
	Provider           string
	Create             sandbox.CreateSpec
	Capabilities       domain.Capabilities
	Labels             map[string]string
	MinReplicas        int
	MaxReplicas        int
	WorkerConcurrency  int
	IdleTTL            time.Duration
	StartupTimeout     time.Duration
	ScaleUpCooldown    time.Duration
	ScaleDownCooldown  time.Duration
	ScaleDownStabilize time.Duration
	Cost               *float64
	Region             string
}

// AutoscalerLimits bound control-plane and provider pressure. Zero values are
// unlimited except ProvisioningConcurrency, which defaults to one.
type AutoscalerLimits struct {
	MaxScaleUpPerRun        int
	ProvisioningConcurrency int
	MaxTotalSandboxes       int
	MaxSandboxesByProvider  map[string]int
	ProviderBackoffBase     time.Duration
	ProviderBackoffMax      time.Duration
}

type ScaleAction string

const (
	ScaleNone      ScaleAction = "none"
	ScaleProvision ScaleAction = "provision"
	ScaleDrain     ScaleAction = "drain"
	ScaleTimeout   ScaleAction = "startup_timeout"
)

// ScaleDecision is emitted for every pool on every successful observation.
// It is intentionally a value type so observers can safely retain it.
type ScaleDecision struct {
	Pool              string
	Provider          string
	Action            ScaleAction
	Reason            string
	Queued            int
	Unmet             int
	IdleSlots         int
	Busy              int
	Ready             int
	Starting          int
	Draining          int
	Current           int
	Desired           int
	Changed           int
	ProvisionFailures int
	ProvisionDuration time.Duration
	ObservedAt        time.Time
	Recovering        int
}

type DecisionObserver func(ScaleDecision)

type BootstrapTokenMinter interface {
	MintBootstrapToken(context.Context, domain.SandboxID, string, string, domain.Capabilities) (string, error)
}

type AutoscalerStore interface {
	ListQueuedJobs(context.Context, store.QueueQuery) ([]domain.Job, error)
	ListSandboxes(context.Context) ([]domain.Sandbox, error)
	ListWorkers(context.Context) ([]domain.Worker, error)
	GetSandbox(context.Context, domain.SandboxID) (domain.Sandbox, error)
	UpsertSandbox(context.Context, domain.Sandbox) error
	SetSandboxState(context.Context, domain.SandboxID, string, time.Time) error
	CreateBootstrapToken(context.Context, store.BootstrapToken) error
	RevokeSandboxCredentials(context.Context, domain.SandboxID) error
	ListSessions(context.Context, ...domain.SessionState) ([]domain.Session, error)
}

type poolState struct {
	lastUp, lastDown time.Time
	belowSince       time.Time
	failures         int
	retryAt          time.Time
	unhealthy        bool
}

// QueueAutoscaler converts compatible queued demand into bounded pool replica
// counts. Runs on the same instance are coalesced, including while Create is in
// flight, which prevents duplicate provisioning from overlapping ticks.
type QueueAutoscaler struct {
	Store             AutoscalerStore
	Providers         ProviderResolver
	Pools             []SandboxPool
	Limits            AutoscalerLimits
	Observe           DecisionObserver
	Clock             Clock
	BootstrapTokens   BootstrapTokenMinter
	BootstrapTokenTTL time.Duration
	gate              gate
	sequence          atomic.Uint64
	mu                sync.Mutex
	state             map[string]poolState
}

func NewQueueAutoscaler(s AutoscalerStore, providers ProviderResolver, pools []SandboxPool, observe DecisionObserver, clock Clock) *QueueAutoscaler {
	return &QueueAutoscaler{Store: s, Providers: providers, Pools: append([]SandboxPool(nil), pools...), Observe: observe, Clock: clock, state: make(map[string]poolState)}
}

func (c *QueueAutoscaler) Run(ctx context.Context) error { return c.gate.run(ctx, c.run) }

func (c *QueueAutoscaler) Start(ctx context.Context, interval time.Duration) error {
	return start(ctx, interval, c.Run)
}

func (c *QueueAutoscaler) run(ctx context.Context) error {
	if err := validatePools(c.Pools); err != nil {
		return err
	}
	at := now(c.Clock)
	jobs, err := c.Store.ListQueuedJobs(ctx, store.QueueQuery{Limit: 100000})
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
	sessions, err := c.Store.ListSessions(ctx, domain.SessionPending, domain.SessionActive, domain.SessionRecovering)
	if err != nil {
		return err
	}
	workerBySandbox := make(map[domain.SandboxID]domain.Worker, len(workers))
	for _, worker := range workers {
		if worker.SandboxID != "" {
			workerBySandbox[worker.SandboxID] = worker
		}
	}

	pools := append([]SandboxPool(nil), c.Pools...)
	sort.Slice(pools, func(i, j int) bool { return pools[i].Name < pools[j].Name })
	demand := make(map[string]int, len(pools))
	for _, job := range jobs {
		if job.SessionID != "" {
			continue
		}
		if pool, ok := selectPool(pools, job.Constraints); ok {
			demand[pool.Name]++
		}
	}
	for _, session := range sessions {
		if session.State == domain.SessionPending {
			demand[session.Pool]++
		}
	}
	totalCurrent := 0
	providerCurrent := make(map[string]int)
	for _, box := range boxes {
		if box.State != "terminated" && box.State != "missing" {
			totalCurrent++
			providerCurrent[box.Provider]++
		}
	}

	for _, pool := range pools {
		provider, err := c.Providers.Get(pool.Provider)
		if err != nil {
			return err
		}
		var owned []domain.Sandbox
		decision := ScaleDecision{Pool: pool.Name, Provider: pool.Provider, Action: ScaleNone, Queued: demand[pool.Name], ObservedAt: at}
		for _, session := range sessions {
			if session.Pool == pool.Name && session.State == domain.SessionRecovering {
				decision.Recovering++
			}
		}
		for _, box := range boxes {
			if box.Capabilities.Labels[poolLabel] != pool.Name {
				continue
			}
			owned = append(owned, box)
			switch box.State {
			case "creating":
				decision.Starting++
			case "draining", "terminating":
				decision.Draining++
			case "running", "ready":
				if worker, ok := workerBySandbox[box.ID]; ok && worker.Health["status"] != string(domain.WorkerDead) {
					decision.Ready++
				} else {
					decision.Starting++
				}
			}
		}
		decision.Current = decision.Ready + decision.Starting + decision.Draining
		remaining := decision.Queued
		ready := append([]domain.Sandbox(nil), owned...)
		sort.Slice(ready, func(i, j int) bool { return ready[i].ID < ready[j].ID })
		active := 0
		for _, box := range ready {
			worker, ok := workerBySandbox[box.ID]
			if !ok || (box.State != "running" && box.State != "ready") || worker.Health["status"] == string(domain.WorkerDead) {
				continue
			}
			slots := worker.AvailableSlots
			if worker.ReservedSessionID != "" {
				slots = 0
			}
			if slots < 0 {
				slots = 0
			}
			if slots > pool.WorkerConcurrency {
				slots = pool.WorkerConcurrency
			}
			decision.IdleSlots += slots
			if slots < pool.WorkerConcurrency || len(worker.ActiveAttempts) > 0 {
				active++
			}
			if remaining > 0 && slots > 0 {
				used := min(slots, remaining)
				remaining -= used
				if slots == pool.WorkerConcurrency && len(worker.ActiveAttempts) == 0 {
					active++
				}
			}
		}
		decision.Busy = active
		decision.Unmet = remaining
		needed := active + int(math.Ceil(float64(remaining)/float64(pool.WorkerConcurrency)))
		decision.Desired = clamp(needed, pool.MinReplicas, pool.MaxReplicas)

		for _, box := range owned {
			_, registered := workerBySandbox[box.ID]
			starting := box.State == "creating" || ((box.State == "running" || box.State == "ready") && !registered)
			if starting && pool.StartupTimeout > 0 && at.Sub(box.CreatedAt) >= pool.StartupTimeout {
				if err = c.Store.SetSandboxState(ctx, box.ID, "terminating", at); err != nil {
					return err
				}
				if err = provider.Terminate(ctx, box.ID); err != nil {
					return err
				}
				decision.Action, decision.Reason, decision.Changed = ScaleTimeout, "sandbox exceeded startup timeout", decision.Changed+1
				decision.Current--
				decision.Starting--
				totalCurrent--
				providerCurrent[pool.Provider]--
			}
		}

		state := c.poolState(pool.Name)
		if decision.Current < decision.Desired {
			if state.unhealthy {
				decision.Reason = "pool is unhealthy after permanent provider configuration failure"
			} else if at.Before(state.retryAt) {
				decision.Reason = "provider backoff circuit is open"
			} else if cooldownElapsed(at, state.lastUp, pool.ScaleUpCooldown) {
				count := decision.Desired - decision.Current
				count = positiveLimit(count, c.Limits.MaxScaleUpPerRun)
				count = capacityLimit(count, c.Limits.MaxTotalSandboxes, totalCurrent)
				count = capacityLimit(count, c.Limits.MaxSandboxesByProvider[pool.Provider], providerCurrent[pool.Provider])
				if count <= 0 {
					decision.Reason = "global or provider sandbox limit reached"
				} else {
					provisionStarted := time.Now()
					created, createErr := c.createMany(ctx, provider, pool, at, count)
					decision.ProvisionDuration = time.Since(provisionStarted)
					decision.ProvisionFailures = count - created
					decision.Changed += created
					totalCurrent += created
					providerCurrent[pool.Provider] += created
					if created > 0 {
						decision.Action, decision.Reason = ScaleProvision, "compatible queued demand exceeds available capacity"
						state.lastUp = at
						state.belowSince = time.Time{}
					}
					if createErr != nil {
						state.failures++
						if errors.Is(createErr, sandbox.ErrInvalid) || errors.Is(createErr, sandbox.ErrAuthentication) {
							state.unhealthy = true
							decision.Reason = "permanent provider authentication or configuration failure"
						} else {
							state.retryAt = at.Add(c.providerBackoff(pool.Name, state.failures))
							decision.Reason = "transient provider capacity failure; backoff circuit opened"
						}
					} else {
						state.failures, state.retryAt = 0, time.Time{}
					}
				}
			} else if decision.Reason == "" {
				decision.Reason = "scale-up cooldown is active"
			}
		} else if decision.Current > decision.Desired {
			if state.belowSince.IsZero() {
				state.belowSince = at
			}
			stable := pool.ScaleDownStabilize <= 0 || at.Sub(state.belowSince) >= pool.ScaleDownStabilize
			if stable && cooldownElapsed(at, state.lastDown, pool.ScaleDownCooldown) {
				count, drainErr := c.drain(ctx, owned, workerBySandbox, decision.Current-decision.Desired, pool.IdleTTL, at)
				if drainErr != nil {
					return drainErr
				}
				if count > 0 {
					decision.Action, decision.Reason, decision.Changed = ScaleDrain, "idle capacity exceeds stabilized desired replicas", decision.Changed+count
					state.lastDown = at
				} else if decision.Reason == "" {
					decision.Reason = "excess capacity is busy or has not reached idle TTL"
				}
			} else if decision.Reason == "" {
				decision.Reason = "scale-down stabilization or cooldown is active"
			}
		} else {
			state.belowSince = time.Time{}
			if decision.Reason == "" {
				decision.Reason = "capacity matches bounded compatible demand"
			}
		}
		c.setPoolState(pool.Name, state)
		if c.Observe != nil {
			c.Observe(decision)
		}
	}
	return nil
}

func (c *QueueAutoscaler) createMany(ctx context.Context, provider sandbox.Provider, pool SandboxPool, at time.Time, count int) (int, error) {
	concurrency := c.Limits.ProvisioningConcurrency
	if concurrency <= 0 {
		concurrency = 1
	}
	if concurrency > count {
		concurrency = count
	}
	jobs := make(chan struct{})
	results := make(chan error, count)
	var wg sync.WaitGroup
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobs {
				results <- c.create(ctx, provider, pool, at)
			}
		}()
	}
	for range count {
		jobs <- struct{}{}
	}
	close(jobs)
	wg.Wait()
	close(results)
	created := 0
	var first error
	for err := range results {
		if err == nil {
			created++
		} else if first == nil {
			first = err
		}
	}
	return created, first
}

func (c *QueueAutoscaler) providerBackoff(pool string, failures int) time.Duration {
	base := c.Limits.ProviderBackoffBase
	if base <= 0 {
		base = time.Second
	}
	maximum := c.Limits.ProviderBackoffMax
	if maximum <= 0 {
		maximum = time.Minute
	}
	delay := base
	for i := 1; i < failures && delay < maximum/2; i++ {
		delay *= 2
	}
	if delay > maximum {
		delay = maximum
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(pool))
	jitter := time.Duration(h.Sum32()%251) * delay / 1000
	if delay+jitter > maximum {
		return maximum
	}
	return delay + jitter
}

func (c *QueueAutoscaler) create(ctx context.Context, provider sandbox.Provider, pool SandboxPool, at time.Time) error {
	spec := pool.Create
	random := make([]byte, 12)
	if _, err := rand.Read(random); err != nil {
		return fmt.Errorf("pool %q sandbox identity: %w", pool.Name, err)
	}
	spec.SandboxID = domain.SandboxID(hex.EncodeToString(random))
	spec.WorkerID = domain.WorkerID(fmt.Sprintf("pool-%s-%d-%d", pool.Name, at.UnixNano(), c.sequence.Add(1)))
	spec.SandboxProvider = pool.Provider
	spec.Capabilities = poolCapabilities(pool)
	spec.Labels = copyLabels(pool.Labels)
	spec.Labels[poolLabel] = pool.Name
	if c.BootstrapTokens != nil {
		token, mintErr := c.BootstrapTokens.MintBootstrapToken(ctx, spec.SandboxID, pool.Provider, pool.Name, spec.Capabilities)
		if mintErr != nil {
			return fmt.Errorf("pool %q bootstrap credential: %w", pool.Name, mintErr)
		}
		spec.BootstrapToken = token
	} else {
		secret := make([]byte, 32)
		if _, err := rand.Read(secret); err != nil {
			return fmt.Errorf("pool %q bootstrap credential: %w", pool.Name, err)
		}
		spec.BootstrapToken = "wbt_" + hex.EncodeToString(secret)
		hash := sha256.Sum256([]byte(spec.BootstrapToken))
		capJSON, _ := json.Marshal(spec.Capabilities)
		capHash := sha256.Sum256(capJSON)
		ttl := c.BootstrapTokenTTL
		if ttl <= 0 {
			ttl = 5 * time.Minute
		}
		if err := c.Store.CreateBootstrapToken(ctx, store.BootstrapToken{TokenHash: hex.EncodeToString(hash[:]), SandboxID: spec.SandboxID, SandboxProvider: pool.Provider, Pool: pool.Name, CapabilityHash: hex.EncodeToString(capHash[:]), ExpiresAt: at.Add(ttl)}); err != nil {
			return fmt.Errorf("pool %q bootstrap credential: %w", pool.Name, err)
		}
	}
	item, err := provider.Create(ctx, spec)
	if err != nil {
		_ = c.Store.RevokeSandboxCredentials(ctx, spec.SandboxID)
		return fmt.Errorf("pool %q provision: %w", pool.Name, err)
	}
	record := fromProvider(pool.Provider, item, at)
	record.Capabilities = spec.Capabilities
	// A fast worker may register while provider.Create is still returning. Keep
	// the authoritative registered worker binding instead of overwriting it
	// with the provider's planned bootstrap identity.
	if existing, getErr := c.Store.GetSandbox(ctx, record.ID); getErr == nil && existing.WorkerID != "" && existing.WorkerID != spec.WorkerID {
		record.WorkerID = existing.WorkerID
		if existing.State == "ready" {
			record.State = existing.State
		}
		if existing.UpdatedAt.After(record.UpdatedAt) {
			record.UpdatedAt = existing.UpdatedAt
		}
	} else if getErr != nil && !errors.Is(getErr, store.ErrNotFound) {
		_ = provider.Terminate(ctx, item.ID)
		_ = c.Store.RevokeSandboxCredentials(ctx, spec.SandboxID)
		return fmt.Errorf("pool %q read registered sandbox: %w", pool.Name, getErr)
	}
	if record.State == "" {
		record.State = "creating"
	}
	if err = c.Store.UpsertSandbox(ctx, record); err != nil {
		// Avoid leaking untracked cloud capacity when persistence fails.
		_ = provider.Terminate(ctx, item.ID)
		_ = c.Store.RevokeSandboxCredentials(ctx, spec.SandboxID)
		return fmt.Errorf("pool %q persist sandbox: %w", pool.Name, err)
	}
	return nil
}

func (c *QueueAutoscaler) drain(ctx context.Context, boxes []domain.Sandbox, workers map[domain.SandboxID]domain.Worker, wanted int, idleTTL time.Duration, at time.Time) (int, error) {
	sort.Slice(boxes, func(i, j int) bool {
		if boxes[i].UpdatedAt.Equal(boxes[j].UpdatedAt) {
			return boxes[i].ID < boxes[j].ID
		}
		return boxes[i].UpdatedAt.Before(boxes[j].UpdatedAt)
	})
	drained := 0
	for _, box := range boxes {
		if drained == wanted {
			break
		}
		if box.State != "running" && box.State != "ready" {
			continue
		}
		worker, registered := workers[box.ID]
		if box.ReservedSessionID != "" || (registered && (worker.ReservedSessionID != "" || len(worker.ActiveAttempts) != 0 || worker.AvailableSlots < worker.MaxConcurrency)) {
			continue
		}
		if idleTTL > 0 && at.Sub(box.UpdatedAt) < idleTTL {
			continue
		}
		if err := c.Store.SetSandboxState(ctx, box.ID, "draining", at); err != nil {
			return drained, err
		}
		drained++
	}
	return drained, nil
}

func selectPool(pools []SandboxPool, constraints domain.RoutingConstraints) (SandboxPool, bool) {
	compatible := make([]SandboxPool, 0, len(pools))
	for _, pool := range pools {
		withinBudget := constraints.MaxCost == nil || (pool.Cost != nil && *pool.Cost <= *constraints.MaxCost)
		preferAvailable := constraints.PreferredProvider != "" && hasProvider(pools, constraints, constraints.PreferredProvider)
		if poolCapabilities(pool).Satisfies(constraints) && withinBudget && (!preferAvailable || pool.Provider == constraints.PreferredProvider) {
			compatible = append(compatible, pool)
		}
	}
	if len(compatible) == 0 {
		return SandboxPool{}, false
	}
	sort.SliceStable(compatible, func(i, j int) bool {
		a, b := compatible[i], compatible[j]
		if constraints.PreferredProvider != "" && (a.Provider == constraints.PreferredProvider) != (b.Provider == constraints.PreferredProvider) {
			return a.Provider == constraints.PreferredProvider
		}
		if constraints.PreferredRegion != "" && (poolRegion(a) == constraints.PreferredRegion) != (poolRegion(b) == constraints.PreferredRegion) {
			return poolRegion(a) == constraints.PreferredRegion
		}
		if a.Cost != nil && b.Cost != nil && *a.Cost != *b.Cost {
			return *a.Cost < *b.Cost
		}
		if a.Cost != nil && b.Cost == nil {
			return true
		}
		return a.Name < b.Name
	})
	return compatible[0], true
}

func hasProvider(pools []SandboxPool, constraints domain.RoutingConstraints, provider string) bool {
	for _, pool := range pools {
		withinBudget := constraints.MaxCost == nil || (pool.Cost != nil && *pool.Cost <= *constraints.MaxCost)
		if pool.Provider == provider && poolCapabilities(pool).Satisfies(constraints) && withinBudget {
			return true
		}
	}
	return false
}
func poolRegion(pool SandboxPool) string {
	if pool.Region != "" {
		return pool.Region
	}
	return pool.Capabilities.Region
}
func poolCapabilities(pool SandboxPool) domain.Capabilities {
	caps := pool.Capabilities
	caps.Labels = copyLabels(caps.Labels)
	for k, v := range pool.Labels {
		caps.Labels[k] = v
	}
	caps.Labels[poolLabel] = pool.Name
	if caps.Region == "" {
		caps.Region = pool.Region
	}
	return caps
}
func copyLabels(source map[string]string) map[string]string {
	out := make(map[string]string, len(source)+1)
	for k, v := range source {
		out[k] = v
	}
	return out
}
func cooldownElapsed(at, previous time.Time, cooldown time.Duration) bool {
	return previous.IsZero() || cooldown <= 0 || at.Sub(previous) >= cooldown
}
func clamp(value, low, high int) int {
	if value < low {
		return low
	}
	if value > high {
		return high
	}
	return value
}
func positiveLimit(value, limit int) int {
	if limit > 0 && value > limit {
		return limit
	}
	return value
}
func capacityLimit(value, limit, current int) int {
	if limit <= 0 {
		return value
	}
	remaining := limit - current
	if remaining < 0 {
		remaining = 0
	}
	return min(value, remaining)
}
func validatePools(pools []SandboxPool) error {
	seen := make(map[string]bool, len(pools))
	for _, pool := range pools {
		if pool.Name == "" || pool.Provider == "" || pool.WorkerConcurrency <= 0 || pool.MinReplicas < 0 || pool.MaxReplicas < pool.MinReplicas || pool.Create.ControlPlaneURL == "" {
			return fmt.Errorf("invalid sandbox pool %q", pool.Name)
		}
		if seen[pool.Name] {
			return fmt.Errorf("duplicate sandbox pool %q", pool.Name)
		}
		seen[pool.Name] = true
	}
	return nil
}
func (c *QueueAutoscaler) poolState(name string) poolState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state[name]
}
func (c *QueueAutoscaler) setPoolState(name string, state poolState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state[name] = state
}
