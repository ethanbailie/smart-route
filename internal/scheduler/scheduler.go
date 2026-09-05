// Package scheduler contains deterministic, provider-neutral job placement policy.
package scheduler

import (
	"sort"
	"strings"
	"time"

	"github.com/ethan/smart-route/internal/domain"
)

type ReasonCode string

const (
	ReasonCapability       ReasonCode = "capability_mismatch"
	ReasonExecutor         ReasonCode = "executor_mismatch"
	ReasonLabels           ReasonCode = "label_mismatch"
	ReasonArchitecture     ReasonCode = "architecture_mismatch"
	ReasonRegion           ReasonCode = "region_mismatch"
	ReasonWorkerHealth     ReasonCode = "worker_unhealthy"
	ReasonConcurrency      ReasonCode = "concurrency_exhausted"
	ReasonSandbox          ReasonCode = "sandbox_unavailable"
	ReasonUpstream         ReasonCode = "upstream_unavailable"
	ReasonUpstreamCooldown ReasonCode = "upstream_cooldown"
	ReasonUpstreamBudget   ReasonCode = "upstream_budget_exhausted"
	ReasonCost             ReasonCode = "cost_exceeded"
	ReasonSelected         ReasonCode = "selected"
)

type Decision struct {
	JobID  domain.JobID
	Worker domain.WorkerID
	Reason ReasonCode
	Score  float64
	Chosen bool
}
type Observer interface{ ObserveSchedulingDecision(Decision) }
type ObserverFunc func(Decision)

func (f ObserverFunc) ObserveSchedulingDecision(d Decision) { f(d) }

type Weights struct{ ExactMatch, PreferredRegion, SandboxAffinity, ProviderAffinity, UpstreamHealth, WorkerLoad, QueueAge, Starvation, Cost float64 }
type Config struct {
	Weights         Weights
	StarvationAfter time.Duration
	Observer        Observer
}
type Request struct {
	Jobs    []domain.Job
	Worker  domain.Worker
	Sandbox domain.Sandbox
	Now     time.Time
	Active  int
}
type Result struct{ Ranked []domain.Job }
type Scheduler interface{ Rank(Request) Result }
type Policy struct{ config Config }

func New(config Config) *Policy {
	if config.Weights == (Weights{}) {
		config.Weights = Weights{ExactMatch: 20, PreferredRegion: 12, SandboxAffinity: 8, ProviderAffinity: 6, UpstreamHealth: 5, WorkerLoad: 5, QueueAge: 1, Starvation: 30, Cost: 1}
	}
	if config.StarvationAfter <= 0 {
		config.StarvationAfter = 5 * time.Minute
	}
	return &Policy{config: config}
}

type scored struct {
	job   domain.Job
	score float64
}

func (p *Policy) Rank(r Request) Result {
	items := make([]scored, 0, len(r.Jobs))
	for _, job := range r.Jobs {
		if reason := eligible(job, r); reason != "" {
			p.observe(Decision{JobID: job.ID, Worker: r.Worker.ID, Reason: reason})
			continue
		}
		items = append(items, scored{job, p.score(job, r)})
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].score != items[j].score {
			return items[i].score > items[j].score
		}
		if !items[i].job.CreatedAt.Equal(items[j].job.CreatedAt) {
			return items[i].job.CreatedAt.Before(items[j].job.CreatedAt)
		}
		return items[i].job.ID < items[j].job.ID
	})
	out := Result{Ranked: make([]domain.Job, len(items))}
	for i := range items {
		out.Ranked[i] = items[i].job
	}
	if len(items) > 0 {
		p.observe(Decision{JobID: items[0].job.ID, Worker: r.Worker.ID, Reason: ReasonSelected, Score: items[0].score, Chosen: true})
	}
	return out
}
func (p *Policy) observe(d Decision) {
	if p.config.Observer != nil {
		p.config.Observer.ObserveSchedulingDecision(d)
	}
}
func eligible(j domain.Job, r Request) ReasonCode {
	w, c := r.Worker, j.Constraints
	if w.MaxConcurrency > 0 && (w.AvailableSlots <= 0 || r.Active >= w.MaxConcurrency) {
		return ReasonConcurrency
	}
	status := strings.ToLower(w.Health["status"])
	if status != "" && status != "ok" && status != "healthy" && status != "ready" {
		return ReasonWorkerHealth
	}
	if r.Sandbox.ID == "" || r.Sandbox.WorkerID != w.ID || (r.Sandbox.State != "" && r.Sandbox.State != "ready" && r.Sandbox.State != "running") {
		return ReasonSandbox
	}
	for _, v := range c.Capabilities {
		if !has(w.Capabilities.Capabilities, v) || !has(r.Sandbox.Capabilities.Capabilities, v) {
			return ReasonCapability
		}
	}
	if c.ExecutorKind != "" && (!has(w.Capabilities.ExecutorKinds, c.ExecutorKind) || !has(r.Sandbox.Capabilities.ExecutorKinds, c.ExecutorKind)) {
		return ReasonExecutor
	}
	for k, v := range c.Labels {
		if w.Capabilities.Labels[k] != v || r.Sandbox.Capabilities.Labels[k] != v {
			return ReasonLabels
		}
	}
	if c.Architecture != "" && (w.Capabilities.Architecture != c.Architecture || r.Sandbox.Capabilities.Architecture != c.Architecture) {
		return ReasonArchitecture
	}
	if c.Region != "" && (w.Capabilities.Region != c.Region || r.Sandbox.Capabilities.Region != c.Region) {
		return ReasonRegion
	}
	if c.RequiredUpstream != "" {
		if !has(w.Capabilities.Upstreams, c.RequiredUpstream) || !has(r.Sandbox.Capabilities.Upstreams, c.RequiredUpstream) {
			return ReasonUpstream
		}
		u, ok := w.UpstreamStatus[c.RequiredUpstream]
		if !ok || u.State == domain.UpstreamUnavailable || u.State == "" {
			return ReasonUpstream
		}
		if u.State == domain.UpstreamCooldown || (!u.CooldownUntil.IsZero() && r.Now.Before(u.CooldownUntil)) {
			return ReasonUpstreamCooldown
		}
		if u.State != domain.UpstreamAvailable {
			return ReasonUpstream
		}
		if u.BudgetRemaining != nil && *u.BudgetRemaining <= 0 {
			return ReasonUpstreamBudget
		}
		if c.MaxCost != nil && u.Cost != nil && *u.Cost > *c.MaxCost {
			return ReasonCost
		}
	}
	return ""
}
func (p *Policy) score(j domain.Job, r Request) float64 {
	w, c, weights := r.Worker, j.Constraints, p.config.Weights
	s := 0.0
	if exact(c, w.Capabilities) {
		s += weights.ExactMatch
	}
	if c.PreferredRegion != "" && c.PreferredRegion == w.Capabilities.Region {
		s += weights.PreferredRegion
	}
	if c.PreferredSandbox != "" && c.PreferredSandbox == w.SandboxID {
		s += weights.SandboxAffinity
	}
	if c.PreferredProvider != "" && c.PreferredProvider == w.SandboxProvider {
		s += weights.ProviderAffinity
	}
	if c.RequiredUpstream != "" {
		s += weights.UpstreamHealth * w.UpstreamStatus[c.RequiredUpstream].Health
	}
	if w.MaxConcurrency > 0 {
		s += weights.WorkerLoad * float64(w.MaxConcurrency-r.Active) / float64(w.MaxConcurrency)
	}
	age := r.Now.Sub(j.CreatedAt)
	if age > 0 {
		s += weights.QueueAge * age.Minutes()
	}
	if age >= p.config.StarvationAfter {
		s += weights.Starvation
	}
	if c.RequiredUpstream != "" {
		if cost := w.UpstreamStatus[c.RequiredUpstream].Cost; cost != nil {
			s -= weights.Cost * *cost
		}
	}
	return s
}
func exact(c domain.RoutingConstraints, a domain.Capabilities) bool {
	return len(c.Capabilities) == len(a.Capabilities) && len(c.Labels) == len(a.Labels) && (c.Architecture == "" || c.Architecture == a.Architecture) && (c.Region == "" || c.Region == a.Region)
}
func has[T comparable](values []T, wanted T) bool {
	for _, v := range values {
		if v == wanted {
			return true
		}
	}
	return false
}
