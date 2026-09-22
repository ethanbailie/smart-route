package scheduler

import (
	"testing"
	"time"

	"github.com/ethanbailie/smart-route/internal/domain"
)

func TestPolicyEligibilityAndRanking(t *testing.T) {
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	base := Request{
		Now:     now,
		Worker:  domain.Worker{ID: "worker", SandboxID: "sandbox", SandboxProvider: "docker", MaxConcurrency: 2, AvailableSlots: 2, Health: map[string]string{"status": "healthy"}, Capabilities: domain.Capabilities{Capabilities: []string{"build"}, Labels: map[string]string{"pool": "main"}, Architecture: domain.ArchitectureAMD64, Region: "west", ExecutorKinds: []domain.ExecutorKind{domain.ExecutorCommand}}},
		Sandbox: domain.Sandbox{ID: "sandbox", WorkerID: "worker", State: "ready", Capabilities: domain.Capabilities{Capabilities: []string{"build"}, Labels: map[string]string{"pool": "main"}, Architecture: domain.ArchitectureAMD64, Region: "west", ExecutorKinds: []domain.ExecutorKind{domain.ExecutorCommand}}},
	}
	constraint := domain.RoutingConstraints{Capabilities: []string{"build"}, Labels: map[string]string{"pool": "main"}, Architecture: domain.ArchitectureAMD64, Region: "west", ExecutorKind: domain.ExecutorCommand}
	tests := []struct {
		name   string
		mutate func(*Request, *domain.RoutingConstraints)
		want   ReasonCode
	}{
		{"capability", func(_ *Request, c *domain.RoutingConstraints) { c.Capabilities = []string{"gpu"} }, ReasonCapability},
		{"executor", func(_ *Request, c *domain.RoutingConstraints) { c.ExecutorKind = domain.ExecutorHTTP }, ReasonExecutor},
		{"labels", func(_ *Request, c *domain.RoutingConstraints) { c.Labels["pool"] = "other" }, ReasonLabels},
		{"architecture", func(_ *Request, c *domain.RoutingConstraints) { c.Architecture = domain.ArchitectureARM64 }, ReasonArchitecture},
		{"region", func(_ *Request, c *domain.RoutingConstraints) { c.Region = "east" }, ReasonRegion},
		{"health", func(r *Request, _ *domain.RoutingConstraints) { r.Worker.Health["status"] = "unhealthy" }, ReasonWorkerHealth},
		{"concurrency", func(r *Request, _ *domain.RoutingConstraints) { r.Active = 2 }, ReasonConcurrency},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := base
			r.Worker.Health = map[string]string{"status": "healthy"}
			c := constraint
			c.Labels = map[string]string{"pool": "main"}
			tt.mutate(&r, &c)
			r.Jobs = []domain.Job{{ID: "job", CreatedAt: now, Constraints: c}}
			var decisions []Decision
			p := New(Config{Observer: ObserverFunc(func(d Decision) { decisions = append(decisions, d) })})
			if got := p.Rank(r); len(got.Ranked) != 0 || len(decisions) != 1 || decisions[0].Reason != tt.want {
				t.Fatalf("ranked=%v decisions=%v", got.Ranked, decisions)
			}
		})
	}

	preferred := constraint
	preferred.PreferredProvider = "docker"
	base.Jobs = []domain.Job{{ID: "b", CreatedAt: now, Constraints: constraint}, {ID: "a", CreatedAt: now, Constraints: preferred}}
	var selected Decision
	p := New(Config{Observer: ObserverFunc(func(d Decision) {
		if d.Chosen {
			selected = d
		}
	})})
	got := p.Rank(base)
	if len(got.Ranked) != 2 || got.Ranked[0].ID != "a" || selected.JobID != "a" || selected.Reason != ReasonSelected {
		t.Fatalf("ranked=%v selected=%+v", got.Ranked, selected)
	}
}
