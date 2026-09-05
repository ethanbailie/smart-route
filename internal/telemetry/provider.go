package telemetry

import (
	"context"
	"time"

	"github.com/ethan/smart-route/internal/domain"
	"github.com/ethan/smart-route/internal/sandbox"
)

// WrapProvider adds optional spans, bounded provider metrics, and structured
// sandbox/provider fields without exposing bootstrap credentials or specs.
func (t *Telemetry) WrapProvider(name string, next sandbox.Provider) sandbox.Provider {
	if !t.Enabled() || next == nil {
		return next
	}
	return &observedProvider{name: name, next: next, telemetry: t}
}

type observedProvider struct {
	name      string
	next      sandbox.Provider
	telemetry *Telemetry
}

func (p *observedProvider) Create(ctx context.Context, spec sandbox.CreateSpec) (item sandbox.Sandbox, err error) {
	started := time.Now()
	ctx, finish := p.telemetry.Start(ctx, "provider.create", "provider", p.name, "sandbox_id", spec.SandboxID)
	defer func() { finish(err); p.telemetry.Provision(p.name, outcome(err), time.Since(started)) }()
	return p.next.Create(ctx, spec)
}
func (p *observedProvider) Get(ctx context.Context, id domain.SandboxID) (item sandbox.Sandbox, err error) {
	ctx, finish := p.telemetry.Start(ctx, "provider.get", "provider", p.name, "sandbox_id", id)
	defer func() { finish(err) }()
	return p.next.Get(ctx, id)
}
func (p *observedProvider) List(ctx context.Context, f sandbox.Filter) (items []sandbox.Sandbox, err error) {
	ctx, finish := p.telemetry.Start(ctx, "provider.list", "provider", p.name)
	defer func() { finish(err) }()
	return p.next.List(ctx, f)
}
func (p *observedProvider) Terminate(ctx context.Context, id domain.SandboxID) (err error) {
	ctx, finish := p.telemetry.Start(ctx, "provider.terminate", "provider", p.name, "sandbox_id", id)
	defer func() { finish(err) }()
	return p.next.Terminate(ctx, id)
}
func outcome(err error) string {
	if err != nil {
		return "failure"
	}
	return "success"
}
