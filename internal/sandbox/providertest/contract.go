// Package providertest contains the reusable behavioral contract for sandbox
// adapters. Provider implementations should call Run from their test package.
package providertest

import (
	"context"
	"errors"
	"testing"

	"github.com/ethan/smart-route/internal/domain"
	"github.com/ethan/smart-route/internal/sandbox"
)

// Factory must return an isolated provider instance for each call.
type Factory func(*testing.T) sandbox.Provider

// Run verifies creation, observation, unique identities, typed missing errors,
// and idempotent termination—the behavior relied on by the control plane.
func Run(t *testing.T, factory Factory) {
	t.Helper()
	ctx := context.Background()
	p := factory(t)
	spec := sandbox.CreateSpec{WorkerID: "worker-1", ControlPlaneURL: "https://control.example", BootstrapToken: "secret", Image: "worker:v1", Labels: map[string]string{"pool": "contract"}}
	first, err := p.Create(ctx, spec)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	second, err := p.Create(ctx, spec)
	if err != nil {
		t.Fatalf("second create: %v", err)
	}
	if first.ID == "" || first.ID == second.ID {
		t.Fatalf("sandbox IDs must be nonempty and unique: %q, %q", first.ID, second.ID)
	}
	observed, err := p.Get(ctx, first.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if observed.ID != first.ID || observed.WorkerID != spec.WorkerID || observed.State != sandbox.StateRunning {
		t.Fatalf("unexpected sandbox: %+v", observed)
	}
	if observed.Labels["pool"] != "contract" || observed.Metadata["image"] != "worker:v1" {
		t.Fatalf("metadata was not preserved: %+v", observed)
	}
	listed, err := p.List(ctx, sandbox.Filter{WorkerID: spec.WorkerID, States: []sandbox.State{sandbox.StateRunning}, Labels: map[string]string{"pool": "contract"}})
	if err != nil || len(listed) != 2 {
		t.Fatalf("list = %+v, %v; want two matching sandboxes", listed, err)
	}
	listed, err = p.List(ctx, sandbox.Filter{Labels: map[string]string{"pool": "other"}})
	if err != nil || len(listed) != 0 {
		t.Fatalf("nonmatching list = %+v, %v", listed, err)
	}
	if _, err = p.Get(ctx, domain.SandboxID("missing")); !errors.Is(err, sandbox.ErrNotFound) {
		t.Fatalf("missing get error = %v, want ErrNotFound", err)
	}
	if err = p.Terminate(ctx, first.ID); err != nil {
		t.Fatalf("terminate: %v", err)
	}
	if err = p.Terminate(ctx, first.ID); err != nil {
		t.Fatalf("repeat terminate: %v", err)
	}
	observed, err = p.Get(ctx, first.ID)
	if err != nil {
		t.Fatalf("get terminated: %v", err)
	}
	if observed.State != sandbox.StateTerminated {
		t.Fatalf("state = %q, want %q", observed.State, sandbox.StateTerminated)
	}
}
