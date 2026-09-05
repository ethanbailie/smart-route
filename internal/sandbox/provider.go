// Package sandbox defines the provider-neutral contract used to provision workers.
package sandbox

import (
	"context"
	"io"
	"time"

	"github.com/ethan/smart-route/internal/domain"
)

// Snapshotter is an optional provider capability. Checkpoint recovery remains
// provider-independent by discovering this interface instead of branching on vendor names.
type Snapshotter interface {
	CreateSnapshot(context.Context, domain.SandboxID, io.Writer) error
	RestoreSnapshot(context.Context, CreateSpec, io.Reader) (Sandbox, error)
}

// State is the normalized lifecycle state exposed by every provider.
type State string

const (
	StateCreating   State = "creating"
	StateRunning    State = "running"
	StateStopped    State = "stopped"
	StateTerminated State = "terminated"
	StateFailed     State = "failed"
	StateUnknown    State = "unknown"
)

// CreateSpec contains everything a sandbox needs to join the control plane.
// BootstrapToken is write-only and providers must not return or persist it.
type CreateSpec struct {
	SandboxID            domain.SandboxID
	WorkerID             domain.WorkerID
	SandboxProvider      string
	ControlPlaneURL      string
	BootstrapToken       string
	Image                string
	Template             string
	CPUClass             string
	MemoryClass          string
	Architecture         domain.Architecture
	Region               string
	WorkerMaxConcurrency int
	// Environment references credential/configuration locators, never secret values.
	Environment       map[string]domain.CredentialRefID
	BootstrapCommand  []string
	BootstrapArtifact string
	MaxLifetime       time.Duration
	Capabilities      domain.Capabilities
	Labels            map[string]string
}

// Sandbox is the provider-neutral description of one provisioned worker.
type Sandbox struct {
	ID           domain.SandboxID
	Provider     string
	ExternalID   string
	WorkerID     domain.WorkerID
	State        State
	Capabilities domain.Capabilities
	Labels       map[string]string
	Metadata     map[string]string
	CreatedAt    time.Time
}

// Filter selects sandboxes. Empty fields match every sandbox.
type Filter struct {
	WorkerID domain.WorkerID
	States   []State
	Labels   map[string]string
}

// Provider provisions and observes worker sandboxes. Terminate must be
// idempotent: terminating an absent or already terminated sandbox succeeds.
type Provider interface {
	Create(context.Context, CreateSpec) (Sandbox, error)
	Get(context.Context, domain.SandboxID) (Sandbox, error)
	List(context.Context, Filter) ([]Sandbox, error)
	Terminate(context.Context, domain.SandboxID) error
}

func Matches(item Sandbox, filter Filter) bool {
	if filter.WorkerID != "" && item.WorkerID != filter.WorkerID {
		return false
	}
	if len(filter.States) > 0 {
		matched := false
		for _, state := range filter.States {
			if item.State == state {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	for key, value := range filter.Labels {
		if item.Labels[key] != value {
			return false
		}
	}
	return true
}
