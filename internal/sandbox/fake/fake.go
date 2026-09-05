// Package fake provides a deterministic in-memory sandbox provider.
package fake

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/ethan/smart-route/internal/domain"
	"github.com/ethan/smart-route/internal/sandbox"
)

type Provider struct {
	mu          sync.Mutex
	next        uint64
	items       map[domain.SandboxID]sandbox.Sandbox
	capacity    int
	createDelay time.Duration
	createCalls int
	createError sandbox.ErrorCode
	snapshots   map[domain.SandboxID][]byte
}

func New() *Provider {
	return &Provider{items: make(map[domain.SandboxID]sandbox.Sandbox), snapshots: make(map[domain.SandboxID][]byte)}
}

func Factory(map[string]string) (sandbox.Provider, error) { return New(), nil }

func (p *Provider) Create(ctx context.Context, spec sandbox.CreateSpec) (sandbox.Sandbox, error) {
	if err := ctx.Err(); err != nil {
		return sandbox.Sandbox{}, err
	}
	if spec.WorkerID == "" || spec.ControlPlaneURL == "" || spec.BootstrapToken == "" {
		return sandbox.Sandbox{}, sandbox.NewError("fake", "create", sandbox.CodeInvalid, fmt.Errorf("worker ID, control-plane URL, and bootstrap token are required"))
	}
	p.mu.Lock()
	p.createCalls++
	delay := p.createDelay
	p.mu.Unlock()
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return sandbox.Sandbox{}, ctx.Err()
		case <-timer.C:
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.createError != "" {
		return sandbox.Sandbox{}, sandbox.NewError("fake", "create", p.createError, fmt.Errorf("configured create failure"))
	}
	if p.capacity > 0 && len(p.items) >= p.capacity {
		return sandbox.Sandbox{}, sandbox.NewError("fake", "create", sandbox.CodeCapacity, fmt.Errorf("capacity exhausted"))
	}
	p.next++
	id := spec.SandboxID
	if id == "" {
		id = domain.SandboxID("fake-" + strconv.FormatUint(p.next, 10))
	}
	item := sandbox.Sandbox{ID: id, Provider: "fake", ExternalID: string(id), WorkerID: spec.WorkerID, State: sandbox.StateRunning, Capabilities: spec.Capabilities, Labels: clone(spec.Labels), Metadata: map[string]string{"image": spec.Image, "template": spec.Template}, CreatedAt: time.Now().UTC()}
	p.items[id] = item
	return item, nil
}

// SetCapacity limits successful creates. Zero means unlimited.
func (p *Provider) SetCapacity(capacity int) { p.mu.Lock(); defer p.mu.Unlock(); p.capacity = capacity }

// SetCreateDelay keeps creates in flight for deterministic controller tests.
func (p *Provider) SetCreateDelay(delay time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.createDelay = delay
}

func (p *Provider) CreateCalls() int { p.mu.Lock(); defer p.mu.Unlock(); return p.createCalls }

// SetCreateError configures a normalized provider failure; empty clears it.
func (p *Provider) SetCreateError(code sandbox.ErrorCode) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.createError = code
}

func (p *Provider) List(ctx context.Context, filter sandbox.Filter) ([]sandbox.Sandbox, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	items := make([]sandbox.Sandbox, 0, len(p.items))
	for _, item := range p.items {
		if sandbox.Matches(item, filter) {
			items = append(items, item)
		}
	}
	return items, nil
}

func (p *Provider) Get(ctx context.Context, id domain.SandboxID) (sandbox.Sandbox, error) {
	if err := ctx.Err(); err != nil {
		return sandbox.Sandbox{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	item, ok := p.items[id]
	if !ok {
		return sandbox.Sandbox{}, &sandbox.ProviderError{Provider: "fake", Operation: "get", SandboxID: string(id), Code: sandbox.CodeNotFound}
	}
	return item, nil
}

func (p *Provider) Terminate(ctx context.Context, id domain.SandboxID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if item, ok := p.items[id]; ok {
		item.State = sandbox.StateTerminated
		p.items[id] = item
	}
	return nil
}

// CreateSnapshot/RestoreSnapshot implement the optional provider snapshot
// strategy for deterministic contracts and chaos tests.
func (p *Provider) CreateSnapshot(ctx context.Context, id domain.SandboxID, w io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	data, ok := p.snapshots[id]
	p.mu.Unlock()
	if !ok {
		return sandbox.NewError("fake", "snapshot", sandbox.CodeNotFound, sandbox.ErrNotFound)
	}
	_, err := w.Write(data)
	return err
}
func (p *Provider) RestoreSnapshot(ctx context.Context, spec sandbox.CreateSpec, r io.Reader) (sandbox.Sandbox, error) {
	if err := ctx.Err(); err != nil {
		return sandbox.Sandbox{}, err
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return sandbox.Sandbox{}, err
	}
	item, err := p.Create(ctx, spec)
	if err == nil {
		p.mu.Lock()
		p.snapshots[item.ID] = append([]byte(nil), data...)
		p.mu.Unlock()
	}
	return item, err
}
func (p *Provider) SetSnapshot(id domain.SandboxID, data []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.snapshots[id] = append([]byte(nil), data...)
}

func (p *Provider) SetState(id domain.SandboxID, state sandbox.State) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	item, ok := p.items[id]
	if !ok {
		return sandbox.ErrNotFound
	}
	item.State = state
	p.items[id] = item
	return nil
}

func clone(source map[string]string) map[string]string {
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
