// Package upstream defines provider-neutral upstream selection and execution.
package upstream

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/ethan/smart-route/internal/domain"
	"github.com/ethan/smart-route/internal/secret"
)

var (
	ErrInvalid      = errors.New("upstream: invalid configuration")
	ErrNotFound     = errors.New("upstream: not found")
	ErrUnauthorized = errors.New("upstream: selection is not authorized")
	ErrUnavailable  = errors.New("upstream: unavailable")
	ErrThrottled    = errors.New("upstream: throttled")
	ErrTimeout      = errors.New("upstream: timed out")
	ErrPermanent    = errors.New("upstream: permanent failure")
)

type ThrottleError struct {
	RetryAfter    time.Duration
	CooldownUntil time.Time
}

func (e *ThrottleError) Error() string        { return ErrThrottled.Error() }
func (e *ThrottleError) Is(target error) bool { return target == ErrThrottled }
func (e *ThrottleError) Until(now time.Time) time.Time {
	if !e.CooldownUntil.IsZero() {
		return e.CooldownUntil
	}
	return now.Add(e.RetryAfter)
}

type Metadata struct {
	Name            string
	Enabled         bool
	Capabilities    []string
	Models          []string
	CredentialRef   domain.CredentialRefID
	DocumentedQuota string
	CooldownUntil   time.Time
	Unavailable     bool
	HealthCheckedAt time.Time
}

type Request struct {
	Payload []byte
	Model   string
	Tags    map[string]string
}

type Response struct {
	StatusCode int
	Data       []byte
	Metadata   map[string]string
}

// Adapter is the only interface a direct upstream integration must implement.
type Adapter interface {
	Execute(context.Context, Request, secret.Bundle) (Response, error)
}

type AdapterFunc func(context.Context, Request, secret.Bundle) (Response, error)

func (f AdapterFunc) Execute(ctx context.Context, req Request, credentials secret.Bundle) (Response, error) {
	return f(ctx, req, credentials)
}

// Injector and Executor allow credentials to be injected into an external
// execution profile instead of implementing a direct Adapter.
type Injector interface {
	Inject(context.Context, Request, secret.Bundle) (Request, error)
}

type Executor interface {
	Execute(context.Context, Request) (Response, error)
}

type Profile struct {
	Injector Injector
	Executor Executor
}

func (p Profile) Execute(ctx context.Context, req Request, credentials secret.Bundle) (Response, error) {
	if p.Injector == nil || p.Executor == nil {
		return Response{}, fmt.Errorf("%w: incomplete execution profile", ErrInvalid)
	}
	injected, err := p.Injector.Inject(ctx, req, credentials)
	if err != nil {
		return Response{}, err
	}
	return p.Executor.Execute(ctx, injected)
}

type Entry struct {
	Metadata Metadata
	Adapter  Adapter
}

type Selection struct {
	Name         string
	Capabilities []string
	Model        string
	Authorized   []string
}

type Registry struct {
	mu      sync.RWMutex
	entries map[string]Entry
}

func NewRegistry(entries ...Entry) (*Registry, error) {
	r := &Registry{entries: make(map[string]Entry, len(entries))}
	for _, entry := range entries {
		if err := r.Register(entry); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// Register is the extension point for new adapters; core domain types do not
// need to know adapter implementations or provider-specific configuration.
func (r *Registry) Register(entry Entry) error {
	if entry.Metadata.Name == "" || entry.Adapter == nil {
		return ErrInvalid
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.entries[entry.Metadata.Name]; exists {
		return fmt.Errorf("%w: duplicate %q", ErrInvalid, entry.Metadata.Name)
	}
	r.entries[entry.Metadata.Name] = entry
	return nil
}

func (r *Registry) Select(selection Selection, now time.Time) (Entry, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if selection.Name == "" && len(selection.Capabilities) == 0 && selection.Model == "" {
		return Entry{}, fmt.Errorf("%w: name or capability selection required", ErrInvalid)
	}
	names := make([]string, 0, len(r.entries))
	for name := range r.entries {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		entry := r.entries[name]
		if selection.Name != "" && name != selection.Name {
			continue
		}
		if !contains(selection.Authorized, name) {
			continue
		}
		if !entry.Metadata.Enabled || entry.Metadata.Unavailable || now.Before(entry.Metadata.CooldownUntil) {
			continue
		}
		if !containsAll(entry.Metadata.Capabilities, selection.Capabilities) || (selection.Model != "" && !contains(entry.Metadata.Models, selection.Model)) {
			continue
		}
		return cloneEntry(entry), nil
	}
	if selection.Name != "" && !contains(selection.Authorized, selection.Name) {
		return Entry{}, ErrUnauthorized
	}
	return Entry{}, ErrNotFound
}

func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.entries))
	for name := range r.entries {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (r *Registry) UpdateHealth(name string, state domain.UpstreamAvailability, cooldownUntil, checkedAt time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.entries[name]
	if !ok {
		return ErrNotFound
	}
	entry.Metadata.HealthCheckedAt = checkedAt
	entry.Metadata.CooldownUntil = cooldownUntil
	entry.Metadata.Unavailable = state == domain.UpstreamUnavailable
	r.entries[name] = entry
	return nil
}

// HealthSnapshot contains scheduler state only, never credentials.
func (r *Registry) HealthSnapshot(now time.Time) map[string]domain.UpstreamState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]domain.UpstreamState, len(r.entries))
	for name, entry := range r.entries {
		m := entry.Metadata
		state := domain.UpstreamAvailable
		if !m.Enabled || m.Unavailable {
			state = domain.UpstreamUnavailable
		} else if now.Before(m.CooldownUntil) {
			state = domain.UpstreamCooldown
		}
		health := 1.0
		if state != domain.UpstreamAvailable {
			health = 0
		}
		out[name] = domain.UpstreamState{State: state, Health: health, CooldownUntil: m.CooldownUntil, Metadata: map[string]string{"health_checked_at": m.HealthCheckedAt.UTC().Format(time.RFC3339Nano)}}
	}
	return out
}

func cloneEntry(e Entry) Entry {
	e.Metadata.Capabilities = append([]string(nil), e.Metadata.Capabilities...)
	e.Metadata.Models = append([]string(nil), e.Metadata.Models...)
	return e
}

func containsAll(have, wanted []string) bool {
	for _, item := range wanted {
		if !contains(have, item) {
			return false
		}
	}
	return true
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
