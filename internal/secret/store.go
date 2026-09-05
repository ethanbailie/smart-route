// Package secret resolves logical credential references inside a worker process.
package secret

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/ethan/smart-route/internal/domain"
)

var ErrNotFound = errors.New("secret: credential reference not found")

// Bundle is deliberately opaque to the control plane. Version and expiry make
// the contract suitable for cloud secret managers without exposing their APIs.
type Bundle struct {
	Values    map[string]string `json:"-"`
	Version   string            `json:"-"`
	ExpiresAt time.Time         `json:"-"`
}

func (b Bundle) Clone() Bundle {
	return Bundle{Values: clone(b.Values), Version: b.Version, ExpiresAt: b.ExpiresAt}
}

// SecretStore and SecretBundle name the integration contract explicitly.
type SecretBundle = Bundle

type Store interface {
	Resolve(context.Context, domain.CredentialRefID) (Bundle, error)
}
type SecretStore = Store

// ConfigStore supports deployment configuration values and environment-backed
// references. Environment maps logical references to variable names, never to
// secret values.
type ConfigStore struct {
	Environment map[domain.CredentialRefID]map[string]string
	Values      map[domain.CredentialRefID]Bundle
}

func (s ConfigStore) Resolve(_ context.Context, ref domain.CredentialRefID) (Bundle, error) {
	if ref == "" {
		return Bundle{}, fmt.Errorf("%w: empty reference", ErrNotFound)
	}
	if b, ok := s.Values[ref]; ok {
		return b.Clone(), nil
	}
	names, ok := s.Environment[ref]
	if !ok {
		return Bundle{}, fmt.Errorf("%w: %s", ErrNotFound, ref)
	}
	b := Bundle{Values: make(map[string]string, len(names))}
	for key, variable := range names {
		value, exists := os.LookupEnv(variable)
		if !exists {
			return Bundle{}, fmt.Errorf("%w: %s", ErrNotFound, ref)
		}
		b.Values[key] = value
	}
	return b, nil
}

// MemoryStore is a concurrency-safe fake for tests and local development.
type MemoryStore struct {
	mu      sync.RWMutex
	bundles map[domain.CredentialRefID]Bundle
}

func NewMemoryStore(values map[domain.CredentialRefID]Bundle) *MemoryStore {
	s := &MemoryStore{bundles: make(map[domain.CredentialRefID]Bundle, len(values))}
	for ref, bundle := range values {
		s.bundles[ref] = bundle.Clone()
	}
	return s
}

func (s *MemoryStore) Resolve(_ context.Context, ref domain.CredentialRefID) (Bundle, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.bundles[ref]
	if !ok {
		return Bundle{}, fmt.Errorf("%w: %s", ErrNotFound, ref)
	}
	return b.Clone(), nil
}

func (s *MemoryStore) Put(ref domain.CredentialRefID, bundle Bundle) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bundles[ref] = bundle.Clone()
}

func clone(values map[string]string) map[string]string {
	out := make(map[string]string, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}
