package sandbox

import (
	"fmt"
	"sort"
)

// ProviderConfig is deliberately untyped at this boundary so provider-specific
// settings do not leak into the control plane configuration model.
type ProviderConfig struct {
	Type   string
	Config map[string]string
}

type Factory func(map[string]string) (Provider, error)

type Registry struct{ providers map[string]Provider }

// NewRegistry constructs configured provider instances using the supplied
// adapter factories. Provider names are deployment-local routing names.
func NewRegistry(config map[string]ProviderConfig, factories map[string]Factory) (*Registry, error) {
	r := &Registry{providers: make(map[string]Provider, len(config))}
	for name, entry := range config {
		if name == "" || entry.Type == "" {
			return nil, fmt.Errorf("provider %q: %w", name, ErrInvalid)
		}
		factory, ok := factories[entry.Type]
		if !ok {
			return nil, fmt.Errorf("provider %q has unknown type %q: %w", name, entry.Type, ErrInvalid)
		}
		provider, err := factory(cloneStrings(entry.Config))
		if err != nil {
			return nil, fmt.Errorf("provider %q: %w", name, err)
		}
		r.providers[name] = provider
	}
	return r, nil
}

func (r *Registry) Get(name string) (Provider, error) {
	provider, ok := r.providers[name]
	if !ok {
		return nil, fmt.Errorf("provider %q: %w", name, ErrNotFound)
	}
	return provider, nil
}

func (r *Registry) Names() []string {
	names := make([]string, 0, len(r.providers))
	for name := range r.providers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func cloneStrings(source map[string]string) map[string]string {
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
