package config

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"gopkg.in/yaml.v3"
)

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if json.Unmarshal(b, &s) == nil {
		return d.UnmarshalText([]byte(s))
	}
	var n int64
	if e := json.Unmarshal(b, &n); e != nil {
		return e
	}
	*d = Duration(n)
	return nil
}

// UnmarshalYAML normalizes snake_case deployment keys to the typed Go model.
// Keeping the normalization centralized also makes unknown nested map names
// (provider, pool label, and secret reference names) remain untouched.
func (c *Config) UnmarshalYAML(n *yaml.Node) error {
	var raw any
	if e := n.Decode(&raw); e != nil {
		return e
	}
	return decodeNormalized(raw, reflect.TypeOf(*c), (*configAlias)(c))
}
func (c *Config) UnmarshalTOML(raw any) error {
	return decodeNormalized(raw, reflect.TypeOf(*c), (*configAlias)(c))
}

type configAlias Config

func decodeNormalized(raw any, t reflect.Type, out any) error {
	v, e := normalizeAt(raw, t, "")
	if e != nil {
		return e
	}
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	return json.Unmarshal(b, out)
}
func normalizeAt(raw any, t reflect.Type, path string) (any, error) {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch x := raw.(type) {
	case map[string]any:
		if t.Kind() == reflect.Map {
			out := map[string]any{}
			for k, v := range x {
				n, e := normalizeAt(v, t.Elem(), joinPath(path, k))
				if e != nil {
					return nil, e
				}
				out[k] = n
			}
			return out, nil
		}
		if t.Kind() != reflect.Struct {
			return x, nil
		}
		out := map[string]any{}
		for k, v := range x {
			field, ok := fieldFor(t, k)
			if !ok {
				return nil, fmt.Errorf("%s: unknown configuration field", joinPath(path, k))
			}
			n, e := normalizeAt(v, field.Type, joinPath(path, k))
			if e != nil {
				return nil, e
			}
			out[field.Name] = n
		}
		return out, nil
	case []any:
		if t.Kind() != reflect.Slice {
			return x, nil
		}
		out := make([]any, len(x))
		for i, v := range x {
			n, e := normalizeAt(v, t.Elem(), fmt.Sprintf("%s[%d]", path, i))
			if e != nil {
				return nil, e
			}
			out[i] = n
		}
		return out, nil
	default:
		return raw, nil
	}
}
func fieldFor(t reflect.Type, key string) (reflect.StructField, bool) {
	wanted := strings.ReplaceAll(strings.ToLower(key), "_", "")
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if strings.ToLower(f.Name) == wanted {
			return f, true
		}
	}
	return reflect.StructField{}, false
}
func joinPath(parent, child string) string {
	if parent == "" {
		return child
	}
	return parent + "." + child
}
