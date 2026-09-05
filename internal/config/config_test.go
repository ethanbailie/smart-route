package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadYAMLAndEnvironmentOverrides(t *testing.T) {
	t.Setenv("SMART_ROUTE_HTTP_LISTEN", "127.0.0.1:9090")
	p := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte("http:\n  public_url: http://127.0.0.1:9090\ndatabase:\n  dsn: test.db\nproviders:\n  local:\n    type: localdocker\npools:\n  - name: default\n    provider: local\n    min_replicas: 0\n    max_replicas: 1\n    worker_concurrency: 1\n")
	if e := os.WriteFile(p, data, 0600); e != nil {
		t.Fatal(e)
	}
	c, e := Load(p)
	if e != nil {
		t.Fatal(e)
	}
	if c.HTTP.Listen != "127.0.0.1:9090" || len(c.Pools) != 1 || c.Pools[0].WorkerConcurrency != 1 {
		t.Fatalf("unexpected config: %#v", c)
	}
}
func TestValidationIsActionableAndDoesNotExposeToken(t *testing.T) {
	c := Default()
	c.HTTP.PublicURL = "relative"
	c.Auth.Token = "top-secret"
	c.Auth.TokenEnv = "ALSO_SECRET"
	e := c.Validate()
	if e == nil || !strings.Contains(e.Error(), "http.public_url") || !strings.Contains(e.Error(), "auth:") {
		t.Fatalf("unexpected error: %v", e)
	}
	if strings.Contains(e.Error(), "top-secret") {
		t.Fatal("validation exposed a secret")
	}
}
func TestLoadRejectsUnknownFieldWithPath(t *testing.T) {
	p := filepath.Join(t.TempDir(), "config.yaml")
	if e := os.WriteFile(p, []byte("http:\n  public_url: http://localhost:8080\n  request_timout: 1s\n"), 0600); e != nil {
		t.Fatal(e)
	}
	_, e := Load(p)
	if e == nil || !strings.Contains(e.Error(), "http.request_timout") {
		t.Fatalf("error = %v", e)
	}
}
func TestLoadRejectsUnknownTOMLFieldWithPath(t *testing.T) {
	p := filepath.Join(t.TempDir(), "config.toml")
	if e := os.WriteFile(p, []byte("[http]\npublic_url = \"http://localhost:8080\"\nrequest_timout = \"1s\"\n"), 0600); e != nil {
		t.Fatal(e)
	}
	_, e := Load(p)
	if e == nil || !strings.Contains(e.Error(), "http.request_timout") {
		t.Fatalf("error = %v", e)
	}
}
