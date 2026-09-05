package worker

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ethan/smart-route/internal/domain"
	"github.com/ethan/smart-route/internal/secret"
	"github.com/ethan/smart-route/internal/upstream"
)

type countingSecrets struct {
	calls  int
	bundle secret.Bundle
}

func (s *countingSecrets) Resolve(context.Context, domain.CredentialRefID) (secret.Bundle, error) {
	s.calls++
	return s.bundle, nil
}

func TestUpstreamExecutorResolvesAtExecutionAndRedacts(t *testing.T) {
	store := &countingSecrets{bundle: secret.Bundle{Values: map[string]string{"token": "top-secret"}}}
	adapter := upstream.AdapterFunc(func(_ context.Context, _ upstream.Request, credentials secret.Bundle) (upstream.Response, error) {
		return upstream.Response{Data: []byte(credentials.Values["token"]), Metadata: map[string]string{"debug": credentials.Values["token"]}}, errors.New("provider exposed top-secret")
	})
	registry, err := upstream.NewRegistry(upstream.Entry{Metadata: upstream.Metadata{Name: "primary", Enabled: true, CredentialRef: "logical-ref"}, Adapter: adapter})
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewUpstreamExecutor(UpstreamConfig{Registry: registry, Secrets: store, Authorized: []string{"primary"}})
	if err != nil || store.calls != 0 {
		t.Fatalf("construction resolved credentials: err=%v calls=%d", err, store.calls)
	}
	_, err = executor.Execute(context.Background(), Job{Payload: json.RawMessage(`{"upstream":"primary","input":{"prompt":"hello"}}`)}, nil)
	if store.calls != 1 {
		t.Fatalf("resolve calls = %d", store.calls)
	}
	var failure *FailureError
	if !errors.As(err, &failure) || strings.Contains(failure.Message, "top-secret") || !strings.Contains(failure.Message, "[REDACTED]") {
		t.Fatalf("failure was not redacted: %#v", err)
	}
	encoded, _ := json.Marshal(store.bundle)
	if strings.Contains(string(encoded), "top-secret") {
		t.Fatalf("bundle JSON leaked a secret: %s", encoded)
	}
	if state := executor.HealthSnapshot()["primary"]; state.State != domain.UpstreamUnavailable || state.Metadata["health_checked_at"] == "" {
		t.Fatalf("permanent failure health = %#v", state)
	}

	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	registry, _ = upstream.NewRegistry(upstream.Entry{Metadata: upstream.Metadata{Name: "primary", Enabled: true, CredentialRef: "logical-ref"}, Adapter: &upstream.Fake{Outcome: upstream.FakeThrottle, RetryAfter: time.Minute}})
	executor, _ = NewUpstreamExecutor(UpstreamConfig{Registry: registry, Secrets: store, Authorized: []string{"primary"}, Now: func() time.Time { return now }})
	_, _ = executor.Execute(context.Background(), Job{Payload: json.RawMessage(`{"upstream":"primary","input":{}}`)}, nil)
	state := executor.HealthSnapshot()["primary"]
	if state.State != domain.UpstreamCooldown || state.CooldownUntil != now.Add(time.Minute) {
		t.Fatalf("throttle health = %#v", state)
	}
	if raw, _ := json.Marshal(state); strings.Contains(string(raw), "logical-ref") || strings.Contains(string(raw), "top-secret") {
		t.Fatalf("health snapshot leaked credentials: %s", raw)
	}
}
