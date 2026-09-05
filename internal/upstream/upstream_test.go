package upstream

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ethan/smart-route/internal/secret"
)

func TestRegistrySelectionCooldownAndFakeOutcomes(t *testing.T) {
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	ready := Entry{Metadata: Metadata{Name: "ready", Enabled: true, Capabilities: []string{"chat"}, Models: []string{"small"}}, Adapter: &Fake{Result: Response{StatusCode: 200}}}
	cooling := Entry{Metadata: Metadata{Name: "cooling", Enabled: true, Capabilities: []string{"chat"}, CooldownUntil: now.Add(time.Minute)}, Adapter: &Fake{}}
	registry, err := NewRegistry(cooling, ready)
	if err != nil {
		t.Fatal(err)
	}
	got, err := registry.Select(Selection{Capabilities: []string{"chat"}, Model: "small", Authorized: []string{"ready", "cooling"}}, now)
	if err != nil || got.Metadata.Name != "ready" {
		t.Fatalf("selection = %q, %v", got.Metadata.Name, err)
	}
	if _, err = registry.Select(Selection{Name: "cooling", Authorized: []string{"cooling"}}, now); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cooldown selection error = %v", err)
	}

	for _, test := range []struct {
		outcome FakeOutcome
		want    error
	}{{FakeSuccess, nil}, {FakeThrottle, ErrThrottled}, {FakeTimeout, ErrTimeout}, {FakePermanent, ErrPermanent}} {
		_, err = (&Fake{Outcome: test.outcome, RetryAfter: time.Minute}).Execute(context.Background(), Request{}, secret.Bundle{})
		if !errors.Is(err, test.want) {
			t.Fatalf("outcome %q error = %v, want %v", test.outcome, err, test.want)
		}
		if test.outcome == FakeThrottle {
			var throttle *ThrottleError
			if !errors.As(err, &throttle) || throttle.Until(now) != now.Add(time.Minute) {
				t.Fatalf("throttle retry guidance = %#v", err)
			}
		}
	}
}
