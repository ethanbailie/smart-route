package telemetry_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/ethan/smart-route/internal/controller"
	"github.com/ethan/smart-route/internal/telemetry"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestAutoscalerAndLeaseMetrics(t *testing.T) {
	r := prometheus.NewRegistry()
	obs := telemetry.New(telemetry.Config{Enabled: true, Metrics: true, Registerer: r})
	obs.ObserveAutoscaler(controller.ScaleDecision{Pool: "cpu", Action: controller.ScaleProvision, Reason: "compatible queued demand exceeds available capacity", Queued: 4, Current: 1, Desired: 3, Changed: 2})
	obs.ObserveAutoscaler(controller.ScaleDecision{Pool: "cpu", Action: controller.ScaleDrain, Reason: "idle capacity exceeds stabilized desired replicas", Current: 3, Desired: 1, Changed: 2})
	obs.LeaseExpired(2)
	obs.Queue("command", "cpu", 1)
	for _, event := range []string{"started", "completed", "failed", "retried"} {
		obs.Job(event)
	}
	obs.QueueWait("command", time.Second)
	obs.LeaseStarted()
	obs.Attempt("completed", 2*time.Second)
	obs.HeartbeatAge(3 * time.Second)
	obs.Claim("leased")
	obs.ClaimWait("leased", 10*time.Millisecond)
	obs.Upstream("primary", "cooldown", 1)
	obs.UpstreamCall("primary", "throttled", true)
	if got := testutil.ToFloat64(metric(t, r, "smart_route_lease_expirations_total")); got != 2 {
		t.Fatalf("lease expirations = %v", got)
	}
	if got := testutil.CollectAndCount(r, "smart_route_autoscaler_decisions_total"); got != 2 {
		t.Fatalf("autoscaler decision series = %d", got)
	}
	required := []string{"smart_route_queue_depth", "smart_route_jobs_total", "smart_route_queue_wait_seconds", "smart_route_attempt_duration_seconds", "smart_route_provisioning_duration_seconds", "smart_route_active_leases", "smart_route_heartbeat_age_seconds", "smart_route_claim_wait_seconds", "smart_route_upstream_requests_total", "smart_route_upstream_throttles_total", "smart_route_pool_desired", "smart_route_pool_current"}
	families, err := r.Gather()
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, family := range families {
		seen[family.GetName()] = true
		for _, sample := range family.Metric {
			for _, label := range sample.Label {
				if label.GetName() == "id" || strings.HasSuffix(label.GetName(), "_id") {
					t.Fatalf("high-cardinality ID label %q on %s", label.GetName(), family.GetName())
				}
			}
		}
	}
	for _, name := range required {
		if !seen[name] {
			t.Errorf("required metric family %s not emitted", name)
		}
	}
}

func TestLogsRedactSecretsAndDisabledIsSilent(t *testing.T) {
	var out bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug}))
	obs := telemetry.New(telemetry.Config{Enabled: true, Logger: logger})
	ctx, finish := obs.Start(context.Background(), "provider.create", "provider", "local", "sandbox_id", "box-1", "bootstrap_token", "very-secret", "payload", strings.Repeat("x", 4096))
	_ = ctx
	finish(nil)
	text := out.String()
	if strings.Contains(text, "very-secret") || strings.Contains(text, strings.Repeat("x", 100)) {
		t.Fatalf("sensitive or unbounded data logged: %s", text)
	}
	if !strings.Contains(text, "[REDACTED]") || !strings.Contains(text, "sandbox_id") {
		t.Fatalf("missing structured/redacted fields: %s", text)
	}
	out.Reset()
	disabled := telemetry.New(telemetry.Config{Logger: logger})
	_, done := disabled.Start(context.Background(), "hidden", "secret", "value")
	done(nil)
	if out.Len() != 0 || disabled.MetricsHandler() != nil {
		t.Fatal("disabled telemetry emitted output")
	}
}

func metric(t *testing.T, gatherer prometheus.Gatherer, name string) prometheus.Collector {
	t.Helper()
	// Gatherers are not collectors, so use a tiny counter projection for testutil.
	families, err := gatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == name {
			c := prometheus.NewCounter(prometheus.CounterOpts{Name: "projected"})
			c.Add(family.Metric[0].GetCounter().GetValue())
			return c
		}
	}
	t.Fatalf("metric %s not found", name)
	return nil
}
