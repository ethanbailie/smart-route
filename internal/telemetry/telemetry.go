// Package telemetry provides the optional, low-cardinality observability surface.
package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ethan/smart-route/internal/controller"
	"github.com/ethan/smart-route/internal/scheduler"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type Config struct {
	Enabled    bool
	Metrics    bool
	Tracing    bool
	Logger     *slog.Logger
	Registerer prometheus.Registerer
	Gatherer   prometheus.Gatherer
}

type Telemetry struct {
	enabled, tracing  bool
	logger            *slog.Logger
	gatherer          prometheus.Gatherer
	tracer            trace.Tracer
	requests          *prometheus.CounterVec
	requestDuration   *prometheus.HistogramVec
	jobs              *prometheus.CounterVec
	queue             *prometheus.GaugeVec
	queueWait         *prometheus.HistogramVec
	attemptDuration   *prometheus.HistogramVec
	workers           *prometheus.GaugeVec
	sandboxes         *prometheus.GaugeVec
	provisioning      *prometheus.CounterVec
	provisionLatency  *prometheus.HistogramVec
	leases            prometheus.Counter
	activeLeases      prometheus.Gauge
	heartbeats        *prometheus.CounterVec
	heartbeatAge      prometheus.Gauge
	claims            *prometheus.CounterVec
	claimWait         *prometheus.HistogramVec
	upstreams         *prometheus.GaugeVec
	upstreamOutcomes  *prometheus.CounterVec
	upstreamThrottles *prometheus.CounterVec
	autoscaler        *prometheus.CounterVec
	desired           *prometheus.GaugeVec
	current           *prometheus.GaugeVec
	cooldown          *prometheus.GaugeVec
	mu                sync.RWMutex
	pools             map[string]PoolStatus
}

type PoolStatus struct {
	Name      string `json:"name"`
	Desired   int    `json:"desired"`
	Current   int    `json:"current"`
	Unhealthy bool   `json:"unhealthy"`
	Cooldown  bool   `json:"cooldown"`
	Reason    string `json:"reason,omitempty"`
}

func New(c Config) *Telemetry {
	t := &Telemetry{enabled: c.Enabled, tracing: c.Enabled && c.Tracing, logger: c.Logger, pools: map[string]PoolStatus{}}
	if !c.Enabled {
		return t
	}
	if t.logger == nil {
		t.logger = slog.Default()
	}
	t.logger = slog.New(&redactingHandler{next: t.logger.Handler(), max: 2048})
	t.tracer = otel.Tracer("smart-route")
	if !c.Metrics {
		return t
	}
	r := c.Registerer
	if r == nil {
		r = prometheus.NewRegistry()
	}
	if c.Gatherer != nil {
		t.gatherer = c.Gatherer
	} else if g, ok := r.(prometheus.Gatherer); ok {
		t.gatherer = g
	}
	t.requests = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "smart_route_http_requests_total", Help: "HTTP requests."}, []string{"method", "route", "status"})
	t.requestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "smart_route_http_request_duration_seconds", Help: "HTTP request duration."}, []string{"method", "route"})
	t.jobs = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "smart_route_jobs_total", Help: "Job lifecycle transitions."}, []string{"event"})
	t.queue = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "smart_route_queue_depth", Help: "Queued jobs by kind and pool requirement."}, []string{"kind", "pool_requirement"})
	t.queueWait = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "smart_route_queue_wait_seconds", Help: "Time jobs wait before a lease."}, []string{"kind"})
	t.attemptDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "smart_route_attempt_duration_seconds", Help: "Attempt execution duration."}, []string{"result"})
	t.workers = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "smart_route_workers", Help: "Workers by health."}, []string{"health"})
	t.sandboxes = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "smart_route_sandboxes", Help: "Sandboxes by provider, pool and state."}, []string{"provider", "pool", "state"})
	t.provisioning = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "smart_route_provisioning_total", Help: "Provisioning outcomes."}, []string{"provider", "result"})
	t.provisionLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "smart_route_provisioning_duration_seconds", Help: "Sandbox provisioning latency."}, []string{"provider"})
	t.leases = prometheus.NewCounter(prometheus.CounterOpts{Name: "smart_route_lease_expirations_total", Help: "Expired leases."})
	t.activeLeases = prometheus.NewGauge(prometheus.GaugeOpts{Name: "smart_route_active_leases", Help: "Currently active leases."})
	t.heartbeats = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "smart_route_heartbeats_total", Help: "Worker heartbeat outcomes."}, []string{"result"})
	t.heartbeatAge = prometheus.NewGauge(prometheus.GaugeOpts{Name: "smart_route_heartbeat_age_seconds", Help: "Age of the stalest worker heartbeat."})
	t.claims = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "smart_route_claims_total", Help: "Worker claim outcomes."}, []string{"result"})
	t.claimWait = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "smart_route_claim_wait_seconds", Help: "Long-poll claim duration."}, []string{"result"})
	t.upstreams = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "smart_route_upstreams", Help: "Upstream health."}, []string{"upstream", "state"})
	t.upstreamOutcomes = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "smart_route_upstream_requests_total", Help: "Upstream call outcomes."}, []string{"upstream", "outcome"})
	t.upstreamThrottles = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "smart_route_upstream_throttles_total", Help: "Upstream throttle events."}, []string{"upstream"})
	t.autoscaler = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "smart_route_autoscaler_decisions_total", Help: "Autoscaler decisions."}, []string{"pool", "action", "reason"})
	t.desired = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "smart_route_pool_desired", Help: "Desired pool size."}, []string{"pool"})
	t.current = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "smart_route_pool_current", Help: "Current pool size."}, []string{"pool"})
	t.cooldown = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "smart_route_pool_cooldown", Help: "Whether pool cooldown/backoff is active."}, []string{"pool"})
	r.MustRegister(t.requests, t.requestDuration, t.jobs, t.queue, t.queueWait, t.attemptDuration, t.workers, t.sandboxes, t.provisioning, t.provisionLatency, t.leases, t.activeLeases, t.heartbeats, t.heartbeatAge, t.claims, t.claimWait, t.upstreams, t.upstreamOutcomes, t.upstreamThrottles, t.autoscaler, t.desired, t.current, t.cooldown)
	return t
}

func (t *Telemetry) Enabled() bool { return t != nil && t.enabled }
func (t *Telemetry) MetricsHandler() http.Handler {
	if t == nil || !t.enabled || t.gatherer == nil {
		return nil
	}
	return promhttp.HandlerFor(t.gatherer, promhttp.HandlerOpts{})
}
func (t *Telemetry) HTTPHandler(next http.Handler) http.Handler {
	if !t.Enabled() {
		return next
	}
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &statusWriter{ResponseWriter: w, status: 200}
		next.ServeHTTP(rw, r)
		route := routeName(r.URL.Path)
		if t.requests != nil {
			t.requests.WithLabelValues(r.Method, route, http.StatusText(rw.status)).Inc()
			t.requestDuration.WithLabelValues(r.Method, route).Observe(time.Since(start).Seconds())
		}
	})
	if t.tracing {
		return otelhttp.NewHandler(h, "http.server")
	}
	return h
}
func (t *Telemetry) HTTPTransport(base http.RoundTripper) http.RoundTripper {
	if t == nil || !t.tracing {
		return base
	}
	if base == nil {
		base = http.DefaultTransport
	}
	return otelhttp.NewTransport(base)
}
func (t *Telemetry) Start(ctx context.Context, name string, attrs ...any) (context.Context, func(error)) {
	if t == nil || !t.enabled {
		return ctx, func(error) {}
	}
	t.logger.Log(ctx, slog.LevelDebug, name, attrs...)
	if !t.tracing {
		return ctx, func(error) {}
	}
	ctx, span := t.tracer.Start(ctx, name, trace.WithAttributes(traceAttrs(attrs)...))
	return ctx, func(err error) {
		if err != nil {
			span.RecordError(err)
		}
		span.End()
	}
}
func traceAttrs(values []any) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, len(values)/2)
	for i := 0; i+1 < len(values); i += 2 {
		key, ok := values[i].(string)
		if !ok {
			continue
		}
		lower := strings.ToLower(key)
		if strings.Contains(lower, "secret") || strings.Contains(lower, "token") || strings.Contains(lower, "credential") || strings.Contains(lower, "authorization") || strings.Contains(lower, "payload") {
			continue
		}
		switch value := values[i+1].(type) {
		case string:
			if len(value) > 2048 {
				value = value[:2048]
			}
			out = append(out, attribute.String(key, value))
		case fmt.Stringer:
			v := value.String()
			if len(v) > 2048 {
				v = v[:2048]
			}
			out = append(out, attribute.String(key, v))
		case int:
			out = append(out, attribute.Int(key, value))
		case int64:
			out = append(out, attribute.Int64(key, value))
		case bool:
			out = append(out, attribute.Bool(key, value))
		}
	}
	return out
}

func (t *Telemetry) ObserveSchedulingDecision(d scheduler.Decision) {
	if !t.Enabled() {
		return
	}
	t.logger.Info("scheduling decision", "job_id", d.JobID, "worker_id", d.Worker, "reason", d.Reason)
}
func (t *Telemetry) ObserveAutoscaler(d controller.ScaleDecision) {
	if !t.Enabled() {
		return
	}
	reason := normalizeReason(d.Reason)
	if t.autoscaler != nil {
		t.autoscaler.WithLabelValues(d.Pool, string(d.Action), reason).Inc()
		t.queue.WithLabelValues("all", d.Pool).Set(float64(d.Queued))
		t.desired.WithLabelValues(d.Pool).Set(float64(d.Desired))
		t.current.WithLabelValues(d.Pool).Set(float64(d.Current))
		if d.Changed+d.ProvisionFailures > 0 {
			if d.Changed > 0 {
				t.provisioning.WithLabelValues(d.Provider, "success").Add(float64(d.Changed))
			}
			if d.ProvisionFailures > 0 {
				t.provisioning.WithLabelValues(d.Provider, "failure").Add(float64(d.ProvisionFailures))
			}
			attempted := d.Changed + d.ProvisionFailures
			if attempted > 0 {
				t.provisionLatency.WithLabelValues(d.Provider).Observe(d.ProvisionDuration.Seconds() / float64(attempted))
			}
		}
	}
	cool := strings.Contains(d.Reason, "cooldown") || strings.Contains(d.Reason, "backoff")
	if t.cooldown != nil {
		if cool {
			t.cooldown.WithLabelValues(d.Pool).Set(1)
		} else {
			t.cooldown.WithLabelValues(d.Pool).Set(0)
		}
	}
	unhealthy := strings.Contains(d.Reason, "permanent") || strings.Contains(d.Reason, "unhealthy")
	t.mu.Lock()
	t.pools[d.Pool] = PoolStatus{Name: d.Pool, Desired: d.Desired, Current: d.Current, Unhealthy: unhealthy, Cooldown: cool, Reason: reason}
	t.mu.Unlock()
	t.logger.Info("autoscaler decision", "pool", d.Pool, "reason", reason, "action", d.Action, "desired", d.Desired, "current", d.Current)
}
func (t *Telemetry) PoolStatuses() []PoolStatus {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]PoolStatus, 0, len(t.pools))
	for _, p := range t.pools {
		out = append(out, p)
	}
	return out
}
func (t *Telemetry) LeaseExpired(n int) {
	if t != nil && t.leases != nil {
		t.leases.Add(float64(n))
		t.activeLeases.Sub(float64(n))
	}
}
func (t *Telemetry) Job(event string) {
	if t != nil && t.jobs != nil {
		t.jobs.WithLabelValues(event).Inc()
	}
}
func (t *Telemetry) Queue(kind, pool string, delta float64) {
	if t != nil && t.queue != nil {
		t.queue.WithLabelValues(kind, pool).Add(delta)
	}
}
func (t *Telemetry) QueueWait(kind string, d time.Duration) {
	if t != nil && t.queueWait != nil {
		t.queueWait.WithLabelValues(kind).Observe(d.Seconds())
	}
}
func (t *Telemetry) Attempt(result string, d time.Duration) {
	if t != nil && t.attemptDuration != nil {
		t.attemptDuration.WithLabelValues(result).Observe(d.Seconds())
		t.activeLeases.Dec()
	}
}
func (t *Telemetry) LeaseStarted() {
	if t != nil && t.activeLeases != nil {
		t.activeLeases.Inc()
	}
}
func (t *Telemetry) ClaimWait(result string, d time.Duration) {
	if t != nil && t.claimWait != nil {
		t.claimWait.WithLabelValues(result).Observe(d.Seconds())
	}
}
func (t *Telemetry) Provision(provider, result string, d time.Duration) {
	if t != nil && t.provisioning != nil {
		t.provisioning.WithLabelValues(provider, result).Inc()
		t.provisionLatency.WithLabelValues(provider).Observe(d.Seconds())
	}
}
func (t *Telemetry) UpstreamCall(name, outcome string, throttled bool) {
	if t != nil && t.upstreamOutcomes != nil {
		t.upstreamOutcomes.WithLabelValues(name, outcome).Inc()
		if throttled {
			t.upstreamThrottles.WithLabelValues(name).Inc()
		}
	}
}
func (t *Telemetry) HeartbeatAge(d time.Duration) {
	if t != nil && t.heartbeatAge != nil {
		t.heartbeatAge.Set(d.Seconds())
	}
}
func (t *Telemetry) Heartbeat(result string) {
	if t != nil && t.heartbeats != nil {
		t.heartbeats.WithLabelValues(result).Inc()
	}
}
func (t *Telemetry) Claim(result string) {
	if t != nil && t.claims != nil {
		t.claims.WithLabelValues(result).Inc()
	}
}
func (t *Telemetry) WorkerHealth(health string, value float64) {
	if t != nil && t.workers != nil {
		t.workers.WithLabelValues(health).Set(value)
	}
}
func (t *Telemetry) Sandbox(provider, pool, state string, value float64) {
	if t != nil && t.sandboxes != nil {
		t.sandboxes.WithLabelValues(provider, pool, state).Set(value)
	}
}
func (t *Telemetry) Upstream(name, state string, value float64) {
	if t != nil && t.upstreams != nil {
		t.upstreams.WithLabelValues(name, state).Set(value)
	}
}

func normalizeReason(v string) string {
	switch {
	case strings.Contains(v, "demand"):
		return "demand"
	case strings.Contains(v, "idle"):
		return "idle"
	case strings.Contains(v, "cooldown"):
		return "cooldown"
	case strings.Contains(v, "backoff"):
		return "backoff"
	case strings.Contains(v, "limit"):
		return "limit"
	case strings.Contains(v, "timeout"):
		return "timeout"
	case strings.Contains(v, "failure") || strings.Contains(v, "unhealthy"):
		return "provider_error"
	default:
		return "steady"
	}
}
func routeName(p string) string {
	if strings.HasPrefix(p, "/v1/jobs/") {
		return "/v1/jobs/{id}"
	}
	if strings.HasPrefix(p, "/v1/worker/attempts/") {
		return "/v1/worker/attempts/{id}"
	}
	return p
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(n int) { w.status = n; w.ResponseWriter.WriteHeader(n) }

type redactingHandler struct {
	next slog.Handler
	max  int
}

func (h *redactingHandler) Enabled(c context.Context, l slog.Level) bool { return h.next.Enabled(c, l) }
func (h *redactingHandler) Handle(c context.Context, r slog.Record) error {
	n := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	r.Attrs(func(a slog.Attr) bool { n.AddAttrs(redact(a, h.max)); return true })
	return h.next.Handle(c, n)
}
func (h *redactingHandler) WithAttrs(a []slog.Attr) slog.Handler {
	out := make([]slog.Attr, len(a))
	for i := range a {
		out[i] = redact(a[i], h.max)
	}
	return &redactingHandler{next: h.next.WithAttrs(out), max: h.max}
}
func (h *redactingHandler) WithGroup(n string) slog.Handler {
	return &redactingHandler{next: h.next.WithGroup(n), max: h.max}
}
func attrsToAny(attrs []slog.Attr) []any {
	out := make([]any, len(attrs))
	for i := range attrs {
		out[i] = attrs[i]
	}
	return out
}

func redact(a slog.Attr, max int) slog.Attr {
	if a.Value.Kind() == slog.KindGroup {
		values := a.Value.Group()
		for i := range values {
			values[i] = redact(values[i], max)
		}
		return slog.Group(a.Key, attrsToAny(values)...)
	}
	k := strings.ToLower(a.Key)
	if strings.Contains(k, "token") || strings.Contains(k, "secret") || strings.Contains(k, "credential") || strings.Contains(k, "authorization") || strings.Contains(k, "payload") {
		return slog.String(a.Key, "[REDACTED]")
	}
	if a.Value.Kind() == slog.KindString && len(a.Value.String()) > max {
		return slog.String(a.Key, a.Value.String()[:max]+"…")
	}
	return a
}
