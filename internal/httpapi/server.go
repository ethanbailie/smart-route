package httpapi

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/ethan/smart-route/internal/buildinfo"
	"github.com/ethan/smart-route/internal/checkpoint"
	"github.com/ethan/smart-route/internal/domain"
	"github.com/ethan/smart-route/internal/sandbox"
	"github.com/ethan/smart-route/internal/scheduler"
	"github.com/ethan/smart-route/internal/store"
	"github.com/ethan/smart-route/internal/telemetry"
)

const (
	CodeInvalidRequest               = "invalid_request"
	CodeDuplicateIdempotencyConflict = "duplicate_idempotency_conflict"
	CodeJobNotFound                  = "job_not_found"
	CodeUnavailableCapacity          = "unavailable_capacity"
	CodeCanceledJob                  = "canceled_job"
	CodeInternalError                = "internal_error"
)

type Config struct {
	RequestTimeout, ReadTimeout, WriteTimeout, IdleTimeout, ShutdownTimeout time.Duration
	HeartbeatInterval, LeaseDuration, WorkerTimeout, MaxClaimWait           time.Duration
	BootstrapTokenTTL, WorkerSessionTTL                                     time.Duration
	Scheduler                                                               scheduler.Scheduler
	ArtifactStore                                                           ArtifactStore
	PublicAuthToken                                                         string
	RequireTLS, InsecureLocalMode                                           bool
	InlineResultBytes, MaxResultBytes, MaxEvents                            int
	Telemetry                                                               *telemetry.Telemetry
	CheckpointAdapter                                                       checkpoint.Adapter
	CheckpointTTL                                                           time.Duration
	RecoveryBackoff                                                         time.Duration
	Providers                                                               interface {
		Get(string) (sandbox.Provider, error)
	}
}
type API struct {
	store                                                                  store.Store
	timeout, heartbeatInterval, leaseDuration, workerTimeout, maxClaimWait time.Duration
	bootstrapTokenTTL, workerSessionTTL                                    time.Duration
	wake                                                                   chan struct{}
	scheduler                                                              scheduler.Scheduler
	artifacts                                                              ArtifactStore
	inlineResultBytes, maxResultBytes, maxEvents                           int
	publicTokenHash                                                        [32]byte
	publicAuth, requireTLS, insecureLocalMode                              bool
	telemetry                                                              *telemetry.Telemetry
	checkpoints                                                            checkpoint.Adapter
	checkpointTTL, recoveryBackoff                                         time.Duration
	providers                                                              interface {
		Get(string) (sandbox.Provider, error)
	}
}

type ArtifactStore interface {
	Put(context.Context, string, []byte) error
	Get(context.Context, string) ([]byte, error)
}
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
type errorEnvelope struct {
	Error Error `json:"error"`
}
type dataEnvelope struct {
	Data any `json:"data"`
}
type Duration time.Duration

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if json.Unmarshal(b, &s) == nil {
		v, e := time.ParseDuration(s)
		*d = Duration(v)
		return e
	}
	var n int64
	if e := json.Unmarshal(b, &n); e != nil {
		return errors.New("duration must be a duration string or nanoseconds")
	}
	*d = Duration(n)
	return nil
}
func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

type Retry struct {
	MaxAttempts int      `json:"max_attempts"`
	Backoff     Duration `json:"backoff,omitempty"`
	MaxBackoff  Duration `json:"max_backoff,omitempty"`
	MaxElapsed  Duration `json:"max_elapsed,omitempty"`
}
type Constraints struct {
	Capabilities      []string          `json:"capabilities"`
	Labels            map[string]string `json:"labels"`
	Upstream          *string           `json:"upstream"`
	Architecture      string            `json:"architecture,omitempty"`
	Region            string            `json:"region,omitempty"`
	ExecutorKind      string            `json:"executor_kind,omitempty"`
	PreferredRegion   string            `json:"preferred_region,omitempty"`
	PreferredSandbox  string            `json:"preferred_sandbox,omitempty"`
	PreferredProvider string            `json:"preferred_provider,omitempty"`
	MaxCost           *float64          `json:"max_cost,omitempty"`
}
type SubmitJob struct {
	IdempotencyKey string          `json:"idempotency_key"`
	Kind           string          `json:"kind"`
	Payload        json.RawMessage `json:"payload"`
	Constraints    Constraints     `json:"constraints"`
	TimeoutSeconds int64           `json:"timeout_seconds"`
	Retry          Retry           `json:"retry"`
	SessionID      string          `json:"session_id,omitempty"`
	DependsOn      []string        `json:"depends_on,omitempty"`
}
type Job struct {
	ID             string          `json:"id"`
	IdempotencyKey string          `json:"idempotency_key"`
	Kind           string          `json:"kind"`
	Payload        json.RawMessage `json:"payload"`
	State          string          `json:"state"`
	Constraints    Constraints     `json:"constraints"`
	TimeoutSeconds int64           `json:"timeout_seconds"`
	TimeoutAt      *time.Time      `json:"timeout_at,omitempty"`
	Retry          Retry           `json:"retry"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	SessionID      string          `json:"session_id,omitempty"`
	DependsOn      []string        `json:"depends_on,omitempty"`
}
type CreateSession struct {
	Pool              string                `json:"pool"`
	Capabilities      []string              `json:"capabilities,omitempty"`
	Labels            map[string]string     `json:"labels,omitempty"`
	PreferredProvider string                `json:"preferred_provider,omitempty"`
	IdleTTL           Duration              `json:"idle_ttl"`
	MaxLifetime       Duration              `json:"max_lifetime"`
	RecoveryPolicy    domain.RecoveryPolicy `json:"recovery_policy"`
	CheckpointMode    domain.CheckpointMode `json:"checkpoint_mode"`
	RebuildPlan       []domain.RebuildStep  `json:"rebuild_plan,omitempty"`
}
type Session struct {
	ID                  string                `json:"id"`
	Pool                string                `json:"pool"`
	Capabilities        domain.Capabilities   `json:"capabilities"`
	PreferredProvider   string                `json:"preferred_provider,omitempty"`
	Labels              map[string]string     `json:"labels,omitempty"`
	SandboxID           string                `json:"sandbox_id,omitempty"`
	WorkerID            string                `json:"worker_id,omitempty"`
	State               string                `json:"state"`
	IdleTTL             Duration              `json:"idle_ttl"`
	MaxLifetime         Duration              `json:"max_lifetime"`
	CreatedAt           time.Time             `json:"created_at"`
	LastActivity        time.Time             `json:"last_activity"`
	IdleExpiresAt       *time.Time            `json:"idle_expires_at,omitempty"`
	ClosedAt            *time.Time            `json:"closed_at,omitempty"`
	Failure             *domain.Failure       `json:"failure,omitempty"`
	RecoveryPolicy      domain.RecoveryPolicy `json:"recovery_policy"`
	CheckpointMode      domain.CheckpointMode `json:"checkpoint_mode"`
	RebuildPlan         []domain.RebuildStep  `json:"rebuild_plan,omitempty"`
	Epoch               uint64                `json:"epoch"`
	RecoveryState       domain.RecoveryState  `json:"recovery_state"`
	RecoveryAttempts    int                   `json:"recovery_attempts"`
	RecoveryAfter       *time.Time            `json:"recovery_after,omitempty"`
	RecoveryError       string                `json:"recovery_error,omitempty"`
	LatestCheckpointID  string                `json:"latest_checkpoint_id,omitempty"`
	RestoreAcknowledged bool                  `json:"restore_acknowledged"`
}
type Attempt struct {
	ID        string          `json:"id"`
	Number    int             `json:"number"`
	State     string          `json:"state"`
	WorkerID  string          `json:"worker_id,omitempty"`
	SandboxID string          `json:"sandbox_id,omitempty"`
	Failure   *domain.Failure `json:"failure,omitempty"`
	StartedAt *time.Time      `json:"started_at,omitempty"`
	EndedAt   *time.Time      `json:"ended_at,omitempty"`
}
type Event struct {
	ID             string            `json:"id"`
	Sequence       uint64            `json:"sequence"`
	Type           string            `json:"type"`
	JobID          string            `json:"job_id"`
	AttemptID      string            `json:"attempt_id,omitempty"`
	OccurredAt     time.Time         `json:"occurred_at"`
	Data           map[string]string `json:"data,omitempty"`
	WorkerSequence uint64            `json:"worker_sequence,omitempty"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
}
type Result struct {
	JobID      string            `json:"job_id"`
	AttemptID  string            `json:"attempt_id"`
	StatusCode int               `json:"status_code"`
	Data       []byte            `json:"data"`
	Metadata   map[string]string `json:"metadata,omitempty"`
	CreatedAt  time.Time         `json:"created_at"`
}
type Worker struct {
	ID           string              `json:"id"`
	Capabilities domain.Capabilities `json:"capabilities"`
	LastSeenAt   time.Time           `json:"last_seen_at"`
}
type Sandbox struct {
	ID           string              `json:"id"`
	WorkerID     string              `json:"worker_id"`
	Capabilities domain.Capabilities `json:"capabilities"`
	State        string              `json:"state"`
	CreatedAt    time.Time           `json:"created_at"`
}

func New(s store.Store, c Config) *API {
	t := c.RequestTimeout
	if t <= 0 {
		t = 30 * time.Second
	}
	heartbeat := c.HeartbeatInterval
	if heartbeat <= 0 {
		heartbeat = 10 * time.Second
	}
	lease := c.LeaseDuration
	if lease <= 0 {
		lease = 30 * time.Second
	}
	workerTimeout := c.WorkerTimeout
	if workerTimeout <= 0 {
		workerTimeout = 3 * heartbeat
	}
	wait := c.MaxClaimWait
	if wait <= 0 || wait >= t {
		wait = t - time.Second
	}
	if wait <= 0 {
		wait = time.Second
	}
	policy := c.Scheduler
	if policy == nil {
		policy = scheduler.New(scheduler.Config{})
	}
	bootstrapTTL, sessionTTL := c.BootstrapTokenTTL, c.WorkerSessionTTL
	if bootstrapTTL <= 0 {
		bootstrapTTL = 5 * time.Minute
	}
	if sessionTTL <= 0 {
		sessionTTL = 5 * time.Minute
	}
	inline, maxResult, maxEvents := c.InlineResultBytes, c.MaxResultBytes, c.MaxEvents
	if inline <= 0 {
		inline = 64 << 10
	}
	if maxResult <= 0 {
		maxResult = 8 << 20
	}
	if maxEvents <= 0 {
		maxEvents = 100
	}
	backoff := c.RecoveryBackoff
	if backoff <= 0 {
		backoff = time.Second
	}
	api := &API{store: s, timeout: t, heartbeatInterval: heartbeat, leaseDuration: lease, workerTimeout: workerTimeout, maxClaimWait: wait, wake: make(chan struct{}, 1), scheduler: policy, artifacts: c.ArtifactStore, inlineResultBytes: inline, maxResultBytes: maxResult, maxEvents: maxEvents, bootstrapTokenTTL: bootstrapTTL, workerSessionTTL: sessionTTL, requireTLS: c.RequireTLS, insecureLocalMode: c.InsecureLocalMode, telemetry: c.Telemetry, checkpoints: c.CheckpointAdapter, checkpointTTL: c.CheckpointTTL, recoveryBackoff: backoff, providers: c.Providers}
	if c.PublicAuthToken != "" {
		api.publicAuth = true
		api.publicTokenHash = sha256.Sum256([]byte(c.PublicAuthToken))
	}
	return api
}
func (a *API) Handler() http.Handler {
	h := http.TimeoutHandler(http.HandlerFunc(a.serveHTTP), a.timeout, `{"error":{"code":"internal_error","message":"request timed out"}}`)
	if a.telemetry != nil {
		return a.telemetry.HTTPHandler(h)
	}
	return h
}
func (a *API) HTTPServer(addr string, c Config) *http.Server {
	return &http.Server{Addr: addr, Handler: a.Handler(), ReadHeaderTimeout: c.ReadTimeout, ReadTimeout: c.ReadTimeout, WriteTimeout: c.WriteTimeout, IdleTimeout: c.IdleTimeout}
}
func Shutdown(ctx context.Context, s *http.Server, t time.Duration) error {
	if t <= 0 {
		t = 10 * time.Second
	}
	c, cancel := context.WithTimeout(ctx, t)
	defer cancel()
	return s.Shutdown(c)
}
func (a *API) serveHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.TLS == nil && a.requireTLS {
		host, _, _ := net.SplitHostPort(r.RemoteAddr)
		ip := net.ParseIP(host)
		if !a.insecureLocalMode || ip == nil || !ip.IsLoopback() {
			fail(w, http.StatusUpgradeRequired, "tls_required", "TLS is required")
			return
		}
	}
	if a.publicAuth && strings.HasPrefix(r.URL.Path, "/v1/") && !strings.HasPrefix(r.URL.Path, "/v1/worker/") {
		auth := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		actual := sha256.Sum256([]byte(auth))
		if auth == "" || subtle.ConstantTimeCompare(actual[:], a.publicTokenHash[:]) != 1 {
			fail(w, http.StatusUnauthorized, "invalid_public_token", "public API credential is invalid")
			return
		}
	}
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/metrics" && a.telemetry != nil && a.telemetry.MetricsHandler() != nil:
		a.telemetry.MetricsHandler().ServeHTTP(w, r)
	case r.URL.Path == "/healthz":
		write(w, 200, dataEnvelope{map[string]string{"status": "ok"}})
	case r.URL.Path == "/readyz":
		write(w, 200, dataEnvelope{map[string]string{"status": "ready"}})
	case r.Method == http.MethodGet && r.URL.Path == "/versionz":
		write(w, 200, dataEnvelope{buildinfo.Current()})
	case r.Method == http.MethodPost && r.URL.Path == "/v1/jobs":
		a.submit(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/sessions":
		a.createSession(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/worker/register":
		a.registerWorker(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/worker/heartbeat":
		a.heartbeatWorker(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/worker/recovery/ack":
		a.acknowledgeRecovery(w, r)
	case (r.Method == http.MethodPost || r.Method == http.MethodGet) && r.URL.Path == "/v1/worker/claim":
		a.claimWorker(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/v1/workers":
		a.workers(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/v1/sandboxes":
		a.sandboxes(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/v1/admin/status":
		a.adminStatus(w, r)
	default:
		p := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(p) == 5 && p[0] == "v1" && p[1] == "worker" && p[2] == "attempts" && r.Method == http.MethodPost {
			a.workerAttemptRoute(w, r, domain.AttemptID(p[3]), p[4])
			return
		}
		if len(p) >= 3 && p[0] == "v1" && p[1] == "jobs" {
			a.jobRoute(w, r, domain.JobID(p[2]), p[3:])
			return
		}
		if len(p) >= 3 && p[0] == "v1" && p[1] == "sessions" {
			a.sessionRoute(w, r, domain.SessionID(p[2]), p[3:])
			return
		}
		fail(w, 404, CodeJobNotFound, "job not found")
	}
}
func (a *API) submit(w http.ResponseWriter, r *http.Request) {
	var req SubmitJob
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20))
	d.DisallowUnknownFields()
	if e := d.Decode(&req); e != nil {
		fail(w, 400, CodeInvalidRequest, e.Error())
		return
	}
	if req.IdempotencyKey == "" || req.Kind == "" || len(req.Payload) == 0 || !json.Valid(req.Payload) || req.TimeoutSeconds < 0 || req.Retry.MaxAttempts < 0 || req.Retry.Backoff < 0 || req.Retry.MaxBackoff < 0 || req.Retry.MaxElapsed < 0 {
		fail(w, 400, CodeInvalidRequest, "idempotency_key, kind, valid payload, and non-negative timeout/retry values are required")
		return
	}
	candidate := requestJob(req, time.Now().UTC())
	if candidate.SessionID != "" {
		session, e := a.store.GetSession(r.Context(), candidate.SessionID)
		if e != nil {
			fail(w, 400, CodeInvalidRequest, "session not found")
			return
		}
		candidate.Constraints.Capabilities = append(candidate.Constraints.Capabilities, session.Capabilities.Capabilities...)
		if candidate.Constraints.Labels == nil {
			candidate.Constraints.Labels = map[string]string{}
		}
		for k, v := range session.Labels {
			candidate.Constraints.Labels[k] = v
		}
		if candidate.Constraints.PreferredProvider == "" {
			candidate.Constraints.PreferredProvider = session.PreferredProvider
		}
	}
	if old, e := a.store.GetJobByIdempotencyKey(r.Context(), req.IdempotencyKey); e == nil {
		if !sameSubmission(old, candidate) {
			fail(w, 409, CodeDuplicateIdempotencyConflict, "idempotency key was already used for a different submission")
			return
		}
		write(w, 200, dataEnvelope{jobDTO(old)})
		return
	} else if !errors.Is(e, store.ErrNotFound) {
		internal(w)
		return
	}
	got, e := a.store.CreateJob(r.Context(), candidate)
	if e != nil {
		if errors.Is(e, store.ErrConflict) || errors.Is(e, store.ErrNotFound) {
			fail(w, 400, CodeInvalidRequest, e.Error())
			return
		}
		internal(w)
		return
	}
	if got.ID != candidate.ID && !sameSubmission(got, candidate) {
		fail(w, 409, CodeDuplicateIdempotencyConflict, "idempotency key was already used for a different submission")
		return
	}
	status := 201
	if got.ID != candidate.ID {
		status = 200
	}
	if status == 201 {
		if a.telemetry != nil {
			a.telemetry.Queue(req.Kind, poolRequirement(candidate), 1)
		}
		select {
		case a.wake <- struct{}{}:
		default:
		}
	}
	write(w, status, dataEnvelope{jobDTO(got)})
}
func requestJob(r SubmitJob, now time.Time) domain.Job {
	up := ""
	if r.Constraints.Upstream != nil {
		up = *r.Constraints.Upstream
	}
	max := r.Retry.MaxAttempts
	if max == 0 {
		max = 1
	}
	deps := make([]domain.JobID, len(r.DependsOn))
	for i, v := range r.DependsOn {
		deps[i] = domain.JobID(v)
	}
	j := domain.Job{ID: domain.JobID(newID("job")), IdempotencyKey: r.IdempotencyKey, Kind: r.Kind, Payload: r.Payload, State: domain.JobQueued, SessionID: domain.SessionID(r.SessionID), DependsOn: deps, Constraints: domain.RoutingConstraints{Capabilities: r.Constraints.Capabilities, Labels: r.Constraints.Labels, Architecture: domain.Architecture(r.Constraints.Architecture), Region: r.Constraints.Region, ExecutorKind: domain.ExecutorKind(r.Constraints.ExecutorKind), RequiredUpstream: up, PreferredRegion: r.Constraints.PreferredRegion, PreferredSandbox: domain.SandboxID(r.Constraints.PreferredSandbox), PreferredProvider: r.Constraints.PreferredProvider, MaxCost: r.Constraints.MaxCost}, RetryPolicy: domain.RetryPolicy{MaxAttempts: max, Backoff: time.Duration(r.Retry.Backoff), MaxBackoff: time.Duration(r.Retry.MaxBackoff), MaxElapsed: time.Duration(r.Retry.MaxElapsed)}, CreatedAt: now, UpdatedAt: now}
	if r.TimeoutSeconds > 0 {
		j.TimeoutAt = now.Add(time.Duration(r.TimeoutSeconds) * time.Second)
	}
	return j
}
func poolRequirement(j domain.Job) string {
	if value := j.Constraints.Labels["smart-route.pool"]; value != "" {
		return value
	}
	return "any"
}

func sameSubmission(a, b domain.Job) bool {
	var av, bv any
	if json.Unmarshal(a.Payload, &av) != nil || json.Unmarshal(b.Payload, &bv) != nil {
		return false
	}
	return a.Kind == b.Kind && reflect.DeepEqual(av, bv) && reflect.DeepEqual(a.Constraints, b.Constraints) && a.RetryPolicy == b.RetryPolicy && a.SessionID == b.SessionID && reflect.DeepEqual(a.DependsOn, b.DependsOn) && timeoutSeconds(a) == timeoutSeconds(b)
}

func sessionDTO(v domain.Session) Session {
	d := Session{ID: string(v.ID), Pool: v.Pool, Capabilities: v.Capabilities, PreferredProvider: v.PreferredProvider, Labels: v.Labels, SandboxID: string(v.SandboxID), WorkerID: string(v.WorkerID), State: string(v.State), IdleTTL: Duration(v.IdleTTL), MaxLifetime: Duration(v.MaxLifetime), CreatedAt: v.CreatedAt, LastActivity: v.LastActivity, Failure: v.Failure, RecoveryPolicy: v.RecoveryPolicy, CheckpointMode: v.CheckpointMode, RebuildPlan: v.RebuildPlan, Epoch: v.Epoch, RecoveryState: v.RecoveryState, RecoveryAttempts: v.RecoveryAttempts, RecoveryError: v.RecoveryError, LatestCheckpointID: v.LatestCheckpointID, RestoreAcknowledged: v.RestoreAcknowledged}
	if !v.RecoveryAfter.IsZero() {
		d.RecoveryAfter = &v.RecoveryAfter
	}
	if !v.IdleExpiresAt.IsZero() {
		d.IdleExpiresAt = &v.IdleExpiresAt
	}
	if !v.ClosedAt.IsZero() {
		d.ClosedAt = &v.ClosedAt
	}
	return d
}
func (a *API) createSession(w http.ResponseWriter, r *http.Request) {
	var req CreateSession
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	d.DisallowUnknownFields()
	if err := d.Decode(&req); err != nil || req.Pool == "" || req.IdleTTL < 0 || req.MaxLifetime < 0 {
		fail(w, 400, CodeInvalidRequest, "pool and non-negative lifetimes are required")
		return
	}
	n := time.Now().UTC()
	labels := map[string]string{}
	for k, v := range req.Labels {
		labels[k] = v
	}
	labels["smart-route.pool"] = req.Pool
	v, err := a.store.CreateSession(r.Context(), domain.Session{ID: domain.SessionID(newID("session")), Pool: req.Pool, Capabilities: domain.Capabilities{Capabilities: req.Capabilities, Labels: labels}, Labels: labels, PreferredProvider: req.PreferredProvider, IdleTTL: time.Duration(req.IdleTTL), MaxLifetime: time.Duration(req.MaxLifetime), CreatedAt: n, LastActivity: n, RecoveryPolicy: req.RecoveryPolicy, CheckpointMode: req.CheckpointMode, RebuildPlan: req.RebuildPlan})
	if err != nil {
		internal(w)
		return
	}
	write(w, 201, dataEnvelope{sessionDTO(v)})
}
func (a *API) sessionRoute(w http.ResponseWriter, r *http.Request, id domain.SessionID, rest []string) {
	if len(rest) == 0 && r.Method == http.MethodGet {
		v, e := a.store.GetSession(r.Context(), id)
		if e != nil {
			jobError(w, e)
			return
		}
		write(w, 200, dataEnvelope{sessionDTO(v)})
		return
	}
	if len(rest) == 1 && rest[0] == "jobs" && r.Method == http.MethodGet {
		xs, e := a.store.ListSessionJobs(r.Context(), id)
		if e != nil {
			jobError(w, e)
			return
		}
		out := make([]Job, len(xs))
		for i, v := range xs {
			out[i] = jobDTO(v)
		}
		write(w, 200, dataEnvelope{out})
		return
	}
	if len(rest) == 1 && rest[0] == "close" && r.Method == http.MethodPost {
		if e := a.store.CloseSession(r.Context(), id, time.Now().UTC()); e != nil {
			jobError(w, e)
			return
		}
		v, e := a.store.GetSession(r.Context(), id)
		if e != nil {
			jobError(w, e)
			return
		}
		write(w, 200, dataEnvelope{sessionDTO(v)})
		return
	}
	if len(rest) == 1 && rest[0] == "recover" && r.Method == http.MethodPost {
		if e := a.store.RequestRecovery(r.Context(), id, time.Now().UTC()); e != nil {
			jobError(w, e)
			return
		}
		v, e := a.store.GetSession(r.Context(), id)
		if e != nil {
			jobError(w, e)
			return
		}
		write(w, 202, dataEnvelope{sessionDTO(v)})
		return
	}
	if len(rest) == 1 && rest[0] == "checkpoints" && r.Method == http.MethodGet {
		xs, e := a.store.ListCheckpoints(r.Context(), id)
		if e != nil {
			jobError(w, e)
			return
		}
		write(w, 200, dataEnvelope{xs})
		return
	}
	if len(rest) == 1 && rest[0] == "recovery-events" && r.Method == http.MethodGet {
		events, e := a.store.ListRecoveryEvents(r.Context(), id)
		if e != nil {
			jobError(w, e)
			return
		}
		write(w, 200, dataEnvelope{events})
		return
	}
	if len(rest) == 1 && rest[0] == "checkpoints" && r.Method == http.MethodPost {
		var req struct {
			Data  []byte `json:"data"`
			Epoch uint64 `json:"epoch"`
		}
		if !decodeWorkerJSON(w, r, &req) {
			return
		}
		cp, e := a.saveCheckpoint(r.Context(), id, req.Epoch, req.Data, time.Now().UTC())
		if e != nil {
			jobError(w, e)
			return
		}
		write(w, 201, dataEnvelope{cp})
		return
	}
	fail(w, 404, CodeJobNotFound, "session not found")
}

func (a *API) saveCheckpoint(ctx context.Context, id domain.SessionID, epoch uint64, data []byte, at time.Time) (domain.Checkpoint, error) {
	if a.checkpoints == nil {
		return domain.Checkpoint{}, fmt.Errorf("checkpoint adapter is not configured")
	}
	s, e := a.store.GetSession(ctx, id)
	if e != nil {
		return domain.Checkpoint{}, e
	}
	if s.RecoveryPolicy != domain.RecoveryCheckpoint || s.State != domain.SessionActive || epoch != s.Epoch {
		return domain.Checkpoint{}, store.ErrConflict
	}
	if strategic, ok := a.checkpoints.(checkpoint.Strategic); ok && strategic.Strategy() == checkpoint.StrategyProviderSnapshot {
		if a.providers == nil || s.SandboxID == "" {
			return domain.Checkpoint{}, fmt.Errorf("provider snapshot strategy requires an active sandbox provider")
		}
		box, e := a.store.GetSandbox(ctx, s.SandboxID)
		if e != nil {
			return domain.Checkpoint{}, e
		}
		provider, e := a.providers.Get(box.Provider)
		if e != nil {
			return domain.Checkpoint{}, e
		}
		snapshotter, ok := provider.(sandbox.Snapshotter)
		if !ok {
			return domain.Checkpoint{}, fmt.Errorf("provider %s does not support native snapshots", box.Provider)
		}
		var native bytes.Buffer
		if e = snapshotter.CreateSnapshot(ctx, s.SandboxID, &native); e != nil {
			return domain.Checkpoint{}, e
		}
		data = native.Bytes()
	}
	cp := domain.Checkpoint{ID: newID("checkpoint"), SessionID: id, Epoch: s.Epoch, Adapter: a.checkpoints.Name(), State: "creating", CreatedAt: at}
	if a.checkpointTTL > 0 {
		cp.ExpiresAt = at.Add(a.checkpointTTL)
	}
	if e = a.store.CreateCheckpoint(ctx, cp); e != nil {
		return cp, e
	}
	cp.Location, cp.Checksum, cp.Size, e = a.checkpoints.Save(ctx, cp, bytes.NewReader(data))
	if e != nil {
		_ = a.store.MarkCheckpoint(ctx, cp.ID, "partial", e.Error())
		return cp, e
	}
	if e = a.store.CompleteCheckpoint(ctx, cp.ID, id, s.Epoch, cp.Location, cp.Checksum, cp.Size, at); e != nil {
		_ = a.checkpoints.Delete(ctx, cp)
		return cp, e
	}
	cp.State = "ready"
	return cp, nil
}
func timeoutSeconds(j domain.Job) int64 {
	if j.TimeoutAt.IsZero() {
		return 0
	}
	return int64(j.TimeoutAt.Sub(j.CreatedAt) / time.Second)
}
func (a *API) jobRoute(w http.ResponseWriter, r *http.Request, id domain.JobID, rest []string) {
	if len(rest) == 0 && r.Method == http.MethodGet {
		j, e := a.store.GetJob(r.Context(), id)
		if e != nil {
			jobError(w, e)
			return
		}
		write(w, 200, dataEnvelope{jobDTO(j)})
		return
	}
	if len(rest) == 1 && rest[0] == "attempts" && r.Method == http.MethodGet {
		j, e := a.store.GetJob(r.Context(), id)
		if e != nil {
			jobError(w, e)
			return
		}
		out := make([]Attempt, len(j.Attempts))
		for i, x := range j.Attempts {
			out[i] = attemptDTO(x)
		}
		write(w, 200, dataEnvelope{out})
		return
	}
	if len(rest) == 1 && rest[0] == "events" && r.Method == http.MethodGet {
		if _, e := a.store.GetJob(r.Context(), id); e != nil {
			jobError(w, e)
			return
		}
		after, e := strconv.ParseUint(r.URL.Query().Get("after"), 10, 64)
		if r.URL.Query().Get("after") == "" {
			after, e = 0, nil
		}
		if e != nil {
			fail(w, 400, CodeInvalidRequest, "after must be a non-negative sequence")
			return
		}
		limit := a.maxEvents
		if raw := r.URL.Query().Get("limit"); raw != "" {
			n, err := strconv.Atoi(raw)
			if err != nil || n <= 0 {
				fail(w, 400, CodeInvalidRequest, "limit must be positive")
				return
			}
			if n < limit {
				limit = n
			}
		}
		xs, e := a.store.ListEvents(r.Context(), id, after, limit)
		if e != nil {
			jobError(w, e)
			return
		}
		out := make([]Event, len(xs))
		for i, x := range xs {
			out[i] = eventDTO(x)
		}
		write(w, 200, dataEnvelope{out})
		return
	}
	if len(rest) == 1 && rest[0] == "result" && r.Method == http.MethodGet {
		result, e := a.store.GetResult(r.Context(), id)
		if e != nil {
			jobError(w, e)
			return
		}
		if result.ArtifactKey != "" {
			if a.artifacts == nil {
				internal(w)
				return
			}
			result.Data, e = a.artifacts.Get(r.Context(), result.ArtifactKey)
			if e != nil {
				internal(w)
				return
			}
		}
		write(w, 200, dataEnvelope{Result{string(result.JobID), string(result.AttemptID), result.StatusCode, result.Data, result.Metadata, result.CreatedAt}})
		return
	}
	if len(rest) == 1 && rest[0] == "cancel" && r.Method == http.MethodPost {
		e := a.store.CancelJob(r.Context(), id, time.Now().UTC())
		if errors.Is(e, store.ErrConflict) {
			fail(w, 409, CodeCanceledJob, "job can no longer be canceled")
			return
		}
		if e != nil {
			jobError(w, e)
			return
		}
		j, e := a.store.GetJob(r.Context(), id)
		if e != nil {
			jobError(w, e)
			return
		}
		write(w, 200, dataEnvelope{jobDTO(j)})
		return
	}
	fail(w, 404, CodeJobNotFound, "job not found")
}
func (a *API) workers(w http.ResponseWriter, r *http.Request) {
	xs, e := a.store.ListWorkers(r.Context())
	if e != nil {
		internal(w)
		return
	}
	out := make([]Worker, len(xs))
	for i, x := range xs {
		out[i] = Worker{string(x.ID), x.Capabilities, x.LastSeenAt}
	}
	write(w, 200, dataEnvelope{out})
}
func (a *API) sandboxes(w http.ResponseWriter, r *http.Request) {
	xs, e := a.store.ListSandboxes(r.Context())
	if e != nil {
		internal(w)
		return
	}
	out := make([]Sandbox, len(xs))
	for i, x := range xs {
		out[i] = Sandbox{string(x.ID), string(x.WorkerID), x.Capabilities, x.State, x.CreatedAt}
	}
	write(w, 200, dataEnvelope{out})
}
func (a *API) adminStatus(w http.ResponseWriter, r *http.Request) {
	jobs, err := a.store.ListQueuedJobs(r.Context(), store.QueueQuery{Limit: 100000})
	if err != nil {
		internal(w)
		return
	}
	workers, err := a.store.ListWorkers(r.Context())
	if err != nil {
		internal(w)
		return
	}
	boxes, err := a.store.ListSandboxes(r.Context())
	if err != nil {
		internal(w)
		return
	}
	workerStates := map[string]int{}
	maxHeartbeatAge := time.Duration(0)
	upstreams := map[string]map[string]int{}
	for _, x := range workers {
		state := x.Health["status"]
		if state == "" {
			state = "unknown"
		}
		workerStates[state]++
		if age := time.Since(x.LastSeenAt); age > maxHeartbeatAge {
			maxHeartbeatAge = age
		}
		for name, u := range x.UpstreamStatus {
			if upstreams[name] == nil {
				upstreams[name] = map[string]int{}
			}
			upstreams[name][string(u.State)]++
		}
	}
	if a.telemetry != nil {
		a.telemetry.HeartbeatAge(maxHeartbeatAge)
		for state, count := range workerStates {
			a.telemetry.WorkerHealth(state, float64(count))
		}
		for name, states := range upstreams {
			for state, count := range states {
				a.telemetry.Upstream(name, state, float64(count))
			}
		}
	}
	sandboxStates := map[string]int{}
	for _, x := range boxes {
		sandboxStates[x.State]++
		if a.telemetry != nil {
			a.telemetry.Sandbox(x.Provider, x.Capabilities.Labels["smart-route.pool"], x.State, 1)
		}
	}
	var oldest *time.Time
	if len(jobs) > 0 {
		v := jobs[0].CreatedAt
		for _, j := range jobs[1:] {
			if j.CreatedAt.Before(v) {
				v = j.CreatedAt
			}
		}
		oldest = &v
	}
	pools := []telemetry.PoolStatus{}
	if a.telemetry != nil {
		pools = a.telemetry.PoolStatuses()
	}
	write(w, http.StatusOK, dataEnvelope{map[string]any{"queue": map[string]any{"depth": len(jobs), "oldest_queued_at": oldest}, "workers": workerStates, "sandboxes": sandboxStates, "pools": pools, "upstreams": upstreams}})
}

func jobDTO(j domain.Job) Job {
	var at *time.Time
	if !j.TimeoutAt.IsZero() {
		x := j.TimeoutAt
		at = &x
	}
	var up *string
	if j.Constraints.RequiredUpstream != "" {
		x := j.Constraints.RequiredUpstream
		up = &x
	}
	constraints := Constraints{Capabilities: j.Constraints.Capabilities, Labels: j.Constraints.Labels, Upstream: up, Architecture: string(j.Constraints.Architecture), Region: j.Constraints.Region, ExecutorKind: string(j.Constraints.ExecutorKind), PreferredRegion: j.Constraints.PreferredRegion, PreferredSandbox: string(j.Constraints.PreferredSandbox), PreferredProvider: j.Constraints.PreferredProvider, MaxCost: j.Constraints.MaxCost}
	deps := make([]string, len(j.DependsOn))
	for i, v := range j.DependsOn {
		deps[i] = string(v)
	}
	return Job{ID: string(j.ID), IdempotencyKey: j.IdempotencyKey, Kind: j.Kind, Payload: j.Payload, State: string(j.State), Constraints: constraints, TimeoutSeconds: timeoutSeconds(j), TimeoutAt: at, Retry: Retry{j.RetryPolicy.MaxAttempts, Duration(j.RetryPolicy.Backoff), Duration(j.RetryPolicy.MaxBackoff), Duration(j.RetryPolicy.MaxElapsed)}, CreatedAt: j.CreatedAt, UpdatedAt: j.UpdatedAt, SessionID: string(j.SessionID), DependsOn: deps}
}
func attemptDTO(a domain.Attempt) Attempt {
	return Attempt{string(a.ID), a.Number, string(a.State), string(a.WorkerID), string(a.SandboxID), a.Failure, a.StartedAt, a.EndedAt}
}
func eventDTO(e domain.Event) Event {
	return Event{string(e.ID), e.Sequence, string(e.Type), string(e.JobID), string(e.AttemptID), e.OccurredAt, e.Data, e.WorkerSequence, e.IdempotencyKey}
}
func newID(p string) string {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		panic(e)
	}
	return p + "_" + hex.EncodeToString(b[:])
}
func write(w http.ResponseWriter, s int, v any)      { w.WriteHeader(s); _ = json.NewEncoder(w).Encode(v) }
func fail(w http.ResponseWriter, s int, c, m string) { write(w, s, errorEnvelope{Error{c, m}}) }
func internal(w http.ResponseWriter)                 { fail(w, 500, CodeInternalError, "internal server error") }
func jobError(w http.ResponseWriter, e error) {
	if errors.Is(e, store.ErrNotFound) {
		fail(w, 404, CodeJobNotFound, "job not found")
	} else {
		internal(w)
	}
}
