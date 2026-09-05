package httpapi

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ethan/smart-route/internal/checkpoint"
	"github.com/ethan/smart-route/internal/domain"
	"github.com/ethan/smart-route/internal/store"
)

const workerProtocolVersion = "1"

type workerCapabilities struct {
	Capabilities  []string              `json:"capabilities"`
	Labels        map[string]string     `json:"labels"`
	Architecture  domain.Architecture   `json:"architecture"`
	Region        string                `json:"region"`
	ExecutorKinds []domain.ExecutorKind `json:"executor_kinds"`
	Upstreams     []string              `json:"upstreams"`
}

func (c workerCapabilities) domain() domain.Capabilities {
	return domain.Capabilities{Capabilities: c.Capabilities, Labels: c.Labels, Architecture: c.Architecture, Region: c.Region, ExecutorKinds: c.ExecutorKinds, Upstreams: c.Upstreams}
}

func validInstanceUUID(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	_, err := hex.DecodeString(strings.ReplaceAll(value, "-", ""))
	return err == nil
}

type registerWorkerRequest struct {
	BootstrapToken  string             `json:"bootstrap_token"`
	InstanceID      string             `json:"instance_id"`
	SandboxID       domain.SandboxID   `json:"sandbox_id"`
	SandboxProvider string             `json:"sandbox_provider"`
	WorkerVersion   string             `json:"worker_version"`
	ProtocolVersion string             `json:"protocol_version"`
	Capabilities    workerCapabilities `json:"capabilities"`
	MaxConcurrency  int                `json:"max_concurrency"`
	SandboxMetadata map[string]string  `json:"sandbox_metadata"`
}

func decodeWorkerJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		fail(w, http.StatusBadRequest, CodeInvalidRequest, err.Error())
		return false
	}
	return true
}

func capabilityHash(c domain.Capabilities) string {
	b, _ := json.Marshal(c)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func (a *API) MintBootstrapToken(ctx context.Context, sandboxID domain.SandboxID, provider, pool string, capabilities domain.Capabilities) (string, error) {
	token := newID("wbt")
	hash := sha256.Sum256([]byte(token))
	err := a.store.CreateBootstrapToken(ctx, store.BootstrapToken{TokenHash: hex.EncodeToString(hash[:]), SandboxID: sandboxID, SandboxProvider: provider, Pool: pool, CapabilityHash: capabilityHash(capabilities), ExpiresAt: time.Now().UTC().Add(a.bootstrapTokenTTL)})
	return token, err
}

func (a *API) registerWorker(w http.ResponseWriter, r *http.Request) {
	var req registerWorkerRequest
	if !decodeWorkerJSON(w, r, &req) {
		return
	}
	if !validInstanceUUID(req.InstanceID) || req.SandboxID == "" || req.SandboxProvider == "" || req.WorkerVersion == "" || req.ProtocolVersion != workerProtocolVersion || req.MaxConcurrency <= 0 {
		fail(w, http.StatusBadRequest, CodeInvalidRequest, "UUID instance_id, sandbox_id, sandbox_provider, worker_version, supported protocol_version, and positive max_concurrency are required")
		return
	}
	now := time.Now().UTC()
	bootstrapHash := sha256.Sum256([]byte(req.BootstrapToken))
	pool := req.Capabilities.Labels["smart-route.pool"]
	if pool == "" {
		pool = req.Capabilities.Labels["pool"]
	}
	if req.BootstrapToken == "" || a.store.ConsumeBootstrapToken(r.Context(), hex.EncodeToString(bootstrapHash[:]), req.SandboxID, req.SandboxProvider, pool, capabilityHash(req.Capabilities.domain()), now) != nil {
		fail(w, http.StatusUnauthorized, "invalid_bootstrap_token", "bootstrap credential is invalid or expired")
		return
	}
	token := newID("wst")
	hash := sha256.Sum256([]byte(token))
	worker := domain.Worker{ID: domain.WorkerID(newID("worker")), InstanceID: req.InstanceID, SandboxID: req.SandboxID, SandboxProvider: req.SandboxProvider, SessionID: newID("session"), SessionTokenHash: hex.EncodeToString(hash[:]), WorkerVersion: req.WorkerVersion, ProtocolVersion: req.ProtocolVersion, Capabilities: req.Capabilities.domain(), MaxConcurrency: req.MaxConcurrency, AvailableSlots: req.MaxConcurrency, SandboxMetadata: req.SandboxMetadata, RegisteredAt: now, LastSeenAt: now, SessionExpiresAt: now.Add(a.workerSessionTTL)}
	// Recovery reservations are persisted before provisioning. Binding them here
	// lets a restarted controller adopt this worker without trusting worker input.
	var recoveryCheckpoint []byte
	if box, err := a.store.GetSandbox(r.Context(), req.SandboxID); err == nil && box.ReservedSessionID != "" {
		if session, err := a.store.GetSession(r.Context(), box.ReservedSessionID); err == nil && session.State == domain.SessionRecovering {
			worker.ReservedSessionID, worker.SessionEpoch = session.ID, session.Epoch
			if session.RecoveryPolicy == domain.RecoveryCheckpoint && a.checkpoints != nil {
				providerStrategy := false
				if strategic, ok := a.checkpoints.(checkpoint.Strategic); ok {
					providerStrategy = strategic.Strategy() == checkpoint.StrategyProviderSnapshot
				}
				if !providerStrategy {
					items, _ := a.store.ListCheckpoints(r.Context(), session.ID)
					for _, cp := range items {
						if cp.State != "ready" || cp.Adapter != a.checkpoints.Name() {
							continue
						}
						reader, e := a.checkpoints.Open(r.Context(), cp)
						if e != nil {
							continue
						}
						recoveryCheckpoint, e = io.ReadAll(reader)
						_ = reader.Close()
						if e == nil {
							break
						}
					}
				}
			}
		}
	}
	if existing, err := a.store.GetWorkerByInstanceID(r.Context(), req.InstanceID); err == nil {
		worker.ID, worker.RegisteredAt, worker.ActiveAttempts = existing.ID, existing.RegisteredAt, existing.ActiveAttempts
		worker.AvailableSlots = existing.AvailableSlots
		if worker.AvailableSlots > worker.MaxConcurrency {
			worker.AvailableSlots = worker.MaxConcurrency
		}
	} else if !errors.Is(err, store.ErrNotFound) {
		internal(w)
		return
	}
	if err := a.store.UpsertWorker(r.Context(), worker); err != nil {
		internal(w)
		return
	}
	if err := a.store.UpsertSandbox(r.Context(), domain.Sandbox{ID: worker.SandboxID, WorkerID: worker.ID, Provider: worker.SandboxProvider, ExternalID: string(worker.SandboxID), Capabilities: worker.Capabilities, State: "ready", CreatedAt: now, UpdatedAt: now}); err != nil {
		internal(w)
		return
	}
	write(w, http.StatusCreated, dataEnvelope{map[string]any{"worker_id": worker.ID, "session_id": worker.SessionID, "session_token": token, "protocol_version": workerProtocolVersion, "heartbeat_interval_seconds": int64(a.heartbeatInterval / time.Second), "lease_duration_seconds": int64(a.leaseDuration / time.Second), "checkpoint": recoveryCheckpoint, "recovery_session_id": worker.ReservedSessionID, "recovery_epoch": worker.SessionEpoch}})
}

func (a *API) acknowledgeRecovery(w http.ResponseWriter, r *http.Request) {
	worker, ok := a.authenticatedWorker(w, r)
	if !ok {
		return
	}
	var req struct {
		SessionID domain.SessionID `json:"session_id"`
		Epoch     uint64           `json:"epoch"`
		Error     string           `json:"error"`
	}
	if !decodeWorkerJSON(w, r, &req) {
		return
	}
	if req.SessionID != worker.ReservedSessionID || req.Epoch != worker.SessionEpoch {
		fail(w, http.StatusConflict, "stale_session_epoch", "recovery acknowledgement does not match worker epoch")
		return
	}
	if req.Error != "" {
		now := time.Now().UTC()
		if a.providers != nil && worker.SandboxID != "" {
			if provider, e := a.providers.Get(worker.SandboxProvider); e == nil {
				_ = provider.Terminate(r.Context(), worker.SandboxID)
			}
			_ = a.store.DeleteSandbox(r.Context(), worker.SandboxID)
		}
		if e := a.store.FailRecovery(r.Context(), req.SessionID, req.Epoch, req.Error, now.Add(a.recoveryBackoff), now); e != nil {
			attemptStoreError(w, e)
			return
		}
		write(w, http.StatusAccepted, dataEnvelope{map[string]string{"status": "restore_failed"}})
		return
	}
	if e := a.store.AcknowledgeRecovery(r.Context(), req.SessionID, req.Epoch, worker.ID, time.Now().UTC()); e != nil {
		attemptStoreError(w, e)
		return
	}
	write(w, http.StatusAccepted, dataEnvelope{map[string]string{"status": "validated"}})
}

func (a *API) authenticatedWorker(w http.ResponseWriter, r *http.Request) (domain.Worker, bool) {
	id := r.Header.Get("X-Smart-Route-Worker-ID")
	if id == "" {
		id = r.Header.Get("X-Worker-ID")
	}
	auth := r.Header.Get("Authorization")
	if id == "" || !strings.HasPrefix(auth, "Bearer ") {
		fail(w, http.StatusUnauthorized, "invalid_worker_session", "worker session credentials are required")
		return domain.Worker{}, false
	}
	worker, err := a.store.GetWorker(r.Context(), domain.WorkerID(id))
	if err != nil {
		fail(w, http.StatusUnauthorized, "invalid_worker_session", "worker session is invalid")
		return domain.Worker{}, false
	}
	hash := sha256.Sum256([]byte(strings.TrimPrefix(auth, "Bearer ")))
	actual := hex.EncodeToString(hash[:])
	if subtle.ConstantTimeCompare([]byte(actual), []byte(worker.SessionTokenHash)) != 1 {
		fail(w, http.StatusUnauthorized, "invalid_worker_session", "worker session is invalid")
		return domain.Worker{}, false
	}
	if worker.SessionExpiresAt.IsZero() || !time.Now().UTC().Before(worker.SessionExpiresAt) {
		fail(w, http.StatusUnauthorized, "expired_worker_session", "worker session is expired")
		return domain.Worker{}, false
	}
	if worker.ProtocolVersion != workerProtocolVersion {
		fail(w, http.StatusConflict, "incompatible_protocol", "worker protocol is no longer supported")
		return domain.Worker{}, false
	}
	if worker.ReservedSessionID != "" {
		session, err := a.store.GetSession(r.Context(), worker.ReservedSessionID)
		valid := session.State == domain.SessionActive && session.WorkerID == worker.ID
		if session.State == domain.SessionRecovering {
			valid = true
		}
		if err != nil || !valid || worker.SessionEpoch != session.Epoch {
			fail(w, http.StatusConflict, "stale_session_epoch", "worker belongs to a stale session epoch")
			return domain.Worker{}, false
		}
	}
	return worker, true
}

type heartbeatRequest struct {
	ActiveAttempts  []domain.AttemptID         `json:"active_attempts"`
	AvailableSlots  int                        `json:"available_slots"`
	SandboxMetadata map[string]string          `json:"sandbox_metadata"`
	Health          map[string]string          `json:"health"`
	Upstreams       map[string]json.RawMessage `json:"upstreams"`
}

func decodeUpstreams(raw map[string]json.RawMessage) (map[string]domain.UpstreamState, error) {
	out := make(map[string]domain.UpstreamState, len(raw))
	for id, value := range raw {
		var state domain.UpstreamState
		if err := json.Unmarshal(value, &state); err != nil {
			var legacy string
			if legacyErr := json.Unmarshal(value, &legacy); legacyErr != nil {
				return nil, err
			}
			switch strings.ToLower(legacy) {
			case "healthy", "ok", "ready", "available":
				state.State, state.Health = domain.UpstreamAvailable, 1
			case "cooldown":
				state.State = domain.UpstreamCooldown
			default:
				state.State = domain.UpstreamUnavailable
			}
		}
		out[id] = state
	}
	return out, nil
}

func (a *API) heartbeatWorker(w http.ResponseWriter, r *http.Request) {
	result := "error"
	defer func() {
		if a.telemetry != nil {
			a.telemetry.Heartbeat(result)
		}
	}()
	worker, ok := a.authenticatedWorker(w, r)
	if !ok {
		return
	}
	var req heartbeatRequest
	if !decodeWorkerJSON(w, r, &req) {
		return
	}
	if req.AvailableSlots < 0 || req.AvailableSlots > worker.MaxConcurrency {
		fail(w, http.StatusBadRequest, CodeInvalidRequest, "available_slots must be between zero and max_concurrency")
		return
	}
	cancellations := make([]domain.AttemptID, 0)
	active := make([]domain.AttemptID, 0, len(req.ActiveAttempts))
	for _, id := range req.ActiveAttempts {
		attempt, err := a.store.GetAttempt(r.Context(), id)
		if err != nil || attempt.WorkerID != worker.ID {
			fail(w, http.StatusConflict, "invalid_active_attempt", "active attempt is not owned by this worker session")
			return
		}
		if !attempt.State.Active() {
			job, jobErr := a.store.GetJob(r.Context(), attempt.JobID)
			if jobErr == nil && workerShouldCancel(job.State) {
				cancellations = append(cancellations, id)
				continue
			}
			fail(w, http.StatusConflict, "invalid_active_attempt", "active attempt is not owned by this worker session")
			return
		}
		active = append(active, id)
	}
	upstreams, err := decodeUpstreams(req.Upstreams)
	if err != nil {
		fail(w, http.StatusBadRequest, CodeInvalidRequest, "invalid upstream state")
		return
	}
	worker.ActiveAttempts, worker.AvailableSlots, worker.SandboxMetadata, worker.Health, worker.UpstreamStatus = active, req.AvailableSlots+len(cancellations), req.SandboxMetadata, req.Health, upstreams
	if worker.AvailableSlots > worker.MaxConcurrency {
		worker.AvailableSlots = worker.MaxConcurrency
	}
	worker.LastSeenAt = time.Now().UTC()
	rotated := newID("wst")
	rotatedHash := sha256.Sum256([]byte(rotated))
	worker.SessionTokenHash = hex.EncodeToString(rotatedHash[:])
	worker.SessionID = newID("session")
	worker.SessionExpiresAt = worker.LastSeenAt.Add(a.workerSessionTTL)
	if err := a.store.UpsertWorker(r.Context(), worker); err != nil {
		internal(w)
		return
	}
	result = "success"
	if a.telemetry != nil {
		a.telemetry.HeartbeatAge(0)
	}
	write(w, http.StatusOK, dataEnvelope{map[string]any{"heartbeat_interval_seconds": int64(a.heartbeatInterval / time.Second), "lease_duration_seconds": int64(a.leaseDuration / time.Second), "server_time": worker.LastSeenAt, "session_id": worker.SessionID, "session_token": rotated, "session_expires_at": worker.SessionExpiresAt, "cancel_attempts": cancellations}})
}
func workerShouldCancel(state domain.JobState) bool {
	return state == domain.JobCanceled || state == domain.JobTimedOut || state == domain.JobSessionLost
}

type claimRequest struct {
	SandboxID   domain.SandboxID `json:"sandbox_id"`
	WaitSeconds int64            `json:"wait_seconds"`
}

func (a *API) claimWorker(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	result := "error"
	defer func() {
		if a.telemetry != nil {
			a.telemetry.Claim(result)
			a.telemetry.ClaimWait(result, time.Since(started))
		}
	}()
	worker, ok := a.authenticatedWorker(w, r)
	if !ok {
		return
	}
	var req claimRequest
	if r.Method == http.MethodPost {
		if r.ContentLength != 0 && !decodeWorkerJSON(w, r, &req) {
			return
		}
	} else {
		req.SandboxID = domain.SandboxID(r.URL.Query().Get("sandbox_id"))
		req.WaitSeconds, _ = strconv.ParseInt(r.URL.Query().Get("wait_seconds"), 10, 64)
	}
	wait := time.Duration(req.WaitSeconds) * time.Second
	if wait < 0 {
		fail(w, http.StatusBadRequest, CodeInvalidRequest, "wait_seconds cannot be negative")
		return
	}
	if wait > a.maxClaimWait {
		wait = a.maxClaimWait
	}
	deadline := time.NewTimer(wait)
	defer deadline.Stop()
	for {
		now := time.Now().UTC()
		ids, _ := a.store.ExpireLeases(r.Context(), now)
		if a.telemetry != nil {
			a.telemetry.LeaseExpired(len(ids))
		}
		workers, err := a.store.ListWorkers(r.Context())
		if err != nil {
			internal(w)
			return
		}
		for _, candidate := range workers {
			if candidate.ID != worker.ID && now.Sub(candidate.LastSeenAt) >= a.workerTimeout {
				ids, _ := a.store.ExpireWorkerLeases(r.Context(), candidate.ID, now)
				if a.telemetry != nil {
					a.telemetry.LeaseExpired(len(ids))
				}
			}
		}
		if req.SandboxID != "" && req.SandboxID != worker.SandboxID {
			fail(w, http.StatusConflict, "sandbox_identity_mismatch", "claim sandbox does not match registered worker sandbox")
			return
		}
		claimCtx := r.Context()
		finishClaim := func(error) {}
		if a.telemetry != nil {
			claimCtx, finishClaim = a.telemetry.Start(claimCtx, "scheduler.claim", "worker_id", worker.ID, "sandbox_id", worker.SandboxID)
		}
		attempt, job, err := a.store.ClaimNextJob(claimCtx, store.ClaimRequest{Worker: worker, SandboxID: worker.SandboxID, Now: now, LeaseDuration: a.leaseDuration, Capacity: worker.MaxConcurrency, Scheduler: a.scheduler})
		finishClaim(err)
		if err == nil {
			result = "leased"
			if a.telemetry != nil {
				a.telemetry.Job("started")
				a.telemetry.Queue(job.Kind, poolRequirement(job), -1)
				a.telemetry.QueueWait(job.Kind, now.Sub(job.CreatedAt))
				a.telemetry.LeaseStarted()
			}
			write(w, http.StatusOK, dataEnvelope{map[string]any{"job": jobDTO(job), "attempt": attemptDTO(attempt), "lease_id": attempt.Lease.ID, "lease_expires_at": attempt.Lease.ExpiresAt}})
			return
		}
		if !errors.Is(err, store.ErrNotFound) && !errors.Is(err, store.ErrConflict) {
			internal(w)
			return
		}
		if wait == 0 || errors.Is(err, store.ErrConflict) {
			result = "empty"
			w.WriteHeader(http.StatusNoContent)
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-deadline.C:
			result = "empty"
			w.WriteHeader(http.StatusNoContent)
			return
		case <-a.wake:
		}
	}
}

type renewRequest struct {
	LeaseSeconds int64 `json:"lease_seconds"`
}
type eventRequest struct {
	Type           string            `json:"type"`
	Data           map[string]string `json:"data"`
	WorkerSequence uint64            `json:"worker_sequence"`
	IdempotencyKey string            `json:"idempotency_key"`
}
type resultRequest struct {
	StatusCode int               `json:"status_code"`
	Data       []byte            `json:"data"`
	Metadata   map[string]string `json:"metadata"`
}
type failureRequest struct {
	Code    string              `json:"code"`
	Message string              `json:"message"`
	Class   domain.FailureClass `json:"class"`
}

func (a *API) workerAttemptRoute(w http.ResponseWriter, r *http.Request, id domain.AttemptID, action string) {
	worker, ok := a.authenticatedWorker(w, r)
	if !ok {
		return
	}
	now := time.Now().UTC()
	switch action {
	case "renew":
		var req renewRequest
		if r.ContentLength != 0 && !decodeWorkerJSON(w, r, &req) {
			return
		}
		d := a.leaseDuration
		if req.LeaseSeconds > 0 && time.Duration(req.LeaseSeconds)*time.Second < d {
			d = time.Duration(req.LeaseSeconds) * time.Second
		}
		until := now.Add(d)
		if err := a.store.RenewLease(r.Context(), id, worker.ID, until); err != nil {
			attemptStoreError(w, err)
			return
		}
		write(w, http.StatusOK, dataEnvelope{map[string]any{"lease_expires_at": until}})
	case "events":
		var req eventRequest
		if !decodeWorkerJSON(w, r, &req) {
			return
		}
		if req.Type == "" {
			fail(w, 400, CodeInvalidRequest, "event type is required")
			return
		}
		event, err := a.store.AppendAttemptEvent(r.Context(), id, worker.ID, domain.Event{Type: domain.EventType(req.Type), Data: req.Data, OccurredAt: now, WorkerSequence: req.WorkerSequence, IdempotencyKey: req.IdempotencyKey})
		if err != nil {
			attemptStoreError(w, err)
			return
		}
		write(w, http.StatusAccepted, dataEnvelope{eventDTO(event)})
	case "result":
		var req resultRequest
		if !decodeWorkerJSON(w, r, &req) {
			return
		}
		if len(req.Data) > a.maxResultBytes {
			fail(w, http.StatusRequestEntityTooLarge, CodeInvalidRequest, "result exceeds configured limit")
			return
		}
		result := domain.JobResult{AttemptID: id, StatusCode: req.StatusCode, Data: req.Data, Metadata: req.Metadata, CreatedAt: now}
		if len(req.Data) > a.inlineResultBytes {
			if a.artifacts == nil {
				fail(w, http.StatusRequestEntityTooLarge, CodeInvalidRequest, "large results require an artifact store")
				return
			}
			result.ArtifactKey = "results/" + string(id)
			if err := a.artifacts.Put(r.Context(), result.ArtifactKey, req.Data); err != nil {
				internal(w)
				return
			}
			result.Data = nil
		}
		if err := a.store.SaveResult(r.Context(), id, worker.ID, result); err != nil {
			attemptStoreError(w, err)
			return
		}
		write(w, http.StatusAccepted, dataEnvelope{map[string]string{"status": "accepted"}})
	case "complete":
		attempt, _ := a.store.GetAttempt(r.Context(), id)
		job, _ := a.store.GetJob(r.Context(), attempt.JobID)
		var req struct {
			Checkpoint []byte `json:"checkpoint"`
		}
		if r.ContentLength != 0 && !decodeWorkerJSON(w, r, &req) {
			return
		}
		if job.SessionID != "" {
			if session, e := a.store.GetSession(r.Context(), job.SessionID); e == nil && session.RecoveryPolicy == domain.RecoveryCheckpoint && session.CheckpointMode == domain.CheckpointAfterSuccess {
				if len(req.Checkpoint) == 0 {
					fail(w, http.StatusConflict, "checkpoint_required", "after_success session requires worker-exported application state")
					return
				}
				if _, e = a.saveCheckpoint(r.Context(), session.ID, session.Epoch, req.Checkpoint, now); e != nil {
					fail(w, http.StatusInternalServerError, "checkpoint_failed", e.Error())
					return
				}
			}
		}
		err := a.store.CompleteAttempt(r.Context(), store.Completion{AttemptID: id, WorkerID: worker.ID, AttemptState: domain.AttemptSucceeded, JobState: domain.JobSucceeded, At: now, Event: domain.Event{Type: domain.EventAttemptTransition, Data: map[string]string{"state": "succeeded"}}})
		if err != nil {
			attemptStoreError(w, err)
			return
		}
		a.observeAttempt(r.Context(), attempt, "completed", now)
		if a.telemetry != nil {
			a.telemetry.Job("completed")
		}
		write(w, http.StatusOK, dataEnvelope{map[string]string{"status": "succeeded"}})
	case "fail":
		attempt, _ := a.store.GetAttempt(r.Context(), id)
		var req failureRequest
		if !decodeWorkerJSON(w, r, &req) {
			return
		}
		if req.Code == "" || (req.Class != domain.FailureRetryable && req.Class != domain.FailureNonRetryable) {
			fail(w, 400, CodeInvalidRequest, "failure code and valid class are required")
			return
		}
		failure := &domain.Failure{Code: req.Code, Message: req.Message, Class: req.Class}
		err := a.store.CompleteAttempt(r.Context(), store.Completion{AttemptID: id, WorkerID: worker.ID, AttemptState: domain.AttemptFailed, Failure: failure, At: now, Event: domain.Event{Type: domain.EventAttemptTransition, Data: map[string]string{"state": "failed", "code": req.Code}}})
		if err != nil {
			attemptStoreError(w, err)
			return
		}
		a.observeAttempt(r.Context(), attempt, "failed", now)
		if a.telemetry != nil {
			job, getErr := a.store.GetJob(r.Context(), attempt.JobID)
			if getErr == nil && job.State == domain.JobQueued {
				a.telemetry.Job("retried")
				a.telemetry.Queue(job.Kind, poolRequirement(job), 1)
			} else {
				a.telemetry.Job("failed")
			}
		}
		write(w, http.StatusOK, dataEnvelope{map[string]string{"status": "failed"}})
	default:
		fail(w, http.StatusNotFound, "attempt_action_not_found", "attempt action not found")
	}
}

func (a *API) observeAttempt(ctx context.Context, attempt domain.Attempt, event string, at time.Time) {
	if a.telemetry == nil {
		return
	}
	d := time.Duration(0)
	if attempt.StartedAt != nil {
		d = at.Sub(*attempt.StartedAt)
	}
	a.telemetry.Attempt(event, d)
	_, finish := a.telemetry.Start(ctx, "attempt."+event, "attempt_id", attempt.ID, "sandbox_id", attempt.SandboxID, "job_id", attempt.JobID)
	finish(nil)
}

func attemptStoreError(w http.ResponseWriter, err error) {
	if errors.Is(err, store.ErrNotFound) {
		fail(w, http.StatusNotFound, "attempt_not_found", "attempt not found or not owned by worker")
	} else if errors.Is(err, store.ErrConflict) {
		fail(w, http.StatusConflict, "attempt_conflict", "attempt is no longer active")
	} else {
		internal(w)
	}
}
