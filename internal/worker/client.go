package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/ethanbailie/smart-route/internal/httpjson"
)

type HTTPControlPlane struct {
	base            string
	http            *http.Client
	mu              sync.RWMutex
	authMu          sync.RWMutex
	workerID, token string
	observer        OperationObserver
}

func NewHTTPControlPlane(base string, client *http.Client) (*HTTPControlPlane, error) {
	u, err := url.Parse(strings.TrimRight(base, "/"))
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("worker: invalid control-plane URL")
	}
	if u.Scheme != "https" {
		host := u.Hostname()
		ip := net.ParseIP(host)
		if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
			return nil, fmt.Errorf("worker: TLS is required for non-local control plane")
		}
	}
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	} else if client.Timeout <= 0 {
		clone := *client
		clone.Timeout = 30 * time.Second
		client = &clone
	}
	return &HTTPControlPlane{base: strings.TrimRight(base, "/"), http: client}, nil
}

func (c *HTTPControlPlane) SetObserver(observer OperationObserver) {
	c.mu.Lock()
	c.observer = observer
	c.mu.Unlock()
}

func (c *HTTPControlPlane) Register(ctx context.Context, r RegistrationRequest) (Registration, error) {
	body := map[string]any{"instance_id": r.InstanceID, "bootstrap_token": r.BootstrapToken, "sandbox_id": r.SandboxID, "sandbox_provider": r.SandboxProvider, "worker_version": r.Version, "protocol_version": "1", "max_concurrency": r.MaxConcurrency, "sandbox_metadata": r.SandboxMetadata, "capabilities": map[string]any{"capabilities": r.Capabilities.Capabilities, "labels": r.Capabilities.Labels, "architecture": r.Capabilities.Architecture, "region": r.Capabilities.Region, "executor_kinds": r.Capabilities.ExecutorKinds}}
	var out struct {
		WorkerID        string `json:"worker_id"`
		Token           string `json:"session_token"`
		Heartbeat       int64  `json:"heartbeat_interval_seconds"`
		Lease           int64  `json:"lease_duration_seconds"`
		Checkpoint      []byte `json:"checkpoint,omitempty"`
		RecoverySession string `json:"recovery_session_id,omitempty"`
		RecoveryEpoch   uint64 `json:"recovery_epoch,omitempty"`
	}
	if err := c.do(ctx, http.MethodPost, "/v1/worker/register", body, &out, false); err != nil {
		return Registration{}, err
	}
	c.mu.Lock()
	c.workerID, c.token = out.WorkerID, out.Token
	c.mu.Unlock()
	return Registration{WorkerID: out.WorkerID, Token: out.Token, Heartbeat: time.Duration(out.Heartbeat) * time.Second, Lease: time.Duration(out.Lease) * time.Second, Checkpoint: out.Checkpoint, RecoverySession: out.RecoverySession, RecoveryEpoch: out.RecoveryEpoch}, nil
}
func (c *HTTPControlPlane) AcknowledgeRecovery(ctx context.Context, id string, epoch uint64) error {
	return c.do(ctx, http.MethodPost, "/v1/worker/recovery/ack", map[string]any{"session_id": id, "epoch": epoch}, nil, true)
}
func (c *HTTPControlPlane) ReportRecoveryFailure(ctx context.Context, id string, epoch uint64, message string) error {
	return c.do(ctx, http.MethodPost, "/v1/worker/recovery/ack", map[string]any{"session_id": id, "epoch": epoch, "error": message}, nil, true)
}
func (c *HTTPControlPlane) Heartbeat(ctx context.Context, ids []string, slots int, metadata map[string]string) ([]string, error) {
	var out struct {
		Cancellations []string `json:"cancel_attempts"`
	}
	if err := c.do(ctx, http.MethodPost, "/v1/worker/heartbeat", map[string]any{"active_attempts": ids, "available_slots": slots, "sandbox_metadata": metadata, "health": map[string]string{"status": "ok"}}, &out, true); err != nil {
		return nil, err
	}
	return out.Cancellations, nil
}
func (c *HTTPControlPlane) Claim(ctx context.Context, wait time.Duration) (*Claim, error) {
	var out struct {
		Job struct {
			ID, Kind  string
			Payload   json.RawMessage `json:"payload"`
			TimeoutAt *time.Time      `json:"timeout_at"`
		} `json:"job"`
		Attempt struct {
			ID string `json:"id"`
		} `json:"attempt"`
		LeaseTill time.Time `json:"lease_expires_at"`
	}
	err := c.do(ctx, http.MethodPost, "/v1/worker/claim", map[string]int64{"wait_seconds": int64(wait / time.Second)}, &out, true)
	if errors.Is(err, httpjson.ErrNoContent) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	job := Job{ID: out.Job.ID, Kind: out.Job.Kind, Payload: out.Job.Payload}
	if out.Job.TimeoutAt != nil {
		job.Timeout = *out.Job.TimeoutAt
	}
	return &Claim{Job: job, AttemptID: out.Attempt.ID, LeaseTill: out.LeaseTill}, nil
}
func (c *HTTPControlPlane) Renew(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodPost, "/v1/worker/attempts/"+url.PathEscape(id)+"/renew", struct{}{}, nil, true)
}
func (c *HTTPControlPlane) Event(ctx context.Context, id string, e Event) error {
	return c.do(ctx, http.MethodPost, "/v1/worker/attempts/"+url.PathEscape(id)+"/events", map[string]any{"type": e.Type, "data": e.Data, "worker_sequence": e.WorkerSequence, "idempotency_key": e.IdempotencyKey}, nil, true)
}
func (c *HTTPControlPlane) Complete(ctx context.Context, id string, r Result) error {
	if err := c.do(ctx, http.MethodPost, "/v1/worker/attempts/"+url.PathEscape(id)+"/result", map[string]any{"status_code": r.StatusCode, "data": r.Data, "metadata": r.Metadata}, nil, true); err != nil {
		return err
	}
	return c.do(ctx, http.MethodPost, "/v1/worker/attempts/"+url.PathEscape(id)+"/complete", struct {
		Checkpoint []byte `json:"checkpoint,omitempty"`
	}{r.Checkpoint}, nil, true)
}
func (c *HTTPControlPlane) Fail(ctx context.Context, id string, e *FailureError) error {
	return c.do(ctx, http.MethodPost, "/v1/worker/attempts/"+url.PathEscape(id)+"/fail", map[string]any{"code": e.Code, "message": e.Message, "class": e.Class}, nil, true)
}

func routeName(path string) string {
	if strings.HasPrefix(path, "/v1/worker/attempts/") {
		return "/v1/worker/attempts/{id}"
	}
	return path
}

func (c *HTTPControlPlane) do(ctx context.Context, method, path string, body, out any, auth bool) (err error) {
	if auth {
		if path == "/v1/worker/heartbeat" {
			c.authMu.Lock()
			defer c.authMu.Unlock()
		} else {
			c.authMu.RLock()
			defer c.authMu.RUnlock()
		}
	}
	c.mu.RLock()
	observer := c.observer
	c.mu.RUnlock()
	finish := func(error) {}
	if observer != nil {
		ctx, finish = observer.Start(ctx, "worker.http", "method", method, "route", routeName(path))
	}
	defer func() { finish(err) }()
	req, err := httpjson.Request(ctx, method, c.base+path, body)
	if err != nil {
		return err
	}
	if auth {
		c.mu.RLock()
		id, token := c.workerID, c.token
		c.mu.RUnlock()
		req.Header.Set("X-Smart-Route-Worker-ID", id)
		req.Header.Set("Authorization", "Bearer "+token)
	}
	res, err := c.http.Do(req)
	if err != nil {
		return err
	}
	return httpjson.Do(res, out)
}
