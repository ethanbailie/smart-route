package worker

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/ethan/smart-route/internal/domain"
	"github.com/ethan/smart-route/internal/secret"
	"github.com/ethan/smart-route/internal/upstream"
)

type UpstreamConfig struct {
	Registry   *upstream.Registry
	Secrets    secret.Store
	Authorized []string
	Now        func() time.Time
	Observer   UpstreamObserver
}

type UpstreamExecutor struct{ config UpstreamConfig }

func NewUpstreamExecutor(config UpstreamConfig) (*UpstreamExecutor, error) {
	if config.Registry == nil || config.Secrets == nil {
		return nil, errors.New("worker: upstream registry and secret store are required")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return &UpstreamExecutor{config: config}, nil
}

func (*UpstreamExecutor) Kind() string { return "upstream" }

type upstreamPayload struct {
	Upstream     string            `json:"upstream"`
	Capabilities []string          `json:"capabilities"`
	Model        string            `json:"model"`
	Input        json.RawMessage   `json:"input"`
	Tags         map[string]string `json:"tags"`
}

func (e *UpstreamExecutor) Execute(ctx context.Context, job Job, _ EventSink) (Result, error) {
	var payload upstreamPayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return Result{}, &FailureError{"invalid_payload", err.Error(), domain.FailureNonRetryable}
	}
	now := e.config.Now()
	entry, err := e.config.Registry.Select(upstream.Selection{Name: payload.Upstream, Capabilities: payload.Capabilities, Model: payload.Model, Authorized: e.config.Authorized}, now)
	if err != nil {
		return Result{}, &FailureError{"upstream_selection", err.Error(), domain.FailureNonRetryable}
	}
	// Resolution intentionally occurs here, after claim and selection, and the
	// bundle is retained only on this execution stack.
	bundle, err := e.config.Secrets.Resolve(ctx, entry.Metadata.CredentialRef)
	if err != nil {
		return Result{}, &FailureError{"credential_unavailable", "credential could not be resolved", domain.FailureNonRetryable}
	}
	finish := func(error) {}
	if e.config.Observer != nil {
		ctx, finish = e.config.Observer.Start(ctx, "upstream.execute", "job_id", job.ID, "upstream", entry.Metadata.Name)
	}
	response, err := entry.Adapter.Execute(ctx, upstream.Request{Payload: append([]byte(nil), payload.Input...), Model: payload.Model, Tags: payload.Tags}, bundle)
	finish(err)
	secretValues := make([]string, 0, len(bundle.Values))
	for _, value := range bundle.Values {
		secretValues = append(secretValues, value)
	}
	if err != nil {
		message := redact(err.Error(), secretValues)
		checkedAt := e.config.Now()
		var throttle *upstream.ThrottleError
		if errors.As(err, &throttle) {
			if e.config.Observer != nil {
				e.config.Observer.UpstreamCall(entry.Metadata.Name, "throttled", true)
				e.config.Observer.Upstream(entry.Metadata.Name, string(domain.UpstreamCooldown), 1)
			}
			until := throttle.Until(checkedAt)
			if !until.After(checkedAt) {
				until = checkedAt.Add(time.Minute)
			}
			_ = e.config.Registry.UpdateHealth(entry.Metadata.Name, domain.UpstreamCooldown, until, checkedAt)
		} else {
			if e.config.Observer != nil {
				e.config.Observer.UpstreamCall(entry.Metadata.Name, "error", false)
				e.config.Observer.Upstream(entry.Metadata.Name, string(domain.UpstreamUnavailable), 1)
			}
			_ = e.config.Registry.UpdateHealth(entry.Metadata.Name, domain.UpstreamUnavailable, time.Time{}, checkedAt)
		}
		switch {
		case errors.Is(err, upstream.ErrThrottled), errors.Is(err, upstream.ErrTimeout), errors.Is(err, context.DeadlineExceeded):
			return Result{}, &FailureError{"upstream_retryable", message, domain.FailureRetryable}
		default:
			return Result{}, &FailureError{"upstream_permanent", message, domain.FailureNonRetryable}
		}
	}
	_ = e.config.Registry.UpdateHealth(entry.Metadata.Name, domain.UpstreamAvailable, time.Time{}, e.config.Now())
	if e.config.Observer != nil {
		e.config.Observer.UpstreamCall(entry.Metadata.Name, "success", false)
		e.config.Observer.Upstream(entry.Metadata.Name, string(domain.UpstreamAvailable), 1)
	}
	return redactResult(Result{StatusCode: response.StatusCode, Data: response.Data, Metadata: response.Metadata}, secretValues), nil
}

func (e *UpstreamExecutor) HealthSnapshot() map[string]domain.UpstreamState {
	return e.config.Registry.HealthSnapshot(e.config.Now())
}

var _ Executor = (*UpstreamExecutor)(nil)
