// Package client provides a typed client for the smart-route v1 HTTP API.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ethan/smart-route/internal/domain"
	"github.com/ethan/smart-route/internal/httpapi"
)

type SubmitJob = httpapi.SubmitJob
type Job = httpapi.Job
type Attempt = httpapi.Attempt
type Event = httpapi.Event
type Result = httpapi.Result
type Worker = httpapi.Worker
type Sandbox = httpapi.Sandbox
type Constraints = httpapi.Constraints
type Retry = httpapi.Retry
type Duration = httpapi.Duration
type CreateSession = httpapi.CreateSession
type Session = httpapi.Session
type Checkpoint = domain.Checkpoint
type RecoveryEvent = domain.RecoveryEvent

const (
	CodeInvalidRequest               = httpapi.CodeInvalidRequest
	CodeDuplicateIdempotencyConflict = httpapi.CodeDuplicateIdempotencyConflict
	CodeJobNotFound                  = httpapi.CodeJobNotFound
	CodeUnavailableCapacity          = httpapi.CodeUnavailableCapacity
	CodeCanceledJob                  = httpapi.CodeCanceledJob
	CodeInternalError                = httpapi.CodeInternalError
)

type APIError struct {
	StatusCode int
	Code       string
	Message    string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("smart-route: %s (%d): %s", e.Code, e.StatusCode, e.Message)
}

type Client struct {
	base *url.URL
	http *http.Client
}

func New(baseURL string, hc *http.Client) (*Client, error) {
	u, e := url.Parse(strings.TrimRight(baseURL, "/"))
	if e != nil {
		return nil, e
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, errors.New("client: base URL must be absolute")
	}
	if hc == nil {
		hc = http.DefaultClient
	}
	return &Client{u, hc}, nil
}
func (c *Client) SubmitJob(ctx context.Context, request SubmitJob) (Job, error) {
	var out Job
	return out, c.do(ctx, http.MethodPost, "/v1/jobs", request, &out)
}
func (c *Client) CreateSession(ctx context.Context, request CreateSession) (Session, error) {
	var out Session
	return out, c.do(ctx, http.MethodPost, "/v1/sessions", request, &out)
}
func (c *Client) GetSession(ctx context.Context, id string) (Session, error) {
	var out Session
	return out, c.do(ctx, http.MethodGet, "/v1/sessions/"+url.PathEscape(id), nil, &out)
}
func (c *Client) CloseSession(ctx context.Context, id string) (Session, error) {
	var out Session
	return out, c.do(ctx, http.MethodPost, "/v1/sessions/"+url.PathEscape(id)+"/close", struct{}{}, &out)
}
func (c *Client) ListSessionJobs(ctx context.Context, id string) ([]Job, error) {
	var out []Job
	return out, c.do(ctx, http.MethodGet, "/v1/sessions/"+url.PathEscape(id)+"/jobs", nil, &out)
}
func (c *Client) RecoverSession(ctx context.Context, id string) (Session, error) {
	var out Session
	return out, c.do(ctx, http.MethodPost, "/v1/sessions/"+url.PathEscape(id)+"/recover", struct{}{}, &out)
}
func (c *Client) CreateCheckpoint(ctx context.Context, id string, data []byte) (Checkpoint, error) {
	var out Checkpoint
	s, err := c.GetSession(ctx, id)
	if err != nil {
		return out, err
	}
	return out, c.do(ctx, http.MethodPost, "/v1/sessions/"+url.PathEscape(id)+"/checkpoints", struct {
		Data  []byte `json:"data"`
		Epoch uint64 `json:"epoch"`
	}{data, s.Epoch}, &out)
}
func (c *Client) ListCheckpoints(ctx context.Context, id string) ([]Checkpoint, error) {
	var out []Checkpoint
	return out, c.do(ctx, http.MethodGet, "/v1/sessions/"+url.PathEscape(id)+"/checkpoints", nil, &out)
}
func (c *Client) ListRecoveryEvents(ctx context.Context, id string) ([]RecoveryEvent, error) {
	var out []RecoveryEvent
	return out, c.do(ctx, http.MethodGet, "/v1/sessions/"+url.PathEscape(id)+"/recovery-events", nil, &out)
}
func (c *Client) GetJob(ctx context.Context, id string) (Job, error) {
	var out Job
	return out, c.do(ctx, http.MethodGet, "/v1/jobs/"+url.PathEscape(id), nil, &out)
}
func (c *Client) ListAttempts(ctx context.Context, id string) ([]Attempt, error) {
	var out []Attempt
	return out, c.do(ctx, http.MethodGet, "/v1/jobs/"+url.PathEscape(id)+"/attempts", nil, &out)
}
func (c *Client) ListEvents(ctx context.Context, id string) ([]Event, error) {
	return c.ListEventsAfter(ctx, id, 0, 0)
}
func (c *Client) ListEventsAfter(ctx context.Context, id string, after uint64, limit int) ([]Event, error) {
	var out []Event
	path := fmt.Sprintf("/v1/jobs/%s/events?after=%d", url.PathEscape(id), after)
	if limit > 0 {
		path += fmt.Sprintf("&limit=%d", limit)
	}
	return out, c.do(ctx, http.MethodGet, path, nil, &out)
}
func (c *Client) GetResult(ctx context.Context, id string) (Result, error) {
	var out Result
	return out, c.do(ctx, http.MethodGet, "/v1/jobs/"+url.PathEscape(id)+"/result", nil, &out)
}
func (c *Client) CancelJob(ctx context.Context, id string) (Job, error) {
	var out Job
	return out, c.do(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(id)+"/cancel", struct{}{}, &out)
}
func (c *Client) ListWorkers(ctx context.Context) ([]Worker, error) {
	var out []Worker
	return out, c.do(ctx, http.MethodGet, "/v1/workers", nil, &out)
}
func (c *Client) ListSandboxes(ctx context.Context) ([]Sandbox, error) {
	var out []Sandbox
	return out, c.do(ctx, http.MethodGet, "/v1/sandboxes", nil, &out)
}
func (c *Client) Health(ctx context.Context) error {
	return c.do(ctx, http.MethodGet, "/healthz", nil, nil)
}
func (c *Client) Ready(ctx context.Context) error {
	return c.do(ctx, http.MethodGet, "/readyz", nil, nil)
}
func (c *Client) WaitTerminal(ctx context.Context, id string, interval time.Duration) (Job, error) {
	if interval <= 0 {
		interval = time.Second
	}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return Job{}, ctx.Err()
		case <-timer.C:
			j, e := c.GetJob(ctx, id)
			if e != nil {
				return Job{}, e
			}
			switch j.State {
			case "succeeded", "failed", "canceled", "timed_out", "dependency_failed", "session_lost":
				return j, nil
			}
			timer.Reset(interval)
		}
	}
}
func (c *Client) do(ctx context.Context, method, path string, body any, out any) error {
	var reader io.Reader
	if body != nil {
		b, e := json.Marshal(body)
		if e != nil {
			return e
		}
		reader = bytes.NewReader(b)
	}
	u := *c.base
	parts := strings.SplitN(path, "?", 2)
	u.Path = strings.TrimRight(c.base.Path, "/") + parts[0]
	if len(parts) == 2 {
		u.RawQuery = parts[1]
	}
	req, e := http.NewRequestWithContext(ctx, method, u.String(), reader)
	if e != nil {
		return e
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, e := c.http.Do(req)
	if e != nil {
		return e
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var envelope struct {
			Error httpapi.Error `json:"error"`
		}
		if e = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&envelope); e != nil {
			return &APIError{resp.StatusCode, "http_error", resp.Status}
		}
		return &APIError{resp.StatusCode, envelope.Error.Code, envelope.Error.Message}
	}
	if out == nil {
		io.Copy(io.Discard, resp.Body)
		return nil
	}
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if e = json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(&envelope); e != nil {
		return e
	}
	return json.Unmarshal(envelope.Data, out)
}
