package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/ethan/smart-route/internal/domain"
)

type HTTPConfig struct {
	Client           *http.Client
	MaxResponseBytes int64
}
type HTTPExecutor struct {
	client *http.Client
	max    int64
}

func NewHTTPExecutor(c HTTPConfig) *HTTPExecutor {
	if c.Client == nil {
		c.Client = &http.Client{Timeout: 30 * time.Second}
	}
	if c.MaxResponseBytes <= 0 {
		c.MaxResponseBytes = 1 << 20
	}
	return &HTTPExecutor{c.Client, c.MaxResponseBytes}
}
func (*HTTPExecutor) Kind() string { return "http" }

type httpPayload struct {
	Method         string            `json:"method"`
	URL            string            `json:"url"`
	Headers        map[string]string `json:"headers"`
	Body           json.RawMessage   `json:"body"`
	TimeoutSeconds int64             `json:"timeout_seconds"`
}

func (e *HTTPExecutor) Execute(ctx context.Context, j Job, _ EventSink) (Result, error) {
	var p httpPayload
	if err := json.Unmarshal(j.Payload, &p); err != nil {
		return Result{}, &FailureError{"invalid_payload", err.Error(), domain.FailureNonRetryable}
	}
	if p.Method == "" {
		p.Method = http.MethodPost
	}
	if p.URL == "" {
		return Result{}, &FailureError{"invalid_payload", "url is required", domain.FailureNonRetryable}
	}
	if p.TimeoutSeconds > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(p.TimeoutSeconds)*time.Second)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(ctx, p.Method, p.URL, bytes.NewReader(p.Body))
	if err != nil {
		return Result{}, &FailureError{"invalid_payload", err.Error(), domain.FailureNonRetryable}
	}
	for k, v := range p.Headers {
		req.Header.Set(k, v)
	}
	res, err := e.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return Result{}, ctx.Err()
		}
		return Result{}, fmt.Errorf("http request: %w", err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, e.max+1))
	if err != nil {
		return Result{}, fmt.Errorf("read response: %w", err)
	}
	if int64(len(body)) > e.max {
		return Result{}, &FailureError{"response_too_large", "HTTP response exceeded configured size", domain.FailureNonRetryable}
	}
	headers := map[string]string{"content_type": res.Header.Get("Content-Type")}
	return Result{StatusCode: res.StatusCode, Data: body, Metadata: headers}, nil
}
