package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ethanbailie/smart-route/internal/domain"
)

type HTTPConfig struct {
	Client           *http.Client
	MaxRequestBytes  int64
	MaxResponseBytes int64
	AllowedHosts     []string
	Secrets          []string
}

type HTTPExecutor struct {
	client  *http.Client
	maxReq  int64
	maxResp int64
	allowed map[string]struct{}
	secrets []string
}

func NewHTTPExecutor(c HTTPConfig) *HTTPExecutor {
	allowed := make(map[string]struct{}, len(c.AllowedHosts))
	for _, h := range c.AllowedHosts {
		allowed[strings.ToLower(strings.TrimSpace(h))] = struct{}{}
	}
	if c.Client == nil {
		c.Client = &http.Client{Timeout: 30 * time.Second}
	} else {
		clone := *c.Client
		c.Client = &clone
	}
	// Redirects must never bypass the host check performed below. Clone an
	// injected client above so applying the executor policy does not mutate it.
	c.Client.CheckRedirect = denyRedirects
	if c.MaxRequestBytes <= 0 {
		c.MaxRequestBytes = 1 << 20
	}
	if c.MaxResponseBytes <= 0 {
		c.MaxResponseBytes = 1 << 20
	}
	return &HTTPExecutor{client: c.Client, maxReq: c.MaxRequestBytes, maxResp: c.MaxResponseBytes, allowed: allowed, secrets: c.Secrets}
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
	u, err := url.Parse(p.URL)
	if err != nil {
		return Result{}, &FailureError{"invalid_payload", err.Error(), domain.FailureNonRetryable}
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return Result{}, &FailureError{"invalid_url", "only http and https URLs are allowed", domain.FailureNonRetryable}
	}
	if _, ok := e.allowed[strings.ToLower(u.Host)]; !ok {
		return Result{}, &FailureError{"url_not_allowed", "URL host is not in allowlist", domain.FailureNonRetryable}
	}
	if int64(len(p.Body)) > e.maxReq {
		return Result{}, &FailureError{"request_too_large", "HTTP request body exceeded configured size", domain.FailureNonRetryable}
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
	body, err := io.ReadAll(io.LimitReader(res.Body, e.maxResp+1))
	if err != nil {
		return Result{}, fmt.Errorf("read response: %w", err)
	}
	if int64(len(body)) > e.maxResp {
		return Result{}, &FailureError{"response_too_large", "HTTP response exceeded configured size", domain.FailureNonRetryable}
	}
	body = []byte(redact(string(body), e.secrets))
	headers := map[string]string{"content_type": res.Header.Get("Content-Type")}
	return Result{StatusCode: res.StatusCode, Data: body, Metadata: headers}, nil
}

func denyRedirects(_ *http.Request, via []*http.Request) error {
	if len(via) >= 1 {
		return http.ErrUseLastResponse
	}
	return nil
}
