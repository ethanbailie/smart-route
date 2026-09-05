package client

import (
	"context"
	"net/http"
)

type AdminStatus struct {
	Queue     any `json:"queue"`
	Workers   any `json:"workers"`
	Sandboxes any `json:"sandboxes"`
	Pools     any `json:"pools"`
	Upstreams any `json:"upstreams"`
}

func (c *Client) AdminStatus(ctx context.Context) (AdminStatus, error) {
	var out AdminStatus
	return out, c.do(ctx, http.MethodGet, "/v1/admin/status", nil, &out)
}

// SetBearerToken configures public API authentication without exposing the token
// through URL parameters or error strings.
func (c *Client) SetBearerToken(token string) {
	if token != "" {
		c.http = &http.Client{Transport: bearerTransport{token: token, next: c.http.Transport}, Timeout: c.http.Timeout, CheckRedirect: c.http.CheckRedirect, Jar: c.http.Jar}
	}
}

type bearerTransport struct {
	token string
	next  http.RoundTripper
}

func (t bearerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header = r.Header.Clone()
	r.Header.Set("Authorization", "Bearer "+t.token)
	n := t.next
	if n == nil {
		n = http.DefaultTransport
	}
	return n.RoundTrip(r)
}
