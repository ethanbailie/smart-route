// Package httpjson implements the shared HTTP envelope convention used by the
// smart-route control plane: JSON request bodies, {"data":...} success
// envelopes, and {"error":{"code","message"}} failure envelopes.
package httpjson

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

// ErrNoContent is returned for 204 responses so callers can distinguish
// "empty" from a decoded payload.
var ErrNoContent = errors.New("no content")

// Error is a non-2xx API failure decoded from the error envelope, or the raw
// response when it is not an envelope.
type Error struct {
	StatusCode int
	Code       string
	Message    string
}

func (e *Error) Error() string {
	return fmt.Sprintf("smart-route: %s (%d): %s", e.Code, e.StatusCode, e.Message)
}

// Request builds a JSON request for method and url. A nil body sends no body.
func Request(ctx context.Context, method, url string, body any) (*http.Request, error) {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return req, nil
}

// Do validates resp and unwraps the data envelope into out (a nil out skips
// decoding). 204 returns ErrNoContent; other non-2xx responses return *Error.
func Do(resp *http.Response, out any) error {
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent {
		_, _ = io.Copy(io.Discard, resp.Body)
		return ErrNoContent
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var envelope struct {
			Error struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&envelope); err == nil && (envelope.Error.Code != "" || envelope.Error.Message != "") {
			return &Error{StatusCode: resp.StatusCode, Code: envelope.Error.Code, Message: envelope.Error.Message}
		}
		return &Error{StatusCode: resp.StatusCode, Code: "http_error", Message: resp.Status}
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(&envelope); err != nil {
		return err
	}
	return json.Unmarshal(envelope.Data, out)
}
