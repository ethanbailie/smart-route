package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ethanbailie/smart-route/internal/domain"
)

type CommandConfig struct {
	Allowlist      []string
	MaxOutputBytes int64
	EmitChunks     bool
	Secrets        []string
	ResolveSecret  func(ref string) (string, bool)
}

type CommandExecutor struct {
	allowed map[string]struct{}
	max     int64
	chunks  bool
	secrets []string
	resolve func(ref string) (string, bool)
}

func NewCommandExecutor(c CommandConfig) *CommandExecutor {
	m := map[string]struct{}{}
	for _, v := range c.Allowlist {
		m[v] = struct{}{}
	}
	if c.MaxOutputBytes <= 0 {
		c.MaxOutputBytes = 1 << 20
	}
	if c.ResolveSecret == nil {
		c.ResolveSecret = defaultResolveSecret
	}
	return &CommandExecutor{m, c.MaxOutputBytes, c.EmitChunks, c.Secrets, c.ResolveSecret}
}

func (*CommandExecutor) Kind() string { return "command" }

type commandPayload struct {
	Command        string            `json:"command"`
	Args           []string          `json:"args"`
	Env            map[string]string `json:"env"`
	TimeoutSeconds int64             `json:"timeout_seconds"`
}

func (e *CommandExecutor) Execute(ctx context.Context, j Job, sink EventSink) (Result, error) {
	var p commandPayload
	if err := json.Unmarshal(j.Payload, &p); err != nil {
		return Result{}, &FailureError{"invalid_payload", err.Error(), domain.FailureNonRetryable}
	}
	if p.Command == "" {
		return Result{}, &FailureError{"invalid_payload", "command is required", domain.FailureNonRetryable}
	}
	if _, ok := e.allowed[p.Command]; !ok {
		return Result{}, &FailureError{"command_not_allowed", "command is not allowlisted", domain.FailureNonRetryable}
	}
	if p.TimeoutSeconds > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(p.TimeoutSeconds)*time.Second)
		defer cancel()
	}
	env, err := e.buildEnv(p.Env)
	if err != nil {
		return Result{}, &FailureError{"secret_error", err.Error(), domain.FailureNonRetryable}
	}
	cmd := exec.Command(p.Command, p.Args...)
	cmd.Env = env
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	out := newBoundedBuffer(e.max)
	stderr := newBoundedBuffer(e.max)
	if e.chunks {
		cmd.Stdout = io.MultiWriter(out, &eventWriter{ctx, sink, "stdout", e.secrets})
		cmd.Stderr = io.MultiWriter(stderr, &eventWriter{ctx, sink, "stderr", e.secrets})
	} else {
		cmd.Stdout = out
		cmd.Stderr = stderr
	}
	if err := cmd.Start(); err != nil {
		return Result{}, fmt.Errorf("start command: %w", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			return Result{}, &FailureError{"command_failed", redact(err.Error()+": "+stderr.String(), e.secrets), domain.FailureNonRetryable}
		}
	case <-ctx.Done():
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		<-done
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return Result{}, ErrTimeout
		}
		return Result{}, ErrCanceled
	}
	return Result{Data: []byte(redact(out.String(), e.secrets)), Metadata: map[string]string{"stderr": redact(stderr.String(), e.secrets), "stdout_truncated": strconv.FormatBool(out.truncated), "stderr_truncated": strconv.FormatBool(stderr.truncated)}}, nil
}

func (e *CommandExecutor) buildEnv(payload map[string]string) ([]string, error) {
	env := make(map[string]string)
	for _, entry := range os.Environ() {
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key, value := parts[0], parts[1]
		if name, ok := strings.CutPrefix(key, "SMART_ROUTE_ENV_REF_"); ok {
			if value == "" {
				continue
			}
			resolved, ok := e.resolve(value)
			if !ok {
				return nil, fmt.Errorf("secret reference %q not found for %s", value, name)
			}
			env[name] = resolved
			continue
		}
		if isBaseEnvVar(key) {
			env[key] = value
		}
	}
	for k, v := range payload {
		if !isSensitiveKey(k) {
			env[k] = v
		}
	}
	out := make([]string, 0, len(env))
	for k, v := range env {
		out = append(out, k+"="+v)
	}
	return out, nil
}

func isBaseEnvVar(key string) bool {
	switch strings.ToUpper(key) {
	case "PATH", "HOME", "USER", "SHELL", "LANG", "TERM", "TZ", "TMPDIR":
		return true
	}
	return strings.HasPrefix(key, "LC_")
}

func isSensitiveKey(key string) bool {
	return strings.HasPrefix(key, "SMART_ROUTE_SECRET_") || strings.HasPrefix(key, "SMART_ROUTE_CREDENTIAL_") || strings.HasPrefix(key, "SMART_ROUTE_ENV_REF_")
}

func defaultResolveSecret(ref string) (string, bool) {
	if v, ok := os.LookupEnv("SMART_ROUTE_SECRET_" + ref); ok {
		return v, true
	}
	if v, ok := os.LookupEnv("SMART_ROUTE_CREDENTIAL_" + ref); ok {
		return v, true
	}
	return "", false
}

type boundedBuffer struct {
	buf       bytes.Buffer
	max       int64
	truncated bool
}

func newBoundedBuffer(n int64) *boundedBuffer { return &boundedBuffer{max: n} }
func (b *boundedBuffer) Write(p []byte) (int, error) {
	original := len(p)
	left := b.max - int64(b.buf.Len())
	if left <= 0 {
		b.truncated = true
		return original, nil
	}
	if int64(len(p)) > left {
		p = p[:left]
		b.truncated = true
	}
	_, _ = b.buf.Write(p)
	return original, nil
}
func (b *boundedBuffer) String() string { return b.buf.String() }

type eventWriter struct {
	ctx     context.Context
	sink    EventSink
	kind    string
	secrets []string
}

func (w *eventWriter) Write(p []byte) (int, error) {
	_ = w.sink.Emit(w.ctx, Event{Type: w.kind, Data: map[string]string{"chunk": redact(string(p), w.secrets)}})
	return len(p), nil
}
func redact(v string, secrets []string) string {
	for _, s := range secrets {
		if s != "" {
			v = strings.ReplaceAll(v, s, "[REDACTED]")
		}
	}
	return v
}
