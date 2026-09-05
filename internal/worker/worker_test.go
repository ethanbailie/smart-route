package worker

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/ethan/smart-route/internal/domain"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

type fakeControl struct {
	mu        sync.Mutex
	claims    []*Claim
	completed chan string
	events    []Event
	results   []Result
}

func (f *fakeControl) AcknowledgeRecovery(context.Context, string, uint64) error { return nil }
func (f *fakeControl) ReportRecoveryFailure(context.Context, string, uint64, string) error {
	return nil
}

func (f *fakeControl) Register(context.Context, RegistrationRequest) (Registration, error) {
	return Registration{Heartbeat: time.Hour, Lease: time.Hour}, nil
}
func (f *fakeControl) Heartbeat(context.Context, []string, int, map[string]string, map[string]domain.UpstreamState) ([]string, error) {
	return nil, nil
}
func (f *fakeControl) Claim(ctx context.Context, _ time.Duration) (*Claim, error) {
	f.mu.Lock()
	if len(f.claims) > 0 {
		c := f.claims[0]
		f.claims = f.claims[1:]
		f.mu.Unlock()
		return c, nil
	}
	f.mu.Unlock()
	<-ctx.Done()
	return nil, ctx.Err()
}
func (f *fakeControl) Renew(context.Context, string) error { return nil }

func (f *fakeControl) Event(_ context.Context, _ string, event Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, event)
	return nil
}
func (f *fakeControl) Complete(_ context.Context, id string, result Result) error {
	f.mu.Lock()
	f.results = append(f.results, result)
	f.mu.Unlock()
	f.completed <- id
	return nil
}
func (f *fakeControl) Fail(context.Context, string, *FailureError) error { return nil }

type testExecutor struct {
	kind string
	run  func(context.Context, Job, EventSink) (Result, error)
}

type unavailableControl struct{ fakeControl }

type recoveryControl struct {
	fakeControl
	acknowledged bool
	failed       bool
}

func (f *recoveryControl) Register(context.Context, RegistrationRequest) (Registration, error) {
	return Registration{Checkpoint: []byte("broken"), RecoverySession: "session", RecoveryEpoch: 2}, nil
}
func (f *recoveryControl) AcknowledgeRecovery(context.Context, string, uint64) error {
	f.acknowledged = true
	return nil
}
func (f *recoveryControl) ReportRecoveryFailure(context.Context, string, uint64, string) error {
	f.failed = true
	return nil
}

func (f *unavailableControl) Event(context.Context, string, Event) error {
	return context.DeadlineExceeded
}

func TestEventRetryBufferIsBounded(t *testing.T) {
	c := &unavailableControl{}
	sink := &attemptSink{control: c, id: "attempt", maxPending: 1}
	if err := sink.Emit(context.Background(), Event{Type: "progress"}); err != nil {
		t.Fatal(err)
	}
	if err := sink.Emit(context.Background(), Event{Type: "progress"}); err == nil {
		t.Fatal("expected backpressure when retry buffer is full")
	}
	if len(sink.pending) != 1 || sink.pending[0].WorkerSequence != 1 || sink.pending[0].IdempotencyKey != "attempt:1" {
		t.Fatalf("pending = %#v", sink.pending)
	}
}

func TestRuntimeDoesNotAcknowledgeFailedRestore(t *testing.T) {
	control := &recoveryControl{}
	executor := testExecutor{kind: "x", run: func(context.Context, Job, EventSink) (Result, error) { return Result{}, nil }}
	runtime, err := New(control, Config{Executors: map[string]Executor{"x": executor}, CheckpointRestore: func(context.Context, []byte) error { return errors.New("corrupt") }})
	if err != nil {
		t.Fatal(err)
	}
	if err = runtime.Run(context.Background()); err == nil || control.acknowledged || !control.failed {
		t.Fatalf("restore error=%v acknowledged=%v failure_reported=%v", err, control.acknowledged, control.failed)
	}
}

func (e testExecutor) Kind() string { return e.kind }
func (e testExecutor) Execute(ctx context.Context, job Job, sink EventSink) (Result, error) {
	return e.run(ctx, job, sink)
}

func TestRuntimeRunsClaimsConcurrentlyAndDrains(t *testing.T) {
	f := &fakeControl{claims: []*Claim{{Job: Job{Kind: "x"}, AttemptID: "a"}, {Job: Job{Kind: "x"}, AttemptID: "b"}}, completed: make(chan string, 2)}
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	ex := testExecutor{kind: "x", run: func(ctx context.Context, _ Job, sink EventSink) (Result, error) {
		started <- struct{}{}
		<-release
		_ = sink.Emit(ctx, Event{Type: "progress", Data: map[string]string{"message": "secret"}})
		return Result{Data: []byte("secret"), Metadata: map[string]string{"detail": "secret"}}, nil
	}}
	r, _ := New(f, Config{Registration: RegistrationRequest{MaxConcurrency: 2}, Executors: map[string]Executor{"x": ex}, ShutdownTimeout: time.Second, Secrets: []string{"secret"}})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	<-started
	<-started
	cancel()
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if len(f.completed) != 2 {
		t.Fatalf("completed %d jobs, want 2", len(f.completed))
	}
	if f.events[0].Data["message"] != "[REDACTED]" || string(f.results[0].Data) != "[REDACTED]" || f.results[0].Metadata["detail"] != "[REDACTED]" {
		t.Fatal("runtime leaked a configured secret")
	}
}

func TestExecutorsEnforceBoundsAndRedact(t *testing.T) {
	cmd := NewCommandExecutor(CommandConfig{Allowlist: []string{"/bin/sh"}, MaxOutputBytes: 8, Secrets: []string{"secret"}})
	result, err := cmd.Execute(context.Background(), Job{Payload: json.RawMessage(`{"command":"/bin/sh","args":["-c","printf secret-more"]}`)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(result.Data) != "[REDACTED]-m" {
		t.Fatalf("command output = %q", result.Data)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("too large")) }))
	defer server.Close()
	httpExecutor := NewHTTPExecutor(HTTPConfig{MaxResponseBytes: 3})
	if _, err = httpExecutor.Execute(context.Background(), Job{Payload: json.RawMessage(`{"url":"` + server.URL + `"}`)}, nil); err == nil {
		t.Fatal("expected bounded response error")
	}
}
