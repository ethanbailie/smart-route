package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ethan/smart-route/internal/domain"
)

type Config struct {
	Registration               RegistrationRequest
	Executors                  map[string]Executor
	ClaimWait, ShutdownTimeout time.Duration
	CancelOnShutdown           bool
	Secrets                    []string
	EventRetryBuffer           int
	UpstreamHealth             func() map[string]domain.UpstreamState
	Observer                   OperationObserver
	CheckpointExport           func(context.Context) ([]byte, error)
	CheckpointRestore          func(context.Context, []byte) error
}

type Runtime struct {
	control          ControlPlane
	cfg              Config
	mu               sync.Mutex
	active           map[string]context.CancelFunc
	wg               sync.WaitGroup
	heartbeat, lease time.Duration
}

func New(control ControlPlane, cfg Config) (*Runtime, error) {
	if control == nil {
		return nil, errors.New("worker: control plane is required")
	}
	if cfg.Registration.MaxConcurrency <= 0 {
		cfg.Registration.MaxConcurrency = 1
	}
	if cfg.ClaimWait <= 0 {
		cfg.ClaimWait = 20 * time.Second
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 30 * time.Second
	}
	if cfg.EventRetryBuffer <= 0 {
		cfg.EventRetryBuffer = 64
	}
	if len(cfg.Executors) == 0 {
		return nil, errors.New("worker: at least one executor is required")
	}
	for key, executor := range cfg.Executors {
		if executor == nil || executor.Kind() == "" {
			return nil, fmt.Errorf("worker: executor %q has no kind", key)
		}
		if key != executor.Kind() {
			return nil, fmt.Errorf("worker: executor registry key %q does not match kind %q", key, executor.Kind())
		}
	}
	return &Runtime{control: control, cfg: cfg, active: map[string]context.CancelFunc{}}, nil
}

func (r *Runtime) Run(ctx context.Context) error {
	reg, err := r.control.Register(ctx, r.cfg.Registration)
	if err != nil {
		return fmt.Errorf("register: %w", err)
	}
	if len(reg.Checkpoint) > 0 {
		if r.cfg.CheckpointRestore == nil {
			return errors.New("worker: recovery checkpoint received but restore is not configured")
		}
		if err = r.cfg.CheckpointRestore(ctx, reg.Checkpoint); err != nil {
			if reg.RecoverySession != "" {
				_ = r.control.ReportRecoveryFailure(ctx, reg.RecoverySession, reg.RecoveryEpoch, redact(err.Error(), r.cfg.Secrets))
			}
			return fmt.Errorf("restore checkpoint: %w", err)
		}
	}
	if reg.RecoverySession != "" {
		if err = r.control.AcknowledgeRecovery(ctx, reg.RecoverySession, reg.RecoveryEpoch); err != nil {
			return fmt.Errorf("acknowledge recovery: %w", err)
		}
	}
	r.heartbeat, r.lease = reg.Heartbeat, reg.Lease
	if r.heartbeat <= 0 {
		r.heartbeat = 10 * time.Second
	}
	if r.lease <= 0 {
		r.lease = 30 * time.Second
	}
	execCtx, stopExec := context.WithCancel(context.Background())
	defer stopExec()
	heartbeatCtx, stopHeartbeat := context.WithCancel(context.Background())
	defer stopHeartbeat()
	errc := make(chan error, 2)
	go func() { errc <- r.heartbeatLoop(heartbeatCtx) }()
	go func() { errc <- r.claimLoop(ctx, execCtx) }()
	select {
	case <-ctx.Done():
	case err = <-errc:
		stopExec()
	}
	if r.cfg.CancelOnShutdown {
		r.cancelAll()
	}
	done := make(chan struct{})
	go func() { r.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(r.cfg.ShutdownTimeout):
		r.cancelAll()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
	}
	stopHeartbeat()
	stopExec()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = r.control.Heartbeat(shutdownCtx, nil, r.cfg.Registration.MaxConcurrency, r.cfg.Registration.SandboxMetadata, r.upstreamHealth())
	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}
func (r *Runtime) claimLoop(claimCtx, runCtx context.Context) error {
	for {
		select {
		case <-claimCtx.Done():
			return nil
		default:
		}
		if r.slots() == 0 {
			select {
			case <-claimCtx.Done():
				return nil
			case <-time.After(50 * time.Millisecond):
				continue
			}
		}
		claim, err := r.control.Claim(claimCtx, r.cfg.ClaimWait)
		if err != nil {
			select {
			case <-claimCtx.Done():
				return nil
			case <-time.After(reconnectDelay):
				continue
			}
		}
		if claim == nil {
			continue
		}
		jobCtx, cancel := context.WithCancel(runCtx)
		if !claim.Job.Timeout.IsZero() {
			var deadlineCancel context.CancelFunc
			jobCtx, deadlineCancel = context.WithDeadline(jobCtx, claim.Job.Timeout)
			old := cancel
			cancel = func() { deadlineCancel(); old() }
		}
		r.mu.Lock()
		r.active[claim.AttemptID] = cancel
		r.mu.Unlock()
		r.wg.Add(1)
		go r.execute(jobCtx, claim)
	}
}
func (r *Runtime) execute(ctx context.Context, c *Claim) {
	defer r.wg.Done()
	defer func() { r.mu.Lock(); delete(r.active, c.AttemptID); r.mu.Unlock() }()
	sink := &attemptSink{control: r.control, id: c.AttemptID, secrets: r.cfg.Secrets, maxPending: r.cfg.EventRetryBuffer}
	renewCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go r.renewLoop(renewCtx, c.AttemptID)
	finishSpan := func(error) {}
	if r.cfg.Observer != nil {
		ctx, finishSpan = r.cfg.Observer.Start(ctx, "executor.execute", "job_id", c.Job.ID, "attempt_id", c.AttemptID, "executor", c.Job.Kind)
	}
	ex, ok := r.cfg.Executors[c.Job.Kind]
	if !ok {
		r.fail(c.AttemptID, &FailureError{Code: "unsupported_executor", Message: "no executor configured for job kind " + c.Job.Kind, Class: domain.FailureNonRetryable})
		return
	}
	result, err := ex.Execute(ctx, c.Job, sink)
	finishSpan(err)
	if err != nil {
		r.fail(c.AttemptID, classify(err, ctx))
		return
	}
	result = redactResult(result, r.cfg.Secrets)
	if r.cfg.CheckpointExport != nil {
		result.Checkpoint, err = r.cfg.CheckpointExport(ctx)
		if err != nil {
			r.fail(c.AttemptID, &FailureError{Code: "checkpoint_export_error", Message: err.Error(), Class: domain.FailureRetryable})
			return
		}
	}
	opCtx, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()
	if err = sink.flush(opCtx); err != nil {
		r.fail(c.AttemptID, &FailureError{Code: "event_delivery_error", Message: err.Error(), Class: domain.FailureRetryable})
		return
	}
	if err = r.control.Complete(opCtx, c.AttemptID, result); err != nil {
		r.fail(c.AttemptID, &FailureError{Code: "result_error", Message: err.Error(), Class: domain.FailureRetryable})
	}
}
func (r *Runtime) renewLoop(ctx context.Context, id string) {
	d := r.lease / 2
	if d < time.Second {
		d = time.Second
	}
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if r.control.Renew(ctx, id) != nil {
				return
			}
		}
	}
}
func (r *Runtime) heartbeatLoop(ctx context.Context) error {
	t := time.NewTicker(r.heartbeat)
	defer t.Stop()
	for {
		canceled, err := r.control.Heartbeat(ctx, r.activeIDs(), r.slots(), r.cfg.Registration.SandboxMetadata, r.upstreamHealth())
		if err != nil && ctx.Err() == nil {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(reconnectDelay):
				continue
			}
		}
		r.cancelAttempts(canceled)
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

const reconnectDelay = 200 * time.Millisecond

func (r *Runtime) cancelAttempts(ids []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, id := range ids {
		if cancel := r.active[id]; cancel != nil {
			cancel()
		}
	}
}
func (r *Runtime) fail(id string, e *FailureError) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clean := *e
	clean.Message = redact(clean.Message, r.cfg.Secrets)
	_ = r.control.Fail(ctx, id, &clean)
}
func (r *Runtime) activeIDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	v := make([]string, 0, len(r.active))
	for id := range r.active {
		v = append(v, id)
	}
	return v
}
func (r *Runtime) slots() int { return r.cfg.Registration.MaxConcurrency - len(r.activeIDs()) }
func (r *Runtime) upstreamHealth() map[string]domain.UpstreamState {
	if r.cfg.UpstreamHealth == nil {
		return nil
	}
	return r.cfg.UpstreamHealth()
}
func (r *Runtime) cancelAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, cancel := range r.active {
		cancel()
	}
}

type attemptSink struct {
	control    ControlPlane
	id         string
	secrets    []string
	mu         sync.Mutex
	sequence   uint64
	pending    []Event
	maxPending int
}

func (s *attemptSink) Emit(ctx context.Context, e Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sequence++
	e.WorkerSequence = s.sequence
	e.IdempotencyKey = fmt.Sprintf("%s:%d", s.id, s.sequence)
	e.Data = redactMap(e.Data, s.secrets)
	if err := s.flushLocked(ctx); err != nil {
		if len(s.pending) >= s.maxPending {
			return fmt.Errorf("worker: event retry buffer full (%d)", s.maxPending)
		}
		s.pending = append(s.pending, e)
		return nil
	}
	if err := s.control.Event(ctx, s.id, e); err != nil {
		if len(s.pending) >= s.maxPending {
			return fmt.Errorf("worker: event retry buffer full (%d)", s.maxPending)
		}
		s.pending = append(s.pending, e)
	}
	return nil
}
func (s *attemptSink) flush(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushLocked(ctx)
}
func (s *attemptSink) flushLocked(ctx context.Context) error {
	for len(s.pending) > 0 {
		if err := s.control.Event(ctx, s.id, s.pending[0]); err != nil {
			return err
		}
		s.pending = s.pending[1:]
	}
	return nil
}
func redactMap(values map[string]string, secrets []string) map[string]string {
	clean := make(map[string]string, len(values))
	for key, value := range values {
		clean[key] = redact(value, secrets)
	}
	return clean
}
func redactResult(result Result, secrets []string) Result {
	result.Data = []byte(redact(string(result.Data), secrets))
	result.Metadata = redactMap(result.Metadata, secrets)
	return result
}
func classify(err error, ctx context.Context) *FailureError {
	if fe, ok := err.(*FailureError); ok {
		return fe
	}
	if errors.Is(err, ErrTimeout) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return &FailureError{"timeout", err.Error(), domain.FailureRetryable}
	}
	if errors.Is(err, ErrCanceled) || errors.Is(ctx.Err(), context.Canceled) {
		return &FailureError{"canceled", err.Error(), domain.FailureRetryable}
	}
	return &FailureError{"executor_error", err.Error(), domain.FailureRetryable}
}
