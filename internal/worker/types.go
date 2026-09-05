// Package worker implements the provider-neutral smart-route worker runtime.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/ethan/smart-route/internal/domain"
)

var (
	ErrCanceled = errors.New("execution canceled")
	ErrTimeout  = errors.New("execution timed out")
)

type FailureError struct {
	Code, Message string
	Class         domain.FailureClass
}

func (e *FailureError) Error() string { return e.Message }

type Job struct {
	ID, Kind string
	Payload  json.RawMessage
	Timeout  time.Time
}

type Result struct {
	StatusCode int
	Data       []byte
	Metadata   map[string]string
	Checkpoint []byte
}

type Event struct {
	Type           string
	Data           map[string]string
	WorkerSequence uint64
	IdempotencyKey string
}

type EventSink interface {
	Emit(context.Context, Event) error
}

type OperationObserver interface {
	Start(context.Context, string, ...any) (context.Context, func(error))
}

type UpstreamObserver interface {
	OperationObserver
	UpstreamCall(string, string, bool)
	Upstream(string, string, float64)
}

type Executor interface {
	Kind() string
	Execute(context.Context, Job, EventSink) (Result, error)
}

type Claim struct {
	Job       Job
	AttemptID string
	LeaseTill time.Time
}

type Registration struct {
	WorkerID, Token  string
	Heartbeat, Lease time.Duration
	Checkpoint       []byte
	RecoverySession  string
	RecoveryEpoch    uint64
}

type RegistrationRequest struct {
	InstanceID, SandboxID, SandboxProvider, Version string
	BootstrapToken                                  string
	Capabilities                                    domain.Capabilities
	MaxConcurrency                                  int
	SandboxMetadata                                 map[string]string
}

type ControlPlane interface {
	Register(context.Context, RegistrationRequest) (Registration, error)
	AcknowledgeRecovery(context.Context, string, uint64) error
	ReportRecoveryFailure(context.Context, string, uint64, string) error
	Heartbeat(context.Context, []string, int, map[string]string, map[string]domain.UpstreamState) ([]string, error)
	Claim(context.Context, time.Duration) (*Claim, error)
	Renew(context.Context, string) error
	Event(context.Context, string, Event) error
	Complete(context.Context, string, Result) error
	Fail(context.Context, string, *FailureError) error
}
