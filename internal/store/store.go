// Package store defines persistence contracts without exposing SQL dialects.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/ethan/smart-route/internal/domain"
	"github.com/ethan/smart-route/internal/scheduler"
)

var (
	ErrNotFound = errors.New("store: not found")
	ErrConflict = errors.New("store: conflict")
)

type QueueQuery struct{ Limit int }
type ClaimRequest struct {
	Worker        domain.Worker
	SandboxID     domain.SandboxID
	Now           time.Time
	LeaseDuration time.Duration
	Capacity      int
	Scheduler     scheduler.Scheduler
}
type Completion struct {
	AttemptID    domain.AttemptID
	WorkerID     domain.WorkerID
	AttemptState domain.AttemptState
	JobState     domain.JobState
	Failure      *domain.Failure
	At           time.Time
	Event        domain.Event
}

type JobStore interface {
	CreateJob(context.Context, domain.Job) (domain.Job, error)
	GetJob(context.Context, domain.JobID) (domain.Job, error)
	GetJobByIdempotencyKey(context.Context, string) (domain.Job, error)
	ListQueuedJobs(context.Context, QueueQuery) ([]domain.Job, error)
	CancelJob(context.Context, domain.JobID, time.Time) error
}
type SessionStore interface {
	CreateSession(context.Context, domain.Session) (domain.Session, error)
	GetSession(context.Context, domain.SessionID) (domain.Session, error)
	ListSessions(context.Context, ...domain.SessionState) ([]domain.Session, error)
	ListSessionJobs(context.Context, domain.SessionID) ([]domain.Job, error)
	CloseSession(context.Context, domain.SessionID, time.Time) error
	BindSession(context.Context, domain.SessionID, domain.WorkerID, domain.SandboxID, time.Time) error
	ExpireSessions(context.Context, time.Time) ([]domain.SessionID, error)
	RequestRecovery(context.Context, domain.SessionID, time.Time) error
	ClaimRecovery(context.Context, domain.SessionID, time.Time) (domain.Session, error)
	CompleteRecovery(context.Context, domain.SessionID, uint64, domain.WorkerID, domain.SandboxID, time.Time) error
	FailRecovery(context.Context, domain.SessionID, uint64, string, time.Time, time.Time) error
	AcknowledgeRecovery(context.Context, domain.SessionID, uint64, domain.WorkerID, time.Time) error
	TerminalRecovery(context.Context, domain.SessionID, uint64, string, time.Time) error
	AppendRecoveryEvent(context.Context, domain.RecoveryEvent) error
	ListRecoveryEvents(context.Context, domain.SessionID) ([]domain.RecoveryEvent, error)
	ListRecoveringSessions(context.Context, time.Time) ([]domain.Session, error)
}
type CheckpointStore interface {
	CreateCheckpoint(context.Context, domain.Checkpoint) error
	CompleteCheckpoint(context.Context, string, domain.SessionID, uint64, string, string, int64, time.Time) error
	MarkCheckpoint(context.Context, string, string, string) error
	ListCheckpoints(context.Context, domain.SessionID) ([]domain.Checkpoint, error)
	ListAllCheckpoints(context.Context) ([]domain.Checkpoint, error)
	GarbageCollectCheckpoints(context.Context, time.Time, int, bool) ([]domain.Checkpoint, error)
}
type LeaseStore interface {
	ClaimNextJob(context.Context, ClaimRequest) (domain.Attempt, domain.Job, error)
	RenewLease(context.Context, domain.AttemptID, domain.WorkerID, time.Time) error
	CompleteAttempt(context.Context, Completion) error
	ExpireLeases(context.Context, time.Time) ([]domain.AttemptID, error)
}
type WorkerStore interface {
	UpsertWorker(context.Context, domain.Worker) error
	GetWorker(context.Context, domain.WorkerID) (domain.Worker, error)
	GetWorkerByInstanceID(context.Context, string) (domain.Worker, error)
	ListWorkers(context.Context) ([]domain.Worker, error)
	ExpireWorkerLeases(context.Context, domain.WorkerID, time.Time) ([]domain.AttemptID, error)
}
type BootstrapToken struct {
	TokenHash, SandboxProvider, Pool, CapabilityHash string
	SandboxID                                        domain.SandboxID
	ExpiresAt                                        time.Time
}
type BootstrapTokenStore interface {
	CreateBootstrapToken(context.Context, BootstrapToken) error
	ConsumeBootstrapToken(context.Context, string, domain.SandboxID, string, string, string, time.Time) error
	RevokeSandboxCredentials(context.Context, domain.SandboxID) error
}
type SandboxStore interface {
	UpsertSandbox(context.Context, domain.Sandbox) error
	GetSandbox(context.Context, domain.SandboxID) (domain.Sandbox, error)
	ListSandboxes(context.Context) ([]domain.Sandbox, error)
	DeleteSandbox(context.Context, domain.SandboxID) error
}
type ControllerStore interface {
	TimeoutJobs(context.Context, time.Time) ([]domain.JobID, error)
	SetWorkerHealth(context.Context, domain.WorkerID, domain.WorkerHealth, time.Time) error
	SetSandboxState(context.Context, domain.SandboxID, string, time.Time) error
}
type AttemptStore interface {
	GetAttempt(context.Context, domain.AttemptID) (domain.Attempt, error)
}
type EventStore interface {
	AppendEvent(context.Context, domain.Event) error
	AppendAttemptEvent(context.Context, domain.AttemptID, domain.WorkerID, domain.Event) (domain.Event, error)
	ListEvents(context.Context, domain.JobID, uint64, int) ([]domain.Event, error)
}
type ResultStore interface {
	SaveResult(context.Context, domain.AttemptID, domain.WorkerID, domain.JobResult) error
	GetResult(context.Context, domain.JobID) (domain.JobResult, error)
}
type Store interface {
	JobStore
	LeaseStore
	WorkerStore
	SandboxStore
	AttemptStore
	EventStore
	ResultStore
	ControllerStore
	BootstrapTokenStore
	SessionStore
	CheckpointStore
	Close() error
}
