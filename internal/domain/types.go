package domain

import (
	"encoding/json"
	"time"
)

type JobID string
type AttemptID string
type WorkerID string
type SandboxID string
type LeaseID string
type EventID string
type UpstreamID string
type CredentialRefID string
type SessionID string

type SessionState string

const (
	SessionPending    SessionState = "pending"
	SessionActive     SessionState = "active"
	SessionDraining   SessionState = "draining"
	SessionClosed     SessionState = "closed"
	SessionLost       SessionState = "lost"
	SessionRecovering SessionState = "recovering"
)

type RecoveryPolicy string

const (
	RecoveryNone       RecoveryPolicy = "none"
	RecoveryCheckpoint RecoveryPolicy = "checkpoint"
	RecoveryRebuild    RecoveryPolicy = "rebuild"
)

type CheckpointMode string

const (
	CheckpointExplicit     CheckpointMode = "explicit"
	CheckpointAfterSuccess CheckpointMode = "after_success"
)

type RecoveryState string

const (
	RecoveryIdle         RecoveryState = "idle"
	RecoveryPending      RecoveryState = "pending"
	RecoveryProvisioning RecoveryState = "provisioning"
	RecoveryRestoring    RecoveryState = "restoring"
	RecoveryFailed       RecoveryState = "failed"
)

func (s SessionState) Terminal() bool { return s == SessionClosed || s == SessionLost }

type Session struct {
	ID                  SessionID
	Pool                string
	Capabilities        Capabilities
	PreferredProvider   string
	Labels              map[string]string
	SandboxID           SandboxID
	WorkerID            WorkerID
	State               SessionState
	IdleTTL             time.Duration
	MaxLifetime         time.Duration
	CreatedAt           time.Time
	LastActivity        time.Time
	IdleExpiresAt       time.Time
	ClosedAt            time.Time
	Failure             *Failure
	RecoveryPolicy      RecoveryPolicy
	CheckpointMode      CheckpointMode
	RebuildPlan         []RebuildStep
	Epoch               uint64
	RecoveryState       RecoveryState
	RecoveryAttempts    int
	RecoveryAfter       time.Time
	RecoveryError       string
	LatestCheckpointID  string
	RestoreAcknowledged bool
}

type RecoveryEvent struct {
	ID         string    `json:"id"`
	SessionID  SessionID `json:"session_id"`
	Epoch      uint64    `json:"epoch"`
	Stage      string    `json:"stage"`
	Message    string    `json:"message,omitempty"`
	OccurredAt time.Time `json:"occurred_at"`
}

type RebuildStep struct {
	Kind           string          `json:"kind"`
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey string          `json:"idempotency_key"`
}

type Checkpoint struct {
	ID        string    `json:"id"`
	SessionID SessionID `json:"session_id"`
	Epoch     uint64    `json:"epoch"`
	Sequence  uint64    `json:"sequence"`
	Adapter   string    `json:"adapter"`
	Location  string    `json:"location"`
	Checksum  string    `json:"checksum"`
	Size      int64     `json:"size"`
	State     string    `json:"state"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
	Error     string    `json:"error,omitempty"`
}

type JobState string

const (
	JobQueued           JobState = "queued"
	JobWaiting          JobState = "waiting"
	JobLeased           JobState = "leased"
	JobRunning          JobState = "running"
	JobSucceeded        JobState = "succeeded"
	JobFailed           JobState = "failed"
	JobCanceled         JobState = "canceled"
	JobTimedOut         JobState = "timed_out"
	JobDependencyFailed JobState = "dependency_failed"
	JobSessionLost      JobState = "session_lost"
)

func (s JobState) Terminal() bool {
	switch s {
	case JobSucceeded, JobFailed, JobCanceled, JobTimedOut, JobDependencyFailed, JobSessionLost:
		return true
	default:
		return false
	}
}

type AttemptState string

const (
	AttemptLeased    AttemptState = "leased"
	AttemptRunning   AttemptState = "running"
	AttemptSucceeded AttemptState = "succeeded"
	AttemptFailed    AttemptState = "failed"
	AttemptCanceled  AttemptState = "canceled"
	AttemptExpired   AttemptState = "lease_expired"
	AttemptLost      AttemptState = "lost"
)

func (s AttemptState) Active() bool { return s == AttemptLeased || s == AttemptRunning }

func (s AttemptState) Terminal() bool {
	switch s {
	case AttemptSucceeded, AttemptFailed, AttemptCanceled, AttemptExpired, AttemptLost:
		return true
	default:
		return false
	}
}

type ExecutorKind string

const (
	ExecutorProcess   ExecutorKind = "process"
	ExecutorContainer ExecutorKind = "container"
	ExecutorRemote    ExecutorKind = "remote"
)

type Architecture string

const (
	ArchitectureAMD64 Architecture = "amd64"
	ArchitectureARM64 Architecture = "arm64"
)

// Capabilities describe what a worker or sandbox can provide without naming a vendor.
type Capabilities struct {
	Capabilities  []string
	Labels        map[string]string
	Architecture  Architecture
	Region        string
	ExecutorKinds []ExecutorKind
	Upstreams     []string
}

// RoutingConstraints describe the capabilities required to execute a job.
type RoutingConstraints struct {
	Capabilities      []string
	Labels            map[string]string
	Architecture      Architecture
	Region            string
	ExecutorKind      ExecutorKind
	RequiredUpstream  string
	PreferredRegion   string
	PreferredSandbox  SandboxID
	PreferredProvider string
	MaxCost           *float64
}

type UpstreamAvailability string

const (
	UpstreamAvailable   UpstreamAvailability = "available"
	UpstreamUnavailable UpstreamAvailability = "unavailable"
	UpstreamCooldown    UpstreamAvailability = "cooldown"
)

type UpstreamState struct {
	State           UpstreamAvailability `json:"state"`
	Health          float64              `json:"health,omitempty"`
	CooldownUntil   time.Time            `json:"cooldown_until,omitempty"`
	BudgetRemaining *float64             `json:"budget_remaining,omitempty"`
	Cost            *float64             `json:"cost,omitempty"`
	Metadata        map[string]string    `json:"metadata,omitempty"`
}

func (c Capabilities) Satisfies(required RoutingConstraints) bool {
	for _, capability := range required.Capabilities {
		if !contains(c.Capabilities, capability) {
			return false
		}
	}
	if required.Architecture != "" && c.Architecture != required.Architecture {
		return false
	}
	if required.Region != "" && c.Region != required.Region {
		return false
	}
	if required.ExecutorKind != "" && !contains(c.ExecutorKinds, required.ExecutorKind) {
		return false
	}
	if required.RequiredUpstream != "" && !contains(c.Upstreams, required.RequiredUpstream) {
		return false
	}
	for key, value := range required.Labels {
		if c.Labels[key] != value {
			return false
		}
	}
	return true
}

func contains[T comparable](values []T, wanted T) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

type RetryPolicy struct {
	MaxAttempts int
	Backoff     time.Duration
	MaxBackoff  time.Duration
	MaxElapsed  time.Duration
}

type FailureClass string

const (
	FailureRetryable    FailureClass = "retryable"
	FailureNonRetryable FailureClass = "non_retryable"
)

type Failure struct {
	Code    string
	Message string
	Class   FailureClass
}

func (f Failure) Retryable() bool { return f.Class == FailureRetryable }

type Lease struct {
	ID        LeaseID
	WorkerID  WorkerID
	AttemptID AttemptID
	ExpiresAt time.Time
}

func (l Lease) Expired(at time.Time) bool { return !at.Before(l.ExpiresAt) }

type Attempt struct {
	ID        AttemptID
	JobID     JobID
	Number    int
	State     AttemptState
	WorkerID  WorkerID
	SandboxID SandboxID
	Lease     Lease
	Failure   *Failure
	StartedAt *time.Time
	EndedAt   *time.Time
}

type Worker struct {
	ID                WorkerID
	InstanceID        string
	SandboxID         SandboxID
	SandboxProvider   string
	SessionID         string
	ReservedSessionID SessionID
	SessionEpoch      uint64
	SessionTokenHash  string
	SessionExpiresAt  time.Time
	WorkerVersion     string
	ProtocolVersion   string
	Capabilities      Capabilities
	MaxConcurrency    int
	AvailableSlots    int
	ActiveAttempts    []AttemptID
	SandboxMetadata   map[string]string
	Health            map[string]string
	UpstreamStatus    map[string]UpstreamState
	RegisteredAt      time.Time
	LastSeenAt        time.Time
}

type WorkerHealth string

const (
	WorkerHealthy WorkerHealth = "healthy"
	WorkerSuspect WorkerHealth = "suspect"
	WorkerDead    WorkerHealth = "dead"
)

type Sandbox struct {
	ID                SandboxID
	WorkerID          WorkerID
	Provider          string
	ExternalID        string
	Capabilities      Capabilities
	State             string
	CreatedAt         time.Time
	UpdatedAt         time.Time
	DrainAt           time.Time
	ReservedSessionID SessionID
}

type Upstream struct {
	ID       UpstreamID
	Name     string
	URL      string
	Metadata map[string]string
}

// CredentialRef contains locator metadata only; secrets are never persisted.
type CredentialRef struct {
	ID        CredentialRefID
	Provider  string
	Reference string
	Metadata  map[string]string
}

type EventType string

const (
	EventJobTransition     EventType = "job_transition"
	EventAttemptCreated    EventType = "attempt_created"
	EventAttemptTransition EventType = "attempt_transition"
	EventProgress          EventType = "progress"
	EventLog               EventType = "log"
	EventResult            EventType = "result"
)

type Event struct {
	ID             EventID
	Sequence       uint64
	Type           EventType
	JobID          JobID
	AttemptID      AttemptID
	OccurredAt     time.Time
	Data           map[string]string
	WorkerSequence uint64
	IdempotencyKey string
}

type JobResult struct {
	JobID       JobID
	AttemptID   AttemptID
	StatusCode  int
	Data        []byte
	ArtifactKey string
	Metadata    map[string]string
	CreatedAt   time.Time
}

type Job struct {
	ID             JobID
	IdempotencyKey string
	Kind           string
	Payload        json.RawMessage
	State          JobState
	Constraints    RoutingConstraints
	RetryPolicy    RetryPolicy
	TimeoutAt      time.Time
	SessionID      SessionID
	DependsOn      []JobID
	Attempts       []Attempt
	Events         []Event
	CreatedAt      time.Time
	UpdatedAt      time.Time
}
