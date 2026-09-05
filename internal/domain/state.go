package domain

import "time"

func CanTransition(from, to JobState) bool {
	if from.Terminal() || from == to {
		return false
	}
	switch from {
	case JobWaiting:
		return to == JobQueued || to == JobCanceled || to == JobDependencyFailed || to == JobSessionLost || to == JobTimedOut
	case JobQueued:
		return to == JobLeased || to == JobCanceled || to == JobTimedOut || to == JobDependencyFailed || to == JobSessionLost
	case JobLeased:
		return to == JobRunning || to == JobQueued || to == JobCanceled || to == JobTimedOut || to == JobSessionLost
	case JobRunning:
		return to == JobSucceeded || to == JobFailed || to == JobQueued || to == JobCanceled || to == JobTimedOut || to == JobSessionLost
	default:
		return false
	}
}

func CanAttemptTransition(from, to AttemptState) bool {
	if from.Terminal() || from == to {
		return false
	}
	switch from {
	case AttemptLeased:
		return to == AttemptRunning || to == AttemptSucceeded || to == AttemptFailed || to == AttemptCanceled || to == AttemptExpired || to == AttemptLost
	case AttemptRunning:
		return to == AttemptSucceeded || to == AttemptFailed || to == AttemptCanceled || to == AttemptExpired || to == AttemptLost
	default:
		return false
	}
}

func (j *Job) Transition(to JobState) error {
	if !CanTransition(j.State, to) {
		code := ErrInvalidTransition
		if j.State.Terminal() {
			code = ErrTerminalState
		}
		return &TransitionError{Entity: "job", From: string(j.State), To: string(to), Code: code}
	}
	j.State = to
	return nil
}

// AddAttempt creates the next monotonically numbered attempt. A job can have only
// one active (leased or running) attempt at a time.
func (j *Job) AddAttempt(attempt Attempt) error {
	if j.State.Terminal() {
		return &DomainError{Code: ErrTerminalState, Entity: "job", Message: "cannot add an attempt to a terminal job"}
	}
	for _, existing := range j.Attempts {
		if existing.State.Active() {
			return &DomainError{Code: ErrActiveAttempt, Entity: "job", Message: "an active attempt already exists"}
		}
	}
	want := 1
	if len(j.Attempts) > 0 {
		want = j.Attempts[len(j.Attempts)-1].Number + 1
	}
	if attempt.Number != want {
		return &DomainError{Code: ErrAttemptNumber, Entity: "attempt", Message: "attempt number must be the next monotonic number"}
	}
	if attempt.State != AttemptLeased {
		return &DomainError{Code: ErrInvalidValue, Entity: "attempt", Message: "new attempts must start leased"}
	}
	if attempt.Lease.AttemptID != attempt.ID {
		return &DomainError{Code: ErrInvalidValue, Entity: "lease", Message: "lease attempt ID must match attempt ID"}
	}
	j.Attempts = append(j.Attempts, attempt)
	return nil
}

func (j *Job) TransitionAttempt(id AttemptID, to AttemptState, failure *Failure, at time.Time) error {
	for index := range j.Attempts {
		attempt := &j.Attempts[index]
		if attempt.ID != id {
			continue
		}
		if !CanAttemptTransition(attempt.State, to) {
			code := ErrInvalidTransition
			if attempt.State.Terminal() {
				code = ErrTerminalState
			}
			return &TransitionError{Entity: "attempt", From: string(attempt.State), To: string(to), Code: code}
		}
		attempt.State = to
		if to == AttemptRunning {
			attempt.StartedAt = &at
		}
		if to.Terminal() {
			attempt.EndedAt = &at
			attempt.Failure = failure
		}
		return nil
	}
	return &DomainError{Code: ErrAttemptNotFound, Entity: "attempt", Message: "attempt does not belong to job"}
}

// AppendEvent preserves an append-only event log by accepting only the next sequence.
func (j *Job) AppendEvent(event Event) error {
	want := uint64(1)
	if len(j.Events) > 0 {
		want = j.Events[len(j.Events)-1].Sequence + 1
	}
	if event.Sequence != want {
		return &DomainError{Code: ErrEventSequence, Entity: "event", Message: "event sequence must be contiguous and increasing"}
	}
	if event.JobID != j.ID {
		return &DomainError{Code: ErrInvalidValue, Entity: "event", Message: "event job ID must match job ID"}
	}
	j.Events = append(j.Events, event)
	return nil
}

// TimedOut concerns the job's execution deadline. Lease expiry is checked
// independently through Lease.Expired and never implies job timeout.
func (j Job) TimedOut(at time.Time) bool {
	return !j.TimeoutAt.IsZero() && !at.Before(j.TimeoutAt)
}
