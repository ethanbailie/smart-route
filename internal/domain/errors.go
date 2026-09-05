package domain

import "fmt"

type ErrorCode string

const (
	ErrInvalidTransition ErrorCode = "invalid_transition"
	ErrTerminalState     ErrorCode = "terminal_state"
	ErrActiveAttempt     ErrorCode = "active_attempt_exists"
	ErrAttemptNumber     ErrorCode = "invalid_attempt_number"
	ErrAttemptNotFound   ErrorCode = "attempt_not_found"
	ErrEventSequence     ErrorCode = "invalid_event_sequence"
	ErrInvalidValue      ErrorCode = "invalid_value"
)

type DomainError struct {
	Code    ErrorCode
	Entity  string
	Message string
}

func (e *DomainError) Error() string {
	return fmt.Sprintf("%s %s: %s", e.Entity, e.Code, e.Message)
}

type TransitionError struct {
	Entity string
	From   string
	To     string
	Code   ErrorCode
}

func (e *TransitionError) Error() string {
	return fmt.Sprintf("%s cannot transition from %q to %q: %s", e.Entity, e.From, e.To, e.Code)
}
