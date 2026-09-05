package sandbox

import (
	"errors"
	"fmt"
)

type ErrorCode string

const (
	CodeInvalid        ErrorCode = "invalid"
	CodeNotFound       ErrorCode = "not_found"
	CodeConflict       ErrorCode = "conflict"
	CodeCapacity       ErrorCode = "capacity"
	CodeAuthentication ErrorCode = "authentication"
	CodeUnavailable    ErrorCode = "unavailable"
	CodeInternal       ErrorCode = "internal"
)

var (
	ErrInvalid        = errors.New("sandbox: invalid request")
	ErrNotFound       = errors.New("sandbox: not found")
	ErrConflict       = errors.New("sandbox: conflict")
	ErrCapacity       = errors.New("sandbox: provider capacity exhausted")
	ErrAuthentication = errors.New("sandbox: provider authentication failed")
	ErrUnavailable    = errors.New("sandbox: provider unavailable")
	ErrInternal       = errors.New("sandbox: internal provider error")
)

// ProviderError gives callers a stable classification while retaining the cause.
type ProviderError struct {
	Provider  string
	Operation string
	SandboxID string
	Code      ErrorCode
	Err       error
}

func (e *ProviderError) Error() string {
	if e == nil {
		return "<nil>"
	}
	message := fmt.Sprintf("sandbox provider %q %s", e.Provider, e.Operation)
	if e.SandboxID != "" {
		message += " " + e.SandboxID
	}
	if e.Err != nil {
		message += ": " + e.Err.Error()
	}
	return message
}

func (e *ProviderError) Unwrap() error { return e.Err }

func (e *ProviderError) Is(target error) bool {
	switch e.Code {
	case CodeInvalid:
		return target == ErrInvalid
	case CodeNotFound:
		return target == ErrNotFound
	case CodeConflict:
		return target == ErrConflict
	case CodeCapacity:
		return target == ErrCapacity
	case CodeAuthentication:
		return target == ErrAuthentication
	case CodeUnavailable:
		return target == ErrUnavailable
	case CodeInternal:
		return target == ErrInternal
	default:
		return false
	}
}

func NewError(provider, operation string, code ErrorCode, err error) error {
	return &ProviderError{Provider: provider, Operation: operation, Code: code, Err: err}
}
