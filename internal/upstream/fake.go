package upstream

import (
	"context"
	"time"

	"github.com/ethan/smart-route/internal/secret"
)

type FakeOutcome string

const (
	FakeSuccess   FakeOutcome = "success"
	FakeThrottle  FakeOutcome = "throttle"
	FakeTimeout   FakeOutcome = "timeout"
	FakePermanent FakeOutcome = "permanent_failure"
)

type Fake struct {
	Outcome    FakeOutcome
	Result     Response
	Delay      time.Duration
	RetryAfter time.Duration
}

func (f *Fake) Execute(ctx context.Context, _ Request, _ secret.Bundle) (Response, error) {
	if f.Delay > 0 {
		select {
		case <-ctx.Done():
			return Response{}, ErrTimeout
		case <-time.After(f.Delay):
		}
	}
	switch f.Outcome {
	case "", FakeSuccess:
		return f.Result, nil
	case FakeThrottle:
		return Response{}, &ThrottleError{RetryAfter: f.RetryAfter}
	case FakeTimeout:
		return Response{}, ErrTimeout
	case FakePermanent:
		return Response{}, ErrPermanent
	default:
		return Response{}, ErrPermanent
	}
}
