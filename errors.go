package reliablewriter

import (
	"errors"
)

var (
	ErrClosed     = errors.New("closed")
	ErrAborted    = errors.New("aborted")
	ErrSimulated  = &RetryableError{Err: errors.New("simulated error")}
	ErrMaxRetries = errors.New("too many retry attempts")
)

type RetryableError struct {
	Err error
}

func (e *RetryableError) Error() string {
	return e.Err.Error()
}

func (e *RetryableError) Unwrap() error {
	return e.Err
}
