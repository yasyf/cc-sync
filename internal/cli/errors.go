package cli

import (
	"context"
	"errors"
	"fmt"
)

// Code is the machine-readable category of a failed command.
type Code string

// Code values; the picker-facing ones match the Orca contract.
const (
	CodeInternal           Code = "internal"
	CodeUsage              Code = "usage"
	CodeNotFound           Code = "not-found"
	CodeNotReady           Code = "not-ready"
	CodeLiveLocalCollision Code = "live-local-collision"
	CodeOrcaNotLocal       Code = "orca-not-local"
	CodeCheckoutConflict   Code = "checkout-conflict"
	CodeUnsupported        Code = "unsupported"
	CodeOrcaUnavailable    Code = "orca-unavailable"
	CodeUnavailable        Code = "unavailable"
	CodeCancelled          Code = "cancelled"
)

// Exit codes by failure class.
const (
	ExitOK          = 0
	ExitError       = 1
	ExitUsage       = 2
	ExitNotFound    = 3
	ExitRefused     = 4
	ExitUnavailable = 5
)

// ExitCode is the process exit code for a failure with code c.
func (c Code) ExitCode() int {
	switch c {
	case CodeUsage:
		return ExitUsage
	case CodeNotFound:
		return ExitNotFound
	case CodeNotReady, CodeLiveLocalCollision, CodeOrcaNotLocal, CodeCheckoutConflict, CodeUnsupported:
		return ExitRefused
	case CodeOrcaUnavailable, CodeUnavailable:
		return ExitUnavailable
	case CodeInternal, CodeCancelled:
		return ExitError
	}
	panic(fmt.Sprintf("cli: unknown code %q", c))
}

// Error is a command failure carrying its Code. Service implementations return
// it (possibly wrapped) to choose the reported code and exit status.
type Error struct {
	Code Code
	Err  error
}

func (e *Error) Error() string { return e.Err.Error() }

func (e *Error) Unwrap() error { return e.Err }

// Errorf builds an *Error with code and a formatted cause; %w is honored.
func Errorf(code Code, format string, args ...any) error {
	return &Error{Code: code, Err: fmt.Errorf(format, args...)}
}

// Classify reports the Code of err: the code of the first *Error in its chain,
// cancelled for a context cancellation, and internal otherwise.
func Classify(err error) Code {
	if e, ok := errors.AsType[*Error](err); ok {
		return e.Code
	}
	if errors.Is(err, context.Canceled) {
		return CodeCancelled
	}
	return CodeInternal
}
