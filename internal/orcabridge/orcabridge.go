// Package orcabridge drives the local Orca runtime's cross-machine recovery
// verbs through the `orca` CLI. Every call scrubs Orca's remote-routing
// environment, never passes remote-selection flags, and verifies that the CLI
// reached the local runtime before touching a workspace.
package orcabridge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"
)

const (
	// DefaultBinary is where the Orca app installs its CLI shim.
	DefaultBinary = "/usr/local/bin/orca"
	// BinaryEnv names an orca CLI that New prefers over DefaultBinary and PATH
	// when Options.Binary is empty.
	BinaryEnv = "CC_SYNC_ORCA_BINARY"
	// DefaultTimeout bounds each `orca` invocation when Options.Timeout is zero.
	DefaultTimeout = 30 * time.Second
	// MaxDescriptorBytes caps a compact recovery descriptor, matching Orca's
	// MAX_RECOVERY_DESCRIPTOR_BYTES.
	MaxDescriptorBytes = 4 << 20

	maxStdoutBytes = 4 * MaxDescriptorBytes
	maxStderrBytes = 8 << 10
	killGrace      = 2 * time.Second
)

var remoteRoutingEnv = []string{"ORCA_ENVIRONMENT", "ORCA_PAIRING_CODE", "ORCA_REMOTE_PAIRING"}

// Options configures a Client.
type Options struct {
	// Binary is an explicit path to the orca CLI; empty resolves $CC_SYNC_ORCA_BINARY,
	// then DefaultBinary, then PATH.
	Binary string
	// Timeout bounds each invocation; zero means DefaultTimeout.
	Timeout time.Duration
}

// Client runs `orca` subcommands against the local runtime.
type Client struct {
	binary  string
	timeout time.Duration
}

// New resolves the orca binary to an absolute path. A missing binary is an
// UnavailableError with ReasonNotInstalled.
func New(opts Options) (*Client, error) {
	explicit := opts.Binary
	if explicit == "" {
		explicit = os.Getenv(BinaryEnv)
	}
	binary, err := resolveBinary(explicit, DefaultBinary)
	if err != nil {
		return nil, err
	}
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = DefaultTimeout
	}
	return newClient(binary, timeout), nil
}

func newClient(binary string, timeout time.Duration) *Client {
	return &Client{binary: binary, timeout: timeout}
}

func resolveBinary(explicit, wellKnown string) (string, error) {
	if explicit != "" {
		return executable(explicit)
	}
	if path, err := executable(wellKnown); err == nil {
		return path, nil
	}
	path, err := exec.LookPath("orca")
	if err != nil {
		return "", &UnavailableError{Reason: ReasonNotInstalled, Detail: err.Error()}
	}
	return executable(path)
}

func executable(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve orca path %q: %w", path, err)
	}
	info, err := os.Stat(abs) //nolint:gosec // G703: stats the operator-chosen orca binary (CC_SYNC_ORCA_BINARY, the well-known path, or PATH) before running it.
	if err != nil {
		return "", &UnavailableError{Reason: ReasonNotInstalled, Detail: err.Error()}
	}
	if info.IsDir() || info.Mode().Perm()&0o111 == 0 {
		return "", &UnavailableError{Reason: ReasonNotInstalled, Detail: abs + " is not executable"}
	}
	return abs, nil
}

func localEnv() []string {
	return slices.DeleteFunc(os.Environ(), func(kv string) bool {
		key, _, _ := strings.Cut(kv, "=")
		return slices.Contains(remoteRoutingEnv, key)
	})
}

type envelope struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result"`
	Error  envelopeError   `json:"error"`
}

type envelopeError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type cappedBuffer struct {
	buf      bytes.Buffer
	max      int
	overflow bool
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	room := b.max - b.buf.Len()
	if len(p) > room {
		b.overflow = true
		b.buf.Write(p[:max(room, 0)])
		return len(p), nil
	}
	b.buf.Write(p)
	return len(p), nil
}

func (c *Client) run(ctx context.Context, stdin []byte, result any, args ...string) error {
	verb := strings.Join(args[:min(2, len(args))], " ")
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, c.binary, args...) //nolint:gosec // G204: binary is the resolved orca CLI and argv is a fixed verb plus exec-level arguments, never a shell string.
	cmd.Env = localEnv()
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	stdout := &cappedBuffer{max: maxStdoutBytes}
	stderr := &cappedBuffer{max: maxStderrBytes}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = killGrace

	start := time.Now()
	runErr := cmd.Run()
	slog.Debug("orca exec", "args", args, "duration", time.Since(start), "err", runErr)
	if ctx.Err() != nil {
		return fmt.Errorf("orca %s: %w", verb, ctx.Err())
	}
	if errors.Is(runErr, fs.ErrNotExist) || errors.Is(runErr, exec.ErrNotFound) {
		return &UnavailableError{Reason: ReasonNotInstalled, Detail: runErr.Error()}
	}
	var exitErr *exec.ExitError
	if runErr != nil && !errors.As(runErr, &exitErr) {
		return fmt.Errorf("orca %s: %w", verb, runErr)
	}
	if stdout.overflow {
		return fmt.Errorf("orca %s: output exceeds %d bytes", verb, maxStdoutBytes)
	}

	var env envelope
	if err := json.Unmarshal(stdout.buf.Bytes(), &env); err != nil {
		return fmt.Errorf("orca %s (%v): decode output: %w; stderr: %q", verb, cmd.ProcessState, err, stderr.buf.String())
	}
	if !env.OK {
		return errorFromEnvelope(verb, env.Error)
	}
	if err := json.Unmarshal(env.Result, result); err != nil {
		return fmt.Errorf("orca %s: decode result: %w", verb, err)
	}
	return nil
}
