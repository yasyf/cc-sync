package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-sync/internal/cli"
	"github.com/yasyf/daemonkit"
	"github.com/yasyf/synckit/rpc"
)

type fakeInstaller struct {
	res   cli.InstallResult
	err   error
	calls int
}

func (f *fakeInstaller) Install(context.Context, cli.InstallRequest) (cli.InstallResult, error) {
	f.calls++
	return f.res, f.err
}

func (f *fakeInstaller) Uninstall(context.Context, cli.UninstallRequest) (cli.UninstallResult, error) {
	return cli.UninstallResult{}, f.err
}

type startingHelper struct {
	fakeHelper
	starting []error
	onCall   func()
	calls    int
}

func (h *startingHelper) Status(ctx context.Context) (HelperStatus, error) {
	h.calls++
	if h.onCall != nil {
		h.onCall()
	}
	if h.calls <= len(h.starting) {
		return HelperStatus{}, h.starting[h.calls-1]
	}
	return h.fakeHelper.Status(ctx)
}

func transport(err error) error {
	return fmt.Errorf("%w: ccsync.status.v1: %w", ErrUnavailable, &rpc.TransportError{Undispatched: true, Err: err})
}

func installConfig(installer *fakeInstaller, helper Helper, timeout time.Duration) Config {
	cfg := newConfig()
	cfg.Installer = installer
	cfg.Helper = helper
	cfg.HelperTimeout = timeout
	cfg.HelperPoll = time.Millisecond
	return cfg
}

func installed() *fakeInstaller {
	return &fakeInstaller{res: cli.InstallResult{ConfigDir: "/Users/me/.config/cc-sync", Synckitd: true}}
}

func TestInstallWaitsForHelper(t *testing.T) {
	ready := fakeHelper{status: HelperStatus{Build: "v0.1.0"}}
	running := cli.Helper{Running: true, Build: "v0.1.0"}
	notReady := transport(daemonkit.ErrNotReady)
	tests := []struct {
		name      string
		req       cli.InstallRequest
		helper    *startingHelper
		want      cli.Helper
		wantCode  cli.Code
		wantCalls int
	}{
		{name: "ready immediately", helper: &startingHelper{fakeHelper: ready}, want: running, wantCalls: 1},
		{name: "not ready twice then ready", helper: &startingHelper{fakeHelper: ready, starting: []error{notReady, notReady}}, want: running, wantCalls: 3},
		{name: "absent then ready", helper: &startingHelper{fakeHelper: ready, starting: []error{transport(daemonkit.ErrAbsent)}}, want: running, wantCalls: 2},
		{name: "draining incumbent then ready", helper: &startingHelper{fakeHelper: ready, starting: []error{transport(daemonkit.ErrDraining), transport(daemonkit.ErrPeerGone)}}, want: running, wantCalls: 3},
		{name: "untrusted helper fails at once", helper: &startingHelper{fakeHelper: fakeHelper{err: transport(daemonkit.ErrUntrusted)}}, wantCode: cli.CodeUnavailable, wantCalls: 1},
		{name: "helper error fails at once", helper: &startingHelper{fakeHelper: fakeHelper{err: errors.New("ccsync.status.v1: boom")}}, wantCode: cli.CodeInternal, wantCalls: 1},
		{name: "no-synckitd reports a stopped helper without waiting", req: cli.InstallRequest{NoSynckitd: true}, helper: &startingHelper{fakeHelper: fakeHelper{err: notReady}}, wantCalls: 1},
		{name: "no-synckitd reports a running helper", req: cli.InstallRequest{NoSynckitd: true}, helper: &startingHelper{fakeHelper: ready}, want: running, wantCalls: 1},
		{name: "no-synckitd helper error fails", req: cli.InstallRequest{NoSynckitd: true}, helper: &startingHelper{fakeHelper: fakeHelper{err: errors.New("ccsync.status.v1: boom")}}, wantCode: cli.CodeInternal, wantCalls: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			installer := installed()
			res, err := New(installConfig(installer, tt.helper, time.Minute)).Install(t.Context(), tt.req)
			if tt.helper.calls != tt.wantCalls {
				t.Errorf("status calls = %d, want %d", tt.helper.calls, tt.wantCalls)
			}
			if installer.calls != 1 {
				t.Errorf("install calls = %d, want 1", installer.calls)
			}
			if tt.wantCode != "" {
				if code := cli.Classify(err); code != tt.wantCode {
					t.Fatalf("Install error %v classified %q, want %q", err, code, tt.wantCode)
				}
				return
			}
			if err != nil {
				t.Fatalf("Install: %v", err)
			}
			want := cli.InstallResult{ConfigDir: "/Users/me/.config/cc-sync", Synckitd: true, Helper: tt.want}
			if res != want {
				t.Errorf("Install = %+v, want %+v", res, want)
			}
		})
	}
}

func TestInstallHelperNeverReady(t *testing.T) {
	helper := &startingHelper{fakeHelper: fakeHelper{err: transport(daemonkit.ErrNotReady)}}
	svc := New(installConfig(installed(), helper, 20*time.Millisecond))
	_, err := svc.Install(t.Context(), cli.InstallRequest{})
	if !errors.Is(err, daemonkit.ErrNotReady) {
		t.Errorf("Install error %v does not wrap ErrNotReady", err)
	}
	if code := cli.Classify(err); code != cli.CodeUnavailable {
		t.Errorf("Install error classified %q, want %q", code, cli.CodeUnavailable)
	}
	if helper.calls < 2 {
		t.Errorf("status calls = %d, want the wait to retry", helper.calls)
	}
	helper.calls = 0
	exit, stdout, _ := run(t, svc, "install", "--json")
	if exit != cli.ExitUnavailable {
		t.Errorf("exit = %d, want %d", exit, cli.ExitUnavailable)
	}
	golden(t, "install_not_ready.json", stdout)
	exit, _, stderr := run(t, svc, "install")
	want := "cc-sync: helper installed but not ready after 20ms; run `cc-sync status` in a few seconds, or `synckitd install` again: unavailable: ccsync.status.v1: rpc transport (undispatched): daemonkit: daemon is starting\n"
	if exit != cli.ExitUnavailable || stderr != want {
		t.Errorf("install = %d %q, want %d %q", exit, stderr, cli.ExitUnavailable, want)
	}
}

func TestInstallWaitHonorsCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	helper := &startingHelper{fakeHelper: fakeHelper{err: transport(daemonkit.ErrNotReady)}, onCall: cancel}
	_, err := New(installConfig(installed(), helper, time.Hour)).Install(ctx, cli.InstallRequest{})
	if code := cli.Classify(err); code != cli.CodeCancelled {
		t.Errorf("Install error %v classified %q, want %q", err, code, cli.CodeCancelled)
	}
	if helper.calls != 1 {
		t.Errorf("status calls = %d, want 1", helper.calls)
	}
}

func TestInstallWaitEndsAtCallerDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	helper := &startingHelper{fakeHelper: fakeHelper{err: transport(daemonkit.ErrNotReady)}}
	_, err := New(installConfig(installed(), helper, time.Hour)).Install(ctx, cli.InstallRequest{})
	if code := cli.Classify(err); code != cli.CodeUnavailable {
		t.Fatalf("Install error %v classified %q, want %q", err, code, cli.CodeUnavailable)
	}
	if !errors.Is(err, daemonkit.ErrNotReady) {
		t.Errorf("Install error %v does not wrap ErrNotReady", err)
	}
	msg := err.Error()
	if !strings.Contains(msg, "run `cc-sync status` in a few seconds") || strings.Contains(msg, time.Hour.String()) {
		t.Errorf("Install error %q, want the recovery step without claiming the 1h wait elapsed", msg)
	}
	if helper.calls < 2 {
		t.Errorf("status calls = %d, want the wait to retry", helper.calls)
	}
}

func TestInstallFailureSkipsHelper(t *testing.T) {
	installer := &fakeInstaller{err: fmt.Errorf("%w: synckitd install: exit status 1", ErrUnavailable)}
	helper := &startingHelper{fakeHelper: fakeHelper{status: HelperStatus{Build: "v0.1.0"}}}
	_, err := New(installConfig(installer, helper, time.Minute)).Install(t.Context(), cli.InstallRequest{})
	if code := cli.Classify(err); code != cli.CodeUnavailable {
		t.Errorf("Install error %v classified %q, want %q", err, code, cli.CodeUnavailable)
	}
	if helper.calls != 0 {
		t.Errorf("status calls = %d, want 0", helper.calls)
	}
}
