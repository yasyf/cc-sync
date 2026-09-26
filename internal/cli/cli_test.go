package cli_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/cc-sync/internal/cli"
)

var update = flag.Bool("update", false, "rewrite golden files")

func golden(t *testing.T, name string, got []byte) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if *update {
		if err := os.WriteFile(path, got, 0o600); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := fs.ReadFile(os.DirFS("testdata"), name)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("%s mismatch:\n--- got\n%s\n--- want\n%s", name, got, want)
	}
}

func oneJSONLine(t *testing.T, stdout string) []byte {
	t.Helper()
	if strings.Count(stdout, "\n") != 1 || !strings.HasSuffix(stdout, "\n") {
		t.Fatalf("stdout is not exactly one line: %q", stdout)
	}
	var indented bytes.Buffer
	if err := json.Indent(&indented, []byte(stdout), "", "  "); err != nil {
		t.Fatalf("stdout is not JSON: %v: %q", err, stdout)
	}
	return indented.Bytes()
}

func TestJSONGolden(t *testing.T) {
	tests := []struct {
		name string
		svc  *fakeService
		args []string
	}{
		{"status_full", &fakeService{status: fullStatus()}, []string{"status", "--json"}},
		{"status_empty", &fakeService{status: cli.StatusResult{Local: cli.LocalHost{Network: cli.Network{Status: cli.NetworkUnknown}}}}, []string{"status", "--json"}},
		{"list_full", &fakeService{list: cli.ListResult{
			GeneratedAt: ts(12, 10),
			Local:       cli.Host{HostID: "host-air", HostName: "air"},
			Items:       []cli.Item{fullItem(), deferredItem(), sparseItem()},
		}}, []string{"list", "--json"}},
		{"list_empty", &fakeService{list: cli.ListResult{GeneratedAt: ts(12, 10), Local: cli.Host{HostID: "host-air", HostName: "air"}}}, []string{"--json", "list"}},
		{"inspect", &fakeService{inspect: fullInspect()}, []string{"inspect", "host-mbp/wt-7f3a", "--json"}},
		{"inspect_sparse", &fakeService{inspect: cli.InspectResult{Item: sparseItem()}}, []string{"inspect", "abc", "--json"}},
		{"pickup_orca", &fakeService{pickup: orcaPickup()}, []string{"pickup", "host-mbp/wt-7f3a", "--json"}},
		{"pickup_cli_only", &fakeService{pickup: cliOnlyPickup()}, []string{"pickup", "0f3c", "--no-orca", "--json"}},
		{"pickup_orca_empty", &fakeService{pickup: cli.PickupResult{Orca: &cli.OrcaPickup{WorktreeID: "orca-wt-9"}}}, []string{"pickup", "0f3c", "--json"}},
		{"pickup_fork_refused", &fakeService{pickup: forkRefusedPickup()}, []string{"pickup", "0f3c", "--on-divergence", "fork", "--json"}},
		{"pickup_partial_sparse", &fakeService{pickup: partialSparsePickup()}, []string{"pickup", "0f3c", "--checkpoint", "d00d", "--allow-partial", "--json"}},
		{"sync", &fakeService{sync: cli.SyncResult{Worktrees: []cli.SyncedWorktree{
			{WorkspaceID: "wt-7f3a", Sessions: []string{"0f3c9a2e-5b1d-4c8e-9a7f-2d6b8e1c4a90"}, Checkpoint: &cli.Checkpoint{ID: "c0ffee1234", Tier: cli.TierLatest, CapturedAt: ts(12, 0)}},
			{WorkspaceID: "wt-01", Deferred: []string{"lfs object missing: assets/model.bin"}},
		}}}, []string{"sync", "--json"}},
		{"install", &fakeService{install: cli.InstallResult{ConfigDir: "/Users/yasyf/.config/cc-sync", Synckitd: true, Helper: cli.Helper{Running: true, Build: "v0.1.0"}}}, []string{"install", "--json"}},
		{"uninstall", &fakeService{uninstall: cli.UninstallResult{Purged: true}}, []string{"uninstall", "--purge", "--json"}},
		{"resume", &fakeService{resume: cli.ResumeResult{SessionID: "0f3c9a2e-5b1d-4c8e-9a7f-2d6b8e1c4a90", Cwd: "/Users/yasyf/Code/monorepo-recovered"}}, []string{"resume", "0f3c", "--json"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := run(tt.svc, tt.args...)
			if got.exit != cli.ExitOK || got.stderr != "" {
				t.Fatalf("exit = %d, stderr = %q; want 0 and empty", got.exit, got.stderr)
			}
			golden(t, tt.name+".json", oneJSONLine(t, got.stdout))
		})
	}
}

func TestHumanGolden(t *testing.T) {
	tests := []struct {
		name string
		svc  *fakeService
		args []string
	}{
		{"status", &fakeService{status: fullStatus()}, []string{"status"}},
		{"list", &fakeService{list: cli.ListResult{Items: []cli.Item{fullItem(), deferredItem(), sparseItem()}}}, []string{"list"}},
		{"list_empty", &fakeService{}, []string{"list"}},
		{"inspect", &fakeService{inspect: fullInspect()}, []string{"inspect", "host-mbp/wt-7f3a"}},
		{"inspect_deferred", &fakeService{inspect: cli.InspectResult{Item: deferredItem()}}, []string{"inspect", "host-mbp/wt-9c1d"}},
		{"pickup_cli_only", &fakeService{pickup: cliOnlyPickup()}, []string{"pickup", "0f3c"}},
		{"pickup_orca", &fakeService{pickup: orcaPickup()}, []string{"pickup", "0f3c"}},
		{"pickup_fork_refused", &fakeService{pickup: forkRefusedPickup()}, []string{"pickup", "0f3c", "--on-divergence", "fork"}},
		{"pickup_partial_sparse", &fakeService{pickup: partialSparsePickup()}, []string{"pickup", "0f3c", "--checkpoint", "d00d", "--allow-partial"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := run(tt.svc, tt.args...)
			if got.exit != cli.ExitOK || got.stderr != "" {
				t.Fatalf("exit = %d, stderr = %q; want 0 and empty", got.exit, got.stderr)
			}
			golden(t, tt.name+".txt", []byte(got.stdout))
		})
	}
}

type envelope struct {
	Version int  `json:"version"`
	OK      bool `json:"ok"`
	Error   struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func TestJSONErrors(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		args     []string
		wantCode string
		wantMsg  string
		wantExit int
	}{
		{"not found", cli.Errorf(cli.CodeNotFound, "no item matches %q", "zz"), []string{"inspect", "host/wt", "--json"}, "not-found", `no item matches "zz"`, 3},
		{"not ready", cli.Errorf(cli.CodeNotReady, "checkpoint c0ffee missing lfs:a.bin"), []string{"pickup", "host/wt", "--json"}, "not-ready", "checkpoint c0ffee missing lfs:a.bin", 4},
		{"wrapped live collision", errors.Join(errors.New("pickup"), cli.Errorf(cli.CodeLiveLocalCollision, "session live")), []string{"pickup", "0f3c", "--json"}, "live-local-collision", "pickup\nsession live", 4},
		{"orca not local", cli.Errorf(cli.CodeOrcaNotLocal, "orca targets a remote runtime"), []string{"pickup", "0f3c", "--json"}, "orca-not-local", "orca targets a remote runtime", 4},
		{"checkout conflict", cli.Errorf(cli.CodeCheckoutConflict, "dest exists"), []string{"pickup", "0f3c", "--json"}, "checkout-conflict", "dest exists", 4},
		{"unsupported", cli.Errorf(cli.CodeUnsupported, "folder workspaces are not recoverable"), []string{"pickup", "0f3c", "--json"}, "unsupported", "folder workspaces are not recoverable", 4},
		{"incompatible", cli.Errorf(cli.CodeIncompatible, "destination claude does not advertise --resume"), []string{"pickup", "0f3c", "--json"}, "incompatible", "destination claude does not advertise --resume", 4},
		{"orca unavailable", cli.Errorf(cli.CodeOrcaUnavailable, "orca not running"), []string{"pickup", "0f3c", "--json"}, "orca-unavailable", "orca not running", 5},
		{"helper unavailable", cli.Errorf(cli.CodeUnavailable, "helper not running"), []string{"status", "--json"}, "unavailable", "helper not running", 5},
		{"internal", errors.New("boom"), []string{"list", "--json"}, "internal", "boom", 1},
		{"unknown flag", nil, []string{"list", "--bogus", "--json"}, "usage", "unknown flag: --bogus", 2},
		{"unknown flag before json", nil, []string{"pickup", "--nope", "0f3c", "--json"}, "usage", "unknown flag: --nope", 2},
		{"unknown command", nil, []string{"--json", "bogus"}, "usage", `unknown command "bogus" for "cc-sync"`, 2},
		{"missing arg", nil, []string{"pickup", "--json"}, "usage", "accepts 1 arg(s), received 0", 2},
		{"extra arg", nil, []string{"status", "extra", "--json"}, "usage", `unknown command "extra" for "cc-sync status"`, 2},
		{"bad selector", nil, []string{"inspect", "/wt", "--json"}, "usage", `invalid item selector "/wt": want <source_host_id>/<workspace_id>`, 2},
		{"bad checkpoint", nil, []string{"pickup", "0f3c", "--checkpoint", "yesterday", "--json"}, "usage", `invalid checkpoint "yesterday": want latest, <id-prefix>, at:<RFC3339>, hourly:-<N>h, or daily:<YYYY-MM-DD>`, 2},
		{"bad progress", nil, []string{"pickup", "0f3c", "--progress", "xml", "--json"}, "usage", `invalid --progress "xml": want ndjson`, 2},
		{"bad on-divergence", nil, []string{"pickup", "0f3c", "--on-divergence", "merge", "--json"}, "usage", `invalid argument "merge" for "--on-divergence" flag: want refuse, keep-local, replace, or fork`, 2},
		{"bad resume id", nil, []string{"pickup", "0f3c", "--resume", "NOPE", "--json"}, "usage", `invalid session id "NOPE": want a lowercase session id or prefix`, 2},
		{"bad sync session", nil, []string{"sync", "--session", "x", "--json"}, "usage", `invalid session id "x": want a lowercase session id or prefix`, 2},
		{"json equals true", cli.Errorf(cli.CodeNotFound, "none"), []string{"list", "--json=true"}, "not-found", "none", 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := run(&fakeService{err: tt.err}, tt.args...)
			if got.exit != tt.wantExit {
				t.Errorf("exit = %d, want %d", got.exit, tt.wantExit)
			}
			if got.stderr != "" {
				t.Errorf("stderr = %q, want empty", got.stderr)
			}
			oneJSONLine(t, got.stdout)
			var env envelope
			dec := json.NewDecoder(strings.NewReader(got.stdout))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&env); err != nil {
				t.Fatalf("decode %q: %v", got.stdout, err)
			}
			if env.Version != 1 || env.OK || env.Error.Code != tt.wantCode || env.Error.Message != tt.wantMsg {
				t.Errorf("envelope = %+v, want version 1, ok false, code %q, message %q", env, tt.wantCode, tt.wantMsg)
			}
		})
	}
}

func TestDivergenceError(t *testing.T) {
	err := fmt.Errorf("pickup: %w", cli.DivergentLocalCopy(
		cli.DivergenceDetails{SessionID: humanSession, LocalLastActivityAt: ts(12, 30), PickedCapturedAt: ts(12, 0)},
		errors.New("session 0f3c9a2e-5b1d-4c8e-9a7f-2d6b8e1c4a90 has local records the picked checkpoint lacks"),
	))
	tests := []struct {
		name       string
		args       []string
		wantStdout string
		wantStderr string
	}{
		{
			"json",
			[]string{"pickup", "0f3c", "--json"},
			`{"version":1,"ok":false,"error":{"code":"divergent-local-copy","message":"pickup: session 0f3c9a2e-5b1d-4c8e-9a7f-2d6b8e1c4a90 has local records the picked checkpoint lacks","details":{"session_id":"0f3c9a2e-5b1d-4c8e-9a7f-2d6b8e1c4a90","local_last_activity_at":"2026-09-26T19:30:00Z","picked_captured_at":"2026-09-26T19:00:00Z"}}}` + "\n",
			"",
		},
		{
			"human",
			[]string{"pickup", "0f3c"},
			"",
			"cc-sync: pickup: session 0f3c9a2e-5b1d-4c8e-9a7f-2d6b8e1c4a90 has local records the picked checkpoint lacks\ncc-sync: re-run with --on-divergence keep-local, replace, or fork\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := run(&fakeService{err: err}, tt.args...)
			if got.exit != cli.ExitRefused || got.stdout != tt.wantStdout || got.stderr != tt.wantStderr {
				t.Errorf("got exit %d stdout %q stderr %q; want exit 4 stdout %q stderr %q", got.exit, got.stdout, got.stderr, tt.wantStdout, tt.wantStderr)
			}
		})
	}
}

func TestInteractivePickup(t *testing.T) {
	launchEnv := []string{"HOME=/Users/yasyf", "PATH=/usr/bin:/bin", "TERM=xterm-256color"}
	withEnvSet := cliOnlyPickup()
	withEnvSet.Sessions[0].Launch.EnvSet = cli.Env{"PATH": "/opt/homebrew/bin:/usr/bin", "CC_SYNC_PICKED": "1"}
	twoRestored := cliOnlyPickup()
	twoRestored.Sessions = append(twoRestored.Sessions, cli.PickedSession{
		SessionID: backgroundSession, Status: cli.SessionRestored, Selected: true, Launch: claudeLaunch("/Users/yasyf/Code/monorepo-recovered", backgroundSession),
	})
	noLaunch := cliOnlyPickup()
	noLaunch.Sessions[0].Launch = nil
	tests := []struct {
		name        string
		pickup      cli.PickupResult
		interactive bool
		execErr     error
		args        []string
		wantExit    int
		wantStdout  string
		wantStderr  string
		wantCalls   []execCall
		wantPickup  bool
	}{
		{
			name: "cli-only execs the selected session", pickup: cliOnlyPickup(), interactive: true, args: []string{"pickup", "0f3c", "--no-orca"},
			wantStdout: "checkout reused: /Users/yasyf/Code/monorepo-recovered on (detached)\nsession 0f3c9a2e-5b1d-4c8e-9a7f-2d6b8e1c4a90: restored\nresuming 0f3c9a2e-5b1d-4c8e-9a7f-2d6b8e1c4a90 with claude in /Users/yasyf/Code/monorepo-recovered\n",
			wantCalls:  []execCall{{argv: []string{"/opt/homebrew/bin/claude", "--resume", humanSession}, dir: "/Users/yasyf/Code/monorepo-recovered", env: launchEnv}},
			wantPickup: true,
		},
		{
			name: "env adjustments", pickup: withEnvSet, interactive: true, args: []string{"pickup", "0f3c"},
			wantStdout: "checkout reused: /Users/yasyf/Code/monorepo-recovered on (detached)\nsession 0f3c9a2e-5b1d-4c8e-9a7f-2d6b8e1c4a90: restored\nresuming 0f3c9a2e-5b1d-4c8e-9a7f-2d6b8e1c4a90 with claude in /Users/yasyf/Code/monorepo-recovered\n",
			wantCalls: []execCall{{
				argv: []string{"/opt/homebrew/bin/claude", "--resume", humanSession},
				dir:  "/Users/yasyf/Code/monorepo-recovered",
				env:  []string{"HOME=/Users/yasyf", "TERM=xterm-256color", "CC_SYNC_PICKED=1", "PATH=/opt/homebrew/bin:/usr/bin"},
			}},
			wantPickup: true,
		},
		{
			name: "orca import without resume execs", pickup: orcaDormantPickup(), interactive: true, args: []string{"pickup", "0f3c"},
			wantStdout: "checkout restored: " + recoveredCheckout + " on feature/sync\nsession 0f3c9a2e-5b1d-4c8e-9a7f-2d6b8e1c4a90: dormant\nsession 7a1e4c2b-9d3f-4b6a-8e5c-1f0a2b3c4d5e: dormant\norca worktree orca-wt-9: 0 resumed, 2 dormant\nresuming 0f3c9a2e-5b1d-4c8e-9a7f-2d6b8e1c4a90 with claude in " + recoveredCheckout + "\n",
			wantCalls:  []execCall{{argv: []string{"/opt/homebrew/bin/claude", "--resume", humanSession}, dir: recoveredCheckout, env: launchEnv}},
			wantPickup: true,
		},
		{
			name: "orca resumed never execs", pickup: orcaPickup(), interactive: true, args: []string{"pickup", "0f3c"},
			wantStdout: "checkout restored: " + recoveredCheckout + " on feature/sync\nsession 0f3c9a2e-5b1d-4c8e-9a7f-2d6b8e1c4a90: resumed\nsession 7a1e4c2b-9d3f-4b6a-8e5c-1f0a2b3c4d5e: dormant\norca worktree orca-wt-9: 1 resumed, 1 dormant\n",
			wantPickup: true,
		},
		{
			name: "json never execs", pickup: cliOnlyPickup(), interactive: true, args: []string{"pickup", "0f3c", "--no-orca", "--json"},
			wantStdout: `{"version":1,"ok":true,"checkpoint":{"id":"c0ffee12","captured_at":"2026-09-26T12:00:00Z","partial":false},"checkout":{"path":"/Users/yasyf/Code/monorepo-recovered","branch":null,"reused":true,"newer":false,"lfs_pending":[],"exact":false,"differences":[],"sparse":null},"sessions":[{"session_id":"0f3c9a2e-5b1d-4c8e-9a7f-2d6b8e1c4a90","status":"restored","selected":true,"launch":{"argv":["/opt/homebrew/bin/claude","--resume","0f3c9a2e-5b1d-4c8e-9a7f-2d6b8e1c4a90"],"dir":"/Users/yasyf/Code/monorepo-recovered","env_unset":["CLAUDECODE","CLAUDE_CODE_ENTRYPOINT"],"env_set":{}}}],"orca":null}` + "\n",
			wantPickup: true,
		},
		{
			name: "non-tty never execs", pickup: cliOnlyPickup(), args: []string{"pickup", "0f3c"},
			wantStdout: "checkout reused: /Users/yasyf/Code/monorepo-recovered on (detached)\nsession 0f3c9a2e-5b1d-4c8e-9a7f-2d6b8e1c4a90: restored\nresume with: cc-sync resume 0f3c9a2e-5b1d-4c8e-9a7f-2d6b8e1c4a90\n",
			wantPickup: true,
		},
		{
			name: "dry run never execs", pickup: cliOnlyPickup(), interactive: true, args: []string{"pickup", "0f3c", "--dry-run"},
			wantStdout: "checkout reused: /Users/yasyf/Code/monorepo-recovered on (detached)\nsession 0f3c9a2e-5b1d-4c8e-9a7f-2d6b8e1c4a90: restored\nresume with: cc-sync resume 0f3c9a2e-5b1d-4c8e-9a7f-2d6b8e1c4a90\n",
			wantPickup: true,
		},
		{
			name: "two selected without orca resume", pickup: twoRestored, interactive: true, args: []string{"pickup", "0f3c", "--resume", "0f3c", "--resume", "7a1e"},
			wantExit:   cli.ExitUsage,
			wantStderr: "cc-sync: sessions 0f3c9a2e-5b1d-4c8e-9a7f-2d6b8e1c4a90, 7a1e4c2b-9d3f-4b6a-8e5c-1f0a2b3c4d5e are restored but not resumed and one terminal resumes one session: run cc-sync resume <session> for each\n",
			wantPickup: true,
		},
		{
			name: "no-orca with two resumes refuses before pickup", pickup: twoRestored, interactive: true, args: []string{"pickup", "0f3c", "--no-orca", "--resume", "0f3c", "--resume", "7a1e"},
			wantExit:   cli.ExitUsage,
			wantStderr: "cc-sync: --no-orca selects 0f3c, 7a1e but one terminal resumes one session: pass one --resume, or --json for every session's launch\n",
		},
		{
			name: "selected session without launch", pickup: noLaunch, interactive: true, args: []string{"pickup", "0f3c"},
			wantExit:   cli.ExitError,
			wantStderr: "cc-sync: selected session 0f3c9a2e-5b1d-4c8e-9a7f-2d6b8e1c4a90 has no launch\n",
			wantPickup: true,
		},
		{
			name: "exec failure", pickup: cliOnlyPickup(), interactive: true, execErr: errors.New("exec /opt/homebrew/bin/claude: no such file or directory"), args: []string{"pickup", "0f3c"},
			wantExit:   cli.ExitError,
			wantStdout: "checkout reused: /Users/yasyf/Code/monorepo-recovered on (detached)\nsession 0f3c9a2e-5b1d-4c8e-9a7f-2d6b8e1c4a90: restored\nresuming 0f3c9a2e-5b1d-4c8e-9a7f-2d6b8e1c4a90 with claude in /Users/yasyf/Code/monorepo-recovered\n",
			wantStderr: "cc-sync: exec /opt/homebrew/bin/claude: no such file or directory\n",
			wantCalls:  []execCall{{argv: []string{"/opt/homebrew/bin/claude", "--resume", humanSession}, dir: "/Users/yasyf/Code/monorepo-recovered", env: launchEnv}},
			wantPickup: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &fakeService{pickup: tt.pickup}
			ex := &fakeExec{err: tt.execErr}
			got := runOn(svc, ex.terminal(tt.interactive), tt.args...)
			if got.exit != tt.wantExit || got.stdout != tt.wantStdout || got.stderr != tt.wantStderr {
				t.Errorf("got exit %d stdout %q stderr %q; want exit %d stdout %q stderr %q", got.exit, got.stdout, got.stderr, tt.wantExit, tt.wantStdout, tt.wantStderr)
			}
			if !reflect.DeepEqual(ex.calls, tt.wantCalls) {
				t.Errorf("exec calls = %#v, want %#v", ex.calls, tt.wantCalls)
			}
			if called := svc.got != nil; called != tt.wantPickup {
				t.Errorf("service called = %t, want %t", called, tt.wantPickup)
			}
		})
	}
}

func TestHumanErrors(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		args       []string
		wantStderr string
		wantExit   int
	}{
		{"service", cli.Errorf(cli.CodeNotFound, "no items"), []string{"list"}, "cc-sync: no items\n", 3},
		{"usage", nil, []string{"list", "--bogus"}, "cc-sync: unknown flag: --bogus\n", 2},
		{"json disabled", nil, []string{"list", "--json=false", "--bogus"}, "cc-sync: unknown flag: --bogus\n", 2},
		{"json after terminator", nil, []string{"pickup", "--", "--json"}, "cc-sync: invalid session id \"--json\": want a lowercase session id or prefix\n", 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := run(&fakeService{err: tt.err}, tt.args...)
			if got.exit != tt.wantExit || got.stdout != "" || got.stderr != tt.wantStderr {
				t.Errorf("got exit %d stdout %q stderr %q; want exit %d stdout empty stderr %q", got.exit, got.stdout, got.stderr, tt.wantExit, tt.wantStderr)
			}
		})
	}
}

func TestUnavailableJSON(t *testing.T) {
	commands := [][]string{
		{"install"},
		{"uninstall"},
		{"list"},
		{"inspect", "h/w"},
		{"status"},
		{"sync"},
		{"pickup", "h/w"},
		{"resume", "0f3c"},
		{"helper-serve"},
	}
	for _, args := range commands {
		t.Run(args[0], func(t *testing.T) {
			got := run(&fakeService{err: cli.Errorf(cli.CodeUnavailable, "helper not running")}, append(args, "--json")...)
			want := `{"version":1,"ok":false,"error":{"code":"unavailable","message":"helper not running"}}` + "\n"
			if got.exit != cli.ExitUnavailable || got.stdout != want {
				t.Errorf("got exit %d stdout %q; want exit 5 stdout %q", got.exit, got.stdout, want)
			}
		})
	}
}

func TestRequests(t *testing.T) {
	at := time.Date(2026, 9, 26, 19, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		args []string
		want any
	}{
		{"install", []string{"install", "--no-synckitd"}, cli.InstallRequest{NoSynckitd: true}},
		{"uninstall", []string{"uninstall"}, cli.UninstallRequest{}},
		{"list filters", []string{"list", "--source", "host-mbp", "--repo", "git@github.com:yasyf/monorepo.git", "--all"}, cli.ListRequest{Source: "host-mbp", Repo: "git@github.com:yasyf/monorepo.git", All: true}},
		{"inspect default checkpoint", []string{"inspect", "host-mbp:0f3c"}, cli.InspectRequest{Target: cli.SessionRef{Source: "host-mbp", ID: "0f3c"}, Checkpoint: cli.LatestCheckpoint{}}},
		{"inspect hourly", []string{"inspect", "host-mbp/wt-7f3a", "--checkpoint", "hourly:-3h"}, cli.InspectRequest{Target: cli.ItemRef{SourceHostID: "host-mbp", WorkspaceID: "wt-7f3a"}, Checkpoint: cli.CheckpointHourly{HoursAgo: 3}}},
		{"sync sessions", []string{"sync", "--session", "0f3c", "--session", "7a1e"}, cli.SyncRequest{Sessions: []string{"0f3c", "7a1e"}}},
		{"pickup defaults", []string{"pickup", "host-mbp/wt-7f3a"}, cli.PickupRequest{
			Target:       cli.ItemRef{SourceHostID: "host-mbp", WorkspaceID: "wt-7f3a"},
			Checkpoint:   cli.LatestCheckpoint{},
			OnDivergence: cli.DivergenceRefuse,
		}},
		{"pickup all flags", []string{"pickup", "host-mbp/wt-7f3a", "--checkpoint", "at:2026-09-26T12:00:00-07:00", "--resume", "0f3c", "--resume", "7a1e", "--on-divergence", "keep-local", "--no-orca", "--dry-run", "--progress", "ndjson", "--allow-partial", "--apply-sparse"}, cli.PickupRequest{
			Target:       cli.ItemRef{SourceHostID: "host-mbp", WorkspaceID: "wt-7f3a"},
			Checkpoint:   cli.CheckpointAt{Time: at},
			AllowPartial: true,
			ApplySparse:  true,
			Resume:       []string{"0f3c", "7a1e"},
			OnDivergence: cli.DivergenceKeepLocal,
			NoOrca:       true,
			DryRun:       true,
		}},
		{"resume", []string{"resume", "0f3c9a2e"}, cli.ResumeRequest{Session: cli.SessionRef{ID: "0f3c9a2e"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &fakeService{}
			if got := run(svc, tt.args...); got.exit != cli.ExitOK {
				t.Fatalf("exit = %d, stderr = %q", got.exit, got.stderr)
			}
			if req, ok := svc.got.(cli.PickupRequest); ok {
				want := tt.want.(cli.PickupRequest)
				if gotAt, ok := req.Checkpoint.(cli.CheckpointAt); ok {
					if wantAt := want.Checkpoint.(cli.CheckpointAt); !gotAt.Time.Equal(wantAt.Time) {
						t.Errorf("checkpoint time = %v, want %v", gotAt.Time, wantAt.Time)
					}
					req.Checkpoint, want.Checkpoint = nil, nil
				}
				svc.got, tt.want = req, want
			}
			if !reflect.DeepEqual(svc.got, tt.want) {
				t.Errorf("request = %#v, want %#v", svc.got, tt.want)
			}
		})
	}
}

func TestPickupProgressNDJSON(t *testing.T) {
	phases := []cli.Progress{
		{Phase: cli.PhaseSelect},
		{Phase: cli.PhaseRestoreCode, Done: ptr(0), Total: ptr(3)},
		{Phase: cli.PhaseRestoreCode, Done: ptr(3), Total: ptr(3)},
		{Phase: cli.PhaseRestoreSessions, Done: ptr(2), Total: ptr(2)},
		{Phase: cli.PhaseOrcaImport},
		{Phase: cli.PhaseOrcaResume, Done: ptr(1), Total: ptr(1)},
	}
	wantProgress := `{"phase":"select"}
{"phase":"restore-code","done":0,"total":3}
{"phase":"restore-code","done":3,"total":3}
{"phase":"restore-sessions","done":2,"total":2}
{"phase":"orca-import"}
{"phase":"orca-resume","done":1,"total":1}
`
	tests := []struct {
		name       string
		args       []string
		wantStderr string
	}{
		{"ndjson", []string{"pickup", "0f3c", "--progress", "ndjson", "--json"}, wantProgress},
		{"silent", []string{"pickup", "0f3c", "--json"}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := run(&fakeService{pickup: orcaPickup(), phases: phases}, tt.args...)
			if got.exit != cli.ExitOK {
				t.Fatalf("exit = %d", got.exit)
			}
			if got.stderr != tt.wantStderr {
				t.Errorf("stderr = %q, want %q", got.stderr, tt.wantStderr)
			}
			golden(t, "pickup_orca.json", oneJSONLine(t, got.stdout))
		})
	}
}

func TestSIGTERMCancelsPickup(t *testing.T) {
	started := make(chan struct{})
	svc := &fakeService{block: started}
	var stdout, stderr bytes.Buffer
	done := make(chan int)
	go func() {
		done <- cli.Execute(svc, []string{"pickup", "0f3c", "--json"}, &stdout, &stderr)
	}()
	<-started
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case exit := <-done:
		want := `{"version":1,"ok":false,"error":{"code":"cancelled","message":"context canceled"}}` + "\n"
		if exit != cli.ExitError || stdout.String() != want || stderr.String() != "" {
			t.Errorf("got exit %d stdout %q stderr %q; want exit 1 stdout %q", exit, stdout.String(), stderr.String(), want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pickup was not cancelled by SIGTERM")
	}
}
