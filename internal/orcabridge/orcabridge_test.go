package orcabridge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

const fakeOrcaScript = `#!/bin/sh
dir="$FAKE_ORCA_DIR"
call=$(printf '%s/calls/%04d' "$dir" "$(ls "$dir/calls" | wc -l | tr -d ' ')")
mkdir "$call"
for a in "$@"; do printf '%s\n' "$a"; done > "$call/argv"
prev=
for a in "$@"; do [ "$prev" = --recovery-launch-file ] && cp "$a" "$call/launch"; prev=$a; done
env > "$call/env"
cat > "$call/stdin"
key=$1
[ "$1" = recovery ] && key="recovery-$2"
[ -f "$dir/$key.sleep" ] && sleep "$(cat "$dir/$key.sleep")"
cat "$dir/$key.json"
exit "$(cat "$dir/$key.exit")"
`

const (
	goldenRuntimeID = "00000000-0000-4000-8000-000000000001"
	describeLocal   = `{"id":"local","ok":true,"result":{"protocol":1,"runtimeId":"` + goldenRuntimeID + `","executionHostId":"local","appVersion":"1.4.212","platform":"darwin","machineName":"test-mac","hostKind":"desktop","localClientInstanceId":null,"capabilities":["cross-machine-recovery.workspace.v1"]},"_meta":{"runtimeId":"` + goldenRuntimeID + `"}}`
	importResult    = `{"id":"local","ok":true,"result":{"importKey":"key-1","disposition":"imported","repoId":"repo-1","worktreeId":"repo-1::/dst","instanceId":"inst-1","presentationSource":{"kind":"client-view","clientKey":"local-renderer"},"idMap":{"tabs":{"t1":"t9"},"groups":{"g1":"g9"},"leaves":{"l1":"l9"},"browsers":{}},"bindings":[{"sourcePaneKey":"t1:l1","localPaneKey":"t9:l9","binding":{"agent":"claude","key":"session_id","id":"sess-1"},"status":"resumed","terminalHandle":"term-1"},{"sourcePaneKey":"t1:l2","localPaneKey":"t9:l8","binding":{"agent":"claude","key":"session_id","id":"sess-2"},"status":"dormant"}],"provenance":{"importKey":"key-1"}}}`
	resumeResult    = `{"ok":true,"result":{"terminalHandle":"term-2","disposition":"created","localPaneKey":"t9:l8"}}`
	listResult      = `{"ok":true,"result":{"bindings":[{"localPaneKey":"t9:l8","worktreeId":"repo-1::/dst","providerSession":{"key":"session_id","id":"sess-2","transcriptPath":"/dst/sess-2.jsonl"},"provenance":{"importKey":"key-1"}}]}}`
	activityResult  = `{"ok":true,"result":{"workspaces":[{"worktreeId":"repo-1::/dst","path":"/dst","lastHumanInputAt":1790000000123,"lastHumanFocusAt":null}]}}`
	exportResult    = "{\"ok\":true,\"result\":{\"descriptor\":{\n  \"version\": 1,\n  \"repo\": {\"path\": \"/src\"}\n}}}"
)

type fakeCall struct {
	argv  []string
	env   map[string]string
	stdin []byte
}

func (c fakeCall) verb() string {
	if c.argv[0] == "recovery" {
		return "recovery " + c.argv[1]
	}
	return c.argv[0]
}

type fakeOrca struct {
	t      *testing.T
	root   *os.Root
	client *Client
}

func newFakeOrca(t *testing.T) *fakeOrca {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	for _, d := range []string{bin, filepath.Join(dir, "calls")} {
		if err := os.Mkdir(d, 0o750); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(bin, "orca"), []byte(fakeOrcaScript), 0o700); err != nil { //nolint:gosec // G306: an executable test stub must be +x.
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+":/usr/bin:/bin")
	t.Setenv("FAKE_ORCA_DIR", dir)
	binary, err := resolveBinary("", filepath.Join(dir, "absent", "orca"))
	if err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	f := &fakeOrca{t: t, root: root, client: newClient(binary, 10*time.Second)}
	f.respond("status", statusFixture(t, nil), 0)
	f.respond("recovery-describe", describeLocal, 0)
	f.respond("recovery-import", importResult, 0)
	f.respond("recovery-export", exportResult, 0)
	f.respond("recovery-resume", resumeResult, 0)
	f.respond("recovery-list", listResult, 0)
	f.respond("recovery-activity", activityResult, 0)
	return f
}

func (f *fakeOrca) respond(key, body string, exit int) {
	f.t.Helper()
	f.write(key+".json", body)
	f.write(key+".exit", fmt.Sprint(exit))
}

func (f *fakeOrca) write(name, body string) {
	f.t.Helper()
	if err := f.root.WriteFile(name, []byte(body), 0o600); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fakeOrca) calls() []fakeCall {
	f.t.Helper()
	entries, err := fs.ReadDir(f.root.FS(), "calls")
	if err != nil {
		f.t.Fatal(err)
	}
	calls := make([]fakeCall, 0, len(entries))
	for _, e := range entries {
		read := func(name string) []byte {
			data, err := f.root.ReadFile(path.Join("calls", e.Name(), name))
			if err != nil {
				f.t.Fatal(err)
			}
			return data
		}
		env := map[string]string{}
		for line := range strings.Lines(string(read("env"))) {
			if k, v, ok := strings.Cut(strings.TrimSuffix(line, "\n"), "="); ok {
				env[k] = v
			}
		}
		calls = append(calls, fakeCall{
			argv:  strings.Split(strings.TrimSuffix(string(read("argv")), "\n"), "\n"),
			env:   env,
			stdin: read("stdin"),
		})
	}
	return calls
}

func (f *fakeOrca) verbs() []string {
	f.t.Helper()
	calls := f.calls()
	verbs := make([]string, 0, len(calls))
	for _, c := range calls {
		verbs = append(verbs, c.verb())
	}
	return verbs
}

func statusFixture(t *testing.T, mutate func(result map[string]any)) string {
	t.Helper()
	golden, err := os.ReadFile("testdata/status-local.golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var env map[string]any
	if err := json.Unmarshal(golden, &env); err != nil {
		t.Fatal(err)
	}
	result := env["result"].(map[string]any)
	runtime := result["runtime"].(map[string]any)
	runtime["capabilities"] = append(runtime["capabilities"].([]any), CapabilityWorkspace, CapabilityPresentation)
	if mutate != nil {
		mutate(result)
	}
	out, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

func errorEnvelope(code string) string {
	return `{"id":"local","ok":false,"error":{"code":"` + code + `","message":"refused by host","data":{"nextSteps":[]}},"_meta":{"runtimeId":null}}`
}

func wantUnavailable(t *testing.T, err error, reason Reason) {
	t.Helper()
	var unavailable *UnavailableError
	if !errors.As(err, &unavailable) || unavailable.Reason != reason {
		t.Fatalf("err = %v, want UnavailableError %s", err, reason)
	}
}

func TestResolveBinary(t *testing.T) {
	dir := t.TempDir()
	onPath := filepath.Join(dir, "path", "orca")
	wellKnown := filepath.Join(dir, "wellknown", "orca")
	explicit := filepath.Join(dir, "explicit", "orca")
	plain := filepath.Join(dir, "plain", "orca")
	for _, p := range []string{onPath, wellKnown, explicit, plain} {
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			t.Fatal(err)
		}
		mode := os.FileMode(0o700)
		if p == plain {
			mode = 0o600
		}
		if err := os.WriteFile(p, []byte("#!/bin/sh\n"), mode); err != nil { //nolint:gosec // G306: an executable test stub must be +x.
			t.Fatal(err)
		}
	}
	absent := filepath.Join(dir, "absent", "orca")
	tests := []struct {
		name      string
		explicit  string
		wellKnown string
		path      string
		want      string
		wantErr   bool
	}{
		{"explicit wins", explicit, wellKnown, filepath.Dir(onPath), explicit, false},
		{"explicit missing", absent, wellKnown, filepath.Dir(onPath), "", true},
		{"explicit not executable", plain, wellKnown, filepath.Dir(onPath), "", true},
		{"well-known before PATH", "", wellKnown, filepath.Dir(onPath), wellKnown, false},
		{"PATH fallback", "", absent, filepath.Dir(onPath), onPath, false},
		{"nowhere", "", absent, filepath.Join(dir, "empty"), "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("PATH", tt.path)
			got, err := resolveBinary(tt.explicit, tt.wellKnown)
			if tt.wantErr {
				wantUnavailable(t, err, ReasonNotInstalled)
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("resolveBinary() = %q, %v; want %q", got, err, tt.want)
			}
		})
	}
}

func TestNewPrefersBinaryEnv(t *testing.T) {
	dir := t.TempDir()
	stub := filepath.Join(dir, "orca")
	if err := os.WriteFile(stub, []byte("#!/bin/sh\n"), 0o700); err != nil { //nolint:gosec // G306: an executable test stub must be +x.
		t.Fatal(err)
	}
	tests := []struct {
		name     string
		env      string
		explicit string
		want     string
		wantErr  bool
	}{
		{"env names the binary", stub, "", stub, false},
		{"explicit beats env", filepath.Join(dir, "absent"), stub, stub, false},
		{"env missing", filepath.Join(dir, "absent"), "", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(BinaryEnv, tt.env)
			c, err := New(Options{Binary: tt.explicit})
			if tt.wantErr {
				wantUnavailable(t, err, ReasonNotInstalled)
				return
			}
			if err != nil || c.binary != tt.want {
				t.Fatalf("New() binary = %v, %v; want %q", c, err, tt.want)
			}
		})
	}
}

func TestStatusParsesObservedShape(t *testing.T) {
	f := newFakeOrca(t)
	golden, err := os.ReadFile("testdata/status-local.golden.json")
	if err != nil {
		t.Fatal(err)
	}
	f.respond("status", string(golden), 0)

	status, err := f.client.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if status.Target.Kind != "local" || !status.App.Running || status.App.PID != 4242 ||
		status.Runtime.State != "ready" || !status.Runtime.Reachable ||
		status.Runtime.RuntimeID != goldenRuntimeID || status.Runtime.AppVersion != "1.4.212" ||
		len(status.Runtime.Capabilities) != 93 || !status.HasCapability("files.pathsExist") {
		t.Fatalf("Status() = %+v", status)
	}
	if status.HasCapability(CapabilityWorkspace) {
		t.Fatal("observed 1.4.212 status unexpectedly advertises recovery")
	}

	_, err = f.client.Verify(t.Context())
	wantUnavailable(t, err, ReasonCapabilityMissing)
	if got := f.verbs(); !slices.Equal(got, []string{"status", "status"}) {
		t.Fatalf("calls = %v", got)
	}
}

func TestVerificationRefusesBeforeImport(t *testing.T) {
	tests := []struct {
		name      string
		status    func(result map[string]any)
		describe  string
		wantErr   Reason
		wantVerbs []string
	}{
		{
			name:      "local runtime imports",
			wantVerbs: []string{"status", "recovery describe", "recovery import"},
		},
		{
			name:      "remote target",
			status:    func(r map[string]any) { r["target"] = map[string]any{"kind": "environment", "environment": "work"} },
			wantErr:   ReasonNotLocal,
			wantVerbs: []string{"status"},
		},
		{
			name: "runtime not running",
			status: func(r map[string]any) {
				r["app"] = map[string]any{"running": false, "pid": nil}
				r["runtime"] = map[string]any{"state": "not_running", "reachable": false}
			},
			wantErr:   ReasonNotRunning,
			wantVerbs: []string{"status"},
		},
		{
			name: "capability missing",
			status: func(r map[string]any) {
				r["runtime"].(map[string]any)["capabilities"] = []string{CapabilityPresentation}
			},
			wantErr:   ReasonCapabilityMissing,
			wantVerbs: []string{"status"},
		},
		{
			name:      "execution host mismatch",
			describe:  strings.Replace(describeLocal, `"executionHostId":"local"`, `"executionHostId":"runtime:peer"`, 1),
			wantErr:   ReasonNotLocal,
			wantVerbs: []string{"status", "recovery describe"},
		},
		{
			name:      "runtime id mismatch",
			describe:  strings.Replace(describeLocal, `"runtimeId":"`+goldenRuntimeID+`"`, `"runtimeId":"other-runtime"`, 1),
			wantErr:   ReasonNotLocal,
			wantVerbs: []string{"status", "recovery describe"},
		},
		{
			name:      "describe refused as remote",
			describe:  errorEnvelope("recovery_local_only"),
			wantErr:   ReasonNotLocal,
			wantVerbs: []string{"status", "recovery describe"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeOrca(t)
			f.respond("status", statusFixture(t, tt.status), 0)
			if tt.describe != "" {
				f.respond("recovery-describe", tt.describe, 0)
			}
			_, err := f.client.Import(t.Context(), ImportRequest{Descriptor: []byte(`{"version":1}`), Checkout: "/dst", CheckpointID: "cp-1"})
			if tt.wantErr == "" && err != nil {
				t.Fatal(err)
			}
			if tt.wantErr != "" {
				wantUnavailable(t, err, tt.wantErr)
			}
			if got := f.verbs(); !slices.Equal(got, tt.wantVerbs) {
				t.Fatalf("calls = %v, want %v", got, tt.wantVerbs)
			}
		})
	}
}

func TestEveryVerbRunsLocallyWithExactArgv(t *testing.T) {
	for _, k := range []string{"ORCA_ENVIRONMENT", "ORCA_PAIRING_CODE", "ORCA_REMOTE_PAIRING", "ORCA_WORKSPACE_ID"} {
		t.Setenv(k, "remote-value")
	}
	f := newFakeOrca(t)
	ctx := t.Context()
	if _, err := f.client.Export(ctx, "/src/wt"); err != nil {
		t.Fatal(err)
	}
	_, err := f.client.Import(ctx, ImportRequest{
		Descriptor:   []byte(`{"version":1}`),
		Checkout:     "/dst",
		CheckpointID: "cp-1",
		PathMap:      []PathMapping{{From: "/src/wt", To: "/dst"}, {From: "/home/a/.claude/projects/-src-wt", To: "/home/b/.claude/projects/-dst"}},
		Resume:       []BindingSelector{"sess-1", "sess-3"},
		PreferClient: "client-7",
		Activate:     true,
		RegisterRepo: true,
		DryRun:       true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.client.Resume(ctx, ByID("repo-1::/dst"), "sess-2", true); err != nil {
		t.Fatal(err)
	}
	if _, err := f.client.List(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := f.client.List(ctx, ByPath("/dst")); err != nil {
		t.Fatal(err)
	}
	if _, err := f.client.Activity(ctx); err != nil {
		t.Fatal(err)
	}

	verification := [][]string{{"status", "--json"}, {"recovery", "describe", "--json"}}
	verbs := [][]string{
		{"recovery", "export", "--worktree", "path:/src/wt", "--json"},
		{
			"recovery", "import", "--descriptor", "-", "--checkout", "/dst", "--checkpoint", "cp-1",
			"--path-map", "/src/wt=/dst", "--path-map", "/home/a/.claude/projects/-src-wt=/home/b/.claude/projects/-dst",
			"--resume", "sess-1", "--resume", "sess-3", "--prefer-client", "client-7",
			"--activate", "--register-repo", "--dry-run", "--json",
		},
		{"recovery", "resume", "--worktree", "id:repo-1::/dst", "--session", "sess-2", "--focus", "--json"},
		{"recovery", "list", "--json"},
		{"recovery", "list", "--worktree", "path:/dst", "--json"},
		{"recovery", "activity", "--json"},
	}
	want := make([][]string, 0, len(verbs)*3)
	for _, verb := range verbs {
		want = append(want, append(slices.Clone(verification), verb)...)
	}
	calls := f.calls()
	got := make([][]string, 0, len(calls))
	for _, c := range calls {
		got = append(got, c.argv)
		for _, k := range remoteRoutingEnv {
			if _, ok := c.env[k]; ok {
				t.Errorf("%v: env carries %s", c.argv, k)
			}
		}
		if c.env["ORCA_WORKSPACE_ID"] != "remote-value" {
			t.Errorf("%v: env dropped ORCA_WORKSPACE_ID", c.argv)
		}
		for _, a := range c.argv {
			if strings.HasPrefix(a, "--environment") || strings.HasPrefix(a, "--pairing-code") || strings.HasPrefix(a, "--host") {
				t.Errorf("%v: remote flag %s", c.argv, a)
			}
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("argv =\n%q\nwant\n%q", got, want)
	}
}

func TestResultsDecode(t *testing.T) {
	f := newFakeOrca(t)
	ctx := t.Context()

	descriptor, err := f.client.Export(ctx, "/src/wt")
	if err != nil {
		t.Fatal(err)
	}
	if string(descriptor) != `{"version":1,"repo":{"path":"/src"}}` {
		t.Fatalf("Export() = %s", descriptor)
	}

	imported, err := f.client.Import(ctx, ImportRequest{Descriptor: []byte(`{}`), Checkout: "/dst", CheckpointID: "cp-1"})
	if err != nil {
		t.Fatal(err)
	}
	wantImport := ImportResult{
		ImportKey: "key-1", Disposition: "imported", RepoID: "repo-1", WorktreeID: "repo-1::/dst", InstanceID: "inst-1",
		PresentationSource: PresentationSource{Kind: "client-view", ClientKey: "local-renderer"},
		IDMap:              IDMap{Tabs: map[string]string{"t1": "t9"}, Groups: map[string]string{"g1": "g9"}, Leaves: map[string]string{"l1": "l9"}, Browsers: map[string]string{}},
		Bindings: []ImportedBinding{
			{SourcePaneKey: "t1:l1", LocalPaneKey: "t9:l9", Binding: RecoveryBindingKey{Agent: AgentClaude, Key: "session_id", ID: "sess-1"}, Status: "resumed", TerminalHandle: "term-1"},
			{SourcePaneKey: "t1:l2", LocalPaneKey: "t9:l8", Binding: RecoveryBindingKey{Agent: AgentClaude, Key: "session_id", ID: "sess-2"}, Status: "dormant"},
		},
		Provenance: json.RawMessage(`{"importKey":"key-1"}`),
	}
	if !reflect.DeepEqual(imported, wantImport) {
		t.Fatalf("Import() = %+v", imported)
	}

	resumed, err := f.client.Resume(ctx, ByID("repo-1::/dst"), "sess-2", false)
	if err != nil || resumed != (ResumeResult{TerminalHandle: "term-2", Disposition: "created", LocalPaneKey: "t9:l8"}) {
		t.Fatalf("Resume() = %+v, %v", resumed, err)
	}

	listed, err := f.client.List(ctx, "")
	wantList := []DormantBinding{{
		LocalPaneKey: "t9:l8", WorktreeID: "repo-1::/dst",
		ProviderSession: ProviderSession{Key: "session_id", ID: "sess-2", TranscriptPath: "/dst/sess-2.jsonl"},
		Provenance:      json.RawMessage(`{"importKey":"key-1"}`),
	}}
	if err != nil || !reflect.DeepEqual(listed, wantList) {
		t.Fatalf("List() = %+v, %v", listed, err)
	}

	activity, err := f.client.Activity(ctx)
	wantActivity := []WorkspaceActivity{{WorktreeID: "repo-1::/dst", Path: "/dst", LastHumanInputAt: time.UnixMilli(1790000000123).UTC()}}
	if err != nil || !reflect.DeepEqual(activity, wantActivity) {
		t.Fatalf("Activity() = %+v, %v", activity, err)
	}
}

func TestErrorEnvelopeMapping(t *testing.T) {
	tests := []struct {
		code       string
		wantReason Reason
	}{
		{"recovery_local_only", ReasonNotLocal},
		{"recovery_unsupported", ReasonCapabilityMissing},
		{"method_not_found", ReasonCapabilityMissing},
		{"incompatible_runtime", ReasonCapabilityMissing},
		{"runtime_unavailable", ReasonNotRunning},
		{"runtime_timeout", ReasonNotRunning},
		{CodeRepoUnregistered, ""},
		{CodeCheckoutMissing, ""},
		{CodeDestinationNotEmpty, ""},
		{CodeDescriptorInvalid, ""},
		{CodeDescriptorTooLarge, ""},
		{CodeSessionLiveLocally, ""},
		{CodeBindingNotFound, ""},
		{CodeSelectorNotFound, ""},
		{CodeRuntimeAccessDenied, ""},
		{CodeInvalidArgument, ""},
	}
	for _, tt := range tests {
		t.Run(tt.code, func(t *testing.T) {
			f := newFakeOrca(t)
			f.respond("recovery-import", errorEnvelope(tt.code), 1)
			_, err := f.client.Import(t.Context(), ImportRequest{Descriptor: []byte(`{}`), Checkout: "/dst", CheckpointID: "cp-1"})
			if tt.wantReason != "" {
				wantUnavailable(t, err, tt.wantReason)
				return
			}
			var refused *RefusedError
			if !errors.As(err, &refused) || *refused != (RefusedError{Code: tt.code, Message: "refused by host"}) {
				t.Fatalf("err = %v, want RefusedError %s", err, tt.code)
			}
		})
	}
}

func TestDescriptorPassthrough(t *testing.T) {
	f := newFakeOrca(t)
	descriptor := []byte("{\"version\":1,\n \"note\":\"café \\u00e9\\n\",\t\"pad\":\"" + strings.Repeat("x", 1<<20) + "\"}\n\x00")
	if _, err := f.client.Import(t.Context(), ImportRequest{Descriptor: descriptor, Checkout: "/dst", CheckpointID: "cp-1"}); err != nil {
		t.Fatal(err)
	}
	calls := f.calls()
	stdins := make([][]byte, 0, len(calls))
	for _, c := range calls {
		stdins = append(stdins, c.stdin)
	}
	if len(stdins) != 3 || len(stdins[0]) != 0 || len(stdins[1]) != 0 || !bytes.Equal(stdins[2], descriptor) {
		t.Fatalf("stdin lengths = %d/%d/%d, want 0/0/%d byte-exact", len(stdins[0]), len(stdins[1]), len(stdins[2]), len(descriptor))
	}
}

func TestDescriptorCap(t *testing.T) {
	f := newFakeOrca(t)
	_, err := f.client.Import(t.Context(), ImportRequest{Descriptor: bytes.Repeat([]byte("x"), MaxDescriptorBytes+1), Checkout: "/dst", CheckpointID: "cp-1"})
	var refused *RefusedError
	if !errors.As(err, &refused) || refused.Code != CodeDescriptorTooLarge {
		t.Fatalf("oversized import err = %v", err)
	}
	if got := f.verbs(); len(got) != 0 {
		t.Fatalf("oversized import ran %v", got)
	}

	f.respond("recovery-export", `{"ok":true,"result":{"descriptor":{"pad":"`+strings.Repeat("x", MaxDescriptorBytes)+`"}}}`, 0)
	_, err = f.client.Export(t.Context(), "/src/wt")
	if !errors.As(err, &refused) || refused.Code != CodeDescriptorTooLarge {
		t.Fatalf("oversized export err = %v", err)
	}
}

func TestTimeoutKillsOrca(t *testing.T) {
	f := newFakeOrca(t)
	f.write("recovery-describe.sleep", "30")
	f.client.timeout = 300 * time.Millisecond
	start := time.Now()
	_, err := f.client.Import(t.Context(), ImportRequest{Descriptor: []byte(`{}`), Checkout: "/dst", CheckpointID: "cp-1"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("timeout took %v", elapsed)
	}
	if got := f.verbs(); !slices.Equal(got, []string{"status", "recovery describe"}) {
		t.Fatalf("calls = %v", got)
	}
}

func TestUndecodableOutput(t *testing.T) {
	f := newFakeOrca(t)
	f.respond("status", "Orca crashed", 2)
	_, err := f.client.Status(t.Context())
	var unavailable *UnavailableError
	var refused *RefusedError
	if err == nil || errors.As(err, &unavailable) || errors.As(err, &refused) || !strings.Contains(err.Error(), "decode output") {
		t.Fatalf("err = %v", err)
	}
}

func TestPathMapRejectsSeparatorInSource(t *testing.T) {
	f := newFakeOrca(t)
	_, err := f.client.Import(t.Context(), ImportRequest{Descriptor: []byte(`{}`), Checkout: "/dst", CheckpointID: "cp-1", PathMap: []PathMapping{{From: "/a=b", To: "/c"}}})
	if err == nil || !strings.Contains(err.Error(), "contains '='") {
		t.Fatalf("err = %v", err)
	}
	if got := f.verbs(); len(got) != 0 {
		t.Fatalf("calls = %v", got)
	}
}
