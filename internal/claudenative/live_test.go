package claudenative

import (
	"context"
	"encoding/json"
	"io/fs"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"
	"time"
)

const (
	liveA = "aaaaaaaa-0000-4000-8000-000000000001"
	liveB = "bbbbbbbb-0000-4000-8000-000000000002"
	liveC = "cccccccc-0000-4000-8000-000000000003"
	liveD = "dddddddd-0000-4000-8000-000000000004"
	liveE = "eeeeeeee-0000-4000-8000-000000000005"
	liveF = "ffffffff-0000-4000-8000-000000000006"
	liveG = "abababab-0000-4000-8000-000000000007"
)

type fakeProcesses []Process

func (f fakeProcesses) Processes(context.Context) ([]Process, error) {
	return f, nil
}

type secretGuardFS struct {
	fs.FS
	t *testing.T
}

func (g secretGuardFS) Open(name string) (fs.File, error) {
	if strings.HasSuffix(name, ".key") {
		g.t.Errorf("opened secret registry file %s", name)
		return nil, fs.ErrPermission
	}
	return g.FS.Open(name)
}

func registryFile(t *testing.T, pid int, sid, procStart string) *fstest.MapFile {
	t.Helper()
	data, err := json.Marshal(map[string]any{
		"pid": pid, "sessionId": sid, "cwd": "/Users/me/proj", "startedAt": 1790000000000,
		"procStart": procStart, "version": "2.1.283", "kind": "interactive", "entrypoint": "cli",
		"status": "busy", "updatedAt": 1790000001000,
	})
	if err != nil {
		t.Fatalf("marshal registry: %v", err)
	}
	return &fstest.MapFile{Data: data}
}

func TestLiveSessions(t *testing.T) {
	registry := fstest.MapFS{
		"100.json":        registryFile(t, 100, liveA, "Sat Sep 26 19:02:43 2026"),
		"100.9f8e7d.key":  &fstest.MapFile{Data: []byte("secret")},
		"200.json":        registryFile(t, 200, liveB, "Sat Sep 26 08:00:00 2026"),
		"300.json":        registryFile(t, 300, liveC, "Sat Sep 26 09:00:00 2026"),
		"800.json":        registryFile(t, 800, liveG, "Sun Sep  6 01:02:03 2026"),
		"800.0a0b0c.key":  &fstest.MapFile{Data: []byte("secret")},
		"notes/README.md": &fstest.MapFile{Data: []byte("x")},
	}
	table := fakeProcesses{
		{PID: 100, Start: "Sat Sep 26 19:02:43 2026", Argv: []string{"claude", "--resume", liveA}},
		{PID: 200, Start: "Sat Sep 26 12:34:56 2026", Argv: []string{"bash"}},
		{PID: 400, Start: "Sat Sep 26 10:00:00 2026", Argv: []string{"claude", "--dangerously-skip-permissions", "-r", liveD}},
		{PID: 500, Start: "Sat Sep 26 11:00:00 2026", Argv: []string{"/usr/local/bin/node", "/opt/homebrew/bin/claude", "--session-id=" + liveE}},
		{PID: 600, Start: "Sat Sep 26 11:00:00 2026", Argv: []string{"vim", "-r", liveF}},
		{PID: 700, Start: "Sat Sep 26 11:00:00 2026", Argv: []string{"claude", "--resume", "my title"}},
		{PID: 800, Start: "Sun Sep 6 01:02:03 2026", Argv: []string{"claude"}},
	}
	got, err := liveSessions(secretGuardFS{FS: registry, t: t}, table)
	if err != nil {
		t.Fatalf("liveSessions() error = %v", err)
	}
	registered := func(pid int, sid, start string) LiveProcess {
		return LiveProcess{
			PID: pid, ProcStart: start, SessionID: SessionID(sid), Cwd: "/Users/me/proj", Version: "2.1.283",
			Kind: "interactive", Entrypoint: "cli", Status: "busy", UpdatedAt: time.UnixMilli(1790000001000),
		}
	}
	want := map[SessionID]LiveProcess{
		liveA: registered(100, liveA, "Sat Sep 26 19:02:43 2026"),
		liveG: registered(800, liveG, "Sun Sep  6 01:02:03 2026"),
		liveD: {PID: 400, ProcStart: "Sat Sep 26 10:00:00 2026", SessionID: liveD},
		liveE: {PID: 500, ProcStart: "Sat Sep 26 11:00:00 2026", SessionID: liveE},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("liveSessions() =\n%+v\nwant\n%+v", got, want)
	}
}

func TestLiveSessionsMissingRegistry(t *testing.T) {
	got, err := LiveSessions(context.Background(), Layout{ConfigDir: t.TempDir()}, fakeProcesses{})
	if err != nil || len(got) != 0 {
		t.Fatalf("LiveSessions() = %v, %v, want empty", got, err)
	}
}

func TestParsePS(t *testing.T) {
	tests := []struct {
		name    string
		out     string
		want    []Process
		wantErr bool
	}{
		{
			name: "rows",
			out: "  123 Sun Sep  6 01:02:03 2026     /usr/local/bin/claude --resume " + liveA + "\n" +
				"    1 Sat Sep 26 07:00:00 2026     /sbin/launchd\n",
			want: []Process{
				{PID: 123, Start: "Sun Sep 6 01:02:03 2026", Argv: []string{"/usr/local/bin/claude", "--resume", liveA}},
				{PID: 1, Start: "Sat Sep 26 07:00:00 2026", Argv: []string{"/sbin/launchd"}},
			},
		},
		{name: "empty", out: "", want: nil},
		{name: "truncated row", out: "123 Sat Sep\n", wantErr: true},
		{name: "bad pid", out: "abc Sat Sep 26 07:00:00 2026 x\n", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parsePS(tt.out)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parsePS() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parsePS() = %+v, want %+v", got, tt.want)
			}
		})
	}
}
