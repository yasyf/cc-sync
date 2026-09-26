package sessionrestore

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/yasyf/cc-sync/internal/claudenative"
)

const testSID = claudenative.SessionID("11111111-2222-4333-8444-555555555555")

var testPaths = PathMap{
	{From: "/src/co", To: "/dst/co"},
	{From: "/src/cfg/projects/-src-co", To: "/dst/cfg/projects/-dst-co"},
	{From: "/src/cfg", To: "/dst/cfg"},
	{From: "/src/home", To: "/dst/home"},
}.sorted()

func withSID(s string) string { return strings.ReplaceAll(s, "SID", string(testSID)) }

func TestPathMapMap(t *testing.T) {
	tests := []struct {
		in, want string
		ok       bool
	}{
		{"/src/co", "/dst/co", true},
		{"/src/co/a/b.go", "/dst/co/a/b.go", true},
		{"/src/cobalt", "/src/cobalt", false},
		{"/src/cfg/projects/-src-co/x.txt", "/dst/cfg/projects/-dst-co/x.txt", true},
		{"/src/cfg/plans/p.md", "/dst/cfg/plans/p.md", true},
		{"/src/home/.zshrc", "/dst/home/.zshrc", true},
		{"/opt/elsewhere", "/opt/elsewhere", false},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, ok := testPaths.Map(tt.in)
			if got != tt.want || ok != tt.ok {
				t.Errorf("Map(%q) = %q, %v; want %q, %v", tt.in, got, ok, tt.want, tt.ok)
			}
		})
	}
}

func TestRelocateRecord(t *testing.T) {
	tests := []struct {
		name, in, want string
		unmapped       []string
	}{
		{
			name: "user typed fields",
			in:   `{"parentUuid":null,"type":"user","message":{"role":"user","content":"look at /src/co/a.go"},"toolUseResult":{"persistedOutputPath":"/src/cfg/projects/-src-co/SID/tool-results/r1.txt","stdout":"/src/co"},"serverClassifierContext":{"context":{"live_cwd":"/src/co/sub","git_state":{"root":"/src/co","cwd":"/src/co/sub"}}},"uuid":"u1","cwd":"/src/co","sessionId":"SID"}`,
			want: `{"parentUuid":null,"type":"user","message":{"role":"user","content":"look at /src/co/a.go"},"toolUseResult":{"persistedOutputPath":"/dst/cfg/projects/-dst-co/SID/tool-results/r1.txt","stdout":"/src/co"},"serverClassifierContext":{"context":{"live_cwd":"/dst/co/sub","git_state":{"root":"/dst/co","cwd":"/dst/co/sub"}}},"uuid":"u1","cwd":"/dst/co","sessionId":"SID"}`,
		},
		{
			name: "environment attachment keeps snapshot and changes",
			in:   `{"parentUuid":"u1","type":"attachment","attachment":{"type":"environment","snapshot":{"workingDirectory":"/src/co","scratchpadDirectory":"/src/home/tmp"},"changes":[{"field":"workingDirectory","from":"/src/old"}],"rendered":"Primary working directory: /src/co"},"uuid":"e1","cwd":"/src/co","sessionId":"SID"}`,
			want: `{"parentUuid":"u1","type":"attachment","attachment":{"type":"environment","snapshot":{"workingDirectory":"/src/co","scratchpadDirectory":"/src/home/tmp"},"changes":[{"field":"workingDirectory","from":"/src/old"}],"rendered":"Primary working directory: /src/co"},"uuid":"e1","cwd":"/dst/co","sessionId":"SID"}`,
		},
		{
			name: "file history snapshot keys and realParentDir",
			in:   `{"type":"file-history-snapshot","messageId":"m1","snapshot":{"messageId":"m1","trackedFileBackups":{"/src/co/a.go":{"backupFileName":"h@v1","version":1,"realParentDir":"/src/co"},"rel.go":{"backupFileName":null,"version":2}},"timestamp":"2026-09-26T10:00:00.000Z"},"isSnapshotUpdate":false}`,
			want: `{"type":"file-history-snapshot","messageId":"m1","snapshot":{"messageId":"m1","trackedFileBackups":{"/dst/co/a.go":{"backupFileName":"h@v1","version":1,"realParentDir":"/dst/co"},"rel.go":{"backupFileName":null,"version":2}},"timestamp":"2026-09-26T10:00:00.000Z"},"isSnapshotUpdate":false}`,
		},
		{
			name: "file history delta without html escaping",
			in:   `{"type":"file-history-delta","trackingPath":"/src/co/a&b<c>.go","backup":{"backupFileName":"h@v2","realParentDir":"/src/co"}}`,
			want: `{"type":"file-history-delta","trackingPath":"/dst/co/a&b<c>.go","backup":{"backupFileName":"h@v2","realParentDir":"/dst/co"}}`,
		},
		{
			name: "relative tracking path stays",
			in:   `{"type":"file-history-delta","trackingPath":"b.go","backup":{"realParentDir":"/src/co"}}`,
			want: `{"type":"file-history-delta","trackingPath":"b.go","backup":{"realParentDir":"/dst/co"}}`,
		},
		{
			name:     "worktree state with an unmapped path",
			in:       `{"type":"worktree-state","worktreeSession":{"originalCwd":"/src/co","preEnterOriginalCwd":"/src/home/x","worktreePath":"/opt/wt"},"sessionId":"SID"}`,
			want:     `{"type":"worktree-state","worktreeSession":{"originalCwd":"/dst/co","preEnterOriginalCwd":"/dst/home/x","worktreePath":"/opt/wt"},"sessionId":"SID"}`,
			unmapped: []string{"/opt/wt"},
		},
		{
			name: "plan mode exit",
			in:   `{"type":"attachment","attachment":{"type":"plan_mode_exit","planFilePath":"/src/cfg/plans/p.md","planExists":true},"cwd":"/src/co"}`,
			want: `{"type":"attachment","attachment":{"type":"plan_mode_exit","planFilePath":"/dst/cfg/plans/p.md","planExists":true},"cwd":"/dst/co"}`,
		},
		{
			name: "task status output",
			in:   `{"type":"attachment","attachment":{"type":"task_status","outputFilePath":"/src/home/out.txt"}}`,
			want: `{"type":"attachment","attachment":{"type":"task_status","outputFilePath":"/dst/home/out.txt"}}`,
		},
		{
			name: "task reminder worktrees",
			in:   `{"type":"attachment","attachment":{"type":"task_reminder","content":[{"id":"1","metadata":{"worktree":"/src/co/wt"}},{"id":"2"}]}}`,
			want: `{"type":"attachment","attachment":{"type":"task_reminder","content":[{"id":"1","metadata":{"worktree":"/dst/co/wt"}},{"id":"2"}]}}`,
		},
		{
			name: "file attachment keeps its content",
			in:   `{"type":"attachment","attachment":{"type":"file","filename":"/src/co/a.go","content":{"type":"text","file":{"filePath":"/src/co/a.go","content":"see /src/co"}}},"cwd":"/src/co"}`,
			want: `{"type":"attachment","attachment":{"type":"file","filename":"/dst/co/a.go","content":{"type":"text","file":{"filePath":"/dst/co/a.go","content":"see /src/co"}}},"cwd":"/dst/co"}`,
		},
		{
			name: "compact file reference",
			in:   `{"type":"attachment","attachment":{"type":"compact_file_reference","filename":"/src/co/b.go"}}`,
			want: `{"type":"attachment","attachment":{"type":"compact_file_reference","filename":"/dst/co/b.go"}}`,
		},
		{
			name: "edited text file",
			in:   `{"type":"attachment","attachment":{"type":"edited_text_file","filename":"/src/co/c.go","snippet":"/src/co"}}`,
			want: `{"type":"attachment","attachment":{"type":"edited_text_file","filename":"/dst/co/c.go","snippet":"/src/co"}}`,
		},
		{
			name: "instructions files",
			in:   `{"type":"attachment","attachment":{"type":"instructions","files":[{"path":"/src/co/CLAUDE.md"},{"path":"/src/home/.claude/CLAUDE.md"}]}}`,
			want: `{"type":"attachment","attachment":{"type":"instructions","files":[{"path":"/dst/co/CLAUDE.md"},{"path":"/dst/home/.claude/CLAUDE.md"}]}}`,
		},
		{
			name:     "unknown record type is verbatim and reported",
			in:       `{"type":"future","where":"/src/co/x","nested":{"p":"/opt/none"}}`,
			want:     `{"type":"future","where":"/src/co/x","nested":{"p":"/opt/none"}}`,
			unmapped: []string{"/src/co/x"},
		},
		{
			name: "untouched record",
			in:   `{"type":"last-prompt","lastPrompt":"cd /src/co","leafUuid":"a1","sessionId":"SID"}`,
			want: `{"type":"last-prompt","lastPrompt":"cd /src/co","leafUuid":"a1","sessionId":"SID"}`,
		},
		{
			name: "invalid json",
			in:   `{"type":"user",`,
			want: `{"type":"user",`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stats := relocation{}
			r := &relocator{paths: testPaths, fromID: testSID, toID: testSID, strip: true, stats: &stats}
			r.begin(transcriptFile)
			got, keep := r.record([]byte(withSID(tt.in)))
			if !keep || string(got) != withSID(tt.want) {
				t.Errorf("record() = %s, %v\nwant %s", got, keep, withSID(tt.want))
			}
			paths := make([]string, 0, len(stats.Unmapped))
			for _, u := range stats.Unmapped {
				paths = append(paths, u.Path)
			}
			if !slices.Equal(paths, tt.unmapped) {
				t.Errorf("unmapped = %v, want %v", paths, tt.unmapped)
			}
		})
	}
}

func TestRelocateRecordFork(t *testing.T) {
	const forked = claudenative.SessionID("99999999-8888-4777-8666-555555555555")
	stats := relocation{}
	r := &relocator{paths: testPaths, fromID: testSID, toID: forked, strip: true, stats: &stats}
	r.begin(transcriptFile)
	in := withSID(`{"type":"user","sessionId":"SID","toolUseResult":{"session_id":"SID","other":"SID","runs":[{"sessionId":"SID"}]},"message":{"content":"SID"},"cwd":"/src/co"}`)
	want := strings.NewReplacer("NEW", string(forked), "OLD", string(testSID)).Replace(
		`{"type":"user","sessionId":"NEW","toolUseResult":{"session_id":"NEW","other":"OLD","runs":[{"sessionId":"NEW"}]},"message":{"content":"OLD"},"cwd":"/dst/co"}`)
	if got, _ := r.record([]byte(in)); string(got) != want {
		t.Errorf("record() = %s\nwant %s", got, want)
	}
}

func TestRelocateJSONLStripsSnapshots(t *testing.T) {
	src := withSID(strings.Join([]string{
		`{"type":"user","uuid":"u1","parentUuid":null,"sessionId":"SID","cwd":"/src/co"}`,
		`{"type":"attachment","attachment":{"type":"prompt_snapshot","systemPrompt":["x"]},"uuid":"s1","parentUuid":"u1","sessionId":"SID"}`,
		`{"type":"attachment","attachment":{"type":"prompt_snapshot","tools":[]},"uuid":"s2","parentUuid":"s1","sessionId":"SID"}`,
		`{"type":"assistant","uuid":"a1","parentUuid":"s2","sessionId":"SID","cwd":"/src/co"}`,
		`{"type":"attachment","attachment":{"type":"prompt_snapshot"},"uuid":"s3","parentUuid":null,"sessionId":"SID"}`,
		`{"type":"system","subtype":"compact_boundary","uuid":"c1","parentUuid":"s3","logicalParentUuid":"s2","sessionId":"SID"}`,
		`{"type":"user","uuid":"u2","parentUuid":"c1","sessionId":"SID"}`,
		`{"type":"user","uuid":"u3","par`,
	}, "\n"))
	tests := []struct {
		name      string
		strip     bool
		want      string
		stripped  int
		rewritten int
	}{
		{
			name:  "strip relinks to the nearest surviving ancestor",
			strip: true,
			want: withSID(`{"type":"user","uuid":"u1","parentUuid":null,"sessionId":"SID","cwd":"/dst/co"}
{"type":"assistant","uuid":"a1","parentUuid":"u1","sessionId":"SID","cwd":"/dst/co"}
{"type":"system","subtype":"compact_boundary","uuid":"c1","parentUuid":null,"logicalParentUuid":"u1","sessionId":"SID"}
{"type":"user","uuid":"u2","parentUuid":"c1","sessionId":"SID"}
`),
			stripped:  3,
			rewritten: 3,
		},
		{
			name:      "no strip keeps snapshots",
			strip:     false,
			want:      strings.ReplaceAll(src[:strings.LastIndex(src, "\n")+1], "/src/co", "/dst/co"),
			rewritten: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stats := relocation{}
			r := &relocator{paths: testPaths, fromID: testSID, toID: testSID, strip: tt.strip, stats: &stats}
			r.begin(transcriptFile)
			var out bytes.Buffer
			if err := r.jsonl(strings.NewReader(src), &out); err != nil {
				t.Fatal(err)
			}
			if out.String() != tt.want {
				t.Errorf("jsonl() =\n%s\nwant\n%s", out.String(), tt.want)
			}
			if stats.Stripped != tt.stripped || stats.Rewritten != tt.rewritten || stats.DroppedTailBytes != int64(len(`{"type":"user","uuid":"u3","par`)) {
				t.Errorf("stats = %+v, want stripped %d rewritten %d", stats, tt.stripped, tt.rewritten)
			}
		})
	}
}

func TestParseCapabilities(t *testing.T) {
	full := "Usage: claude [options]\n  -c, --continue  Continue\n  -r, --resume [value]  Resume a conversation\n  --append-system-prompt <prompt>  Append\n  --system-prompt-snapshot <mode>  Snapshot\n"
	tests := []struct {
		name, version, help string
		want                Capabilities
	}{
		{"all flags", "2.1.283 (Claude Code)\n", full, Capabilities{Version: "2.1.283", Resume: true, AppendSystemPrompt: true, SystemPromptSnapshot: true}},
		{"no append", "2.1.290 (Claude Code)", "  -r, --resume [value]  Resume\n", Capabilities{Version: "2.1.290", Resume: true}},
		{"lookalike flag only", "3.0.0", "  --resume-session-at <id>  Resume at\n  --append-system-prompt-file <f>\n", Capabilities{Version: "3.0.0"}},
		{"unparsed version", "nightly\n", "", Capabilities{Version: "nightly"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseCapabilities(tt.version, tt.help); got != tt.want {
				t.Errorf("parseCapabilities() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestProbeProjectDirs(t *testing.T) {
	cwd := "/Users/bob/Code/ünï proj"
	tests := []struct {
		name    string
		dirs    map[string]string
		wantErr bool
	}{
		{"no projects", nil, false},
		{"matching name", map[string]string{claudenative.ProjectDirName(cwd): `{"type":"user","cwd":"` + cwd + `"}`}, false},
		{"one match among strays", map[string]string{"stray": `{"cwd":"/a/b"}`, claudenative.ProjectDirName(cwd): `{"cwd":"` + cwd + `"}`}, false},
		{"only mismatches", map[string]string{"-Users-bob-Code-ün-proj": `{"type":"summary"}` + "\n" + `{"cwd":"` + cwd + `"}`}, true},
		{"no cwd anywhere", map[string]string{"x": `{"type":"summary"}`}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := t.TempDir()
			for name, body := range tt.dirs {
				dir := filepath.Join(cfg, "projects", name)
				if err := os.MkdirAll(dir, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, string(testSID)+".jsonl"), []byte(body+"\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			err := probeProjectDirs(cfg)
			var inc *IncompatibleError
			if got := errors.As(err, &inc); got != tt.wantErr || (got && inc.Capability != "project-dir-encoding") {
				t.Errorf("probeProjectDirs() = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
