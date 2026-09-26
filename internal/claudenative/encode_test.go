package claudenative

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseSessionID(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    SessionID
		wantErr bool
	}{
		{name: "canonical", in: "0f8e2a4c-1b3d-4e5f-9a7b-6c5d4e3f2a1b", want: "0f8e2a4c-1b3d-4e5f-9a7b-6c5d4e3f2a1b"},
		{name: "uppercase lowered", in: "0F8E2A4C-1B3D-4E5F-9A7B-6C5D4E3F2A1B", want: "0f8e2a4c-1b3d-4e5f-9a7b-6c5d4e3f2a1b"},
		{name: "title", in: "my session", wantErr: true},
		{name: "missing dash", in: "0f8e2a4c11b3d-4e5f-9a7b-6c5d4e3f2a1b", wantErr: true},
		{name: "non hex", in: "0f8e2a4c-1b3d-4e5f-9a7b-6c5d4e3f2a1g", wantErr: true},
		{name: "short", in: "0f8e2a4c-1b3d-4e5f-9a7b-6c5d4e3f2a1", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseSessionID(tt.in)
			if tt.wantErr {
				if !errors.Is(err, ErrInvalidSessionID) {
					t.Fatalf("ParseSessionID(%q) error = %v, want ErrInvalidSessionID", tt.in, err)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("ParseSessionID(%q) = %q, %v, want %q", tt.in, got, err, tt.want)
			}
		})
	}
}

func TestProjectDirName(t *testing.T) {
	tests := []struct {
		name     string
		cwd      string
		sanitize string
		want     string
	}{
		{name: "ascii", cwd: "/Users/yasyf/Code/cc-sync", want: "-Users-yasyf-Code-cc-sync"},
		{name: "punctuation", cwd: "/Users/me/My Project_v2.0/(draft)!@#$%^&*~`'\"", want: "-Users-me-My-Project-v2-0--draft-------------"},
		{name: "accent is one unit", cwd: "/Users/me/café", want: "-Users-me-caf-"},
		{name: "emoji is two units", cwd: "/Users/me/😀x", want: "-Users-me---x"},
		{name: "cjk", cwd: "/Users/me/中文目录", want: "-Users-me-----"},
		{name: "199 units", cwd: "/" + strings.Repeat("a", 198), want: "-" + strings.Repeat("a", 198)},
		{name: "200 units", cwd: "/" + strings.Repeat("a", 199), want: "-" + strings.Repeat("a", 199)},
		{
			name:     "201 units hashed",
			cwd:      "/" + strings.Repeat("a", 200),
			sanitize: "-" + strings.Repeat("a", 200),
			want:     "-" + strings.Repeat("a", 199) + "-b6ymvl",
		},
		{
			name:     "300 units positive hash",
			cwd:      "/Users/me/" + strings.Repeat("deep/", 58),
			sanitize: "-Users-me-" + strings.Repeat("deep-", 58),
			want:     "-Users-me-" + strings.Repeat("deep-", 38) + "-cpdk1t",
		},
		{
			name:     "surrogates past 200 units",
			cwd:      "/Users/me/" + strings.Repeat("é😀", 80),
			sanitize: "-Users-me-" + strings.Repeat("-", 240),
			want:     "-Users-me-" + strings.Repeat("-", 190) + "-no2tzz",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ProjectDirName(tt.cwd); got != tt.want {
				t.Errorf("ProjectDirName() = %q, want %q", got, tt.want)
			}
			wantSanitized := tt.sanitize
			if wantSanitized == "" {
				wantSanitized = tt.want
			}
			if got := SanitizePath(tt.cwd); got != wantSanitized {
				t.Errorf("SanitizePath() = %q, want %q", got, wantSanitized)
			}
		})
	}
}

func TestLayoutPaths(t *testing.T) {
	id := SessionID("0f8e2a4c-1b3d-4e5f-9a7b-6c5d4e3f2a1b")
	cwd := "/Users/me/my.proj"
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"projects", ProjectsDir("/h/.claude"), "/h/.claude/projects"},
		{"transcript", TranscriptPath("/h/.claude", cwd, id), "/h/.claude/projects/-Users-me-my-proj/" + string(id) + ".jsonl"},
		{"session dir", SessionDir("/h/.claude", cwd, id), "/h/.claude/projects/-Users-me-my-proj/" + string(id)},
		{"scratchpad", ScratchpadDir("/private/tmp", 502, cwd, id), "/private/tmp/claude-502/-Users-me-my-proj/" + string(id)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("got %q, want %q", tt.got, tt.want)
			}
		})
	}
}

func TestDefaultLayout(t *testing.T) {
	home := t.TempDir()
	tests := []struct {
		name    string
		env     string
		want    string
		wantErr error
	}{
		{name: "canonical", env: "", want: filepath.Join(home, ".claude")},
		{name: "explicit root", env: "/Volumes/alt/claude", want: "/Volumes/alt/claude"},
		{name: "pool projection", env: filepath.Join(home, ".cc-pool", "config", "acct1"), wantErr: ErrPoolProjection},
		{name: "pool root", env: filepath.Join(home, ".cc-pool", "config"), wantErr: ErrPoolProjection},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("HOME", home)
			t.Setenv("CLAUDE_CONFIG_DIR", tt.env)
			got, err := DefaultLayout()
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("DefaultLayout() error = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("DefaultLayout() error = %v", err)
			}
			if got.ConfigDir != tt.want || got.UID != os.Getuid() || got.TmpRoot != defaultTmpRoot() {
				t.Errorf("DefaultLayout() = %+v, want ConfigDir %q", got, tt.want)
			}
		})
	}
}

func TestProjectDirNameMatchesRealProjects(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("UserHomeDir: %v", err)
	}
	root := filepath.Join(home, ".claude", "projects")
	dirs, err := os.ReadDir(root)
	if errors.Is(err, fs.ErrNotExist) {
		t.Skip("no ~/.claude/projects on this host")
	}
	if err != nil {
		t.Fatalf("read %s: %v", root, err)
	}
	var checked int
	var mismatches []string
	for _, d := range dirs {
		if !d.IsDir() {
			continue
		}
		matches, err := filepath.Glob(filepath.Join(root, d.Name(), "*.jsonl"))
		if err != nil {
			t.Fatalf("glob: %v", err)
		}
		for _, path := range matches {
			if _, err := ParseSessionID(strings.TrimSuffix(filepath.Base(path), ".jsonl")); err != nil {
				continue
			}
			cwd := firstRecordCwd(t, path)
			if cwd == "" {
				continue
			}
			checked++
			if got := ProjectDirName(cwd); got != d.Name() && !hasRelocatedRecord(t, path) {
				mismatches = append(mismatches, d.Name()+" != ProjectDirName(cwd) "+got)
			}
		}
	}
	t.Logf("checked %d real transcripts", checked)
	if len(mismatches) > 0 {
		t.Errorf("%d project dirs disagree with ProjectDirName:\n%s", len(mismatches), strings.Join(mismatches, "\n"))
	}
}

func firstRecordCwd(t *testing.T, path string) string {
	t.Helper()
	var cwd string
	scanLines(t, path, func(line []byte) bool {
		var r struct {
			Cwd string `json:"cwd"`
		}
		if json.Unmarshal(line, &r) == nil && r.Cwd != "" {
			cwd = r.Cwd
			return false
		}
		return true
	})
	return cwd
}

func hasRelocatedRecord(t *testing.T, path string) bool {
	t.Helper()
	found := false
	scanLines(t, path, func(line []byte) bool {
		found = bytes.Contains(line, []byte(`"relocatedCwd"`))
		return !found
	})
	return found
}

func scanLines(t *testing.T, path string, visit func(line []byte) bool) {
	t.Helper()
	f, err := os.Open(path) //nolint:gosec // G304: a real transcript under ~/.claude/projects, opened read-only.
	if err != nil {
		t.Fatalf("open transcript: %v", err)
	}
	defer func() { _ = f.Close() }()
	br := bufio.NewReaderSize(f, 1<<20)
	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 && !visit(line) {
			return
		}
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			t.Fatalf("read transcript: %v", err)
		}
	}
}
