package sessionrestore_test

import (
	"bytes"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"unicode/utf16"
)

const corpusDir = "testdata/native"

var corpusFS = os.DirFS(corpusDir)

type corpusManifest struct {
	Sessions []corpusSession `json:"sessions"`
}

type corpusSession struct {
	Name      string       `json:"name"`
	SessionID string       `json:"sessionId"`
	Cwd       string       `json:"cwd"`
	Files     []corpusFile `json:"files"`
	Symlinks  []corpusLink `json:"symlinks"`
}

type corpusFile struct {
	Fixture string `json:"fixture"`
	Native  string `json:"native"`
}

type corpusLink struct {
	Native string `json:"native"`
	Target string `json:"target"`
}

type corpusRoots struct {
	cwd, config, tmp string
}

func (r corpusRoots) expand(s string) string {
	return strings.NewReplacer(
		"{{SRC_CWD}}", r.cwd,
		"{{SRC_CONFIG}}", r.config,
		"{{SRC_TMP}}", r.tmp,
		"{{SRC_PROJECT}}", projectDirName(r.cwd),
	).Replace(s)
}

func loadCorpus(t *testing.T) corpusManifest {
	t.Helper()
	data, err := fs.ReadFile(corpusFS, "manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	var m corpusManifest
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("decode corpus manifest: %v", err)
	}
	return m
}

func (s corpusSession) transcript() corpusFile {
	return s.Files[slices.IndexFunc(s.Files, func(f corpusFile) bool {
		return filepath.Base(f.Native) == s.SessionID+".jsonl"
	})]
}

func installCorpus(t *testing.T, s corpusSession, r corpusRoots, base string) map[string][]byte {
	t.Helper()
	root, err := os.OpenRoot(base)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := root.Close(); err != nil {
			t.Error(err)
		}
	}()
	rel := func(p string) string {
		r, err := filepath.Rel(base, p)
		if err != nil || !filepath.IsLocal(r) {
			t.Fatalf("native path %s is outside %s", p, base)
		}
		if err := root.MkdirAll(filepath.Dir(r), 0o750); err != nil {
			t.Fatal(err)
		}
		return r
	}
	installed := map[string][]byte{}
	for _, f := range s.Files {
		raw, err := fs.ReadFile(corpusFS, f.Fixture)
		if err != nil {
			t.Fatal(err)
		}
		dst := r.expand(f.Native)
		installed[dst] = []byte(r.expand(string(raw)))
		if err := root.WriteFile(rel(dst), installed[dst], 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for _, l := range s.Symlinks {
		dst := r.expand(l.Native)
		if err := root.Symlink(r.expand(l.Target), rel(dst)); err != nil {
			t.Fatal(err)
		}
		installed[dst] = installed[r.expand(l.Target)]
	}
	return installed
}

func recordKind(rec map[string]any) string {
	kind, _ := rec["type"].(string)
	if a, ok := rec["attachment"].(map[string]any); ok {
		return kind + ":" + a["type"].(string)
	}
	if sub, ok := rec["subtype"].(string); ok {
		return kind + ":" + sub
	}
	return kind
}

func completeRecords(t *testing.T, data []byte) ([]map[string]any, []byte) {
	t.Helper()
	cut := bytes.LastIndexByte(data, '\n') + 1
	var recs []map[string]any
	for line := range bytes.Lines(data[:cut]) {
		var rec map[string]any
		if err := json.Unmarshal(line, &rec); err != nil {
			t.Fatalf("decode record %q: %v", line, err)
		}
		recs = append(recs, rec)
	}
	return recs, data[cut:]
}

func absoluteStrings(v any, roots []string, visit func(string)) {
	switch v := v.(type) {
	case string:
		for _, root := range roots {
			if strings.HasPrefix(v, root+"/") {
				visit(v)
			}
		}
	case []any:
		for _, e := range v {
			absoluteStrings(e, roots, visit)
		}
	case map[string]any:
		for _, e := range v {
			absoluteStrings(e, roots, visit)
		}
	}
}

func TestSyntheticCorpus(t *testing.T) {
	m := loadCorpus(t)
	tests := []struct {
		name      string
		partial   bool
		longCwd   bool
		wantKinds []string
		verbatim  []string
	}{
		{
			name: "main",
			wantKinds: []string{
				"assistant", "attachment:environment", "attachment:plan_mode", "attachment:plan_mode_exit",
				"attachment:prompt_snapshot", "custom-title", "file-history-snapshot", "last-prompt", "queue-operation",
				"relocated", "system:compact_boundary", "user",
			},
			verbatim: []string{" ", "😀", "“smart”", `\"quoted\"`, `back\\slash`, "a <b> & c"},
		},
		{
			name:      "long",
			longCwd:   true,
			wantKinds: []string{"assistant", "attachment:environment", "attachment:prompt_snapshot", "last-prompt", "user"},
		},
		{
			name:      "partial",
			partial:   true,
			wantKinds: []string{"assistant", "attachment:environment", "attachment:prompt_snapshot", "user"},
		},
	}
	if got := len(m.Sessions); got != len(tests) {
		t.Fatalf("corpus has %d sessions, want %d", got, len(tests))
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := m.Sessions[i]
			if s.Name != tt.name {
				t.Fatalf("session %d = %q, want %q", i, s.Name, tt.name)
			}
			root, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			r := corpusRoots{
				cwd:    filepath.Join(root, "work", s.Cwd),
				config: filepath.Join(root, "config"),
				tmp:    filepath.Join(root, "scratch"),
			}
			installed := installCorpus(t, s, r, root)

			units := len(utf16.Encode([]rune(r.cwd)))
			enc := projectDirName(r.cwd)
			if got := units > 200; got != tt.longCwd {
				t.Errorf("cwd has %d UTF-16 units; long = %v, want %v", units, got, tt.longCwd)
			}
			if tt.longCwd && (len(enc) <= 201 || enc[200] != '-') {
				t.Errorf("project dir %q lacks the hash suffix of a >200-unit cwd", enc)
			}

			for path, data := range installed {
				if bytes.Contains(data, []byte("{{")) {
					t.Errorf("%s keeps an unexpanded placeholder", path)
				}
			}
			for _, l := range s.Symlinks {
				native, target := r.expand(l.Native), r.expand(l.Target)
				if got, err := os.Readlink(native); err != nil || got != target {
					t.Errorf("symlink %s -> %q (%v), want %q", native, got, err, target)
				}
				if _, ok := installed[target]; !ok {
					t.Errorf("symlink %s dangles: the corpus does not provide %s", native, target)
				}
			}

			raw := installed[r.expand(s.transcript().Native)]
			for _, want := range tt.verbatim {
				if !bytes.Contains(raw, []byte(want)) {
					t.Errorf("transcript lacks verbatim %q", want)
				}
			}
			recs, tail := completeRecords(t, raw)
			if got := len(tail) > 0; got != tt.partial {
				t.Errorf("partial tail = %v, want %v", got, tt.partial)
			}
			if len(tail) > 0 && json.Valid(tail) {
				t.Errorf("partial tail %q is a complete record", tail)
			}

			seen := map[string]bool{}
			parents := map[string]bool{}
			var kinds []string
			for n, rec := range recs {
				kinds = append(kinds, recordKind(rec))
				if sid, ok := rec["sessionId"]; ok && sid != s.SessionID {
					t.Errorf("record %d sessionId = %v, want %s", n, sid, s.SessionID)
				}
				for _, key := range []string{"parentUuid", "logicalParentUuid"} {
					if p, ok := rec[key].(string); ok {
						if !seen[p] {
							t.Errorf("record %d %s %s precedes its target", n, key, p)
						}
						parents[p] = true
					}
				}
				if id, ok := rec["uuid"].(string); ok {
					if seen[id] {
						t.Errorf("record %d reuses uuid %s", n, id)
					}
					seen[id] = true
				}
				absoluteStrings(rec, []string{r.config, r.tmp}, func(p string) {
					if _, ok := installed[p]; !ok {
						t.Errorf("record %d names %s, which the corpus does not provide", n, p)
					}
				})
			}
			for n, rec := range recs {
				if recordKind(rec) == "attachment:prompt_snapshot" && !parents[rec["uuid"].(string)] {
					t.Errorf("prompt_snapshot record %d is outside the parentUuid chain", n)
				}
			}
			slices.Sort(kinds)
			if got := slices.Compact(kinds); !slices.Equal(got, tt.wantKinds) {
				t.Errorf("record kinds = %v, want %v", got, tt.wantKinds)
			}
		})
	}
}

func TestSyntheticCorpusCarriesNoLocalIdentity(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	forbidden := []string{home, "/Users/", "/private/tmp/claude-" + strconv.Itoa(os.Getuid())}
	err = fs.WalkDir(corpusFS, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, err := fs.ReadFile(corpusFS, path)
		if err != nil {
			return err
		}
		for _, f := range forbidden {
			if bytes.Contains(data, []byte(f)) {
				t.Errorf("%s contains %q", path, f)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func projectDirName(p string) string {
	units := utf16.Encode([]rune(p))
	b := make([]byte, len(units))
	for i, u := range units {
		switch {
		case u >= '0' && u <= '9', u >= 'A' && u <= 'Z', u >= 'a' && u <= 'z':
			b[i] = byte(u)
		default:
			b[i] = '-'
		}
	}
	if len(b) <= 200 {
		return string(b)
	}
	var hash int32
	for _, u := range units {
		hash = hash<<5 - hash + int32(u)
	}
	n := int64(hash)
	if n < 0 {
		n = -n
	}
	return string(b[:200]) + "-" + strconv.FormatInt(n, 36)
}
