package sessionarchive

import (
	"cmp"
	"io/fs"
	"slices"
	"time"

	"github.com/yasyf/synckit/artifact"
)

// Format is the Manifest schema version Capture writes.
const Format = 1

// Media labels of the artifacts Capture stores.
const (
	MediaTranscript  = "cc-sync.transcript"
	MediaFile        = "cc-sync.session-file"
	MediaManifest    = "cc-sync.session-manifest"
	MediaSession     = "cc-sync.session"
	MediaSessionDeps = "cc-sync.session-deps"
)

// Root names the native directory an Entry's Path is relative to; Manifest.Roots
// maps each Root to its absolute source directory.
type Root string

// Roots of one session's native storage.
const (
	RootTranscript  Root = "transcript"
	RootSession     Root = "session"
	RootFileHistory Root = "file-history"
	RootTasks       Root = "tasks"
	RootPlans       Root = "plans"
	RootPasteCache  Root = "paste-cache"
	RootScratchpad  Root = "scratchpad"
)

// Stat is the source file identity the stat cache compares: an Entry whose
// Stat is unchanged is reused without reading the file.
type Stat struct {
	Inode   uint64    `json:"inode"`
	Size    int64     `json:"size"`
	ModTime time.Time `json:"mod_time"`
}

func (s Stat) same(o Stat) bool {
	return s.Inode == o.Inode && s.Size == o.Size && s.ModTime.Equal(o.ModTime)
}

// Entry is one archived directory, symlink, or file. Size counts the stored
// bytes; a frozen JSONL stores only its complete records and records the
// partial tail it left behind in DroppedTailBytes. References lists the
// root-qualified sidecars a JSONL mentions.
type Entry struct {
	Root             Root         `json:"root"`
	Path             string       `json:"path"`
	Mode             fs.FileMode  `json:"mode"`
	Size             int64        `json:"size,omitempty"`
	DroppedTailBytes int64        `json:"dropped_tail_bytes,omitempty"`
	Ref              artifact.Ref `json:"ref,omitzero"`
	Link             string       `json:"link,omitempty"`
	Stat             Stat         `json:"stat,omitzero"`
	References       []string     `json:"references,omitempty"`
}

// Key is the Entry's root-qualified path, the form Completeness uses.
func (e Entry) Key() string {
	return string(e.Root) + "/" + e.Path
}

// Completeness lists what the archive lacks: Missing items the session
// references that are absent at the source, and Deferred files that changed
// under every read and wait for the next capture.
type Completeness struct {
	Missing  []string `json:"missing,omitempty"`
	Deferred []string `json:"deferred,omitempty"`
}

// Manifest describes one captured session; its JSON is stored as MediaManifest.
type Manifest struct {
	Format         int             `json:"format"`
	SessionID      string          `json:"session_id"`
	SourceHost     string          `json:"source_host"`
	CapturedAt     time.Time       `json:"captured_at"`
	ClaudeVersion  string          `json:"claude_version"`
	ConfigDir      string          `json:"config_dir"`
	Home           string          `json:"home"`
	TmpRoot        string          `json:"tmp_root"`
	UID            int             `json:"uid"`
	ProjectDirName string          `json:"project_dir_name"`
	Cwd            string          `json:"cwd"`
	OriginalCwd    string          `json:"original_cwd"`
	GitBranch      string          `json:"git_branch,omitempty"`
	Title          string          `json:"title,omitempty"`
	LastHuman      time.Time       `json:"last_human,omitzero"`
	LastAutonomous time.Time       `json:"last_autonomous,omitzero"`
	Roots          map[Root]string `json:"roots"`
	Entries        []Entry         `json:"entries"`
	Completeness   Completeness    `json:"completeness"`
}

// Refs returns the unique artifact refs of every archived file, ordered by digest.
func (m Manifest) Refs() []artifact.Ref {
	refs := make([]artifact.Ref, 0, len(m.Entries))
	for _, e := range m.Entries {
		if e.Ref.Digest != "" {
			refs = append(refs, e.Ref)
		}
	}
	slices.SortFunc(refs, func(a, b artifact.Ref) int { return cmp.Compare(a.Digest, b.Digest) })
	return slices.CompactFunc(refs, func(a, b artifact.Ref) bool { return a.Digest == b.Digest })
}

// Complete reports whether nothing is missing or deferred.
func (m Manifest) Complete() bool {
	return len(m.Completeness.Missing) == 0 && len(m.Completeness.Deferred) == 0
}

// Archive is one stored capture: Ref is the MediaSession group whose closure
// holds the manifest blob at ManifestRef (its first dependency) and every
// archived file.
type Archive struct {
	Ref         artifact.Ref `json:"ref"`
	ManifestRef artifact.Ref `json:"manifest_ref"`
	Manifest    Manifest     `json:"manifest"`
}
