// Package claudenative reads Claude Code's native on-disk state: the project
// directory encoding, the transcript and sidecar layout, the live-session
// registry, and an incremental session inventory. It never writes inside a
// Claude config dir or scratchpad and takes no Claude locks.
package claudenative

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

// ErrPoolProjection reports a config dir that is a cc-pool File Provider
// projection of ~/.claude; scanning it would count every session twice.
var ErrPoolProjection = errors.New("config dir is a cc-pool projection of ~/.claude")

// Layout locates one Claude config dir and the per-user scratchpad root.
type Layout struct {
	ConfigDir string
	TmpRoot   string
	UID       int
}

// DefaultLayout resolves $CLAUDE_CONFIG_DIR, else the canonical ~/.claude,
// and refuses a cc-pool projection under ~/.cc-pool/config.
func DefaultLayout() (Layout, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return Layout{}, fmt.Errorf("resolve home dir: %w", err)
	}
	configDir := os.Getenv("CLAUDE_CONFIG_DIR")
	if configDir == "" {
		configDir = filepath.Join(home, ".claude")
	}
	configDir, err = filepath.Abs(configDir)
	if err != nil {
		return Layout{}, fmt.Errorf("resolve config dir: %w", err)
	}
	pool := filepath.Join(home, ".cc-pool", "config")
	if configDir == pool || strings.HasPrefix(configDir, pool+string(filepath.Separator)) {
		return Layout{}, fmt.Errorf("%w: %s (unset CLAUDE_CONFIG_DIR)", ErrPoolProjection, configDir)
	}
	return Layout{ConfigDir: configDir, TmpRoot: defaultTmpRoot(), UID: os.Getuid()}, nil
}

func defaultTmpRoot() string {
	if runtime.GOOS == "darwin" {
		return "/private/tmp"
	}
	return "/tmp"
}

// ProjectsDir holds one directory per encoded project cwd.
func ProjectsDir(configDir string) string {
	return filepath.Join(configDir, "projects")
}

// TranscriptPath is where Claude writes the main JSONL transcript of a session started in cwd.
func TranscriptPath(configDir, cwd string, id SessionID) string {
	return filepath.Join(ProjectsDir(configDir), ProjectDirName(cwd), string(id)+".jsonl")
}

// SessionDir holds a session's subagent transcripts, spilled tool results, and workflows.
func SessionDir(configDir, cwd string, id SessionID) string {
	return filepath.Join(ProjectsDir(configDir), ProjectDirName(cwd), string(id))
}

// ScratchpadDir is the session's volatile scratch space under the per-user tmp root.
func ScratchpadDir(tmpRoot string, uid int, cwd string, id SessionID) string {
	return filepath.Join(tmpRoot, "claude-"+strconv.Itoa(uid), ProjectDirName(cwd), string(id))
}

func (l Layout) historyPath() string {
	return filepath.Join(l.ConfigDir, "history.jsonl")
}

func (l Layout) fileHistoryDir(id SessionID) string {
	return filepath.Join(l.ConfigDir, "file-history", string(id))
}

func (l Layout) taskListDir(listID string) string {
	return filepath.Join(l.ConfigDir, "tasks", listID)
}

func (l Layout) planPath(slug string) string {
	return filepath.Join(l.ConfigDir, "plans", slug+".md")
}

func (l Layout) registryDir() string {
	return filepath.Join(l.ConfigDir, "sessions")
}
