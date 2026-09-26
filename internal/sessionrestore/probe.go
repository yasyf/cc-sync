package sessionrestore

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/yasyf/cc-sync/internal/claudenative"
)

const probeDirLimit = 16

// IncompatibleError names a concrete capability or storage format the
// destination lacks; it is the only reason the adapter refuses a Claude build.
type IncompatibleError struct {
	Capability string
	Detail     string
}

func (e *IncompatibleError) Error() string {
	return fmt.Sprintf("claude is missing %s: %s", e.Capability, e.Detail)
}

// Runner runs the destination claude binary with args and returns its stdout.
type Runner func(ctx context.Context, args ...string) ([]byte, error)

// SystemRunner runs `claude` from PATH, the binary Launch.Argv resumes with.
func SystemRunner() Runner {
	return func(ctx context.Context, args ...string) ([]byte, error) {
		out, err := exec.CommandContext(ctx, "claude", args...).Output() //nolint:gosec // G204: the claude binary Launch resumes with, run with the probe's fixed --version and --help flags.
		if err != nil {
			return nil, fmt.Errorf("run claude %s: %w", strings.Join(args, " "), err)
		}
		return out, nil
	}
}

// Capabilities is what the destination claude advertises through
// --version and --help.
type Capabilities struct {
	Version              string `json:"version"`
	Resume               bool   `json:"resume"`
	AppendSystemPrompt   bool   `json:"append_system_prompt"`
	SystemPromptSnapshot bool   `json:"system_prompt_snapshot"`
}

var versionPattern = regexp.MustCompile(`\d+\.\d+\.\d+\S*`)

// ProbeCapabilities runs `claude --version` and `claude --help` once.
func ProbeCapabilities(ctx context.Context, run Runner) (Capabilities, error) {
	version, err := run(ctx, "--version")
	if err != nil {
		return Capabilities{}, fmt.Errorf("probe claude version: %w", err)
	}
	help, err := run(ctx, "--help")
	if err != nil {
		return Capabilities{}, fmt.Errorf("probe claude help: %w", err)
	}
	return parseCapabilities(string(version), string(help)), nil
}

func parseCapabilities(version, help string) Capabilities {
	c := Capabilities{
		Version:              versionPattern.FindString(version),
		Resume:               advertises(help, "--resume"),
		AppendSystemPrompt:   advertises(help, "--append-system-prompt"),
		SystemPromptSnapshot: advertises(help, "--system-prompt-snapshot"),
	}
	if c.Version == "" {
		c.Version = strings.TrimSpace(version)
	}
	return c
}

func advertises(help, flag string) bool {
	return regexp.MustCompile(`(?m)^\s+(?:-\w,\s+)?` + regexp.QuoteMeta(flag) + `(?:[\s,=]|$)`).MatchString(help)
}

func probeProjectDirs(configDir string) error {
	projects := claudenative.ProjectsDir(configDir)
	entries, err := os.ReadDir(projects)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read %s: %w", projects, err)
	}
	checked := 0
	var seen []string
	for _, e := range entries {
		if !e.IsDir() || checked == probeDirLimit {
			continue
		}
		cwd, err := firstCwd(filepath.Join(projects, e.Name()))
		if err != nil {
			return err
		}
		if cwd == "" {
			continue
		}
		checked++
		if claudenative.ProjectDirName(cwd) == e.Name() {
			return nil
		}
		seen = append(seen, e.Name()+" <- "+cwd)
	}
	if checked == 0 {
		return nil
	}
	return &IncompatibleError{
		Capability: "project-dir-encoding",
		Detail:     fmt.Sprintf("no existing project dir under %s is named ProjectDirName(its transcript cwd): %s", projects, strings.Join(seen, "; ")),
	}
}

func firstCwd(dir string) (string, error) {
	transcripts, err := filepath.Glob(filepath.Join(dir, "*.jsonl"))
	if err != nil {
		return "", fmt.Errorf("glob %s: %w", dir, err)
	}
	sort.Strings(transcripts)
	for _, path := range transcripts {
		cwd, err := headCwd(path)
		if err != nil || cwd != "" {
			return cwd, err
		}
	}
	return "", nil
}

func headCwd(path string) (string, error) {
	f, err := os.Open(path) //nolint:gosec // G304: a transcript under the destination projects dir, opened read-only.
	if err != nil {
		return "", fmt.Errorf("open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<16), 1<<26)
	for i := 0; i < 64 && sc.Scan(); i++ {
		if cwd, ok := decodeString(memberValue(sc.Bytes(), "cwd")); ok {
			return cwd, nil
		}
	}
	if err := sc.Err(); err != nil {
		return "", fmt.Errorf("read %s: %w", path, err)
	}
	return "", nil
}
