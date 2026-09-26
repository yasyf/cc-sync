// Package cli builds the cobra command tree and its JSON surface.
package cli

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"os/exec"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/yasyf/cc-sync/internal/version"
)

type app struct {
	svc  Service
	term Terminal
	json bool
}

// Terminal is the process surface an interactive pickup hands to claude.
// Interactive reports whether stdin and stdout are both terminals, Environ is
// the environment a launched session starts from, and Exec replaces the
// process with argv run in dir under env, returning only on failure.
type Terminal struct {
	Interactive bool
	Environ     []string
	Exec        func(argv []string, dir string, env []string) error
}

// ProcessTerminal is this process's Terminal, execing with syscall.Exec.
func ProcessTerminal() Terminal {
	return Terminal{
		Interactive: isTerminal(os.Stdin) && isTerminal(os.Stdout),
		Environ:     os.Environ(),
		Exec:        execIn,
	}
}

func isTerminal(f *os.File) bool {
	return term.IsTerminal(int(f.Fd()))
}

func execIn(argv []string, dir string, env []string) error {
	if err := os.Chdir(dir); err != nil {
		return fmt.Errorf("enter %s: %w", dir, err)
	}
	path, err := exec.LookPath(argv[0])
	if err != nil {
		return fmt.Errorf("find %s: %w", argv[0], err)
	}
	return fmt.Errorf("exec %s: %w", path, syscall.Exec(path, argv, env)) //nolint:gosec // G204: argv is the pickup launch of the destination claude, exec'd directly and never through a shell.
}

// Command is the cc-sync command line bound to a Service.
type Command struct {
	app  *app
	root *cobra.Command
}

// New builds the cc-sync command line over svc; an interactive pickup hands
// the selected session to claude through term.
func New(svc Service, term Terminal) *Command {
	a := &app{svc: svc, term: term}
	return &Command{app: a, root: a.rootCmd()}
}

// Execute runs cc-sync against svc on the process terminal with SIGINT and
// SIGTERM cancelling the command's context, and returns the process exit code.
func Execute(svc Service, args []string, stdout, stderr io.Writer) int {
	c := New(svc, ProcessTerminal())
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return c.Run(ctx, args, stdout, stderr)
}

// Run executes args under ctx and returns the process exit code. A failure
// prints exactly one error envelope line on stdout under --json, and a plain
// message on stderr otherwise.
func (c *Command) Run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	c.root.SetArgs(args)
	c.root.SetOut(stdout)
	c.root.SetErr(stderr)
	err := c.root.ExecuteContext(ctx)
	if err == nil {
		return ExitOK
	}
	code := Classify(err)
	if c.app.json || requestsJSON(args) {
		if werr := writeJSON(stdout, newFailure(code, err)); werr != nil {
			slog.Error("write error envelope", "code", code, "err", werr)
		}
	} else if _, werr := fmt.Fprintf(stderr, "cc-sync: %v\n%s", err, humanHint(code)); werr != nil {
		slog.Error("write error", "code", code, "err", werr)
	}
	return code.ExitCode()
}

func humanHint(code Code) string {
	if code == CodeDivergentLocalCopy {
		return "cc-sync: re-run with --on-divergence keep-local, replace, or fork\n"
	}
	return ""
}

func requestsJSON(args []string) bool {
	on := false
	for _, arg := range args {
		if arg == "--" {
			break
		}
		name, value, hasValue := strings.Cut(arg, "=")
		if name != "--json" {
			continue
		}
		if !hasValue {
			on = true
			continue
		}
		if b, err := strconv.ParseBool(value); err == nil {
			on = b
		}
	}
	return on
}

func (a *app) rootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "cc-sync",
		Short:         "Recover Claude Code sessions, their uncommitted Git work, and Orca workspace layouts on another synckit peer.",
		Version:       version.String(),
		SilenceUsage:  true,
		SilenceErrors: true,
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) > 0 {
				return Errorf(CodeUsage, "unknown command %q for \"cc-sync\"", args[0])
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, _ []string) error { return cmd.Help() },
	}
	root.CompletionOptions.DisableDefaultCmd = true
	root.SetVersionTemplate("{{.Version}}\n")
	root.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return &Error{Code: CodeUsage, Err: err}
	})
	root.PersistentFlags().BoolVar(&a.json, "json", false, `print one JSON document with "version":1 on stdout`)
	root.AddCommand(
		a.installCmd(),
		a.uninstallCmd(),
		a.listCmd(),
		a.inspectCmd(),
		a.statusCmd(),
		a.syncCmd(),
		a.pickupCmd(),
		a.resumeCmd(),
		a.helperServeCmd(),
	)
	return root
}

func usageArgs(check cobra.PositionalArgs) cobra.PositionalArgs {
	return func(cmd *cobra.Command, args []string) error {
		if err := check(cmd, args); err != nil {
			return &Error{Code: CodeUsage, Err: err}
		}
		return nil
	}
}

func (a *app) emit(cmd *cobra.Command, payload any, human func(*printer)) error {
	if a.json {
		return writeJSON(cmd.OutOrStdout(), payload)
	}
	p := &printer{w: cmd.OutOrStdout()}
	human(p)
	if p.err != nil {
		return fmt.Errorf("write output: %w", p.err)
	}
	return nil
}

func validateSessionIDs(ids []string) error {
	for _, id := range ids {
		if err := validateSessionID(id); err != nil {
			return err
		}
	}
	return nil
}

func (a *app) installCmd() *cobra.Command {
	var req InstallRequest
	cmd := &cobra.Command{
		Use:   "install",
		Short: "Create cc-sync's state, register it with synckitd, and start the helper",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			res, err := a.svc.Install(cmd.Context(), req)
			if err != nil {
				return err
			}
			return a.emit(cmd, struct {
				header
				InstallResult
			}{okHeader, res}, func(p *printer) {
				helper := "not running"
				if res.Helper.Running {
					helper = "running"
				}
				p.printf("installed in %s; helper %s\n", res.ConfigDir, helper)
			})
		},
	}
	cmd.Flags().BoolVar(&req.NoSynckitd, "no-synckitd", false, "skip registering with and installing synckitd")
	return cmd
}

func (a *app) uninstallCmd() *cobra.Command {
	var req UninstallRequest
	cmd := &cobra.Command{
		Use:   "uninstall",
		Short: "Unregister cc-sync from synckitd and stop the helper",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			res, err := a.svc.Uninstall(cmd.Context(), req)
			if err != nil {
				return err
			}
			return a.emit(cmd, struct {
				header
				UninstallResult
			}{okHeader, res}, func(p *printer) {
				if res.Purged {
					p.printf("uninstalled and purged local state\n")
					return
				}
				p.printf("uninstalled\n")
			})
		},
	}
	cmd.Flags().BoolVar(&req.Purge, "purge", false, "also delete cc-sync's local state and checkpoints")
	return cmd
}

func (a *app) listCmd() *cobra.Command {
	var req ListRequest
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List recoverable workspaces from every peer",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			res, err := a.svc.List(cmd.Context(), req)
			if err != nil {
				return err
			}
			return a.emit(cmd, newListJSON(res), func(p *printer) { renderList(p, res) })
		},
	}
	cmd.Flags().StringVar(&req.Source, "source", "", "only items captured on this host")
	cmd.Flags().StringVar(&req.Repo, "repo", "", "only items of this repo path or origin")
	cmd.Flags().BoolVar(&req.All, "all", false, "include items that are not ready")
	return cmd
}

func (a *app) inspectCmd() *cobra.Command {
	var checkpoint string
	cmd := &cobra.Command{
		Use:   "inspect <selector|session>",
		Short: "Show one workspace's checkpoints, completeness, and delivery",
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE: func(cmd *cobra.Command, args []string) error {
			target, err := ParseTarget(args[0])
			if err != nil {
				return err
			}
			sel, err := ParseCheckpoint(checkpoint)
			if err != nil {
				return err
			}
			res, err := a.svc.Inspect(cmd.Context(), InspectRequest{Target: target, Checkpoint: sel})
			if err != nil {
				return err
			}
			return a.emit(cmd, inspectJSON{header: okHeader, Selector: res.Ref().String(), InspectResult: res},
				func(p *printer) { renderInspect(p, res) })
		},
	}
	cmd.Flags().StringVar(&checkpoint, "checkpoint", "latest", "latest, <id-prefix>, at:<RFC3339>, hourly:-<N>h, or daily:<YYYY-MM-DD>")
	return cmd
}

func (a *app) statusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show the helper, scheduler, network, and per-peer delivery",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			res, err := a.svc.Status(cmd.Context())
			if err != nil {
				return err
			}
			return a.emit(cmd, struct {
				header
				StatusResult
			}{okHeader, res}, func(p *printer) { renderStatus(p, res) })
		},
	}
}

func (a *app) syncCmd() *cobra.Command {
	var sessions []string
	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Capture now and wait for the capture round to finish",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := validateSessionIDs(sessions); err != nil {
				return err
			}
			res, err := a.svc.Sync(cmd.Context(), SyncRequest{Sessions: sessions})
			if err != nil {
				return err
			}
			return a.emit(cmd, struct {
				header
				SyncResult
			}{okHeader, res}, func(p *printer) { renderSync(p, res) })
		},
	}
	cmd.Flags().StringArrayVar(&sessions, "session", nil, "capture only the worktree of this session id (repeatable)")
	return cmd
}

func (a *app) pickupCmd() *cobra.Command {
	var (
		checkpoint string
		progress   string
		req        PickupRequest
	)
	cmd := &cobra.Command{
		Use:   "pickup <selector|session>",
		Short: "Restore a workspace's code, sessions, and Orca layout on this machine",
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE: func(cmd *cobra.Command, args []string) error {
			target, err := ParseTarget(args[0])
			if err != nil {
				return err
			}
			sel, err := ParseCheckpoint(checkpoint)
			if err != nil {
				return err
			}
			if err := validateSessionIDs(req.Resume); err != nil {
				return err
			}
			report, err := progressReporter(progress, cmd.ErrOrStderr())
			if err != nil {
				return err
			}
			interactive := a.interactive(req)
			if interactive && req.NoOrca && len(req.Resume) > 1 {
				return Errorf(CodeUsage, "--no-orca selects %s but one terminal resumes one session: pass one --resume, or --json for every session's launch", strings.Join(req.Resume, ", "))
			}
			req.Target, req.Checkpoint, req.Progress = target, sel, report
			res, err := a.svc.Pickup(cmd.Context(), req)
			if err != nil {
				return err
			}
			var launching *PickedSession
			if interactive {
				if launching, err = sessionToLaunch(res); err != nil {
					return err
				}
			}
			if err := a.emit(cmd, struct {
				header
				PickupResult
			}{okHeader, res}, func(p *printer) { renderPickup(p, res, launching) }); err != nil {
				return err
			}
			if launching == nil {
				return nil
			}
			l := launching.Launch
			return a.term.Exec(l.Argv, l.Dir, launchEnv(a.term.Environ, *l))
		},
	}
	req.OnDivergence = DivergenceRefuse
	cmd.Flags().StringVar(&checkpoint, "checkpoint", "latest", "latest, <id-prefix>, at:<RFC3339>, hourly:-<N>h, or daily:<YYYY-MM-DD>")
	cmd.Flags().StringArrayVar(&req.Resume, "resume", nil, "resume this session id; others import dormant (repeatable)")
	cmd.Flags().Var(&req.OnDivergence, "on-divergence", "when a local copy of a picked session diverged: refuse, keep-local, replace, or fork")
	cmd.Flags().BoolVar(&req.NoOrca, "no-orca", false, "skip the Orca import")
	cmd.Flags().BoolVar(&req.DryRun, "dry-run", false, "report what pickup would do without changing anything")
	cmd.Flags().StringVar(&progress, "progress", "", "emit progress on stderr; the only format is ndjson")
	return cmd
}

func (a *app) interactive(req PickupRequest) bool {
	return a.term.Interactive && !a.json && !req.DryRun
}

func sessionToLaunch(res PickupResult) (*PickedSession, error) {
	var pending []*PickedSession
	for i := range res.Sessions {
		if s := &res.Sessions[i]; s.Selected && s.Status != SessionResumed {
			pending = append(pending, s)
		}
	}
	switch len(pending) {
	case 0:
		return nil, nil
	case 1:
		if pending[0].Launch == nil {
			return nil, Errorf(CodeInternal, "selected session %s has no launch", pending[0].SessionID)
		}
		return pending[0], nil
	}
	ids := make([]string, 0, len(pending))
	for _, s := range pending {
		ids = append(ids, s.SessionID)
	}
	return nil, Errorf(CodeUsage, "sessions %s are restored but not resumed and one terminal resumes one session: run cc-sync resume <session> for each", strings.Join(ids, ", "))
}

func launchEnv(base []string, l Launch) []string {
	env := make([]string, 0, len(base)+len(l.EnvSet))
	for _, kv := range base {
		name, _, _ := strings.Cut(kv, "=")
		if _, set := l.EnvSet[name]; set || slices.Contains(l.EnvUnset, name) {
			continue
		}
		env = append(env, kv)
	}
	for _, name := range slices.Sorted(maps.Keys(l.EnvSet)) {
		env = append(env, name+"="+l.EnvSet[name])
	}
	return env
}

func progressReporter(format string, stderr io.Writer) (func(Progress), error) {
	switch format {
	case "":
		return func(Progress) {}, nil
	case "ndjson":
		return (&ndjsonProgress{w: stderr}).report, nil
	}
	return nil, Errorf(CodeUsage, "invalid --progress %q: want ndjson", format)
}

func (a *app) resumeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "resume <session>",
		Short: "Continue a picked-up session with claude in its restored directory",
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE: func(cmd *cobra.Command, args []string) error {
			ref, err := ParseSession(args[0])
			if err != nil {
				return err
			}
			res, err := a.svc.Resume(cmd.Context(), ResumeRequest{Session: ref})
			if err != nil {
				return err
			}
			return a.emit(cmd, struct {
				header
				ResumeResult
			}{okHeader, res}, func(p *printer) { p.printf("resumed %s in %s\n", res.SessionID, res.Cwd) })
		},
	}
}

func (a *app) helperServeCmd() *cobra.Command {
	return &cobra.Command{
		Use:    "helper-serve",
		Short:  "Run the resident helper (started by synckitd)",
		Hidden: true,
		Args:   usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			return a.svc.HelperServe(cmd.Context())
		},
	}
}
