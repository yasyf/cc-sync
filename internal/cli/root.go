// Package cli builds the cobra command tree and its JSON surface.
package cli

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/yasyf/cc-sync/internal/version"
)

type app struct {
	svc  Service
	json bool
}

// Command is the cc-sync command line bound to a Service.
type Command struct {
	app  *app
	root *cobra.Command
}

// New builds the cc-sync command line over svc.
func New(svc Service) *Command {
	a := &app{svc: svc}
	return &Command{app: a, root: a.rootCmd()}
}

// Execute runs cc-sync against svc with SIGINT and SIGTERM cancelling the
// command's context, and returns the process exit code.
func Execute(svc Service, args []string, stdout, stderr io.Writer) int {
	c := New(svc)
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
	} else if _, werr := fmt.Fprintf(stderr, "cc-sync: %v\n", err); werr != nil {
		slog.Error("write error", "code", code, "err", werr)
	}
	return code.ExitCode()
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
			req.Target, req.Checkpoint, req.Progress = target, sel, report
			res, err := a.svc.Pickup(cmd.Context(), req)
			if err != nil {
				return err
			}
			return a.emit(cmd, struct {
				header
				PickupResult
			}{okHeader, res}, func(p *printer) { renderPickup(p, res) })
		},
	}
	cmd.Flags().StringVar(&checkpoint, "checkpoint", "latest", "latest, <id-prefix>, at:<RFC3339>, hourly:-<N>h, or daily:<YYYY-MM-DD>")
	cmd.Flags().StringArrayVar(&req.Resume, "resume", nil, "resume this session id; others import dormant (repeatable)")
	cmd.Flags().BoolVar(&req.NoOrca, "no-orca", false, "skip the Orca import")
	cmd.Flags().BoolVar(&req.DryRun, "dry-run", false, "report what pickup would do without changing anything")
	cmd.Flags().StringVar(&progress, "progress", "", "emit progress on stderr; the only format is ndjson")
	return cmd
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
