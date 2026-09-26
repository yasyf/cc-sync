// Command cc-sync: Recover Claude Code sessions, their uncommitted Git work, and Orca workspace layouts on another synckit peer.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/yasyf/cc-sync/internal/cli"
	applog "github.com/yasyf/cc-sync/internal/log"
)

func main() {
	applog.Setup()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := cli.NewRootCmd().ExecuteContext(ctx); err != nil {
		// Minimal error handling: report on stderr and exit non-zero. As the CLI
		// grows, map typed errors to exit codes here (see STYLEGUIDE.md § Error Handling).
		fmt.Fprintln(os.Stderr, "cc-sync:", err)
		os.Exit(1)
	}
}
