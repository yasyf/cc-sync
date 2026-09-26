// Package cli builds the cobra command tree.
package cli

import (
	"github.com/spf13/cobra"

	"github.com/yasyf/cc-sync/internal/version"
)

// NewRootCmd builds the root command and registers its subcommands.
func NewRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "cc-sync",
		Short:         "Recover Claude Code sessions, their uncommitted Git work, and Orca workspace layouts on another synckit peer.",
		Version:       version.String(),
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.SetVersionTemplate("{{.Version}}\n")
	root.AddCommand(newHelloCmd())
	return root
}
