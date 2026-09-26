// Command cc-sync: Recover Claude Code sessions, their uncommitted Git work, and Orca workspace layouts on another synckit peer.
package main

import (
	"os"

	"github.com/yasyf/cc-sync/internal/cli"
	applog "github.com/yasyf/cc-sync/internal/log"
)

func main() {
	applog.Setup()
	os.Exit(cli.Execute(cli.UnavailableService{}, os.Args[1:], os.Stdout, os.Stderr))
}
