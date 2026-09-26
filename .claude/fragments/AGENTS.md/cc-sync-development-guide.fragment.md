# cc-sync Development Guide

Recover Claude Code sessions, their uncommitted Git work, and Orca workspace layouts on another synckit peer. Distributed via Homebrew: `brew install yasyf/tap/cc-sync`.

## Repository Structure

```
cc-sync/
├── cmd/cc-sync/           # main package — the CLI entry point
├── internal/
│   ├── cli/               # cobra command tree (root + the `hello` starter)
│   ├── version/           # build version, stamped via -ldflags
│   └── log/               # slog setup
├── .github/               # GitHub Actions workflows
├── AGENTS.md              # This file — shared conventions
└── README.md              # Project overview
```
