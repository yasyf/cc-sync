# ![cc-sync](docs/assets/readme-banner.webp)

**Recover Claude Code sessions, their uncommitted Git work, and Orca workspace layouts on another synckit peer.** Recovery is planned; only `hello` and `--version` ship today.

[![Release](https://img.shields.io/github/v/release/yasyf/cc-sync?sort=semver)](https://github.com/yasyf/cc-sync/releases)
[![CI](https://img.shields.io/github/actions/workflow/status/yasyf/cc-sync/ci.yml?branch=main&label=ci)](https://github.com/yasyf/cc-sync/actions/workflows/ci.yml)
[![License: PolyForm-Noncommercial-1.0.0](https://img.shields.io/badge/License-PolyForm--Noncommercial--1.0.0-blue.svg)](https://github.com/yasyf/cc-sync/blob/main/LICENSE)

## Get started

```bash
brew install yasyf/tap/cc-sync
cc-sync hello
```

<details>
<summary>Without Homebrew</summary>

```bash
go install github.com/yasyf/cc-sync/cmd/cc-sync@latest
```

</details>

<img src="docs/assets/demo.png" alt="Terminal running cc-sync hello and printing Hello from cc-sync!" width="700">

Driving with an agent? Paste this:

```text
Install cc-sync with `brew install yasyf/tap/cc-sync`, then confirm that
`cc-sync --version` and `cc-sync hello` run successfully.
```

---

## Use cases

### Pick up after your laptop dies

Planned recovery starts with `cc-sync pickup` on a surviving synckit peer and
restores a checkpoint into a fresh recovery branch and worktree without
overwriting destination work. The checkpoint carries the full Claude Code
transcript in JSON Lines format and session sidecar tree, including subagents,
tool results, tasks, plans, and file history. Pickup relocates the session and
resumes it natively under the same session ID.

### Resume your Orca workspace elsewhere

Planned Orca recovery imports tab groups, panes, editor paths, browser URLs, and
agent-session bindings from your checkpoint on another peer. Orca opens the
workspace with all sessions dormant; only the session you select resumes.

### Keep uncommitted work recoverable without committing

Planned checkpoints preserve unpushed commits, staged and unstaged content as
distinct states, deletions, file modes, symlinks, and untracked source files that
Git doesn't ignore. You can recover unfinished work on another peer without first
committing it; ignored build output and dependencies stay out of checkpoints.

## How it works

The planned resident cc-sync consumer uses the reposync registry to find
repositories and checkpoints them across the synckit mesh. Pickup stays explicit
on the destination peer. Bulk transfer pauses on cellular, expensive, constrained,
or unknown networks. A checkpoint becomes recoverable only after every referenced
artifact is durable on a peer. Retention keeps the latest checkpoint, hourly
checkpoints for 24 hours, and daily checkpoints for seven days.

Status: Pre-release skeleton; only `hello` and `--version` ship. Recovery commands are planned for upcoming releases.

Licensed under [PolyForm-Noncommercial-1.0.0](LICENSE).
