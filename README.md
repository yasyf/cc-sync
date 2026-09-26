# ![cc-sync](docs/assets/readme-banner.webp)

**Recover Claude Code sessions, their uncommitted Git work, and Orca workspace layouts on another synckit peer.** Only the `hello` greeting works today; recovery is not implemented.

[![Release](https://img.shields.io/github/v/release/yasyf/cc-sync?sort=semver)](https://github.com/yasyf/cc-sync/releases)
[![CI](https://img.shields.io/github/actions/workflow/status/yasyf/cc-sync/ci.yml?branch=main&label=ci)](https://github.com/yasyf/cc-sync/actions/workflows/ci.yml)
[![License: PolyForm-Noncommercial-1.0.0](https://img.shields.io/badge/License-PolyForm--Noncommercial--1.0.0-blue.svg)](https://github.com/yasyf/cc-sync/blob/main/LICENSE)

## Get started

Requires Go 1.26 or newer. Put the directory from `go env GOBIN` first on your `PATH`;
when that setting is empty, use `$(go env GOPATH)/bin`. Install and print a greeting:

```bash
go install github.com/yasyf/cc-sync/cmd/cc-sync@latest
cc-sync hello
```

<img src="docs/assets/demo.png" alt="Terminal running cc-sync hello and printing Hello from cc-sync!" width="700">

Driving with an agent? Paste this:

```text
Install cc-sync with `go install github.com/yasyf/cc-sync/cmd/cc-sync@latest`,
then run `cc-sync hello` and confirm it prints `Hello from cc-sync!`.
Docs: https://github.com/yasyf/cc-sync#readme
```

---

Status: Command skeleton; no session, Git, or Orca recovery.

Licensed under [PolyForm-Noncommercial-1.0.0](LICENSE).
