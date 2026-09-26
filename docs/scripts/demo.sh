#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
task build
PATH="$PWD/bin:$PATH" freeze --execute "cc-sync hello" --window --output docs/assets/demo.png
