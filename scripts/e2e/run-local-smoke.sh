#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

bash "$ROOT_DIR/scripts/e2e/reset-local-env.sh"
bash "$ROOT_DIR/scripts/e2e/run-local-playwright.sh" "$@"
