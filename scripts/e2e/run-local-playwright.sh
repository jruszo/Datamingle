#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
FRONTEND_DIR="$ROOT_DIR/frontend"
FRONTEND_URL="${E2E_FRONTEND_URL:-http://127.0.0.1:5173}"
START_FRONTEND="${E2E_START_FRONTEND:-0}"
VITE_PID=""

log() {
  printf '[e2e-frontend] %s\n' "$1"
}

if [[ -z "${E2E_BROWSER_CHANNEL:-}" ]] && command -v google-chrome >/dev/null 2>&1; then
  export E2E_BROWSER_CHANNEL="chrome"
  log "Using system Google Chrome for Playwright"
fi

cleanup() {
  if [[ -n "$VITE_PID" ]]; then
    kill "$VITE_PID" 2>/dev/null || true
    wait "$VITE_PID" 2>/dev/null || true
  fi
}

wait_for_frontend() {
  for _ in $(seq 1 60); do
    if curl -sf "$FRONTEND_URL" >/dev/null; then
      return 0
    fi
    sleep 1
  done

  return 1
}

verify_frontend() {
  cd "$FRONTEND_DIR"
  node -e "const { chromium } = require('@playwright/test'); (async () => { const launchOptions = process.env.E2E_BROWSER_CHANNEL ? { channel: process.env.E2E_BROWSER_CHANNEL } : {}; const browser = await chromium.launch(launchOptions); const page = await browser.newPage(); await page.goto('${FRONTEND_URL}/login', { waitUntil: 'networkidle' }); const email = await page.locator('[data-testid=login-email]').count(); const password = await page.locator('[data-testid=login-password]').count(); const submit = await page.locator('[data-testid=login-submit]').count(); await browser.close(); if (email === 0 || password === 0 || submit === 0) { process.exit(1); } })().catch(async (error) => { console.error(error); process.exit(1); });"
}

if [[ "$START_FRONTEND" == "1" ]]; then
  if wait_for_frontend && verify_frontend; then
    log "Reusing existing local Vite frontend on ${FRONTEND_URL}"
  else
    trap cleanup EXIT
    log "Starting local Vite frontend on ${FRONTEND_URL}"
    (
      cd "$FRONTEND_DIR"
      npm run dev -- --host 127.0.0.1 --port 5173 --strictPort
    ) &
    VITE_PID="$!"
  fi
else
  log "Using existing local Vite frontend on ${FRONTEND_URL}"
fi

if ! wait_for_frontend; then
  log "Frontend did not become reachable on ${FRONTEND_URL}"
  exit 1
fi

if ! verify_frontend; then
  log "Frontend is reachable but did not render the expected local login form. Restart the local Vite server and try again."
  exit 1
fi

cd "$FRONTEND_DIR"
export PLAYWRIGHT_HTML_REPORT="${PLAYWRIGHT_HTML_REPORT:-$FRONTEND_DIR/e2e-playwright-report}"
npx playwright test "$@"
