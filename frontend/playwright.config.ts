import { defineConfig } from '@playwright/test'

const baseURL = process.env.E2E_FRONTEND_URL || 'http://127.0.0.1:5173'
const channel = process.env.E2E_BROWSER_CHANNEL || undefined

export default defineConfig({
  testDir: './tests/e2e',
  fullyParallel: false,
  workers: 1,
  timeout: 120_000,
  expect: {
    timeout: 15_000,
  },
  reporter: [['list'], ['html', { open: 'never' }]],
  use: {
    baseURL,
    channel,
    trace: 'retain-on-failure',
    screenshot: 'on',
    video: 'retain-on-failure',
  },
})
