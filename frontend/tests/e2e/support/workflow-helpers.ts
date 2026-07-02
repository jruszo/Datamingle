import { execFileSync } from 'node:child_process'
import { mkdirSync } from 'node:fs'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

import { expect, type Browser, type BrowserContext, type Page, type TestInfo } from '@playwright/test'

const POLL_INTERVAL_MS = 2_000
const DEFAULT_TIMEOUT_MS = 120_000
const REPO_ROOT = fileURLToPath(new URL('../../../../', import.meta.url))
const E2E_SCREENSHOT_ROOT = join(REPO_ROOT, 'frontend', 'e2e-screenshots')
const E2E_PASSWORD = 'SecurePass123!'

export type DemoUser = 'demo_admin' | 'demo_requester' | 'demo_pm' | 'demo_dba'

export async function createRoleSession(browser: Browser, username: DemoUser) {
  const context = await browser.newContext({ acceptDownloads: true })
  const page = await context.newPage()

  await loginAs(page, username)

  return { context, page }
}

export async function createLocalUserSession(
  browser: Browser,
  email: string,
  password = E2E_PASSWORD,
) {
  const context = await browser.newContext({ acceptDownloads: true })
  const page = await context.newPage()

  try {
    await loginWithLocalUser(page, email, password)
  } catch (error) {
    await context.close()
    throw error
  }

  return { context, page }
}

export async function loginWithLocalUser(page: Page, email: string, password = E2E_PASSWORD) {
  await page.goto('/login')
  await page.getByTestId('login-email').fill(email)
  await page.getByTestId('login-password').fill(password)
  await page.getByTestId('login-submit').click()
  await expect(page).toHaveURL(/\/$/)
  await expect(page.getByTitle('Logout')).toBeVisible()
}

export async function loginAs(page: Page, username: DemoUser) {
  const tokens = issueDemoTokens(username)

  await page.goto('/login')
  await page.evaluate((issuedTokens) => {
    localStorage.setItem('datamingle.access_token', issuedTokens.access)
    localStorage.setItem('datamingle.refresh_token', issuedTokens.refresh)
  }, tokens)
  await page.goto('/')
  await expect(page.getByRole('link', { name: 'Workflows' })).toBeVisible()
}

function issueDemoTokens(username: DemoUser) {
  const script = [
    'import json',
    'from django.conf import settings',
    'from django.contrib.auth import BACKEND_SESSION_KEY, HASH_SESSION_KEY, SESSION_KEY',
    'from importlib import import_module',
    'from allauth.headless.tokens.strategies.jwt import internal',
    'from sql.models import Users',
    `user = Users.objects.get(username=${JSON.stringify(username)})`,
    'SessionStore = import_module(settings.SESSION_ENGINE).SessionStore',
    'session = SessionStore()',
    'session[SESSION_KEY] = str(user.pk)',
    'session[BACKEND_SESSION_KEY] = "common.auth_backends.TeamPermissionBackend"',
    'session[HASH_SESSION_KEY] = user.get_session_auth_hash()',
    'session.save()',
    'access = internal.create_access_token(user, session, {})',
    'refresh = internal.create_refresh_token(user, session)',
    'session.save()',
    'print(json.dumps({"access": access, "refresh": refresh}))',
  ].join('; ')
  const output = execFileSync(
    'docker',
    ['exec', '-w', '/opt/datamingle/backend', 'datamingle-app', 'python', 'manage.py', 'shell', '-c', script],
    {
      cwd: REPO_ROOT,
      encoding: 'utf8',
      stdio: 'pipe',
    },
  )

  return parseJsonLine<{ access: string; refresh: string }>(output)
}

function parseJsonLine<T>(output: string): T {
  const jsonLine = output
    .trim()
    .split(/\r?\n/)
    .reverse()
    .find((line) => line.trim().startsWith('{'))

  if (!jsonLine) {
    throw new Error(`Expected JSON object in command output:\n${output}`)
  }

  return JSON.parse(jsonLine) as T
}

export function uniqueWorkflowName(prefix: string) {
  const suffix = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
  return `${prefix} ${suffix}`
}

export function seedLocalDemo() {
  seedE2EEnvironment()
}

export function seedE2EEnvironment() {
  execFileSync(
    'docker',
    ['exec', '-w', '/opt/datamingle/backend', 'datamingle-app', 'python', 'manage.py', 'seed_e2e_environment'],
    {
      cwd: REPO_ROOT,
      stdio: 'pipe',
    },
  )
}

export async function captureE2EScreenshot(page: Page, testInfo: TestInfo, name: string) {
  const safeTitle = sanitizePathSegment(testInfo.title)
  const safeName = sanitizePathSegment(name)
  const screenshotDir = join(E2E_SCREENSHOT_ROOT, safeTitle)
  const screenshotPath = join(screenshotDir, `${safeName}.png`)

  mkdirSync(screenshotDir, { recursive: true })
  await page.screenshot({ path: screenshotPath, fullPage: true })
  await testInfo.attach(name, { path: screenshotPath, contentType: 'image/png' })
  console.log(`[e2e-screenshot] ${screenshotPath}`)

  return screenshotPath
}

function sanitizePathSegment(value: string) {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/(^-|-$)/g, '')
    || 'screenshot'
}

export async function fillSqlEditor(page: Page, testId: string, sql: string) {
  const editor = page.locator(`[data-testid="${testId}"] .cm-content`).first()
  await editor.click()
  await page.keyboard.press('ControlOrMeta+A')
  await page.keyboard.press('Backspace')
  await page.keyboard.insertText(sql)
}

export async function expectSqlEditorToContain(page: Page, testId: string, expected: string) {
  await expect(page.locator(`[data-testid="${testId}"] .cm-content`).first()).toContainText(expected)
}

export async function clickAndAcceptDialogIfPresent(
  page: Page,
  action: () => Promise<void>,
  timeoutMs = 1_500,
) {
  const dialogPromise = page
    .waitForEvent('dialog', { timeout: timeoutMs })
    .then(async (dialog) => {
      await dialog.accept()
      return true
    })
    .catch(() => false)

  await action()
  await dialogPromise
}

export function workflowIdFromUrl(page: Page) {
  const match = page.url().match(/\/workflows\/(\d+)/)
  if (!match) {
    throw new Error(`Could not determine workflow id from URL: ${page.url()}`)
  }

  return Number(match[1])
}

export function archiveIdFromUrl(page: Page) {
  const match = page.url().match(/\/archives\/(\d+)/)
  if (!match) {
    throw new Error(`Could not determine archive id from URL: ${page.url()}`)
  }

  return Number(match[1])
}

export async function openWorkflowDetail(page: Page, workflowId: number) {
  await page.goto(`/workflows/${workflowId}`)
  await expect(page.getByTestId('workflow-detail-refresh')).toBeVisible()
}

export async function openArchiveDetail(page: Page, archiveId: number) {
  await page.goto(`/archives/${archiveId}`)
  await expect(page.getByTestId('archive-detail-refresh')).toBeVisible()
}

export async function waitForWorkflowAction(page: Page, testId: string, timeoutMs = DEFAULT_TIMEOUT_MS) {
  await pollWorkflowDetail(
    page,
    async () => {
      const action = page.getByTestId(testId)
      return (await action.count()) > 0 && await action.isVisible()
    },
    timeoutMs,
    `action "${testId}"`,
  )
}

export async function waitForArchiveAction(page: Page, testId: string, timeoutMs = DEFAULT_TIMEOUT_MS) {
  await pollArchiveDetail(
    page,
    async () => {
      const action = page.getByTestId(testId)
      return (await action.count()) > 0 && await action.isVisible()
    },
    timeoutMs,
    `action "${testId}"`,
  )
}

export async function waitForWorkflowStatus(page: Page, expectedStatus: string, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const expected = expectedStatus.toLowerCase()
  await pollWorkflowDetail(
    page,
    async () => (await readWorkflowStatus(page)).toLowerCase().includes(expected),
    timeoutMs,
    `status "${expectedStatus}"`,
  )
}

export async function readWorkflowStatus(page: Page) {
  const text = await page.getByTestId('workflow-detail-status').textContent()
  return text?.trim() ?? ''
}

export async function readArchiveStatus(page: Page) {
  const text = await page.getByTestId('archive-detail-status').textContent()
  return text?.trim() ?? ''
}

export async function waitForArchiveExecutionState(
  page: Page,
  expectedText: string,
  timeoutMs = DEFAULT_TIMEOUT_MS,
) {
  const expected = expectedText.toLowerCase()
  await pollArchiveDetail(
    page,
    async () => {
      const text = await page.getByTestId('archive-execution-state').textContent()
      return text?.toLowerCase().includes(expected) ?? false
    },
    timeoutMs,
    `archive state "${expectedText}"`,
  )
}

export async function waitForArchiveLogRows(page: Page, timeoutMs = DEFAULT_TIMEOUT_MS) {
  await pollArchiveDetail(
    page,
    async () => await page.locator('[data-testid^="archive-log-item-"]').count() > 0,
    timeoutMs,
    'archive log rows',
  )
}

export async function assertExecutionRowsPresent(page: Page) {
  await expect(page.getByText('No execution rows recorded yet.')).toHaveCount(0)
}

export async function assertReviewRowsPresent(page: Page) {
  await expect(page.getByText('No review rows recorded.')).toHaveCount(0)
}

export async function closeRoleSessions(...contexts: Array<BrowserContext | undefined>) {
  await Promise.all(
    contexts.filter(Boolean).map(async (context) => {
      try {
        await context!.close()
      } catch {
        // Ignore already-closed browser contexts during timeout cleanup.
      }
    }),
  )
}

export function setSystemConfigValues(values: Record<string, boolean | number | string>) {
  const assignments = Object.entries(values)
    .map(([key, value]) => {
      if (typeof value === 'boolean') {
        return `cfg.set(${JSON.stringify(key)}, ${value ? 'True' : 'False'})`
      }
      if (typeof value === 'number') {
        return `cfg.set(${JSON.stringify(key)}, ${value})`
      }
      return `cfg.set(${JSON.stringify(key)}, ${JSON.stringify(value)})`
    })
    .join('; ')

  execFileSync(
    'docker',
    [
      'exec',
      '-w',
      '/opt/datamingle/backend',
      'datamingle-app',
      'python',
      'manage.py',
      'shell',
      '-c',
      `from common.config import SysConfig; cfg = SysConfig(); ${assignments}`,
    ],
    {
      cwd: REPO_ROOT,
      stdio: 'pipe',
    },
  )
}

async function pollWorkflowDetail(
  page: Page,
  condition: () => Promise<boolean>,
  timeoutMs: number,
  description: string,
) {
  const deadline = Date.now() + timeoutMs
  let lastStatus = ''

  while (Date.now() < deadline) {
    if (await condition()) {
      return
    }

    lastStatus = await readWorkflowStatus(page)
    await page.getByTestId('workflow-detail-refresh').click()
    await page.waitForTimeout(POLL_INTERVAL_MS)
  }

  throw new Error(`Timed out waiting for ${description}. Last visible status: ${lastStatus || 'unknown'}`)
}

async function pollArchiveDetail(
  page: Page,
  condition: () => Promise<boolean>,
  timeoutMs: number,
  description: string,
) {
  const deadline = Date.now() + timeoutMs
  let lastStatus = ''

  while (Date.now() < deadline) {
    if (await condition()) {
      return
    }

    lastStatus = await readArchiveStatus(page)
    await page.getByTestId('archive-detail-refresh').click()
    await page.waitForTimeout(POLL_INTERVAL_MS)
  }

  throw new Error(`Timed out waiting for ${description}. Last visible status: ${lastStatus || 'unknown'}`)
}
