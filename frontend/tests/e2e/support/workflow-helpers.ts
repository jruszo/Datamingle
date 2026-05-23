import { execFileSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'

import { expect, type Browser, type BrowserContext, type Page } from '@playwright/test'

const POLL_INTERVAL_MS = 2_000
const DEFAULT_TIMEOUT_MS = 120_000
const REPO_ROOT = fileURLToPath(new URL('../../../../', import.meta.url))

export type DemoUser = 'demo_admin' | 'demo_requester' | 'demo_pm' | 'demo_dba'

export async function createRoleSession(browser: Browser, username: DemoUser) {
  const context = await browser.newContext({ acceptDownloads: true })
  const page = await context.newPage()

  await loginAs(page, username)

  return { context, page }
}

export async function loginAs(page: Page, username: DemoUser) {
  const tokens = issueDemoTokens(username)

  await page.goto('/login')
  await page.evaluate((issuedTokens) => {
    localStorage.setItem('archery.access_token', issuedTokens.access)
    localStorage.setItem('archery.refresh_token', issuedTokens.refresh)
  }, tokens)
  await page.goto('/')
  await expect(page.getByRole('link', { name: 'Dashboard' })).toBeVisible()
}

function issueDemoTokens(username: DemoUser) {
  const script = [
    'import json',
    'from rest_framework_simplejwt.tokens import RefreshToken',
    'from sql.models import Users',
    `user = Users.objects.get(username=${JSON.stringify(username)})`,
    'refresh = RefreshToken.for_user(user)',
    'print(json.dumps({"access": str(refresh.access_token), "refresh": str(refresh)}))',
  ].join('\n')
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
  execFileSync(
    'docker',
    ['exec', '-w', '/opt/datamingle/backend', 'datamingle-app', 'python', 'manage.py', 'seed_local_demo'],
    {
      cwd: REPO_ROOT,
      stdio: 'pipe',
    },
  )
  ensureE2eDemoUsers()
}

function ensureE2eDemoUsers() {
  const script = [
    'from django.contrib.auth.models import Group',
    'from sql.models import Users',
    'specs = {',
    '  "demo_admin": {"display": "Demo Admin", "group": "superadmin", "staff": True, "superuser": True},',
    '  "demo_requester": {"display": "Demo Requester", "group": "RD", "staff": False, "superuser": False},',
    '  "demo_pm": {"display": "Demo PM", "group": "PM", "staff": False, "superuser": False},',
    '  "demo_dba": {"display": "Demo DBA", "group": "DBA", "staff": False, "superuser": False},',
    '}',
    'for username, spec in specs.items():',
    '    user, _ = Users.objects.get_or_create(username=username, defaults={"email": f"{username}@example.com"})',
    '    user.email = f"{username}@example.com"',
    '    user.display = spec["display"]',
    '    user.is_staff = spec["staff"]',
    '    user.is_superuser = spec["superuser"]',
    '    user.is_active = True',
    '    user.set_unusable_password()',
    '    user.save()',
    '    group = Group.objects.get(name=spec["group"])',
    '    user.groups.set([group])',
    'print("e2e demo users ready")',
  ].join('\n')

  execFileSync(
    'docker',
    ['exec', '-w', '/opt/datamingle/backend', 'datamingle-app', 'python', 'manage.py', 'shell', '-c', script],
    {
      cwd: REPO_ROOT,
      stdio: 'pipe',
    },
  )
}

export async function fillSqlEditor(page: Page, testId: string, sql: string) {
  const editor = page.locator(`[data-testid="${testId}"] .cm-content`).first()
  await editor.click()
  await page.keyboard.press('ControlOrMeta+A')
  await page.keyboard.press('Backspace')
  await page.keyboard.insertText(sql)
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
