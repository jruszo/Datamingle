import { execFileSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'

import { expect, type Browser, type BrowserContext, type Page } from '@playwright/test'

const DEMO_PASSWORD = 'demo123'
const POLL_INTERVAL_MS = 2_000
const DEFAULT_TIMEOUT_MS = 120_000
const REPO_ROOT = fileURLToPath(new URL('../../../../', import.meta.url))

export type DemoUser = 'demo_requester' | 'demo_pm' | 'demo_dba'

export async function createRoleSession(browser: Browser, username: DemoUser) {
  const context = await browser.newContext({ acceptDownloads: true })
  const page = await context.newPage()

  await loginAs(page, username)

  return { context, page }
}

export async function loginAs(page: Page, username: DemoUser) {
  await page.goto('/login')

  await page.getByTestId('login-username').fill(username)
  await page.getByTestId('login-password').fill(DEMO_PASSWORD)
  await page.getByTestId('login-submit').click()

  await page.waitForURL((url) => !url.pathname.endsWith('/login'))
  await expect(page.getByRole('link', { name: 'Workflows' })).toBeVisible()
}

export function uniqueWorkflowName(prefix: string) {
  const suffix = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
  return `${prefix} ${suffix}`
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

export async function openWorkflowDetail(page: Page, workflowId: number) {
  await page.goto(`/workflows/${workflowId}`)
  await expect(page.getByTestId('workflow-detail-refresh')).toBeVisible()
}

export async function expectWorkflowBackupFlag(page: Page, workflowId: number, expected: boolean) {
  const detail = await page.evaluate(async (id) => {
    const token = window.localStorage.getItem('archery.access_token') || ''
    if (!token) {
      throw new Error('Missing access token for workflow detail fetch.')
    }

    const response = await fetch(`/api/v1/workflow/${id}/`, {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    })

    if (!response.ok) {
      throw new Error(`Failed to fetch workflow detail: ${response.status}`)
    }

    const payload = await response.json()
    return payload.data ?? payload
  }, workflowId)

  expect(detail.is_backup).toBe(expected)
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

export function setBackupSwitchEnabled(enabled: boolean) {
  execFileSync(
    'docker',
    [
      'exec',
      'datamingle-app',
      'python',
      'manage.py',
      'shell',
      '-c',
      `from common.config import SysConfig; SysConfig().set("enable_backup_switch", ${enabled ? 'True' : 'False'})`,
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
