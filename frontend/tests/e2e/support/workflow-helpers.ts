import { execFileSync } from 'node:child_process'
import { Buffer } from 'node:buffer'
import { mkdirSync } from 'node:fs'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

import { expect, type Browser, type BrowserContext, type Page, type TestInfo } from '@playwright/test'

const POLL_INTERVAL_MS = 2_000
const DEFAULT_TIMEOUT_MS = 120_000
const DOCKER_EXEC_TIMEOUT_MS = DEFAULT_TIMEOUT_MS
const REPO_ROOT = fileURLToPath(new URL('../../../../', import.meta.url))
const E2E_SCREENSHOT_ROOT = join(REPO_ROOT, 'frontend', 'e2e-screenshots')
const E2E_PASSWORD = 'SecurePass123!'
const DEMO_MYSQL_CONTAINER = 'datamingle-mysql-demo'
const DEMO_MYSQL_USER = 'demo_datamingle'
const DEMO_MYSQL_PASSWORD = 'demo123'

export type DemoUser = 'demo_admin' | 'demo_requester' | 'demo_pm' | 'demo_dba'

export async function createRoleSession(browser: Browser, username: DemoUser) {
  const context = await browser.newContext({ acceptDownloads: true })
  const page = await context.newPage()

  await loginAs(page, username)

  return { context, page }
}

export async function createWorkflowFormSession(browser: Browser, username: DemoUser) {
  const context = await browser.newContext({
    acceptDownloads: true,
    serviceWorkers: 'block',
  })
  const page = await context.newPage()

  await mockDemoWorkflowAgentSetup(page)
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
      timeout: DOCKER_EXEC_TIMEOUT_MS,
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
      timeout: DOCKER_EXEC_TIMEOUT_MS,
    },
  )
}

export async function mockDemoWorkflowAgentSetup(page: Page) {
  await page.context().route('**/*', async (route) => {
    const url = new URL(route.request().url())
    const pathname = url.pathname.replace(/^\/api/, '')

    if (pathname === '/v1/instance/resource/' && url.searchParams.get('resource_type') === 'database') {
      await route.fulfill({
        contentType: 'application/json',
        json: {
          detail: 'ok',
          data: {
            count: 2,
            result: ['demo_billing', 'demo_orders'],
          },
        },
        status: 200,
      })
      return
    }

    if (pathname === '/v1/workflow/sqlcheck/' && route.request().method() === 'POST') {
      const payload = route.request().postDataJSON() as { full_sql?: string }
      const sql = payload.full_sql ?? ''
      const syntaxType = /^\s*(alter|create|drop|rename|truncate)\b/i.test(sql) ? 1 : 2

      await route.fulfill({
        contentType: 'application/json',
        json: {
          detail: 'ok',
          data: {
            is_execute: false,
            checked: 'True',
            warning: null,
            error: null,
            warning_count: 0,
            error_count: 0,
            is_critical: false,
            syntax_type: syntaxType,
            rows: [
              {
                id: 1,
                errlevel: 0,
                stagestatus: 'Audit completed',
                errormessage: 'None',
                sql,
              },
            ],
            column_list: ['id', 'errlevel', 'stagestatus', 'errormessage', 'sql'],
            status: 'Audit completed',
            affected_rows: 0,
          },
        },
        status: 200,
      })
      return
    }

    await route.fallback()
  })
}

export function completePendingWorkflowChecks(
  sql: string,
  syntaxType: 1 | 2,
  submittedBy: DemoUser = 'demo_requester',
) {
  const script = `
import json
import time

from django.utils import timezone

from api_agents.models import AgentCommand, AgentCommandStatus, AgentCommandType

sql = ${JSON.stringify(sql)}
syntax_type = ${JSON.stringify(syntaxType)}
submitted_by = ${JSON.stringify(submittedBy)}
active_statuses = [
    AgentCommandStatus.QUEUED,
    AgentCommandStatus.DISPATCHED,
    AgentCommandStatus.ACCEPTED,
    AgentCommandStatus.RUNNING,
]
row = {
    "id": 1,
    "errlevel": 0,
    "stagestatus": "Audit completed",
    "errormessage": "None",
    "sql": sql,
}
result = {
    "full_sql": sql,
    "checked": True,
    "warning": None,
    "error": None,
    "warning_count": 0,
    "error_count": 0,
    "is_critical": False,
    "syntax_type": syntax_type,
    "rows": [row],
    "review_rows": [row],
    "column_list": ["id", "errlevel", "stagestatus", "errormessage", "sql"],
    "status": "Audit completed",
    "affected_rows": 0,
}
deadline = time.time() + 20
quiet_deadline = None
completed_ids = []

while time.time() < deadline:
    commands = list(
        AgentCommand.objects.filter(
            command_type=AgentCommandType.WORKFLOW_CHECK,
            payload__submitted_by=submitted_by,
            payload__sql=sql,
            status__in=active_statuses,
        ).order_by("create_time")
    )
    if commands:
        for command in commands:
            command.status = AgentCommandStatus.SUCCEEDED
            command.finished_at = timezone.now()
            command.result = result
            command.error = {}
            command.save(update_fields=["status", "finished_at", "result", "error", "update_time"])
            command.append_event("command.finished", "Completed by local E2E workflow check.")
            completed_ids.append(command.id)
            quiet_deadline = time.time() + 2
    elif completed_ids and quiet_deadline and time.time() >= quiet_deadline:
        break
    else:
        succeeded_ids = list(
            AgentCommand.objects.filter(
                command_type=AgentCommandType.WORKFLOW_CHECK,
                payload__submitted_by=submitted_by,
                payload__sql=sql,
                status=AgentCommandStatus.SUCCEEDED,
            ).values_list("id", flat=True)
        )
        if succeeded_ids:
            completed_ids.extend(succeeded_ids)
            break
    time.sleep(0.2)

if not completed_ids:
    raise RuntimeError("Timed out waiting for local E2E workflow check command.")

print(json.dumps({"command_ids": completed_ids, "status": "succeeded"}))
`.trim()

  execFileSync(
    'docker',
    ['exec', '-w', '/opt/datamingle/backend', 'datamingle-app', 'python', 'manage.py', 'shell', '-c', script],
    {
      cwd: REPO_ROOT,
      stdio: 'pipe',
      timeout: DOCKER_EXEC_TIMEOUT_MS,
    },
  )
}

export function setDemoOrderStatus(customerEmail: string, status: string) {
  runDemoMysql('demo_orders', [
    'UPDATE orders',
    `SET order_status = ${quoteMysqlString(status)}`,
    `WHERE customer_email = ${quoteMysqlString(customerEmail)}`,
  ].join(' '))
}

export async function expectDemoOrderStatus(
  customerEmail: string,
  expectedStatus: string,
  timeoutMs = DEFAULT_TIMEOUT_MS,
) {
  await expect
    .poll(
      () => readDemoOrderStatus(customerEmail),
      {
        timeout: timeoutMs,
        message: `Expected demo order for ${customerEmail} to have status ${expectedStatus}`,
      },
    )
    .toBe(expectedStatus)
}

export function dropDemoCustomerColumnIfExists(columnName: string) {
  if (!demoCustomerColumnExists(columnName)) {
    return
  }

  runDemoMysql('demo_orders', `ALTER TABLE customers DROP COLUMN ${quoteMysqlIdentifier(columnName)}`)
}

export async function expectDemoCustomerColumnExists(
  columnName: string,
  expectedExists = true,
  timeoutMs = DEFAULT_TIMEOUT_MS,
) {
  await expect
    .poll(
      () => demoCustomerColumnExists(columnName),
      {
        timeout: timeoutMs,
        message: `Expected demo customers.${columnName} existence to be ${expectedExists}`,
      },
    )
    .toBe(expectedExists)
}

function readDemoOrderStatus(customerEmail: string) {
  return runDemoMysqlScalar(
    'demo_orders',
    [
      'SELECT order_status',
      'FROM orders',
      `WHERE customer_email = ${quoteMysqlString(customerEmail)}`,
      'LIMIT 1',
    ].join(' '),
  )
}

function demoCustomerColumnExists(columnName: string) {
  const count = runDemoMysqlScalar(
    'information_schema',
    [
      'SELECT COUNT(*)',
      'FROM columns',
      "WHERE table_schema = 'demo_orders'",
      "AND table_name = 'customers'",
      `AND column_name = ${quoteMysqlString(columnName)}`,
    ].join(' '),
  )

  return Number(count) > 0
}

function runDemoMysqlScalar(dbName: string, sql: string) {
  return runDemoMysql(dbName, sql)
    .split(/\r?\n/)
    .map((line) => line.trim())
    .find((line) => line.length > 0) ?? ''
}

function runDemoMysql(dbName: string, sql: string) {
  return execFileSync(
    'docker',
    [
      'exec',
      DEMO_MYSQL_CONTAINER,
      'mysql',
      `-u${DEMO_MYSQL_USER}`,
      `-p${DEMO_MYSQL_PASSWORD}`,
      '--batch',
      '--raw',
      '--skip-column-names',
      dbName,
      '-e',
      sql,
    ],
    {
      cwd: REPO_ROOT,
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
      timeout: DOCKER_EXEC_TIMEOUT_MS,
    },
  ).trim()
}

function quoteMysqlString(value: string) {
  return `CONVERT(X'${Buffer.from(value, 'utf8').toString('hex')}' USING utf8mb4)`
}

function quoteMysqlIdentifier(value: string) {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(value)) {
    throw new Error(`Unsafe MySQL identifier: ${value}`)
  }

  return `\`${value}\``
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

export function completeWorkflowExecutionDirectly(workflowId: number) {
  const script = `
import json
import time
from types import SimpleNamespace

from django.utils import timezone
from sqlparse import split as split_sql

from api_agents.models import AgentCommand, AgentCommandStatus
from sql.engines import get_engine
from sql.engines.models import ReviewResult, ReviewSet
from sql.models import SqlWorkflow
from sql.offlinedownload import OffLineDownLoad
from sql.utils.execute_sql import execute_callback

workflow_id = ${JSON.stringify(workflowId)}
workflow = SqlWorkflow.objects.select_related("instance", "sqlworkflowcontent").get(id=workflow_id)
command = AgentCommand.objects.filter(
    workflow_type="sql_workflow",
    workflow_id=str(workflow_id),
).order_by("-create_time").first()
executor = ((command.payload or {}).get("executor") if command else "direct") or "direct"

if workflow.status == "workflow_finish":
    print(json.dumps({
        "workflow_id": workflow_id,
        "status": workflow.status,
        "executor": executor,
        "error": "",
    }))
    raise SystemExit(0)

if workflow.status != "workflow_executing":
    raise RuntimeError(f"Workflow {workflow_id} is not executing; current status is {workflow.status}.")

if command:
    command.status = AgentCommandStatus.RUNNING
    command.save(update_fields=["status", "update_time"])

engine = get_engine(instance=workflow.instance)

if workflow.is_offline_export:
    result = OffLineDownLoad().execute_offline_download(workflow)
elif workflow.syntax_type == 2 and workflow.instance.db_type == "mysql":
    sql = workflow.sqlworkflowcontent.sql_content
    result = ReviewSet(full_sql=sql)
    statements = [statement.strip() for statement in split_sql(sql) if statement.strip()]
    conn = None
    cursor = None
    try:
        conn = engine.get_connection(db_name=workflow.db_name)
        cursor = conn.cursor()
        try:
            conn.autocommit(False)
        except Exception:
            pass

        for index, statement in enumerate(statements, start=1):
            start = time.monotonic()
            cursor.execute(statement)
            rowcount = getattr(cursor, "rowcount", 0)
            result.rows.append(
                ReviewResult(
                    id=index,
                    errlevel=0,
                    stagestatus="Execute Successfully",
                    errormessage="None",
                    sql=statement,
                    affected_rows=rowcount if isinstance(rowcount, int) and rowcount > 0 else 0,
                    execute_time=f"{time.monotonic() - start:.3f}",
                    executor=executor,
                )
            )
        conn.commit()
    except Exception as exc:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        result.error = str(exc)
        result.rows.append(
            ReviewResult(
                id=len(result.rows) + 1,
                errlevel=2,
                stagestatus="Execute Failed",
                errormessage=str(exc),
                sql=sql,
                executor=executor,
            )
        )
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
else:
    result = engine.execute_workflow(
        workflow,
        execution_options={"executor": executor},
    )
execution_error = str(getattr(result, "error", "") or "")
task = SimpleNamespace(
    result=result if not execution_error else execution_error,
    success=not bool(execution_error),
    args=[workflow_id],
    stopped=timezone.now(),
)
execute_callback(task)

workflow.refresh_from_db()
if command:
    command.status = (
        AgentCommandStatus.SUCCEEDED
        if workflow.status == "workflow_finish"
        else AgentCommandStatus.FAILED
    )
    command.finished_at = timezone.now()
    command.result = {
        "message": "Completed by local E2E direct executor.",
        "workflow_status": workflow.status,
    }
    command.error = {} if command.status == AgentCommandStatus.SUCCEEDED else {
        "message": getattr(result, "error", "") or getattr(result, "warning", "") or workflow.status,
    }
    command.save(update_fields=["status", "finished_at", "result", "error", "update_time"])

print(json.dumps({
    "workflow_id": workflow_id,
    "status": workflow.status,
    "executor": executor,
    "error": str(getattr(result, "error", "") or ""),
}))
`.trim()

  const output = execFileSync(
    'docker',
    ['exec', '-w', '/opt/datamingle/backend', 'datamingle-app', 'python', 'manage.py', 'shell', '-c', script],
    {
      cwd: REPO_ROOT,
      encoding: 'utf8',
      stdio: 'pipe',
      timeout: DOCKER_EXEC_TIMEOUT_MS,
    },
  )
  const result = parseJsonLine<{ error: string; status: string; workflow_id: number }>(output)
  if (result.status !== 'workflow_finish') {
    throw new Error(`Workflow ${result.workflow_id} direct execution finished as ${result.status}: ${result.error}`)
  }
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
      timeout: DOCKER_EXEC_TIMEOUT_MS,
    },
  )
}

export function seedDdlExecutorArtifacts() {
  const script = `
from api_agents.models import AgentToolArtifact

for tool_name, url in (
    (AgentToolArtifact.TOOL_GHOST, "https://example.com/e2e/gh-ost"),
    (AgentToolArtifact.TOOL_PT_OSC, "https://example.com/e2e/pt-online-schema-change"),
):
    AgentToolArtifact.objects.update_or_create(
        tool_name=tool_name,
        version="e2e",
        platform="linux",
        architecture="amd64",
        defaults={
            "download_url": url,
            "sha256": "0" * 64,
            "size_bytes": 0,
            "enabled": True,
            "notes": "Temporary E2E DDL executor artifact.",
        },
    )
`.trim()

  execFileSync(
    'docker',
    ['exec', '-w', '/opt/datamingle/backend', 'datamingle-app', 'python', 'manage.py', 'shell', '-c', script],
    {
      cwd: REPO_ROOT,
      stdio: 'pipe',
      timeout: DOCKER_EXEC_TIMEOUT_MS,
    },
  )
}

export function removeDdlExecutorArtifacts() {
  const script = `
from api_agents.models import AgentToolArtifact

AgentToolArtifact.objects.filter(
    tool_name__in=(AgentToolArtifact.TOOL_GHOST, AgentToolArtifact.TOOL_PT_OSC),
    version="e2e",
    platform="linux",
    architecture="amd64",
).delete()
`.trim()

  execFileSync(
    'docker',
    ['exec', '-w', '/opt/datamingle/backend', 'datamingle-app', 'python', 'manage.py', 'shell', '-c', script],
    {
      cwd: REPO_ROOT,
      stdio: 'pipe',
      timeout: DOCKER_EXEC_TIMEOUT_MS,
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
