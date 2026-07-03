import { expect, test, type Page, type Route } from '@playwright/test'

import {
  captureE2EScreenshot,
  closeRoleSessions,
  createRoleSession,
  seedLocalDemo,
} from './support/workflow-helpers'

type Dashboard = {
  id: number
  name: string
  description: string
  created_by: { id: number; username: string; display: string } | null
  has_icon: boolean
  is_favorite: boolean
  revision: number
  time_range_mode: 'relative' | 'absolute'
  time_range_seconds: number
  time_range_start: string
  time_range_end: string
  refresh_interval_seconds: number
  variables: unknown[]
  panels: unknown[]
  create_time: string
  update_time: string
}

const now = '2026-07-02T12:00:00Z'
const dashboardUser = { id: 1, username: 'demo_admin', display: 'Demo Admin' }

function appEnvelope(data: unknown, detail = 'ok') {
  return { detail, data }
}

function metricsEnvelope(data: unknown) {
  return { status: 'success', data }
}

function paginated(results: unknown[]) {
  return { count: results.length, next: null, previous: null, results }
}

function dashboardFixture(overrides: Partial<Dashboard> = {}): Dashboard {
  return {
    id: 301,
    name: 'Existing Observability',
    description: 'Shared e2e dashboard',
    created_by: dashboardUser,
    has_icon: false,
    is_favorite: true,
    revision: 3,
    time_range_mode: 'relative',
    time_range_seconds: 3600,
    time_range_start: '',
    time_range_end: '',
    refresh_interval_seconds: 0,
    variables: [],
    panels: [],
    create_time: now,
    update_time: now,
    ...overrides,
  }
}

async function fulfillJson(route: Route, body: unknown) {
  await route.fulfill({
    contentType: 'application/json',
    body: JSON.stringify(body),
  })
}

async function mockAgentApis(page: Page) {
  const agent = {
    id: 77,
    organization_id: 'org-e2e',
    name: 'e2e-agent-primary',
    display_name: 'E2E Agent Primary',
    status: 'online',
    hostname: 'agent-host-01',
    platform: 'linux',
    architecture: 'amd64',
    agent_version: '1.2.3',
    last_seen_at: now,
    last_connected_at: now,
    last_disconnected_at: null,
    last_config_revision: 4,
    desired_config_revision: 5,
    enabled: true,
    local_node: null,
    local_node_name: '',
    assignment_count: 1,
    create_time: now,
    update_time: now,
  }
  const instance = {
    id: 501,
    instance_name: 'demo-mysql-workflow',
    type: 'master',
    db_type: 'mysql',
    host: '127.0.0.1',
    port: 3306,
    user: 'root',
    workflow_enabled: true,
    queryable: true,
    monitoring_enabled: true,
    monitoring_labels: {},
    team_ids: [],
    create_time: now,
    update_time: now,
  }
  const command = {
    id: 900,
    instance: 501,
    instance_name: 'demo-mysql-workflow',
    workflow_type: 'DDL',
    workflow_id: '42',
    command_type: 'schema_change',
    status: 'running',
    queued_at: now,
    started_at: now,
    finished_at: null,
    cancel_requested_at: null,
    create_time: now,
  }
  let assignment = {
    id: 701,
    instance: instance.id,
    node: null,
    node_assignment: null,
    local_node: null,
    inherited: false,
    instance_name: instance.instance_name,
    db_type: instance.db_type,
    host: instance.host,
    port: instance.port,
    workflow_enabled: true,
    enabled: false,
    modules: [],
    capabilities: [],
    command_enabled: false,
    metrics_enabled: true,
    online_schema_enabled: false,
    logs_enabled: false,
    create_time: now,
    update_time: now,
  }
  let cancelled = false
  const assignmentRequests: unknown[] = []

  await page.route('**/api/v1/instance/**', async (route) => {
    await fulfillJson(route, appEnvelope(paginated([instance])))
  })

  await page.route('**/api/v1/agents/**', async (route) => {
    const request = route.request()
    const url = new URL(request.url())
    const path = url.pathname
    const method = request.method()

    if (path.endsWith('/api/v1/agents/') && method === 'POST') {
      const created = {
        ...agent,
        id: 78,
        name: 'e2e-agent-created',
        display_name: 'E2E Agent Created',
        api_key: 'e2e-secret-agent-key',
        api_key_backend: 'database',
        install_command: 'datamingle-agent install --key e2e-secret-agent-key',
        metadata: {},
        assignments: [],
        recent_commands: [],
      }
      await fulfillJson(route, appEnvelope(created, 'Agent created.'))
      return
    }

    if (path.endsWith('/api/v1/agents/') && method === 'GET') {
      await fulfillJson(route, appEnvelope(paginated([agent])))
      return
    }

    if (path.endsWith('/api/v1/agents/77/') && method === 'GET') {
      await fulfillJson(route, appEnvelope({
        ...agent,
        metadata: { mode: 'e2e' },
        assignments: [assignment],
        recent_commands: [command],
      }))
      return
    }

    if (path.endsWith('/api/v1/agents/77/assignments/') && method === 'PUT') {
      const payload = request.postDataJSON() as { assignments: Array<typeof assignment> }
      assignmentRequests.push(payload)
      const next = payload.assignments[0]
      assignment = {
        ...assignment,
        enabled: Boolean(next?.enabled),
        modules: next?.modules ?? [],
        command_enabled: Boolean(next?.command_enabled),
        metrics_enabled: Boolean(next?.metrics_enabled),
        online_schema_enabled: Boolean(next?.online_schema_enabled),
        logs_enabled: Boolean(next?.logs_enabled),
      }
      await fulfillJson(route, appEnvelope([assignment], 'Assignments saved.'))
      return
    }

    if (path.endsWith('/api/v1/agents/77/commands/') && method === 'GET') {
      await fulfillJson(route, appEnvelope(paginated([
        cancelled ? { ...command, status: 'cancelled', cancel_requested_at: now } : command,
      ])))
      return
    }

    if (path.endsWith('/api/v1/agents/77/commands/900/') && method === 'GET') {
      await fulfillJson(route, appEnvelope({
        ...(cancelled ? { ...command, status: 'cancelled', cancel_requested_at: now } : command),
        payload: { sql: 'ALTER TABLE customers ADD COLUMN e2e_agent INT' },
        result: cancelled ? { cancelled: true } : { rows: 0 },
        error: {},
        lease_owner: 'agent-host-01',
        lease_expires_at: null,
        events: [
          {
            id: 1,
            event_type: 'accepted',
            message: 'Command accepted by agent.',
            payload: {},
            create_time: now,
          },
        ],
      }))
      return
    }

    if (path.endsWith('/api/v1/agents/77/commands/900/cancel/') && method === 'POST') {
      cancelled = true
      await fulfillJson(route, appEnvelope({
        ...command,
        status: 'cancelled',
        cancel_requested_at: now,
        payload: {},
        result: { cancelled: true },
        error: {},
        lease_owner: 'agent-host-01',
        lease_expires_at: null,
        events: [],
      }, 'Command cancelled.'))
      return
    }

    await route.abort()
  })

  return { assignmentRequests }
}

async function mockMetricsAndDashboards(page: Page) {
  const dashboards = new Map<number, Dashboard>([
    [301, dashboardFixture()],
    [302, dashboardFixture({ id: 302, name: 'Capacity Planning', is_favorite: false, panels: [{}] })],
  ])
  const createdPayloads: unknown[] = []
  const deletedDashboardIds: number[] = []

  await page.route('**/api/v1/metrics/**', async (route) => {
    const request = route.request()
    const url = new URL(request.url())
    const path = url.pathname
    const method = request.method()

    if (path.endsWith('/api/v1/metrics/dashboards/') && method === 'GET') {
      const favoriteOnly = url.searchParams.get('favorite') === 'true'
      const results = [...dashboards.values()].filter((dashboard) => !favoriteOnly || dashboard.is_favorite)
      await fulfillJson(route, appEnvelope(results))
      return
    }

    if (path.endsWith('/api/v1/metrics/dashboards/') && method === 'POST') {
      const payload = request.postDataJSON() as Partial<Dashboard>
      createdPayloads.push(payload)
      const created = dashboardFixture({
        id: 399,
        name: payload.name || 'Created dashboard',
        description: payload.description || '',
        is_favorite: false,
        panels: payload.panels || [],
        variables: payload.variables || [],
        revision: 1,
      })
      dashboards.set(created.id, created)
      await fulfillJson(route, appEnvelope(created, 'Dashboard created.'))
      return
    }

    const dashboardMatch = path.match(/\/api\/v1\/metrics\/dashboards\/(\d+)\/$/)
    if (dashboardMatch && method === 'GET') {
      const dashboard = dashboards.get(Number(dashboardMatch[1]))
      await fulfillJson(route, appEnvelope(dashboard || dashboardFixture({ id: Number(dashboardMatch[1]) })))
      return
    }

    if (dashboardMatch && method === 'DELETE') {
      const id = Number(dashboardMatch[1])
      deletedDashboardIds.push(id)
      dashboards.delete(id)
      await fulfillJson(route, appEnvelope({}))
      return
    }

    if (path.endsWith('/api/v1/metrics/names')) {
      const search = url.searchParams.get('search') || ''
      const names = ['node_cpu_seconds_total', 'mysql_global_status_threads_connected']
        .filter((name) => !search || name.includes(search))
      await fulfillJson(route, metricsEnvelope(names))
      return
    }

    if (path.endsWith('/api/v1/metrics/labels')) {
      await fulfillJson(route, metricsEnvelope(['__name__', 'instance_name', 'job', 'mode', 'node_name']))
      return
    }

    if (path.includes('/api/v1/metrics/label/') && path.endsWith('/values')) {
      await fulfillJson(route, metricsEnvelope(['demo-mysql-workflow', 'agent-host-01']))
      return
    }

    if (path.endsWith('/api/v1/metrics/series')) {
      await fulfillJson(route, metricsEnvelope([
        {
          __name__: 'node_cpu_seconds_total',
          instance_name: 'demo-mysql-workflow',
          job: 'node',
          mode: 'idle',
          node_name: 'agent-host-01',
        },
        {
          __name__: 'node_cpu_seconds_total',
          instance_name: 'demo-mysql-workflow',
          job: 'node',
          mode: 'user',
          node_name: 'agent-host-01',
        },
      ]))
      return
    }

    if (path.endsWith('/api/v1/metrics/metadata')) {
      await fulfillJson(route, metricsEnvelope({
        node_cpu_seconds_total: [
          {
            type: 'counter',
            help: 'Seconds the CPUs spent in each mode.',
            unit: 'seconds',
          },
        ],
      }))
      return
    }

    if (path.endsWith('/api/v1/metrics/query')) {
      await fulfillJson(route, metricsEnvelope({
        resultType: 'vector',
        result: [
          {
            metric: { instance_name: 'demo-mysql-workflow', mode: 'idle' },
            value: [1_783_000_000, '2'],
          },
          {
            metric: { instance_name: 'demo-mysql-workflow', mode: 'user' },
            value: [1_783_000_000, '4'],
          },
        ],
      }))
      return
    }

    if (path.endsWith('/api/v1/metrics/query_range')) {
      await fulfillJson(route, metricsEnvelope({
        resultType: 'matrix',
        result: [
          {
            metric: { instance_name: 'demo-mysql-workflow', mode: 'idle' },
            values: [[1_783_000_000, '2'], [1_783_000_060, '3']],
          },
        ],
      }))
      return
    }

    if (path.endsWith('/api/v1/metrics/ai/availability')) {
      await fulfillJson(route, appEnvelope({ available: false }))
      return
    }

    await route.abort()
  })

  return { createdPayloads, deletedDashboardIds }
}

test.describe.serial('observability and admin workflows', () => {
  test.beforeEach(() => {
    seedLocalDemo()
  })

  test('manages agent provisioning, assignments, and command cancellation', async ({
    browser,
  }, testInfo) => {
    const admin = await createRoleSession(browser, 'demo_admin')
    const { assignmentRequests } = await mockAgentApis(admin.page)

    try {
      await admin.page.goto('/agents')
      await expect(admin.page.getByRole('heading', { name: 'Agents', level: 2 })).toBeVisible()
      await expect(admin.page.getByText('E2E Agent Primary')).toBeVisible()

      await admin.page.getByPlaceholder('Filter agents by name, host, version, or status').fill('primary')
      await expect(admin.page.getByText('agent-host-01')).toBeVisible()

      await admin.page.getByRole('button', { name: 'Create Agent' }).click()
      await expect(admin.page.getByRole('heading', { name: 'Create Agent' })).toBeVisible()
      await admin.page.getByPlaceholder('prod-db-agent-01').fill('e2e-agent-created')
      await admin.page.getByPlaceholder('Production DB Agent').fill('E2E Agent Created')
      await admin.page.getByRole('button', { name: /^Create$/ }).click()
      await expect(admin.page.locator('textarea[readonly]').first()).toHaveValue('e2e-secret-agent-key')
      await expect(admin.page.locator('textarea[readonly]').nth(1)).toHaveValue(
        'datamingle-agent install --key e2e-secret-agent-key',
      )
      await captureE2EScreenshot(admin.page, testInfo, 'agent-created-api-key')
      await admin.page.getByRole('button', { name: 'Done' }).click()

      await admin.page.getByRole('row', { name: /E2E Agent Primary/ }).getByRole('button', { name: 'View' }).click()
      await expect(admin.page.getByRole('heading', { name: 'E2E Agent Primary' })).toBeVisible()

      const assignmentRow = admin.page.getByRole('row', { name: /demo-mysql-workflow/ }).first()
      await assignmentRow.getByRole('checkbox').nth(0).check()
      await assignmentRow.getByRole('checkbox').nth(1).check()
      await admin.page.getByRole('button', { name: 'Save Assignments' }).click()
      await expect.poll(() => assignmentRequests.length).toBeGreaterThan(0)
      expect(JSON.stringify(assignmentRequests.at(-1))).toContain('"command_enabled":true')
      await captureE2EScreenshot(admin.page, testInfo, 'agent-assignment-saved')

      const commandRow = admin.page.getByRole('row', { name: /schema_change/ }).first()
      await commandRow.getByRole('button', { name: 'Details' }).click()
      await expect(admin.page.getByText('ALTER TABLE customers ADD COLUMN e2e_agent INT')).toBeVisible()
      await commandRow.getByRole('button', { name: 'Cancel' }).click()
      await expect(admin.page.getByRole('row', { name: /cancelled/ })).toBeVisible()
      await captureE2EScreenshot(admin.page, testInfo, 'agent-command-cancelled')
    } finally {
      await closeRoleSessions(admin.context)
    }
  })

  test('explores metrics and saves a graph to a new dashboard', async ({ browser }, testInfo) => {
    const admin = await createRoleSession(browser, 'demo_admin')
    const { createdPayloads } = await mockMetricsAndDashboards(admin.page)

    try {
      await admin.page.goto('/metrics')
      await expect(admin.page.getByRole('heading', { name: 'Metrics Explorer', level: 2 })).toBeVisible()
      await admin.page.getByPlaceholder('Search metrics').fill('cpu')
      await expect(admin.page.getByRole('button', { name: 'node_cpu_seconds_total' })).toBeVisible()
      await admin.page.getByRole('button', { name: 'node_cpu_seconds_total' }).click()

      await expect(admin.page.getByText('Seconds the CPUs spent in each mode.')).toBeVisible()
      await expect(admin.page.getByText('2 active now')).toBeVisible()
      await expect(admin.page.getByRole('cell', { name: 'mode' })).toBeVisible()
      await captureE2EScreenshot(admin.page, testInfo, 'metrics-detail')

      await admin.page.getByRole('button', { name: 'Graph metric' }).click()
      await expect(admin.page.getByText('Graph')).toBeVisible()
      await expect(admin.page.getByText('node_cpu_seconds_total', { exact: true })).toBeVisible()
      await expect(admin.page.getByText('1 series').first()).toBeVisible()

      await admin.page.getByRole('button', { name: 'Add to dashboard' }).click()
      await expect(admin.page.getByRole('heading', { name: 'Add graph to dashboard' })).toBeVisible()
      await admin.page.getByLabel('Dashboard').selectOption('new')
      await admin.page.getByLabel('New dashboard name').fill('E2E Metrics Dashboard')
      await admin.page.getByLabel('Panel title').fill('CPU Idle Rate')
      await admin.page.getByRole('button', { name: 'Add graph' }).click()

      await expect(admin.page.getByText('Graph added to dashboard.')).toBeVisible()
      await expect.poll(() => createdPayloads.length).toBe(1)
      const payload = createdPayloads[0] as { name?: string; panels?: Array<{ title?: string; queries?: Array<{ query?: string }> }> }
      expect(payload.name).toBe('E2E Metrics Dashboard')
      expect(payload.panels?.[0]?.title).toBe('CPU Idle Rate')
      expect(payload.panels?.[0]?.queries?.[0]?.query).toContain('rate(node_cpu_seconds_total')
      await captureE2EScreenshot(admin.page, testInfo, 'metrics-graph-added-to-dashboard')
    } finally {
      await closeRoleSessions(admin.context)
    }
  })

  test('creates, filters, opens, and deletes dashboards', async ({ browser }, testInfo) => {
    const admin = await createRoleSession(browser, 'demo_admin')
    const { deletedDashboardIds } = await mockMetricsAndDashboards(admin.page)

    try {
      await admin.page.goto('/dashboards')
      await expect(admin.page.getByRole('heading', { name: 'Dashboards', level: 2 })).toBeVisible()
      await expect(admin.page.getByText('Existing Observability')).toBeVisible()
      await expect(admin.page.getByText('Capacity Planning')).toBeVisible()

      await admin.page.getByRole('button', { name: 'Favorites only' }).click()
      await expect(admin.page.getByText('Existing Observability')).toBeVisible()
      await expect(admin.page.getByText('Capacity Planning')).toHaveCount(0)

      await admin.page.getByRole('button', { name: 'New dashboard' }).click()
      await expect(admin.page.getByRole('heading', { name: 'Create dashboard' })).toBeVisible()
      await admin.page.getByLabel('Name').fill('E2E Blank Dashboard')
      await admin.page.getByRole('button', { name: /^Create$/ }).click()
      await admin.page.waitForURL('/dashboards/399')
      await expect(admin.page.getByText('E2E Blank Dashboard')).toBeVisible()
      await captureE2EScreenshot(admin.page, testInfo, 'dashboard-created-detail')

      await admin.page.goto('/dashboards')
      admin.page.once('dialog', async (dialog) => {
        expect(dialog.message()).toContain('Existing Observability')
        await dialog.accept()
      })
      await admin.page.getByRole('row', { name: /Existing Observability/ }).getByTitle('Delete dashboard').click()
      await expect.poll(() => deletedDashboardIds).toContain(301)
      await expect(admin.page.getByText('Existing Observability')).toHaveCount(0)
      await captureE2EScreenshot(admin.page, testInfo, 'dashboard-deleted')
    } finally {
      await closeRoleSessions(admin.context)
    }
  })

  test('loads the reports workspace snapshot metrics', async ({ browser }, testInfo) => {
    const admin = await createRoleSession(browser, 'demo_admin')

    try {
      await admin.page.goto('/reports')
      await expect(admin.page.getByRole('heading', { name: 'Operational Reports' })).toBeVisible()
      await expect(admin.page.getByText('Approval Rate')).toBeVisible()
      await expect(admin.page.getByText('Avg. Review Time')).toBeVisible()
      await expect(admin.page.getByText('Query Success')).toBeVisible()
      await captureE2EScreenshot(admin.page, testInfo, 'reports-snapshot')
    } finally {
      await closeRoleSessions(admin.context)
    }
  })
})
