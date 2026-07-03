import { execFileSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'

import { expect, test, type Page, type Response, type Route } from '@playwright/test'

import {
  captureE2EScreenshot,
  closeRoleSessions,
  createRoleSession,
  seedE2EEnvironment,
} from './support/workflow-helpers'

const INVENTORY_SEARCH_PLACEHOLDER =
  'Filter instances by name, host, user, ID, detected hostname, or detected version'
const E2E_POLICY_NAME = 'E2E Inventory Policy'
const SINGLE_STAGE_TEAM = 'Demo Workflow Single Stage'
const MULTI_STAGE_TEAM = 'Demo Workflow Multi Stage'
const REPO_ROOT = fileURLToPath(new URL('../../../../', import.meta.url))

type InventoryInstance = {
  id: number
  name: string
  host: string
  editedHost: string
}

type WorkflowPolicyFixture = {
  id: number
  stepId: number
  permissionGroupId: number
  permissionGroupName: string
}

function uniqueInventoryInstance(): InventoryInstance {
  const suffix = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
  return {
    id: 0,
    name: `e2e-inventory-${suffix}`,
    host: `e2e-inventory-${suffix}.local`,
    editedHost: `e2e-inventory-${suffix}-edited.local`,
  }
}

function escaped(value: string) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}

async function fulfillJson(route: Route, body: unknown) {
  await route.fulfill({
    contentType: 'application/json',
    body: JSON.stringify(body),
  })
}

async function mockSavedConnectionTests(page: Page) {
  await page.route(/\/api\/v1\/instance\/\d+\/test-connection\/$/, async (route) => {
    if (route.request().method() !== 'POST') {
      await route.continue()
      return
    }
    await fulfillJson(route, {
      detail: 'Connection successful.',
      data: { success: true, message: 'Connection successful.' },
    })
  })
}

async function mockWorkflowPolicies(page: Page, policy: WorkflowPolicyFixture) {
  await page.route('**/api/v1/workflow/policies/**', async (route) => {
    await fulfillJson(route, {
      detail: 'ok',
      data: {
        count: 1,
        next: null,
        previous: null,
        results: [
          {
            id: policy.id,
            name: E2E_POLICY_NAME,
            description: 'E2E workflow policy for inventory tests.',
            is_active: true,
            steps: [
              {
                id: policy.stepId,
                order: 1,
                permission_group: policy.permissionGroupId,
                permission_group_name: policy.permissionGroupName,
              },
            ],
            created_by: 'demo_admin',
            updated_by: 'demo_admin',
            can_edit: true,
          },
        ],
      },
    })
  })
}

function e2eInventoryPolicy(): WorkflowPolicyFixture {
  const script = [
    'import json',
    'from sql.models import WorkflowPolicy',
    `policy = WorkflowPolicy.objects.get(name=${JSON.stringify(E2E_POLICY_NAME)})`,
    'step = policy.steps.order_by("order").select_related("permission_group").first()',
    'print(json.dumps({"id": policy.id, "stepId": step.id, "permissionGroupId": step.permission_group_id, "permissionGroupName": step.permission_group.name}))',
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
  const jsonLine = output
    .trim()
    .split(/\r?\n/)
    .reverse()
    .find((line) => line.trim().startsWith('{'))

  if (!jsonLine) {
    throw new Error(`Expected workflow policy JSON in command output:\n${output}`)
  }

  return JSON.parse(jsonLine) as WorkflowPolicyFixture
}

async function openInventory(page: Page) {
  await page.goto('/inventory')
  await expect(page.getByRole('heading', { name: 'Inventory', level: 2 })).toBeVisible()
}

async function findInventoryRow(page: Page, instanceName: string) {
  await page.getByPlaceholder(INVENTORY_SEARCH_PLACEHOLDER).fill(instanceName)
  const row = page.getByRole('row', { name: new RegExp(escaped(instanceName)) })
  await expect(row).toBeVisible()
  return row
}

async function fillInventoryCreateForm(page: Page, instance: InventoryInstance) {
  await page.getByTestId('inventory-instance-name').fill(instance.name)
  await page.getByTestId('inventory-host').fill(instance.host)
  await page.getByTestId('inventory-instance-type').selectOption('master')
  await page.getByTestId('inventory-db-type').selectOption('mysql')
  await page.getByTestId('inventory-port').fill('3316')
  await page.getByTestId('inventory-user').fill('e2e_inventory_user')
  await page.getByTestId('inventory-password').fill('e2e_inventory_password')
  await page.getByTestId('inventory-db-name').fill('e2e_inventory_db')
  await page.getByTestId('inventory-charset').fill('utf8mb4')
  await page.getByTestId('inventory-visible-db-regex').fill('^e2e_.*$')
  await page.getByTestId('inventory-hidden-db-regex').fill('^(mysql|sys)$')
  await page.getByTestId('inventory-workflow-enabled').check()
  await expect(page.getByTestId('inventory-workflow-policy')).toContainText(E2E_POLICY_NAME)
  await page.getByTestId('inventory-workflow-policy').selectOption({ label: E2E_POLICY_NAME })
  await page.getByTestId('inventory-ssl-enabled').check()
  await expect(page.getByTestId('inventory-verify-ssl')).toBeEnabled()
  await page.getByTestId('inventory-verify-ssl').uncheck()
  await page.getByTestId('inventory-team-select').selectOption({ label: SINGLE_STAGE_TEAM })
}

async function selectedTeamNames(page: Page) {
  return page.getByTestId('inventory-team-select').locator('option:checked').allTextContents()
}

async function saveInventoryAndExpectOk(
  page: Page,
  responsePredicate: (response: Response) => boolean,
  actionName: string,
) {
  const responsePromise = page.waitForResponse(responsePredicate)
  await page.getByTestId('inventory-save').click()
  const response = await responsePromise
  if (response.ok()) {
    return response
  }

  const responseBody = await response.text().catch(() => '')
  throw new Error(`${actionName} failed with HTTP ${response.status()}: ${responseBody.slice(0, 500)}`)
}

test.describe.serial('inventory management workflows', () => {
  test.beforeEach(() => {
    seedE2EEnvironment()
  })

  test('browses, filters, and tests a saved inventory connection', async ({ browser }, testInfo) => {
    const admin = await createRoleSession(browser, 'demo_admin')

    try {
      await mockSavedConnectionTests(admin.page)
      await openInventory(admin.page)

      await expect(admin.page.getByText('demo-mysql-workflow')).toBeVisible()
      await expect(admin.page.getByText('demo-pgsql-workflow')).toBeVisible()
      await expect(admin.page.getByTestId('inventory-add-instance')).toBeVisible()

      await admin.page.getByPlaceholder(INVENTORY_SEARCH_PLACEHOLDER).fill('demo-mysql-workflow')
      await expect(admin.page.getByText('demo-mysql-workflow')).toBeVisible()
      await expect(admin.page.getByText('demo-pgsql-workflow')).toHaveCount(0)

      await admin.page.getByTestId('inventory-db-type-filter').selectOption('mysql')
      await expect(admin.page.getByText('demo-mysql-workflow')).toBeVisible()
      await captureE2EScreenshot(admin.page, testInfo, 'inventory-filtered-list')

      const row = admin.page.getByRole('row', { name: /demo-mysql-workflow/ })
      await row.getByTestId('inventory-row-test-connection').click()
      await expect(admin.page.getByTestId('inventory-feedback')).toContainText(
        'demo-mysql-workflow: Connection successful.',
      )
      await captureE2EScreenshot(admin.page, testInfo, 'inventory-list-connection-tested')
    } finally {
      await closeRoleSessions(admin.context)
    }
  })

  test('blocks users without inventory permissions', async ({ browser }, testInfo) => {
    const requester = await createRoleSession(browser, 'demo_requester')

    try {
      await requester.page.goto('/inventory')
      await expect(requester.page.getByTestId('inventory-error')).toContainText(
        'You do not have permission to access Datamingle inventory.',
      )
      await expect(requester.page.getByTestId('inventory-add-instance')).toHaveCount(0)
      await expect(requester.page.getByTestId('inventory-row-test-connection')).toHaveCount(0)
      await captureE2EScreenshot(requester.page, testInfo, 'inventory-access-denied')
    } finally {
      await closeRoleSessions(requester.context)
    }
  })

  test('creates, edits, and reloads an inventory instance', async ({ browser }, testInfo) => {
    const admin = await createRoleSession(browser, 'demo_admin')
    const instance = uniqueInventoryInstance()

    try {
      const policy = e2eInventoryPolicy()
      await mockWorkflowPolicies(admin.page, policy)
      await admin.page.goto('/inventory/new')
      await expect(admin.page.getByRole('heading', { name: 'Add Instance', level: 2 })).toBeVisible()

      await admin.page.getByTestId('inventory-save').click()
      await expect(admin.page.getByTestId('inventory-form-error')).toContainText('Instance name cannot be blank.')

      await fillInventoryCreateForm(admin.page, instance)

      const draftConnectionResponse = admin.page.waitForResponse((response) => {
        const url = new URL(response.url())
        return response.request().method() === 'POST' && url.pathname.endsWith('/api/v1/instance/test-connection/')
      })
      await admin.page.getByTestId('inventory-test-connection').click()
      const draftResponse = await draftConnectionResponse
      expect(draftResponse.status()).toBe(400)
      await expect(admin.page.getByTestId('inventory-connection-message')).toContainText(
        'Save the service and assign it to an online agent before testing the connection.',
      )

      const createResponse = await saveInventoryAndExpectOk(admin.page, (response) => {
        const url = new URL(response.url())
        return response.request().method() === 'POST' && url.pathname.endsWith('/api/v1/instance/')
      }, 'Create inventory instance')
      const createPayload = await createResponse.json() as { data: { id: number } }
      instance.id = createPayload.data.id

      await expect(admin.page).toHaveURL(/\/inventory\?created=/)
      await expect(admin.page.getByTestId('inventory-feedback')).toContainText(`Instance "${instance.name}" created successfully.`)
      const createdRow = await findInventoryRow(admin.page, instance.name)
      await expect(createdRow).toContainText(instance.host)
      await captureE2EScreenshot(admin.page, testInfo, 'inventory-created-row')

      await createdRow.getByTestId('inventory-edit').click()
      await expect(admin.page.getByRole('heading', { name: 'Edit Instance', level: 2 })).toBeVisible()
      await expect(admin.page.getByTestId('inventory-instance-name')).toHaveValue(instance.name)
      await expect(admin.page.getByTestId('inventory-host')).toHaveValue(instance.host)
      await expect(admin.page.getByTestId('inventory-db-name')).toHaveValue('e2e_inventory_db')
      expect(await selectedTeamNames(admin.page)).toEqual([SINGLE_STAGE_TEAM])

      await admin.page.getByTestId('inventory-host').fill(instance.editedHost)
      await admin.page.getByTestId('inventory-port').fill('3317')
      await admin.page.getByTestId('inventory-user').fill('e2e_inventory_editor')
      await admin.page.getByTestId('inventory-db-name').fill('e2e_inventory_db_edited')
      await admin.page.getByTestId('inventory-visible-db-regex').fill('^e2e_inventory_.*$')
      await admin.page.getByTestId('inventory-hidden-db-regex').fill('^mysql$')
      await admin.page.getByTestId('inventory-charset').fill('utf8')
      await admin.page.getByTestId('inventory-team-select').selectOption({ label: MULTI_STAGE_TEAM })

      await saveInventoryAndExpectOk(admin.page, (response) => {
        const url = new URL(response.url())
        return response.request().method() === 'PUT' && url.pathname.endsWith(`/api/v1/instance/${instance.id}/`)
      }, 'Update inventory instance')

      await expect(admin.page).toHaveURL(/\/inventory\?edited=/)
      await expect(admin.page.getByTestId('inventory-feedback')).toContainText(`Instance "${instance.name}" updated successfully.`)

      await admin.page.goto(`/inventory/${instance.id}`)
      await expect(admin.page.getByTestId('inventory-host')).toHaveValue(instance.editedHost)
      await expect(admin.page.getByTestId('inventory-port')).toHaveValue('3317')
      await expect(admin.page.getByTestId('inventory-user')).toHaveValue('e2e_inventory_editor')
      await expect(admin.page.getByTestId('inventory-db-name')).toHaveValue('e2e_inventory_db_edited')
      await expect(admin.page.getByTestId('inventory-visible-db-regex')).toHaveValue('^e2e_inventory_.*$')
      await expect(admin.page.getByTestId('inventory-hidden-db-regex')).toHaveValue('^mysql$')
      await expect(admin.page.getByTestId('inventory-charset')).toHaveValue('utf8')
      expect(await selectedTeamNames(admin.page)).toEqual([MULTI_STAGE_TEAM])
      await captureE2EScreenshot(admin.page, testInfo, 'inventory-edited-detail')
    } finally {
      await closeRoleSessions(admin.context)
    }
  })
})
