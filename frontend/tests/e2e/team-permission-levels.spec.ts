import { expect, test, type Page, type Response } from '@playwright/test'

import {
  captureE2EScreenshot,
  clickAndAcceptDialogIfPresent,
  closeRoleSessions,
  createLocalUserSession,
  seedE2EEnvironment,
} from './support/workflow-helpers'

const ADMIN_EMAIL = 'e2e-admin@datamingle.dev'
const REQUESTER_EMAIL = 'e2e-requester@datamingle.dev'
const REQUESTER_LABEL = 'E2E Requester (e2e-requester@datamingle.dev)'
const QUERY_PERMISSIONS = ['menu_query', 'menu_sqlquery', 'query_submit']
const DEMO_NODE_LABEL = 'demo-mysql-node'
const DEMO_SERVICE_LABEL = 'demo-mysql-workflow | mysql | mysql_demo'

function uniqueName(prefix: string) {
  return `${prefix}${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
}

async function expectOk(response: Response, action: string) {
  if (response.ok()) {
    return
  }

  const body = await response.text().catch(() => '')
  throw new Error(`${action} failed with HTTP ${response.status()}: ${body.slice(0, 500)}`)
}

async function openPermissionLevel(page: Page, levelId: number) {
  await page.goto(`/settings/permission-levels/${levelId}`)
  await expect(page.getByTestId('permission-level-detail')).toBeVisible()
}

function permissionCheckbox(page: Page, codename: string) {
  const permissionCode = codename.startsWith('sql.') ? codename : `sql.${codename}`
  return page.getByTestId(`permission-level-permission-${permissionCode}`)
}

async function setPermission(page: Page, codename: string, checked: boolean) {
  const checkbox = permissionCheckbox(page, codename)
  if (checked) {
    await checkbox.check()
  } else {
    await checkbox.uncheck()
  }
}

async function savePermissionLevel(page: Page, method: 'POST' | 'PUT', action: string) {
  const responsePromise = page.waitForResponse((response) => {
    const pathname = new URL(response.url()).pathname
    return response.request().method() === method && /\/api\/v1\/permission-levels\/(?:\d+\/)?$/.test(pathname)
  })
  await page.getByTestId('permission-level-save').click()
  const response = await responsePromise
  await expectOk(response, action)
  return response
}

async function createPermissionLevel(page: Page, levelName: string, permissions = QUERY_PERMISSIONS) {
  await page.goto('/settings/permission-levels/new')
  await expect(page.getByRole('heading', { name: 'Create Permission Level' })).toBeVisible()
  await page.getByTestId('permission-level-name').fill(levelName)
  for (const codename of permissions) {
    await setPermission(page, codename, true)
  }

  const response = await savePermissionLevel(page, 'POST', 'Create permission level')
  const payload = await response.json() as { data: { id: number } }
  await expect(page).toHaveURL(/\/settings\/permission-levels\/\d+$/)
  return payload.data.id
}

async function deletePermissionLevel(page: Page, levelId: number) {
  await openPermissionLevel(page, levelId)
  await expect(page.getByTestId('permission-level-delete')).toBeEnabled()

  const responsePromise = page.waitForResponse((response) => {
    const pathname = new URL(response.url()).pathname
    return response.request().method() === 'DELETE' && pathname.endsWith(`/api/v1/permission-levels/${levelId}/`)
  })
  await clickAndAcceptDialogIfPresent(page, async () => {
    await page.getByTestId('permission-level-delete').click()
  })
  await expectOk(await responsePromise, 'Delete permission level')
  await expect(page).toHaveURL(/\/settings\/permission-levels$/)
}

async function openTeam(page: Page, teamId: number) {
  await page.goto(`/settings/teams/${teamId}`)
  await expect(page.getByTestId('team-detail')).toBeVisible()
}

async function memberRow(page: Page, email = REQUESTER_EMAIL) {
  const row = page.getByTestId('team-member-row').filter({ hasText: email })
  await expect(row).toBeVisible()
  return row
}

async function expectTeamAssignments(page: Page, teamName: string, levelName: string) {
  await expect(page.getByTestId('team-name')).toHaveValue(teamName)
  const row = await memberRow(page)
  await expect(row.getByTestId('team-member-role').locator('option:checked')).toHaveText(levelName)
  await expect(page.getByTestId('team-node-select').locator('option:checked')).toContainText(DEMO_NODE_LABEL)
  await expect(page.getByTestId('team-service-assigned-select')).toContainText('demo-mysql-workflow')
}

async function saveTeam(page: Page, method: 'POST' | 'PUT', teamId: number | null, action: string) {
  const responsePromise = page.waitForResponse((response) => {
    const pathname = new URL(response.url()).pathname
    if (response.request().method() !== method) {
      return false
    }
    return teamId === null
      ? pathname.endsWith('/api/v1/teams/')
      : pathname.endsWith(`/api/v1/teams/${teamId}/`)
  })
  await page.getByTestId('team-save').click()
  const response = await responsePromise
  await expectOk(response, action)
  return response
}

async function deleteTeam(page: Page, teamId: number) {
  await openTeam(page, teamId)
  const responsePromise = page.waitForResponse((response) => {
    const pathname = new URL(response.url()).pathname
    return response.request().method() === 'DELETE' && pathname.endsWith(`/api/v1/teams/${teamId}/`)
  })
  await clickAndAcceptDialogIfPresent(page, async () => {
    await page.getByTestId('team-delete').click()
  })
  await expectOk(await responsePromise, 'Delete team')
  await expect(page).toHaveURL(/\/settings\/teams$/)
}

test.describe.serial('team and permission level management workflows', () => {
  test.beforeEach(() => seedE2EEnvironment())

  test('creates, edits, reloads, and deletes a permission level', async ({ browser }, testInfo) => {
    const admin = await createLocalUserSession(browser, ADMIN_EMAIL)
    const levelName = uniqueName('E2E Permission Level CRUD ')
    const editedLevelName = `${levelName} Edited`

    try {
      await admin.page.goto('/settings/permission-levels/new')
      await expect(admin.page.getByRole('heading', { name: 'Create Permission Level' })).toBeVisible()
      await admin.page.getByTestId('permission-level-save').click()
      await expect(admin.page.getByTestId('permission-level-form-error')).toContainText(
        'Permission level name is required.',
      )

      await admin.page.getByTestId('permission-level-name').fill(levelName)
      await setPermission(admin.page, 'menu_query', true)
      await setPermission(admin.page, 'query_submit', true)
      const createResponse = await savePermissionLevel(admin.page, 'POST', 'Create permission level')
      const createPayload = await createResponse.json() as { data: { id: number } }
      const levelId = createPayload.data.id
      await expect(admin.page).toHaveURL(/\/settings\/permission-levels\/\d+$/)
      await expect(admin.page.getByTestId('permission-level-membership-count')).toContainText('0 active')
      await captureE2EScreenshot(admin.page, testInfo, 'permission-level-created')

      await openPermissionLevel(admin.page, levelId)
      await expect(admin.page.getByTestId('permission-level-name')).toHaveValue(levelName)
      await expect(permissionCheckbox(admin.page, 'menu_query')).toBeChecked()
      await expect(permissionCheckbox(admin.page, 'query_submit')).toBeChecked()

      await admin.page.getByTestId('permission-level-name').fill(editedLevelName)
      await setPermission(admin.page, 'menu_sqlquery', true)
      await setPermission(admin.page, 'menu_query', false)
      await savePermissionLevel(admin.page, 'PUT', 'Update permission level')
      await expect(admin.page.getByTestId('permission-level-feedback')).toContainText('Permission level saved.')

      await openPermissionLevel(admin.page, levelId)
      await expect(admin.page.getByTestId('permission-level-name')).toHaveValue(editedLevelName)
      await expect(permissionCheckbox(admin.page, 'menu_query')).not.toBeChecked()
      await expect(permissionCheckbox(admin.page, 'menu_sqlquery')).toBeChecked()
      await expect(permissionCheckbox(admin.page, 'query_submit')).toBeChecked()
      await captureE2EScreenshot(admin.page, testInfo, 'permission-level-edited')

      await deletePermissionLevel(admin.page, levelId)
      await expect(admin.page.getByText(editedLevelName)).toHaveCount(0)
      await captureE2EScreenshot(admin.page, testInfo, 'permission-level-deleted')
    } finally {
      await closeRoleSessions(admin.context)
    }
  })

  test('creates, edits, verifies, and deletes a team with assigned access', async ({ browser }, testInfo) => {
    const admin = await createLocalUserSession(browser, ADMIN_EMAIL)
    let requester: Awaited<ReturnType<typeof createLocalUserSession>> | undefined
    const levelName = uniqueName('E2E Permission Level Team ')
    const teamName = uniqueName('E2E Team ')
    const editedTeamName = `${teamName} Edited`

    try {
      const levelId = await createPermissionLevel(admin.page, levelName)

      await admin.page.goto('/settings/teams/new')
      await expect(admin.page.getByRole('heading', { name: 'Create Team' })).toBeVisible()
      await admin.page.getByTestId('team-save').click()
      await expect(admin.page.getByTestId('team-form-error')).toContainText('Team name cannot be blank.')

      await admin.page.getByTestId('team-name').fill(teamName)
      await admin.page.getByTestId('team-member-add-select').selectOption({ label: REQUESTER_LABEL })
      await admin.page.getByTestId('team-member-add').click()
      await (await memberRow(admin.page)).getByTestId('team-member-role').selectOption({ label: levelName })
      await admin.page.getByTestId('team-node-select').selectOption({ label: DEMO_NODE_LABEL })
      await admin.page.getByTestId('team-service-available-filter').fill('demo-mysql-workflow')
      await admin.page.getByTestId('team-service-available-select').selectOption({ label: DEMO_SERVICE_LABEL })
      await admin.page.getByTestId('team-service-add-selected').click()
      await expect(admin.page.getByTestId('team-service-assigned-select')).toContainText('demo-mysql-workflow')

      const createTeamResponse = await saveTeam(admin.page, 'POST', null, 'Create team')
      const createTeamPayload = await createTeamResponse.json() as { data: { team_id: number } }
      const teamId = createTeamPayload.data.team_id
      await expect(admin.page).toHaveURL(/\/settings\/teams\/\d+$/)
      await expectTeamAssignments(admin.page, teamName, levelName)
      await captureE2EScreenshot(admin.page, testInfo, 'team-created')

      await openTeam(admin.page, teamId)
      await expectTeamAssignments(admin.page, teamName, levelName)
      await admin.page.getByTestId('team-name').fill(editedTeamName)
      await saveTeam(admin.page, 'PUT', teamId, 'Update team')
      await expect(admin.page.getByTestId('team-feedback')).toContainText('Team updated successfully.')

      await openTeam(admin.page, teamId)
      await expectTeamAssignments(admin.page, editedTeamName, levelName)
      await captureE2EScreenshot(admin.page, testInfo, 'team-edited')

      requester = await createLocalUserSession(browser, REQUESTER_EMAIL)
      await requester.page.goto('/profile')
      await expect(requester.page.getByText(editedTeamName)).toBeVisible()
      await requester.page.goto('/queries')
      await expect(requester.page.getByTestId('query-console-page')).toBeVisible()
      await expect(requester.page.getByTestId('query-console-no-access')).toHaveCount(0)
      await expect(requester.page.getByTestId('query-console-history-tab')).toBeVisible()
      await captureE2EScreenshot(requester.page, testInfo, 'assigned-user-query-access')

      await closeRoleSessions(requester.context)
      requester = undefined

      await deleteTeam(admin.page, teamId)
      await expect(admin.page.getByText(editedTeamName)).toHaveCount(0)
      await deletePermissionLevel(admin.page, levelId)
      await captureE2EScreenshot(admin.page, testInfo, 'team-permission-cleanup')
    } finally {
      await closeRoleSessions(admin.context, requester?.context)
    }
  })
})
