import { expect, test, type Page } from '@playwright/test'

import {
  captureE2EScreenshot,
  clickAndAcceptDialogIfPresent,
  closeRoleSessions,
  createLocalUserSession,
  seedE2EEnvironment,
} from './support/workflow-helpers'

const E2E_USER_PASSWORD = 'SecurePass123!'
const TEAM_NAME = 'Demo Workflow Single Stage'
const TEAM_ROLE = 'QA'

type LocalSession = Awaited<ReturnType<typeof createLocalUserSession>>

type ManagedUser = {
  display: string
  email: string
}

function uniqueManagedUser(): ManagedUser {
  const suffix = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
  return {
    display: `E2E Managed ${suffix}`,
    email: `e2e-user-mgmt-${suffix}@datamingle.dev`,
  }
}

function escaped(value: string) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}

async function openUserManagement(page: Page) {
  await page.goto('/settings/users')
  await expect(page.getByRole('heading', { name: 'User Management', level: 2 })).toBeVisible()
}

async function findUserRow(page: Page, user: ManagedUser) {
  await page.getByPlaceholder('Filter users by name, username, email, or ID').fill(user.email)
  const row = page.getByRole('row', { name: new RegExp(escaped(user.email)) })
  await expect(row).toBeVisible()
  return row
}

async function createManagedUser(page: Page, user: ManagedUser) {
  await openUserManagement(page)
  await page.getByTestId('user-management-create-open').click()
  await expect(page.getByTestId('user-management-create-dialog')).toBeVisible()
  await page.getByTestId('user-management-create-email').fill(user.email)
  await page.getByTestId('user-management-create-display').fill(user.display)
  await page.getByTestId('user-management-create-password').fill(E2E_USER_PASSWORD)
  await page.getByTestId('user-management-create-submit').click()

  await expect(page.getByTestId('user-management-feedback')).toContainText(`User ${user.email} created.`)
  const row = await findUserRow(page, user)
  await expect(row).toContainText(user.display)
  await expect(row.getByTestId('user-management-status')).toContainText('Active')
}

async function openManagedUserDetail(page: Page, user: ManagedUser) {
  const row = await findUserRow(page, user)
  await row.getByTestId('user-management-open').click()
  await expect(page.getByTestId('user-management-detail')).toBeVisible()
}

async function grantTeamRole(page: Page, user: ManagedUser) {
  await page.goto('/settings/teams')
  await page.getByPlaceholder('Filter teams by name or ID').fill(TEAM_NAME)
  await expect(page.getByRole('cell', { name: TEAM_NAME })).toBeVisible()
  await page.getByRole('row', { name: new RegExp(escaped(TEAM_NAME)) }).getByRole('link', { name: 'Open' }).click()

  await expect(page.getByRole('heading', { name: 'Edit Team' })).toBeVisible()
  await page.getByTestId('team-member-add-select').selectOption({
    label: `${user.display} (${user.email})`,
  })
  await page.getByTestId('team-member-add').click()

  const memberRow = page.getByTestId('team-member-row').filter({ hasText: user.email })
  await expect(memberRow).toBeVisible()
  await memberRow.getByTestId('team-member-role').selectOption({ label: TEAM_ROLE })
  await page.getByTestId('team-save').click()
  await expect(page.getByText('Team updated successfully.')).toBeVisible()
}

async function setUserActiveState(page: Page, user: ManagedUser, active: boolean) {
  await openUserManagement(page)
  const row = await findUserRow(page, user)
  const expectedAction = active ? 'Reactivate' : 'Deactivate'
  await expect(row.getByTestId('user-management-toggle-active')).toContainText(expectedAction)
  await clickAndAcceptDialogIfPresent(page, async () => {
    await row.getByTestId('user-management-toggle-active').click()
  })
  await expect(row.getByTestId('user-management-status')).toContainText(active ? 'Active' : 'Inactive')
}

async function deleteManagedUser(page: Page, user: ManagedUser) {
  await openUserManagement(page)
  const row = await findUserRow(page, user)
  await clickAndAcceptDialogIfPresent(page, async () => {
    await row.getByTestId('user-management-delete').click()
  })
  await page.getByPlaceholder('Filter users by name, username, email, or ID').fill(user.email)
  await expect(page.getByRole('row', { name: new RegExp(escaped(user.email)) })).toHaveCount(0)
}

async function expectManagedUserNavigation(page: Page) {
  await expect(page.getByRole('link', { name: 'Workflows' })).toBeVisible()
  await expect(page.getByRole('link', { name: 'Queries' })).toBeVisible()
  await expect(page.getByRole('link', { name: 'User Management' })).toHaveCount(0)
}

test.describe.serial('local user management workflows', () => {
  test.beforeEach(async () => {
    await seedE2EEnvironment()
  })

  test('creates a local user and allows the user to sign in', async ({ browser }, testInfo) => {
    const user = uniqueManagedUser()
    const admin = await createLocalUserSession(browser, 'e2e-admin@datamingle.dev')
    let managedUser: LocalSession | undefined

    try {
      await createManagedUser(admin.page, user)
      await openManagedUserDetail(admin.page, user)
      await expect(admin.page.getByTestId('user-management-detail')).toContainText(user.display)
      await expect(admin.page.getByTestId('user-management-detail')).toContainText(user.email)
      await expect(admin.page.getByTestId('user-management-detail-status')).toContainText('Active')
      await captureE2EScreenshot(admin.page, testInfo, 'admin-created-user')

      managedUser = await createLocalUserSession(browser, user.email, E2E_USER_PASSWORD)
      await expect(managedUser.page.getByText(user.display)).toBeVisible()
      await expect(managedUser.page.getByText(user.email)).toBeVisible()
      await expect(managedUser.page.getByRole('link', { name: 'User Management' })).toHaveCount(0)
      await captureE2EScreenshot(managedUser.page, testInfo, 'created-user-signed-in')
    } finally {
      await closeRoleSessions(admin.context, managedUser?.context)
    }
  })

  test('assigns team access, enforces deactivation, and deletes the user', async ({ browser }, testInfo) => {
    const user = uniqueManagedUser()
    const admin = await createLocalUserSession(browser, 'e2e-admin@datamingle.dev')
    let activeUser: LocalSession | undefined
    let reactivatedUser: LocalSession | undefined
    let inactiveContext: LocalSession['context'] | undefined

    try {
      await createManagedUser(admin.page, user)
      await grantTeamRole(admin.page, user)
      await captureE2EScreenshot(admin.page, testInfo, 'admin-assigned-team-access')

      activeUser = await createLocalUserSession(browser, user.email, E2E_USER_PASSWORD)
      await expectManagedUserNavigation(activeUser.page)

      await setUserActiveState(admin.page, user, false)
      inactiveContext = await browser.newContext({ acceptDownloads: true })
      const inactivePage = await inactiveContext.newPage()
      await inactivePage.goto('/login')
      await inactivePage.getByTestId('login-email').fill(user.email)
      await inactivePage.getByTestId('login-password').fill(E2E_USER_PASSWORD)
      await inactivePage.getByTestId('login-submit').click()
      await expect(inactivePage).toHaveURL(/\/login$/)
      await expect(inactivePage.getByTestId('login-error')).toBeVisible()
      await captureE2EScreenshot(inactivePage, testInfo, 'inactive-user-login-blocked')

      await setUserActiveState(admin.page, user, true)
      reactivatedUser = await createLocalUserSession(browser, user.email, E2E_USER_PASSWORD)
      await expectManagedUserNavigation(reactivatedUser.page)
      await captureE2EScreenshot(reactivatedUser.page, testInfo, 'reactivated-user-signed-in')

      await deleteManagedUser(admin.page, user)
      await captureE2EScreenshot(admin.page, testInfo, 'admin-deleted-user')
    } finally {
      await closeRoleSessions(admin.context, activeUser?.context, reactivatedUser?.context, inactiveContext)
    }
  })
})
