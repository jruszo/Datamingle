import { expect, test, type Page } from '@playwright/test'

import {
  captureE2EScreenshot,
  closeRoleSessions,
  createLocalUserSession,
  seedE2EEnvironment,
  uniqueWorkflowName,
} from './support/workflow-helpers'

type LocalSession = Awaited<ReturnType<typeof createLocalUserSession>>

async function grantTeamRole(page: Page, teamName: string, userDisplay: string, username: string, roleName: string) {
  await page.goto('/settings/teams')
  await page.getByPlaceholder('Filter teams by name or ID').fill(teamName)
  await expect(page.getByRole('cell', { name: teamName })).toBeVisible()
  await page.getByRole('row', { name: new RegExp(teamName) }).getByRole('link', { name: 'Open' }).click()

  await expect(page.getByRole('heading', { name: 'Edit Team' })).toBeVisible()
  await page.getByTestId('team-member-add-select').selectOption({
    label: `${userDisplay} (${username})`,
  })
  await page.getByTestId('team-member-add').click()

  const memberRow = page.getByTestId('team-member-row').filter({ hasText: username })
  await expect(memberRow).toBeVisible()
  await memberRow.getByTestId('team-member-role').selectOption({ label: roleName })
  await page.getByTestId('team-save').click()
  await expect(page.getByText('Team updated successfully.')).toBeVisible()
}

test.describe.serial('local user permission workflows', () => {
  test.beforeEach(async () => {
    await seedE2EEnvironment()
  })

  test('grants team roles, requests access, approves it, and shows the active grant', async ({
    browser,
  }, testInfo) => {
    const admin = await createLocalUserSession(browser, 'e2e-admin@datamingle.dev')
    let requester: LocalSession | undefined
    let reviewer: LocalSession | undefined

    try {
      await grantTeamRole(
        admin.page,
        'Demo Workflow Single Stage',
        'E2E Reviewer',
        'e2e-reviewer@datamingle.dev',
        'DBA',
      )
      await grantTeamRole(
        admin.page,
        'Demo Workflow Multi Stage',
        'E2E Requester',
        'e2e-requester@datamingle.dev',
        'QA',
      )
      await captureE2EScreenshot(admin.page, testInfo, 'admin-granted-team-roles')

      requester = await createLocalUserSession(browser, 'e2e-requester@datamingle.dev')
      const requestTitle = uniqueWorkflowName('E2E permission request')
      const requestReason = 'Need temporary query access for the browser approval workflow.'

      await requester.page.goto('/permission-management')
      await requester.page.getByRole('button', { name: 'Request access' }).first().click()
      await requester.page.getByLabel('Title').fill(requestTitle)
      await requester.page.getByLabel('Target type').selectOption('team')
      await requester.page.getByLabel('Request for').selectOption('user')
      await requester.page.getByLabel('Access duration').selectOption('temporary')
      await requester.page.getByLabel('Team').selectOption({ label: 'Demo Workflow Single Stage' })
      await requester.page.getByLabel('Permission level').selectOption({ label: 'QA' })
      await requester.page.getByLabel('Reason').fill(requestReason)
      await requester.page.getByRole('button', { name: 'Submit request' }).click()

      await expect(requester.page.getByTestId('permission-request-detail')).toBeVisible()
      await expect(requester.page.getByTestId('permission-request-detail')).toContainText(requestTitle)
      await expect(requester.page.getByTestId('permission-request-detail-status')).toContainText('Pending review')
      await expect(requester.page.getByTestId('permission-review-node').filter({ hasText: 'DBA' })).toContainText(
        'Current',
      )
      await captureE2EScreenshot(requester.page, testInfo, 'requester-pending-request')

      const requestId = new URL(requester.page.url()).searchParams.get('requestId')
      expect(requestId).toBeTruthy()

      reviewer = await createLocalUserSession(browser, 'e2e-reviewer@datamingle.dev')
      await reviewer.page.goto(`/permission-management?requestId=${requestId}`)
      await expect(reviewer.page.getByTestId('permission-request-detail')).toBeVisible()
      await expect(reviewer.page.getByTestId('permission-request-detail')).toContainText(requestTitle)
      await reviewer.page.getByTestId('permission-review-remark').fill('Approved by permission E2E')
      await reviewer.page.getByTestId('permission-review-approve').click()
      await expect(reviewer.page.getByText('Request approved.')).toBeVisible()
      await captureE2EScreenshot(reviewer.page, testInfo, 'reviewer-approved-request')

      await requester.page.goto(`/permission-management?requestId=${requestId}`)
      await expect(requester.page.getByTestId('permission-request-detail')).toBeVisible()
      await expect(requester.page.getByTestId('permission-request-detail-status')).toContainText('Approved')
      await captureE2EScreenshot(requester.page, testInfo, 'requester-approved-request')

      await requester.page.goto('/permission-management')
      await requester.page.getByTestId('permission-active-access-tab').click()
      const grantRow = requester.page.getByTestId('permission-grant-row').filter({
        hasText: 'Demo Workflow Single Stage',
      })
      await expect(grantRow).toBeVisible()
      await expect(grantRow).toContainText('QA')
      await expect(grantRow).toContainText('Temporary')
      await captureE2EScreenshot(requester.page, testInfo, 'requester-active-grant')
    } finally {
      await closeRoleSessions(admin.context, requester?.context, reviewer?.context)
    }
  })
})
