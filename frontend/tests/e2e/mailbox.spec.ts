import { expect, test } from '@playwright/test'

import {
  clickAndAcceptDialogIfPresent,
  closeRoleSessions,
  createRoleSession,
  fillSqlEditor,
  seedLocalDemo,
  uniqueWorkflowName,
  workflowIdFromUrl,
} from './support/workflow-helpers'

type RoleSession = Awaited<ReturnType<typeof createRoleSession>>

test.describe.serial('mailbox', () => {
  test.beforeEach(() => {
    seedLocalDemo()
  })

  test('shows approval notifications in the bell dropdown and full mailbox page', async ({ browser }) => {
    const requester = await createRoleSession(browser, 'demo_requester')
    let dba: RoleSession | undefined

    try {
      const workflowName = uniqueWorkflowName('mailbox notification')
      const ddlColumn = `mailbox_${Date.now().toString(36).slice(-8)}`

      await requester.page.goto('/workflows/ddl/new')
      await requester.page.getByTestId('workflow-name').fill(workflowName)
      await requester.page.getByTestId('workflow-group').selectOption({ label: 'Demo Workflow Single Stage' })
      await requester.page.getByTestId('workflow-instance').selectOption({ label: 'demo-mysql-workflow / MYSQL' })
      await requester.page.getByTestId('workflow-db').selectOption('demo_orders')
      await fillSqlEditor(
        requester.page,
        'workflow-sql-editor',
        `ALTER TABLE customers ADD COLUMN ${ddlColumn} VARCHAR(16) NOT NULL DEFAULT 'bronze' COMMENT 'Mailbox notification column';`,
      )

      await requester.page.getByRole('button', { name: 'SQL check' }).click()
      await expect(
        requester.page.getByText('This check is current and the SQL is classified as DDL.'),
      ).toBeVisible()
      await clickAndAcceptDialogIfPresent(requester.page, () =>
        requester.page.getByTestId('workflow-submit').click(),
      )
      await requester.page.waitForURL(/\/workflows\/\d+$/)
      const workflowId = workflowIdFromUrl(requester.page)

      dba = await createRoleSession(browser, 'demo_dba')

      await dba.page.getByTestId('app-mailbox-button').click()
      await expect(dba.page.getByTestId('app-mailbox-menu')).toBeVisible()
      await expect(dba.page.getByTestId('app-mailbox-menu')).toContainText(workflowName)
      await expect(dba.page.getByTestId('app-mailbox-menu')).toContainText('Approval needed')

      await dba.page.getByTestId('app-mailbox-view-all').click()
      await dba.page.waitForURL('/mailbox')
      await expect(dba.page.getByTestId('mailbox-page-title')).toBeVisible()

      const mailboxItem = dba.page.getByTestId(/mailbox-item-\d+/).filter({ hasText: workflowName }).first()
      await expect(mailboxItem).toBeVisible()
      await mailboxItem.focus()
      await dba.page.keyboard.press('Space')

      await dba.page.waitForURL(new RegExp(`/workflows/${workflowId}$`))
      await expect(dba.page.getByTestId('workflow-detail-refresh')).toBeVisible()

      await dba.page.goto('/mailbox')
      await expect(dba.page.getByTestId('mailbox-page-title')).toBeVisible()
      await dba.page.getByTestId('mailbox-filter-state').selectOption('unread')
      await expect(dba.page.getByText(workflowName)).toHaveCount(0)
    } finally {
      await closeRoleSessions(requester.context, dba?.context)
    }
  })
})
