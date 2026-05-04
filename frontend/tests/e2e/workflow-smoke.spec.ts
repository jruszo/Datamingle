import { readFile } from 'node:fs/promises'

import { expect, test } from '@playwright/test'

import {
  assertExecutionRowsPresent,
  assertReviewRowsPresent,
  clickAndAcceptDialogIfPresent,
  closeRoleSessions,
  createRoleSession,
  fillSqlEditor,
  openWorkflowDetail,
  setSystemConfigValues,
  uniqueWorkflowName,
  waitForWorkflowAction,
  waitForWorkflowStatus,
  workflowIdFromUrl,
} from './support/workflow-helpers'

type RoleSession = Awaited<ReturnType<typeof createRoleSession>>

test.describe.serial('workflow smoke', () => {
  test.beforeEach(() => {
    setSystemConfigValues({
      gh_ost: '',
      pt_osc: '',
    })
  })

  test.afterEach(() => {
    setSystemConfigValues({
      gh_ost: '',
      pt_osc: '',
    })
  })

  test('opens workflow detail on a dedicated page and restores filtered list state on return', async ({ browser }) => {
    const requester = await createRoleSession(browser, 'demo_requester')

    try {
      const workflowName = uniqueWorkflowName('ddl list smoke')
      const ddlColumn = `list_${Date.now().toString(36).slice(-8)}`

      await requester.page.goto('/workflows/ddl/new')
      await requester.page.getByTestId('workflow-name').fill(workflowName)
      await requester.page.getByTestId('workflow-group').selectOption({ label: 'Demo Workflow Single Stage' })
      await requester.page.getByTestId('workflow-instance').selectOption({ label: 'demo-mysql-workflow / MYSQL' })
      await requester.page.getByTestId('workflow-db').selectOption('demo_orders')
      await fillSqlEditor(
        requester.page,
        'workflow-sql-editor',
        `ALTER TABLE customers ADD COLUMN ${ddlColumn} VARCHAR(16) NOT NULL DEFAULT 'bronze' COMMENT 'List smoke column';`,
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

      await requester.page.goto('/workflows')
      await requester.page.getByTestId('workflow-filter-syntax-type').selectOption('1')
      await requester.page.getByRole('button', { name: 'Apply filters' }).click()
      await requester.page.waitForURL(/\/workflows\?syntaxType=1$/)

      const workflowListItem = requester.page.getByTestId(`workflow-list-item-${workflowId}`)
      await expect(workflowListItem).toBeVisible()
      await workflowListItem.click()

      await requester.page.waitForURL(/\/workflows\/\d+\?returnTo=/)
      await expect(requester.page.getByTestId('workflow-detail-refresh')).toBeVisible()
      await expect(requester.page.getByText('Workflow List')).toHaveCount(0)

      await requester.page.getByTestId('workflow-detail-back').click()

      await requester.page.waitForURL(/\/workflows\?syntaxType=1$/)
      await expect(requester.page.getByTestId('workflow-filter-syntax-type')).toHaveValue('1')
      await expect(requester.page.getByTestId(`workflow-list-item-${workflowId}`)).toBeVisible()
    } finally {
      await closeRoleSessions(requester.context)
    }
  })

  test('runs a DDL single-stage workflow from submit through execution', async ({ browser }) => {
    const requester = await createRoleSession(browser, 'demo_requester')
    let dba: RoleSession | undefined

    try {
      const workflowName = uniqueWorkflowName('ddl smoke')
      const ddlColumn = `loyalty_${Date.now().toString(36).slice(-8)}`

      await requester.page.goto('/workflows/ddl/new')
      await requester.page.getByTestId('workflow-name').fill(workflowName)
      await requester.page.getByTestId('workflow-group').selectOption({ label: 'Demo Workflow Single Stage' })
      await expect(requester.page.getByTestId('workflow-approval-preview')).toHaveText('DBA')
      await requester.page.getByTestId('workflow-instance').selectOption({ label: 'demo-mysql-workflow / MYSQL' })
      await requester.page.getByTestId('workflow-db').selectOption('demo_orders')
      await fillSqlEditor(
        requester.page,
        'workflow-sql-editor',
        `ALTER TABLE customers ADD COLUMN ${ddlColumn} VARCHAR(16) NOT NULL DEFAULT 'bronze' COMMENT 'Loyalty tier';`,
      )

      await requester.page.getByRole('button', { name: 'SQL check' }).click()
      await expect(
        requester.page.getByText('This check is current and the SQL is classified as DDL.'),
      ).toBeVisible()

      await clickAndAcceptDialogIfPresent(requester.page, () =>
        requester.page.getByTestId('workflow-submit').click(),
      )
      await requester.page.waitForURL(/\/workflows\/\d+$/)
      await expect(requester.page.getByTestId('workflow-detail-refresh')).toBeVisible()
      const workflowId = workflowIdFromUrl(requester.page)

      dba = await createRoleSession(browser, 'demo_dba')
      await openWorkflowDetail(dba.page, workflowId)
      await waitForWorkflowAction(dba.page, 'workflow-approve')
      await dba.page.getByLabel('Remark').fill('Approved by DBA smoke test')
      await dba.page.getByTestId('workflow-approve').click()
      await assertReviewRowsPresent(dba.page)

      await waitForWorkflowAction(dba.page, 'workflow-execute-now')
      await dba.page.getByTestId('workflow-execute-now').click()
      await waitForWorkflowStatus(dba.page, 'Finished')
      await assertExecutionRowsPresent(dba.page)
    } finally {
      await closeRoleSessions(requester.context, dba?.context)
    }
  })

  test('requires selecting a compatible DDL executor when multiple executors are available', async ({ browser }) => {
    setSystemConfigValues({
      gh_ost: '/bin/echo',
      pt_osc: '/bin/echo',
    })

    const requester = await createRoleSession(browser, 'demo_requester')
    let dba: RoleSession | undefined

    try {
      const workflowName = uniqueWorkflowName('ddl executor smoke')
      const ddlColumn = `executor_${Date.now().toString(36).slice(-8)}`

      await requester.page.goto('/workflows/ddl/new')
      await requester.page.getByTestId('workflow-name').fill(workflowName)
      await requester.page.getByTestId('workflow-group').selectOption({ label: 'Demo Workflow Single Stage' })
      await requester.page.getByTestId('workflow-instance').selectOption({ label: 'demo-mysql-workflow / MYSQL' })
      await requester.page.getByTestId('workflow-db').selectOption('demo_orders')
      await fillSqlEditor(
        requester.page,
        'workflow-sql-editor',
        `ALTER TABLE customers ADD COLUMN ${ddlColumn} VARCHAR(16) NOT NULL DEFAULT 'bronze' COMMENT 'Executor smoke column';`,
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
      await openWorkflowDetail(dba.page, workflowId)
      await waitForWorkflowAction(dba.page, 'workflow-approve')
      await dba.page.getByLabel('Remark').fill('Approved for executor smoke test')
      await dba.page.getByTestId('workflow-approve').click()

      await waitForWorkflowAction(dba.page, 'workflow-execute-now')
      const executorSelect = dba.page.getByTestId('workflow-ddl-executor')
      await expect(executorSelect).toBeVisible()
      await expect(executorSelect.locator('option')).toHaveCount(4)
      await expect(dba.page.getByTestId('workflow-execute-now')).toBeDisabled()

      await executorSelect.selectOption('gh-ost')
      await expect(dba.page.getByTestId('workflow-execute-now')).toBeEnabled()
      await dba.page.getByTestId('workflow-execute-now').click()

      await waitForWorkflowStatus(dba.page, 'Finished')
      await assertExecutionRowsPresent(dba.page)
      await expect(
        dba.page.getByText('Workflow execution started (executor: gh-ost)'),
      ).toBeVisible()
    } finally {
      await closeRoleSessions(requester.context, dba?.context)
    }
  })

  test('runs a DML multi-stage workflow through PM and DBA approvals', async ({ browser }) => {
    const requester = await createRoleSession(browser, 'demo_requester')
    let pm: RoleSession | undefined
    let dba: RoleSession | undefined

    try {
      const workflowName = uniqueWorkflowName('dml smoke')

      await requester.page.goto('/workflows/dml/new')
      await requester.page.getByTestId('workflow-name').fill(workflowName)
      await requester.page.getByTestId('workflow-group').selectOption({ label: 'Demo Workflow Multi Stage' })
      await expect(requester.page.getByTestId('workflow-approval-preview')).toHaveText('PM -> DBA')
      await requester.page.getByTestId('workflow-instance').selectOption({ label: 'demo-mysql-workflow / MYSQL' })
      await requester.page.getByTestId('workflow-db').selectOption('demo_orders')
      await fillSqlEditor(
        requester.page,
        'workflow-sql-editor',
        "UPDATE orders SET order_status = 'processing' WHERE customer_email = 'noah@example.com';",
      )

      await requester.page.getByRole('button', { name: 'SQL check' }).click()
      await expect(
        requester.page.getByText('This check is current and the SQL is classified as DML.'),
      ).toBeVisible()

      await clickAndAcceptDialogIfPresent(requester.page, () =>
        requester.page.getByTestId('workflow-submit').click(),
      )
      await requester.page.waitForURL(/\/workflows\/\d+$/)
      await expect(requester.page.getByTestId('workflow-detail-refresh')).toBeVisible()
      const workflowId = workflowIdFromUrl(requester.page)

      pm = await createRoleSession(browser, 'demo_pm')
      await openWorkflowDetail(pm.page, workflowId)
      await waitForWorkflowAction(pm.page, 'workflow-approve')
      await pm.page.getByLabel('Remark').fill('Approved by PM smoke test')
      await pm.page.getByTestId('workflow-approve').click()

      dba = await createRoleSession(browser, 'demo_dba')
      await openWorkflowDetail(dba.page, workflowId)
      await waitForWorkflowAction(dba.page, 'workflow-approve')
      await dba.page.getByLabel('Remark').fill('Approved by DBA smoke test')
      await dba.page.getByTestId('workflow-approve').click()
      await assertReviewRowsPresent(dba.page)

      await waitForWorkflowAction(dba.page, 'workflow-execute-now')
      await dba.page.getByTestId('workflow-execute-now').click()
      await waitForWorkflowStatus(dba.page, 'Finished')
      await assertExecutionRowsPresent(dba.page)
    } finally {
      await closeRoleSessions(requester.context, pm?.context, dba?.context)
    }
  })

  test('runs an export workflow through approval, execution, and download', async ({ browser }, testInfo) => {
    const requester = await createRoleSession(browser, 'demo_requester')
    let dba: RoleSession | undefined

    try {
      const workflowName = uniqueWorkflowName('export smoke')

      await requester.page.goto('/workflows/export/new')
      await requester.page.getByTestId('export-workflow-name').fill(workflowName)
      await requester.page.getByTestId('export-workflow-group').selectOption({ label: 'Demo Workflow Single Stage' })
      await expect(requester.page.getByTestId('export-approval-preview')).toHaveText('DBA')
      await requester.page.getByTestId('export-workflow-instance').selectOption({ label: 'demo-mysql-workflow / MYSQL' })
      await requester.page.getByTestId('export-workflow-db').selectOption('demo_billing')
      await requester.page.getByTestId('export-format').selectOption('csv')
      await fillSqlEditor(
        requester.page,
        'export-sql-editor',
        'SELECT invoice_number, invoice_status, amount_due FROM invoices ORDER BY invoice_number;',
      )

      await requester.page.getByRole('button', { name: 'Validate export' }).click()
      await expect(requester.page.getByText('Validation summary')).toBeVisible()
      await expect(requester.page.getByText('Ready')).toBeVisible()

      await clickAndAcceptDialogIfPresent(requester.page, () =>
        requester.page.getByTestId('export-submit').click(),
      )
      await requester.page.waitForURL(/\/workflows\/\d+$/)
      await expect(requester.page.getByTestId('workflow-detail-refresh')).toBeVisible()
      const workflowId = workflowIdFromUrl(requester.page)

      dba = await createRoleSession(browser, 'demo_dba')
      await openWorkflowDetail(dba.page, workflowId)
      await waitForWorkflowAction(dba.page, 'workflow-approve')
      await dba.page.getByLabel('Remark').fill('Approved by DBA smoke test')
      await dba.page.getByTestId('workflow-approve').click()

      await waitForWorkflowAction(dba.page, 'workflow-execute-now')
      await dba.page.getByTestId('workflow-execute-now').click()
      await waitForWorkflowStatus(dba.page, 'Finished')
      await waitForWorkflowAction(dba.page, 'workflow-download-export')

      const downloadPromise = dba.page.waitForEvent('download')
      await dba.page.getByTestId('workflow-download-export').click()
      const download = await downloadPromise
      const outputPath = testInfo.outputPath(download.suggestedFilename())

      await download.saveAs(outputPath)

      const content = await readFile(outputPath, 'utf8')
      expect(content).toContain('invoice_number')
      expect(content).toContain('INV-1001')
      expect(content).toContain('invoice_status')
    } finally {
      await closeRoleSessions(requester.context, dba?.context)
    }
  })
})
