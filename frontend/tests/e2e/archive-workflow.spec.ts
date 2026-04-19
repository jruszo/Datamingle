import { expect, test } from '@playwright/test'

import {
  archiveIdFromUrl,
  closeRoleSessions,
  createRoleSession,
  openArchiveDetail,
  seedLocalDemo,
  uniqueWorkflowName,
  waitForArchiveAction,
} from './support/workflow-helpers'

type RoleSession = Awaited<ReturnType<typeof createRoleSession>>

test.describe.serial('archive workflow', () => {
  test.beforeEach(() => {
    seedLocalDemo()
  })

  test('requires auth for the archive page and shows navigation for archive users', async ({ browser, page }) => {
    await page.goto('/archives')
    await page.waitForURL(/\/login$/)

    const requester = await createRoleSession(browser, 'demo_requester')

    try {
      await expect(requester.page.getByRole('link', { name: 'Archives' })).toBeVisible()
      await requester.page.goto('/archives')
      await expect(
        requester.page.getByRole('main').getByRole('heading', { name: 'Archives' }),
      ).toBeVisible()
    } finally {
      await closeRoleSessions(requester.context)
    }
  })

  test('creates, approves, and queues a one-time archive workflow from detail', async ({ browser }) => {
    const requester = await createRoleSession(browser, 'demo_requester')
    let dba: RoleSession | undefined

    try {
      const archiveName = uniqueWorkflowName('one-time archive')

      await requester.page.goto('/archives/new')
      await requester.page.getByTestId('archive-title').fill(archiveName)
      await requester.page.getByTestId('archive-group').selectOption({ label: 'Demo Workflow Single Stage' })
      await expect(requester.page.locator('body')).toContainText('DBA')
      await requester.page.getByTestId('archive-instance').selectOption({ label: 'demo-mysql-workflow · mysql' })
      await requester.page.getByTestId('archive-db').selectOption('demo_orders')
      await requester.page.getByTestId('archive-table').selectOption('orders')
      await requester.page.getByTestId('archive-condition').fill('1 = 0')
      await requester.page.getByTestId('archive-method').selectOption('dml')
      await requester.page.getByRole('button', { name: 'Submit archive' }).click()

      await requester.page.waitForURL(/\/archives\/\d+/)
      const archiveId = archiveIdFromUrl(requester.page)

      dba = await createRoleSession(browser, 'demo_dba')
      await openArchiveDetail(dba.page, archiveId)
      await waitForArchiveAction(dba.page, 'archive-approve')
      await dba.page.locator('textarea').first().fill('Approved by archive smoke test')
      await dba.page.getByTestId('archive-approve').click()

      await waitForArchiveAction(dba.page, 'archive-run-now')
      await dba.page.getByTestId('archive-run-now').click()
      await expect(dba.page.locator('body')).toContainText('Archive execution queued.')
      await expect(dba.page.locator('body')).toContainText('Archive Queued')
    } finally {
      await closeRoleSessions(requester.context, dba?.context)
    }
  })

  test('creates a scheduled archive, toggles schedule state, and preserves list filters on back', async ({ browser }) => {
    const requester = await createRoleSession(browser, 'demo_requester')
    let dba: RoleSession | undefined

    try {
      const archiveName = uniqueWorkflowName('scheduled archive')

      await requester.page.goto('/archives/new')
      await requester.page.getByTestId('archive-title').fill(archiveName)
      await requester.page.getByTestId('archive-execution-mode').selectOption('scheduled')
      await requester.page.getByTestId('archive-group').selectOption({ label: 'Demo Workflow Single Stage' })
      await requester.page.getByTestId('archive-instance').selectOption({ label: 'demo-mysql-workflow · mysql' })
      await requester.page.getByTestId('archive-db').selectOption('demo_orders')
      await requester.page.getByTestId('archive-table').selectOption('orders')
      await requester.page.getByTestId('archive-condition').fill('1 = 0')
      await requester.page.getByTestId('archive-method').selectOption('dml')
      await requester.page.getByTestId('archive-schedule-frequency').selectOption('daily')
      await requester.page.getByTestId('archive-schedule-time').fill('02:00')
      await requester.page.getByRole('button', { name: 'Submit archive' }).click()

      await requester.page.waitForURL(/\/archives\/\d+/)
      const archiveId = archiveIdFromUrl(requester.page)

      dba = await createRoleSession(browser, 'demo_dba')
      await openArchiveDetail(dba.page, archiveId)
      await waitForArchiveAction(dba.page, 'archive-approve')
      await dba.page.locator('textarea').first().fill('Approved scheduled archive')
      await dba.page.getByTestId('archive-approve').click()

      await waitForArchiveAction(dba.page, 'archive-disable')
      await dba.page.getByTestId('archive-disable').click()
      await waitForArchiveAction(dba.page, 'archive-enable')
      await dba.page.getByTestId('archive-enable').click()
      await waitForArchiveAction(dba.page, 'archive-disable')

      await requester.page.goto('/archives')
      await requester.page.getByTestId('archive-filter-search').fill(archiveName)
      await requester.page.getByTestId('archive-filter-mode').selectOption('scheduled')
      await requester.page.getByRole('button', { name: 'Apply filters' }).click()

      await requester.page.waitForURL((url) => (
        url.pathname === '/archives'
        && url.searchParams.get('search') === archiveName
        && url.searchParams.get('executionMode') === 'scheduled'
      ))
      await requester.page.getByTestId(`archive-list-item-${archiveId}`).click()
      await requester.page.waitForURL(/\/archives\/\d+\?returnTo=/)
      await requester.page.getByTestId('archive-detail-back').click()

      await requester.page.waitForURL((url) => (
        url.pathname === '/archives'
        && url.searchParams.get('search') === archiveName
        && url.searchParams.get('executionMode') === 'scheduled'
      ))
      await expect(requester.page.getByTestId('archive-filter-search')).toHaveValue(archiveName)
      await expect(requester.page.getByTestId('archive-filter-mode')).toHaveValue('scheduled')
      await expect(requester.page.getByTestId(`archive-list-item-${archiveId}`)).toBeVisible()
    } finally {
      await closeRoleSessions(requester.context, dba?.context)
    }
  })

  test('shows pt-archiver as a mysql-only archive method choice', async ({ browser }) => {
    const requester = await createRoleSession(browser, 'demo_requester')

    try {
      await requester.page.goto('/archives/new')
      await requester.page.getByTestId('archive-group').selectOption({ label: 'Demo Workflow Single Stage' })

      await requester.page.getByTestId('archive-instance').selectOption({ label: 'demo-mysql-workflow · mysql' })
      await expect(requester.page.getByTestId('archive-method').locator('option')).toHaveCount(2)
      await expect(requester.page.getByTestId('archive-method')).toContainText('pt-archiver')

      await requester.page.getByTestId('archive-instance').selectOption({ label: 'demo-pgsql-workflow · pgsql' })
      await expect(requester.page.getByTestId('archive-method').locator('option')).toHaveCount(1)
      await expect(requester.page.getByTestId('archive-method')).not.toContainText('pt-archiver')
    } finally {
      await closeRoleSessions(requester.context)
    }
  })
})
