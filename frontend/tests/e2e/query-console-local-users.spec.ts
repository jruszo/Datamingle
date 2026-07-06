import { expect, test, type Page } from '@playwright/test'

import {
  captureE2EScreenshot,
  closeRoleSessions,
  createLocalUserSession,
  expectSqlEditorToContain,
  fillSqlEditor,
  seedE2EEnvironment,
} from './support/workflow-helpers'

const DEMO_QUERY_USER = 'demo-requester@datamingle.dev'
const BLOCKED_QUERY_USER = 'e2e-requester@datamingle.dev'
const QUERY_INSTANCE = 'demo-mysql-workflow'
// Mirrors QueryMetadataExplorer nodeTestId's kind-dbName-name-id composition.
// Update this prefix when nodeTestId or rawIdTestIdSegment changes.
const QUERY_DATABASE_NODE_PREFIX = 'query-console-node-select-database-demo-orders-demo-orders-id-'
const SQL_EDITOR = 'query-console-sql-editor'

function uniqueQueryMarker() {
  return `e2e_query_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`
}

async function openQueryConsole(page: Page) {
  await page.goto('/queries')
  await expect(page.getByTestId('query-console-page')).toBeVisible()
}

async function selectDemoOrdersDatabase(page: Page) {
  await page.getByTestId('query-console-instance-select').selectOption({ label: QUERY_INSTANCE })
  const databaseNode = page.locator(`[data-testid^="${QUERY_DATABASE_NODE_PREFIX}"]`)
  await expect(databaseNode).toHaveCount(1)
  await expect(databaseNode).toBeVisible()
  await databaseNode.click()
  await expect(page.getByTestId(SQL_EDITOR)).toBeVisible()
}

async function searchHistory(page: Page, marker: string) {
  await page.getByTestId('query-console-history-tab').click()
  await page.getByTestId('query-console-history-search').fill(marker)
  await page.getByTestId('query-console-history-search-submit').click()
  const row = page.getByTestId('query-console-history-row').filter({ hasText: marker })
  await expect(row).toBeVisible()
  return row
}

test.describe.serial('local user query console workflows', () => {
  test.beforeEach(async () => {
    await seedE2EEnvironment()
  })

  test('runs a demo query, favorites it, restores it, and reruns it from history', async ({
    browser,
  }, testInfo) => {
    const marker = uniqueQueryMarker()
    const favoriteAlias = `E2E favorite ${marker}`
    const sql = [
      `select '${marker}' as e2e_marker, email, full_name`,
      'from customers',
      "where email = 'ava@example.com'",
    ].join('\n')
    const requester = await createLocalUserSession(browser, DEMO_QUERY_USER)

    try {
      await openQueryConsole(requester.page)
      await selectDemoOrdersDatabase(requester.page)
      await fillSqlEditor(requester.page, SQL_EDITOR, sql)
      await requester.page.getByTestId('query-console-run-query').click()

      const resultPanel = requester.page.getByTestId('query-console-result-panel')
      await expect(resultPanel).toContainText('1 rows returned')
      await expect(resultPanel).toContainText(marker)
      await expect(resultPanel).toContainText('ava@example.com')
      await expect(resultPanel).toContainText('Ava Carter')
      await captureE2EScreenshot(requester.page, testInfo, 'query-console-result')

      let historyRow = await searchHistory(requester.page, marker)
      requester.page.once('dialog', async (dialog) => {
        expect(dialog.type()).toBe('prompt')
        await dialog.accept(favoriteAlias)
      })
      await historyRow.getByTestId('query-console-history-favorite').click()
      historyRow = await searchHistory(requester.page, marker)
      await expect(historyRow).toContainText(favoriteAlias)
      await captureE2EScreenshot(requester.page, testInfo, 'query-console-favorite-history')

      await requester.page.getByTestId('query-console-result-tab').first().click()
      await requester.page.getByTestId('query-console-common-query-select').selectOption({ label: favoriteAlias })
      await expectSqlEditorToContain(requester.page, SQL_EDITOR, marker)
      await captureE2EScreenshot(requester.page, testInfo, 'query-console-common-query-restored')

      historyRow = await searchHistory(requester.page, marker)
      await historyRow.getByTestId('query-console-history-rerun').click()
      await expectSqlEditorToContain(requester.page, SQL_EDITOR, marker)
      await requester.page.getByTestId('query-console-run-query').click()
      await expect(requester.page.getByTestId('query-console-result-panel')).toContainText(marker)
      await captureE2EScreenshot(requester.page, testInfo, 'query-console-history-rerun-result')
    } finally {
      await closeRoleSessions(requester.context)
    }
  })

  test('blocks a local user without query workspace permissions', async ({ browser }, testInfo) => {
    const requester = await createLocalUserSession(browser, BLOCKED_QUERY_USER)

    try {
      await openQueryConsole(requester.page)
      await expect(requester.page.getByTestId('query-console-no-access')).toBeVisible()
      await expect(requester.page.getByTestId('query-console-run-query')).toHaveCount(0)
      await captureE2EScreenshot(requester.page, testInfo, 'query-console-no-access')
    } finally {
      await closeRoleSessions(requester.context)
    }
  })
})
