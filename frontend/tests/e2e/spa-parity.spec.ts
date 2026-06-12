import { expect, test, type Route } from '@playwright/test'

import { closeRoleSessions, createRoleSession, seedLocalDemo } from './support/workflow-helpers'

function envelope(data: unknown, detail = 'ok') {
  return { detail, data }
}

function paginated(results: unknown[]) {
  return { count: results.length, next: null, previous: null, results }
}

test.describe.serial('SPA bootstrap parity surfaces', () => {
  test.beforeEach(() => {
    seedLocalDemo()
  })

  test('exposes migrated inventory, audit, and instance operation pages to admins', async ({
    browser,
  }) => {
    const admin = await createRoleSession(browser, 'demo_admin')

    try {
      const pages = [
        { path: '/inventory/data-dictionary', heading: 'Data Dictionary' },
        { path: '/audit', heading: 'Audit' },
        { path: '/instance-operations/databases', heading: 'Database Management' },
        { path: '/instance-operations/accounts', heading: 'Instance Accounts' },
        { path: '/instance-operations/parameters', heading: 'Parameter Settings' },
        { path: '/instance-operations/diagnostics', heading: 'Session Diagnostics' },
      ]

      for (const page of pages) {
        await admin.page.goto(page.path)
        await expect(
          admin.page.getByRole('heading', { name: page.heading, level: 2 }),
        ).toBeVisible()
      }

      const navigation = admin.page.getByRole('navigation')
      await expect(navigation.getByRole('link', { name: 'Data Dictionary' })).toBeVisible()
      await expect(navigation.getByRole('link', { name: 'Audit' })).toBeVisible()
      await expect(navigation.getByRole('link', { name: 'Databases' })).toBeVisible()
      await expect(navigation.getByRole('link', { name: 'Accounts' })).toBeVisible()
      await expect(navigation.getByRole('link', { name: 'Parameters' })).toBeVisible()
      await expect(navigation.getByRole('link', { name: 'Diagnostics' })).toBeVisible()
    } finally {
      await closeRoleSessions(admin.context)
    }
  })

  test('does not expose retired bootstrap feature routes through SPA navigation', async ({
    browser,
  }) => {
    const admin = await createRoleSession(browser, 'demo_admin')

    try {
      await admin.page.goto('/')
      const navigation = admin.page.getByRole('navigation')

      await expect(navigation.getByRole('link', { name: 'SQL Analysis' })).toHaveCount(0)
      await expect(navigation.getByRole('link', { name: 'Optimization Tools' })).toHaveCount(0)
      await expect(navigation.getByRole('link', { name: 'Slow Query Logs' })).toHaveCount(0)
      await expect(navigation.getByRole('link', { name: 'My2SQL' })).toHaveCount(0)
      await expect(navigation.getByRole('link', { name: 'SchemaSync' })).toHaveCount(0)
      await expect(navigation.getByRole('link', { name: 'Related Documentation' })).toHaveCount(0)
    } finally {
      await closeRoleSessions(admin.context)
    }
  })

  test('supports data dictionary browse, detail, and export states', async ({ browser }) => {
    const admin = await createRoleSession(browser, 'demo_admin')

    try {
      await admin.page.route('**/api/v1/instance/data-dictionary/instances/', async (route) => {
        await route.fulfill({
          contentType: 'application/json',
          body: JSON.stringify(
            envelope([
              {
                id: 101,
                instance_name: 'mock-mysql',
                db_type: 'mysql',
                label: 'mock-mysql (mysql)',
              },
            ]),
          ),
        })
      })
      await admin.page.route('**/api/v1/instance/data-dictionary/databases/**', async (route) => {
        await route.fulfill({
          contentType: 'application/json',
          body: JSON.stringify(envelope({ count: 1, result: ['appdb'] })),
        })
      })
      await admin.page.route('**/api/v1/instance/data-dictionary/tables/**', async (route) => {
        await route.fulfill({
          contentType: 'application/json',
          body: JSON.stringify(
            envelope({
              count: 1,
              result: [{ group: 'a', tables: [['accounts', 'Account table']] }],
            }),
          ),
        })
      })
      await admin.page.route('**/api/v1/instance/data-dictionary/table/**', async (route) => {
        await route.fulfill({
          contentType: 'application/json',
          body: JSON.stringify(
            envelope({
              meta_data: { column_list: ['table_name', 'table_rows'], rows: ['accounts', 12] },
              desc: { column_list: ['Column Name', 'Column Type'], rows: [['id', 'int']] },
              index: { column_list: ['Column Name', 'Index Name'], rows: [['id', 'PRIMARY']] },
              create_sql: [['accounts', 'CREATE TABLE accounts (id int)']],
            }),
          ),
        })
      })
      await admin.page.route('**/api/v1/instance/data-dictionary/export/**', async (route) => {
        await route.fulfill({
          status: 200,
          headers: {
            'content-type': 'text/html',
            'content-disposition': 'attachment; filename="mock-mysql_appdb.html"',
          },
          body: '<html><body>dictionary</body></html>',
        })
      })

      await admin.page.goto('/inventory/data-dictionary')
      await expect(
        admin.page.getByRole('heading', { name: 'Data Dictionary', level: 2 }),
      ).toBeVisible()
      await expect(admin.page.getByLabel('Instance')).toHaveValue('101')
      await expect(admin.page.getByLabel('Database')).toHaveValue('appdb')
      await expect(admin.page.getByRole('button', { name: /accounts/ })).toBeVisible()

      await admin.page.getByPlaceholder('Search tables or comments').fill('account')
      await expect(admin.page.getByRole('button', { name: /accounts/ })).toBeVisible()
      await admin.page.getByRole('button', { name: /accounts/ }).click()
      await expect(admin.page.getByRole('heading', { name: 'Columns' })).toBeVisible()
      await expect(admin.page.getByText('CREATE TABLE accounts')).toBeVisible()

      const downloadPromise = admin.page.waitForEvent('download')
      await admin.page.getByRole('button', { name: 'Export' }).click()
      const download = await downloadPromise
      expect(download.suggestedFilename()).toBe('mock-mysql_appdb.html')
      await expect(admin.page.getByText('Data dictionary export prepared.')).toBeVisible()
    } finally {
      await closeRoleSessions(admin.context)
    }
  })

  test('supports audit filters and audit view navigation', async ({ browser }) => {
    const admin = await createRoleSession(browser, 'demo_admin')
    const requestedUrls: string[] = []

    try {
      await admin.page.route('**/api/v1/audit/general/**', async (route) => {
        requestedUrls.push(route.request().url())
        await route.fulfill({
          contentType: 'application/json',
          body: JSON.stringify(
            envelope(
              paginated([
                {
                  user_id: 1,
                  user_name: 'demo_admin',
                  user_display: 'Demo Admin',
                  action: 'Login',
                  extra_info: 'SPA parity audit row',
                  action_time: '2026-04-24T12:00:00Z',
                },
              ]),
            ),
          ),
        })
      })
      await admin.page.route('**/api/v1/audit/query/**', async (route) => {
        requestedUrls.push(route.request().url())
        await route.fulfill({
          contentType: 'application/json',
          body: JSON.stringify(
            envelope(
              paginated([
                {
                  id: 7,
                  instance_name: 'mock-mysql',
                  db_name: 'appdb',
                  sqllog: 'select 1',
                  effect_row: 1,
                  cost_time: '1ms',
                  username: 'demo_admin',
                  user_display: 'Demo Admin',
                  priv_check: true,
                  hit_rule: false,
                  masking: false,
                  favorite: false,
                  alias: '',
                  create_time: '2026-04-24T12:01:00Z',
                },
              ]),
            ),
          ),
        })
      })
      await admin.page.route('**/api/v1/audit/sql-workflow/**', async (route) => {
        requestedUrls.push(route.request().url())
        await route.fulfill({
          contentType: 'application/json',
          body: JSON.stringify(
            envelope(
              paginated([
                {
                  id: 42,
                  workflow_name: 'Audit workflow row',
                  demand_url: '',
                  team_id: 1,
                  team_name: 'DBA',
                  instance_id: 101,
                  instance_name: 'mock-mysql',
                  db_name: 'appdb',
                  schema_name: '',
                  syntax_type: 1,
                  syntax_type_label: 'DDL',
                  is_backup: false,
                  engineer: 'demo_admin',
                  engineer_display: 'Demo Admin',
                  status: 'workflow_finish',
                  status_label: 'Finished',
                  run_date_start: null,
                  run_date_end: null,
                  create_time: '2026-04-24T12:02:00Z',
                  finish_time: null,
                  is_offline_export: 0,
                  export_format: null,
                },
              ]),
            ),
          ),
        })
      })

      await admin.page.goto('/audit')
      await expect(admin.page.getByText('SPA parity audit row')).toBeVisible()

      await admin.page.getByPlaceholder('Search audit records').fill('demo_admin')
      await expect
        .poll(() => requestedUrls.some((url) => url.includes('search=demo_admin')))
        .toBeTruthy()

      await admin.page.getByRole('button', { name: 'Query' }).click()
      await expect(admin.page.getByText('select 1')).toBeVisible()

      await admin.page.getByRole('button', { name: 'SQL Workflows' }).click()
      await admin.page.getByLabel('Status').selectOption('workflow_finish')
      await expect(admin.page.getByRole('link', { name: 'Audit workflow row' })).toBeVisible()
      await expect
        .poll(() => requestedUrls.some((url) => url.includes('status=workflow_finish')))
        .toBeTruthy()
    } finally {
      await closeRoleSessions(admin.context)
    }
  })

  test('supports key instance operation actions with deterministic API responses', async ({
    browser,
  }) => {
    const admin = await createRoleSession(browser, 'demo_admin')
    const calls: string[] = []

    try {
      await admin.page.route('**/api/v1/instance-operations/database/instances/', async (route) => {
        await route.fulfill({
          contentType: 'application/json',
          body: JSON.stringify(
            envelope([
              { id: 201, instance_name: 'ops-mysql', db_type: 'mysql', label: 'ops-mysql (mysql)' },
            ]),
          ),
        })
      })
      const handleDatabaseRoute = async (route: Route) => {
        calls.push(`database-${route.request().method()}`)
        if (route.request().method() === 'POST') {
          await route.fulfill({
            contentType: 'application/json',
            body: JSON.stringify(
              envelope(
                { id: 1, db_name: 'newdb', owner: 'demo_admin', remark: 'Created', saved: true },
                'Database created successfully.',
              ),
            ),
          })
          return
        }
        await route.fulfill({
          contentType: 'application/json',
          body: JSON.stringify(
            envelope({
              count: 1,
              results: [{ db_name: 'appdb', owner: '', remark: '', saved: false }],
            }),
          ),
        })
      }
      await admin.page.route('**/api/v1/instance-operations/database/', handleDatabaseRoute)
      await admin.page.route('**/api/v1/instance-operations/database/?*', handleDatabaseRoute)

      await admin.page.goto('/instance-operations/databases')
      await expect(
        admin.page.getByRole('heading', { name: 'Database Management', level: 2 }),
      ).toBeVisible()
      await admin.page.getByRole('button', { name: 'New database' }).click()
      await admin.page.getByPlaceholder('appdb').fill('newdb')
      await admin.page.getByPlaceholder('jane.doe').fill('demo_admin')
      await admin.page.getByRole('button', { name: /^Save$/ }).click()
      await expect(admin.page.getByText('Database "newdb" created.')).toBeVisible()

      await admin.page.route('**/api/v1/instance-operations/account/instances/', async (route) => {
        await route.fulfill({
          contentType: 'application/json',
          body: JSON.stringify(
            envelope([
              { id: 201, instance_name: 'ops-mysql', db_type: 'mysql', label: 'ops-mysql (mysql)' },
            ]),
          ),
        })
      })
      const handleAccountRoute = async (route: Route) => {
        calls.push(`account-${route.request().method()}`)
        if (route.request().method() === 'POST') {
          await route.fulfill({
            contentType: 'application/json',
            body: JSON.stringify(
              envelope(
                { id: 1, user: 'app_user', host: '%', saved: true },
                'Account created successfully.',
              ),
            ),
          })
          return
        }
        await route.fulfill({
          contentType: 'application/json',
          body: JSON.stringify(
            envelope({
              count: 1,
              results: [
                {
                  user: 'app',
                  host: '%',
                  user_host: '`app`@`%`',
                  privileges: ['SELECT'],
                  is_locked: 'N',
                  remark: '',
                  saved: true,
                },
              ],
            }),
          ),
        })
      }
      await admin.page.route('**/api/v1/instance-operations/account/', handleAccountRoute)
      await admin.page.route('**/api/v1/instance-operations/account/?*', handleAccountRoute)

      await admin.page.goto('/instance-operations/accounts')
      await admin.page.getByRole('button', { name: 'New account' }).click()
      await admin.page.getByPlaceholder('app_user').fill('app_user')
      await admin.page.getByPlaceholder('Required').fill('StrongPass123!')
      await admin.page.getByRole('button', { name: /^Save$/ }).click()
      await expect(admin.page.getByText('Account "app_user" created.')).toBeVisible()

      await admin.page.route('**/api/v1/instance-operations/param/instances/', async (route) => {
        await route.fulfill({
          contentType: 'application/json',
          body: JSON.stringify(
            envelope([
              { id: 201, instance_name: 'ops-mysql', db_type: 'mysql', label: 'ops-mysql (mysql)' },
            ]),
          ),
        })
      })
      await admin.page.route('**/api/v1/instance-operations/param/?**', async (route) => {
        await route.fulfill({
          contentType: 'application/json',
          body: JSON.stringify(
            envelope({
              count: 1,
              results: [
                {
                  variable_name: 'max_connections',
                  runtime_value: '100',
                  default_value: '151',
                  valid_values: '[1-100000]',
                  description: 'Maximum simultaneous connections',
                  editable: true,
                  configured: true,
                },
              ],
            }),
          ),
        })
      })
      await admin.page.route('**/api/v1/instance-operations/param/history/**', async (route) => {
        await route.fulfill({
          contentType: 'application/json',
          body: JSON.stringify(envelope({ count: 0, results: [] })),
        })
      })
      await admin.page.route('**/api/v1/instance-operations/param/edit/', async (route) => {
        calls.push('param-edit')
        await route.fulfill({
          contentType: 'application/json',
          body: JSON.stringify(envelope({}, 'Parameter updated successfully.')),
        })
      })

      await admin.page.goto('/instance-operations/parameters')
      await admin.page.getByRole('button', { name: 'Edit' }).click()
      await admin.page.getByLabel('Runtime value').fill('200')
      await admin.page.getByRole('button', { name: /^Save$/ }).click()
      await expect(admin.page.getByText('Parameter "max_connections" updated.')).toBeVisible()

      await admin.page.route(
        '**/api/v1/instance-operations/diagnostic/instances/',
        async (route) => {
          await route.fulfill({
            contentType: 'application/json',
            body: JSON.stringify(
              envelope([
                {
                  id: 201,
                  instance_name: 'ops-mysql',
                  db_type: 'mysql',
                  label: 'ops-mysql (mysql)',
                },
              ]),
            ),
          })
        },
      )
      await admin.page.route(
        '**/api/v1/instance-operations/diagnostic/processes/**',
        async (route) => {
          await route.fulfill({
            contentType: 'application/json',
            body: JSON.stringify(
              envelope({
                count: 1,
                results: [
                  {
                    id: 101,
                    user: 'app',
                    host: '127.0.0.1',
                    db: 'appdb',
                    command: 'Query',
                    time: 3,
                    state: 'executing',
                    info: 'select 1',
                  },
                ],
              }),
            ),
          })
        },
      )
      await admin.page.route(
        '**/api/v1/instance-operations/diagnostic/kill/preview/',
        async (route) => {
          calls.push('kill-preview')
          await route.fulfill({
            contentType: 'application/json',
            body: JSON.stringify(envelope({ kill_sql: 'kill 101;' })),
          })
        },
      )
      await admin.page.route('**/api/v1/instance-operations/diagnostic/kill/', async (route) => {
        calls.push('kill')
        await route.fulfill({
          contentType: 'application/json',
          body: JSON.stringify(envelope({}, 'Sessions terminated successfully.')),
        })
      })

      admin.page.on('dialog', (dialog) => dialog.accept())
      await admin.page.goto('/instance-operations/diagnostics')
      await admin.page.getByRole('checkbox').check()
      await admin.page.getByRole('button', { name: 'Preview kill' }).click()
      await expect(admin.page.getByText('kill 101;')).toBeVisible()
      await admin.page.getByRole('button', { name: 'Kill sessions' }).click()
      await expect(admin.page.getByText('Selected sessions terminated.')).toBeVisible()

      expect(calls).toContain('database-POST')
      expect(calls).toContain('account-POST')
      expect(calls).toContain('param-edit')
      expect(calls).toContain('kill-preview')
      expect(calls).toContain('kill')
    } finally {
      await closeRoleSessions(admin.context)
    }
  })
})
