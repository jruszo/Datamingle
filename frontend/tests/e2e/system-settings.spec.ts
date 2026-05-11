import { expect, test } from '@playwright/test'

import { closeRoleSessions, createRoleSession } from './support/workflow-helpers'

test.describe.serial('system settings', () => {
  test('blocks non-staff users from the system settings route', async ({ browser }) => {
    const requester = await createRoleSession(browser, 'demo_requester')

    try {
      await requester.page.goto('/settings/system')
      await requester.page.waitForURL((url) => !url.pathname.startsWith('/settings/system'))
      await expect(requester.page).toHaveURL(/\/$/)
      await expect(requester.page.getByTestId('settings-system-title')).toHaveCount(0)
      await expect(requester.page.getByRole('link', { name: 'System Settings' })).toHaveCount(0)
    } finally {
      await closeRoleSessions(requester.context)
    }
  })

  test('allows demo admin to save and reload system settings', async ({ browser }) => {
    const admin = await createRoleSession(browser, 'demo_admin')
    let originalTitleSuffix = ''

    try {
      await admin.page.goto('/settings/system')
      await expect(admin.page.getByTestId('settings-system-title')).toBeVisible()
      await expect(admin.page.getByRole('link', { name: 'System Settings' })).toBeVisible()
      await expect(admin.page.getByTestId('settings-save')).toBeEnabled()

      const titleSuffixField = admin.page.getByTestId('settings-field-custom_title_suffix')
      originalTitleSuffix = await titleSuffixField.inputValue()

      const suffix = `e2e-${Date.now().toString(36)}`

      await titleSuffixField.fill(suffix)
      await admin.page.getByTestId('settings-save').click()

      await expect(admin.page.getByText('System settings saved.')).toBeVisible()
      await admin.page.reload()
      await expect(admin.page.getByTestId('settings-system-title')).toBeVisible()
      await expect(admin.page.getByTestId('settings-save')).toBeEnabled()
      await expect(titleSuffixField).toHaveValue(suffix)

      await admin.page.getByTestId('settings-test-storage').click()
      await expect(admin.page.getByText('Storage connection test succeeded.')).toBeVisible()
    } finally {
      if (!admin.page.isClosed()) {
        await admin.page.goto('/settings/system')
        await expect(admin.page.getByTestId('settings-save')).toBeEnabled()
        await admin.page.getByTestId('settings-field-custom_title_suffix').fill(originalTitleSuffix)
        await admin.page.getByTestId('settings-save').click()
        await expect(admin.page.getByText('System settings saved.')).toBeVisible()
      }
      await closeRoleSessions(admin.context)
    }
  })

  test('allows demo admin to persist Celery settings', async ({ browser }) => {
    const admin = await createRoleSession(browser, 'demo_admin')
    const brokerUrl = 'redis://127.0.0.1:6379/5'
    const resultBackend = 'redis://127.0.0.1:6379/6'
    const queueName = 'celery-e2e-queue'
    let originalBrokerUrl = ''
    let originalResultBackend = ''
    let originalQueue = ''
    let originalSoftLimit = ''
    let originalHardLimit = ''

    try {
      await admin.page.goto('/settings/system')
      await expect(admin.page.getByTestId('settings-system-title')).toBeVisible()
      await expect(admin.page.getByTestId('settings-save')).toBeEnabled()
      await expect(admin.page.getByTestId('settings-field-task_backend')).toHaveCount(0)

      const brokerField = admin.page.getByTestId('settings-field-celery_broker_url')
      const resultBackendField = admin.page.getByTestId('settings-field-celery_result_backend')
      const queueField = admin.page.getByTestId('settings-field-celery_task_default_queue')
      const softLimitField = admin.page.getByTestId('settings-field-celery_task_soft_time_limit')
      const hardLimitField = admin.page.getByTestId('settings-field-celery_task_time_limit')

      await expect(brokerField).toBeVisible()
      await expect(resultBackendField).toBeVisible()
      await expect(queueField).toBeVisible()
      await expect(softLimitField).toBeVisible()
      await expect(hardLimitField).toBeVisible()

      originalBrokerUrl = await brokerField.inputValue()
      originalResultBackend = await resultBackendField.inputValue()
      originalQueue = await queueField.inputValue()
      originalSoftLimit = await softLimitField.inputValue()
      originalHardLimit = await hardLimitField.inputValue()

      await brokerField.fill(brokerUrl)
      await resultBackendField.fill(resultBackend)
      await queueField.fill(queueName)
      await softLimitField.fill('30')
      await hardLimitField.fill('60')
      await admin.page.getByTestId('settings-save').click()

      await expect(admin.page.getByText('System settings saved.')).toBeVisible()
      await admin.page.reload()
      await expect(admin.page.getByTestId('settings-field-task_backend')).toHaveCount(0)
      await expect(admin.page.getByTestId('settings-field-celery_broker_url')).toHaveValue(brokerUrl)
      await expect(admin.page.getByTestId('settings-field-celery_result_backend')).toHaveValue(resultBackend)
      await expect(admin.page.getByTestId('settings-field-celery_task_default_queue')).toHaveValue(queueName)
      await expect(admin.page.getByTestId('settings-field-celery_task_soft_time_limit')).toHaveValue('30')
      await expect(admin.page.getByTestId('settings-field-celery_task_time_limit')).toHaveValue('60')
    } finally {
      if (!admin.page.isClosed()) {
        await admin.page.goto('/settings/system')
        await expect(admin.page.getByTestId('settings-save')).toBeEnabled()
        await admin.page.getByTestId('settings-field-celery_broker_url').fill(originalBrokerUrl)
        await admin.page.getByTestId('settings-field-celery_result_backend').fill(originalResultBackend)
        await admin.page.getByTestId('settings-field-celery_task_default_queue').fill(originalQueue)
        await admin.page.getByTestId('settings-field-celery_task_soft_time_limit').fill(originalSoftLimit)
        await admin.page.getByTestId('settings-field-celery_task_time_limit').fill(originalHardLimit)
        await admin.page.getByTestId('settings-save').click()
        await expect(admin.page.getByText('System settings saved.')).toBeVisible()
      }
      await closeRoleSessions(admin.context)
    }
  })
})
