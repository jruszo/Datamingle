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

  test('allows demo admin to switch the task backend to Celery and persist Celery settings', async ({ browser }) => {
    const admin = await createRoleSession(browser, 'demo_admin')
    let originalTaskBackend = 'django_q'
    let originalBrokerUrl = ''
    let originalResultBackend = ''
    let originalQueue = ''
    let originalSoftLimit = ''
    let originalHardLimit = ''

    try {
      await admin.page.goto('/settings/system')
      await expect(admin.page.getByTestId('settings-system-title')).toBeVisible()
      await expect(admin.page.getByTestId('settings-save')).toBeEnabled()

      const taskBackendField = admin.page.getByTestId('settings-field-task_backend')
      await expect(taskBackendField).toBeEnabled()
      originalTaskBackend = await taskBackendField.inputValue()

      if (originalTaskBackend === 'celery') {
        originalBrokerUrl = await admin.page.getByTestId('settings-field-celery_broker_url').inputValue()
        originalResultBackend = await admin.page.getByTestId('settings-field-celery_result_backend').inputValue()
        originalQueue = await admin.page.getByTestId('settings-field-celery_task_default_queue').inputValue()
        originalSoftLimit = await admin.page.getByTestId('settings-field-celery_task_soft_time_limit').inputValue()
        originalHardLimit = await admin.page.getByTestId('settings-field-celery_task_time_limit').inputValue()
      }

      await taskBackendField.selectOption('celery')
      await expect(admin.page.getByTestId('settings-field-celery_broker_url')).toBeVisible()
      await expect(admin.page.getByTestId('settings-field-celery_result_backend')).toBeVisible()
      await expect(admin.page.getByTestId('settings-field-celery_task_default_queue')).toBeVisible()
      await expect(admin.page.getByTestId('settings-field-celery_task_soft_time_limit')).toBeVisible()
      await expect(admin.page.getByTestId('settings-field-celery_task_time_limit')).toBeVisible()

      const brokerUrl = `redis://127.0.0.1:63${Date.now().toString().slice(-2)}/5`
      const resultBackend = `redis://127.0.0.1:63${Date.now().toString().slice(-2)}/6`
      const queueName = `celery-e2e-${Date.now().toString(36)}`

      await admin.page.getByTestId('settings-field-celery_broker_url').fill(brokerUrl)
      await admin.page.getByTestId('settings-field-celery_result_backend').fill(resultBackend)
      await admin.page.getByTestId('settings-field-celery_task_default_queue').fill(queueName)
      await admin.page.getByTestId('settings-field-celery_task_soft_time_limit').fill('30')
      await admin.page.getByTestId('settings-field-celery_task_time_limit').fill('60')
      await admin.page.getByTestId('settings-save').click()

      await expect(admin.page.getByText('System settings saved.')).toBeVisible()
      await admin.page.reload()
      await expect(taskBackendField).toHaveValue('celery')
      await expect(admin.page.getByTestId('settings-field-celery_broker_url')).toHaveValue(brokerUrl)
      await expect(admin.page.getByTestId('settings-field-celery_result_backend')).toHaveValue(resultBackend)
      await expect(admin.page.getByTestId('settings-field-celery_task_default_queue')).toHaveValue(queueName)
      await expect(admin.page.getByTestId('settings-field-celery_task_soft_time_limit')).toHaveValue('30')
      await expect(admin.page.getByTestId('settings-field-celery_task_time_limit')).toHaveValue('60')

      await taskBackendField.selectOption('django_q')
      await expect(admin.page.getByTestId('settings-field-celery_broker_url')).toHaveCount(0)
      await expect(admin.page.getByTestId('settings-field-celery_result_backend')).toHaveCount(0)
    } finally {
      if (!admin.page.isClosed()) {
        await admin.page.goto('/settings/system')
        await expect(admin.page.getByTestId('settings-save')).toBeEnabled()
        await admin.page.getByTestId('settings-field-task_backend').selectOption(originalTaskBackend)
        if (originalTaskBackend === 'celery') {
          await admin.page.getByTestId('settings-field-celery_broker_url').fill(originalBrokerUrl)
          await admin.page.getByTestId('settings-field-celery_result_backend').fill(originalResultBackend)
          await admin.page.getByTestId('settings-field-celery_task_default_queue').fill(originalQueue)
          await admin.page.getByTestId('settings-field-celery_task_soft_time_limit').fill(originalSoftLimit)
          await admin.page.getByTestId('settings-field-celery_task_time_limit').fill(originalHardLimit)
        }
        await admin.page.getByTestId('settings-save').click()
        await expect(admin.page.getByText('System settings saved.')).toBeVisible()
      }
      await closeRoleSessions(admin.context)
    }
  })
})
