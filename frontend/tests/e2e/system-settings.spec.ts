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
})
