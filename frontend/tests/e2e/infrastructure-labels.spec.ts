import { expect, test } from '@playwright/test'

import { closeRoleSessions, createRoleSession } from './support/workflow-helpers'

test('saves and reloads node monitoring labels', async ({ browser }) => {
  const admin = await createRoleSession(browser, 'demo_admin')
  const labelName = `e2e_label_${Date.now().toString(36)}`

  try {
    await admin.page.goto('/infrastructure')
    await admin.page.getByText('demo-mysql-node', { exact: true }).click()
    await admin.page.getByRole('button', { name: 'Edit Node' }).click()
    await admin.page.getByTestId('monitoring-label-add').click()
    await admin.page.getByTestId('monitoring-label-name').last().fill(labelName)
    await admin.page.getByTestId('monitoring-label-value').last().fill('saved')

    const updateResponse = admin.page.waitForResponse(
      (response) =>
        response.request().method() === 'PATCH' &&
        /\/api\/v1\/infrastructure\/nodes\/\d+\/$/.test(response.url()),
    )
    await admin.page.getByTestId('node-save').click()
    const response = await updateResponse
    expect(response.ok()).toBe(true)
    expect((await response.json()).data.monitoring_labels[labelName]).toBe('saved')

    await expect(admin.page.getByText('Node updated.')).toBeVisible()
    await admin.page.getByRole('button', { name: 'Edit Node' }).click()
    await expect(admin.page.getByTestId('monitoring-label-name').last()).toHaveValue(labelName)
    await expect(admin.page.getByTestId('monitoring-label-value').last()).toHaveValue('saved')

    await admin.page.getByTestId('monitoring-label-remove').last().click()
    await admin.page.getByTestId('node-save').click()
    await expect(admin.page.getByText('Node updated.')).toBeVisible()
  } finally {
    await closeRoleSessions(admin.context)
  }
})
