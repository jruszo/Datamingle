import { describe, expect, it } from 'vitest'

import {
  getFeatureModules,
  getFirstVisibleSettingsItem,
  getNavigationItems,
  getVisibleNavigationItems,
} from '@/app/feature-registry'
import type { CurrentUserContext } from '@/lib/api'

function buildUser(overrides: Partial<CurrentUserContext> = {}): CurrentUserContext {
  return {
    id: 1,
    username: 'tester',
    display: 'Test User',
    email: 'tester@example.com',
    avatar_url: '',
    is_workos_managed: false,
    is_superuser: false,
    is_staff: false,
    is_active: true,
    groups: [],
    resource_groups: [],
    permissions: [],
    ...overrides,
  }
}

describe('feature registry', () => {
  it('loads the community modules and no enterprise modules by default', () => {
    const ids = getFeatureModules().map((module) => module.id)

    expect(ids).toEqual([
      'auth',
      'dashboard',
      'reports',
      'inventory',
      'agents',
      'instance-operations',
      'workflows',
      'archives',
      'queries',
      'permissions',
      'audit',
      'mailbox',
      'settings',
    ])
  })

  it('sorts primary navigation items by manifest order', () => {
    const labels = getNavigationItems('primary').map((item) => item.label)

    expect(labels).toEqual([
      'Dashboard',
      'Inventory',
      'Data Dictionary',
      'Agents',
      'Instance Databases',
      'Instance Accounts',
      'Parameters',
      'Diagnostics',
      'Workflows',
      'Archives',
      'Queries',
      'Permission Management',
      'Reports',
      'Audit',
      'Profile',
    ])
  })

  it('filters primary navigation items by access rules', () => {
    const labels = getVisibleNavigationItems(
      'primary',
      buildUser({ permissions: ['sql.menu_instance', 'sql.menu_archive'] }),
    ).map((item) => item.label)

    expect(labels).toContain('Inventory')
    expect(labels).toContain('Archives')
    expect(labels).not.toContain('Permission Management')
  })

  it('resolves the first settings route from visible settings entries', () => {
    expect(
      getFirstVisibleSettingsItem(buildUser({ is_staff: true }))?.to,
    ).toBe('/settings/system')

    expect(
      getFirstVisibleSettingsItem(
        buildUser({ permissions: ['sql.menu_system', 'auth.view_group'] }),
      )?.to,
    ).toBe('/settings/groups')

    expect(
      getFirstVisibleSettingsItem(
        buildUser({ permissions: ['sql.menu_instance'] }),
      )?.to,
    ).toBe('/settings/instance-tags')

    expect(getFirstVisibleSettingsItem(buildUser())).toBeNull()
  })
})
