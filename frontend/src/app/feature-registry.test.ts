import { describe, expect, it } from 'vitest'

import {
  getFeatureModules,
  getFeatureRoutes,
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
    is_superuser: false,
    is_staff: false,
    is_active: true,
    groups: [],
    teams: [],
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
      'dashboards',
      'reports',
      'infrastructure',
      'metrics',
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
      'Home',
      'Dashboards',
      'Nodes',
      'Cluster topology',
      'Metrics Explorer',
      'Instances',
      'Data Dictionary',
      'Accounts',
      'Parameters',
      'Diagnostics',
      'Query history',
      'Archives',
      'Workflow requests',
      'Policies',
      'Access requests',
      'Reports',
      'Audit',
    ])
  })

  it('filters primary navigation items by access rules', () => {
    const labels = getVisibleNavigationItems(
      'primary',
      buildUser({ permissions: ['sql.menu_instance', 'sql.menu_archive'] }),
    ).map((item) => item.label)

    expect(labels).toContain('Nodes')
    expect(labels).toContain('Instances')
    expect(labels).toContain('Archives')
    expect(labels).not.toContain('Access requests')
  })

  it('shows the merged data dictionary entry to database managers', () => {
    const labels = getVisibleNavigationItems(
      'primary',
      buildUser({ permissions: ['sql.menu_database'] }),
    ).map((item) => item.label)

    expect(labels).toContain('Data Dictionary')
    expect(labels).not.toContain('Databases')
  })

  it('keeps legacy database management redirects free of duplicate route metadata', () => {
    const redirectRoute = getFeatureRoutes().find(
      (route) => route.path === '/instance-operations/databases',
    )

    expect(redirectRoute?.redirect).toBe('/inventory/data-dictionary')
    expect(redirectRoute?.meta).toBeUndefined()
  })

  it('resolves the first settings route from visible settings entries', () => {
    expect(getFirstVisibleSettingsItem(buildUser({ is_staff: true }))?.to).toBe('/settings/system')

    expect(
      getFirstVisibleSettingsItem(
        buildUser({ permissions: ['sql.menu_system', 'auth.view_group'] }),
      )?.to,
    ).toBe('/settings/teams')

    expect(getFirstVisibleSettingsItem(buildUser({ permissions: ['sql.menu_instance'] }))?.to).toBe(
      '/settings/instance-tags',
    )

    expect(getFirstVisibleSettingsItem(buildUser())).toBeNull()
  })
})
