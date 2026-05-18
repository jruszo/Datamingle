import { describe, expect, it } from 'vitest'

import { canAccessRequirement, hasPermission } from '@/shared/auth/access'
import type { CurrentUserContext } from '@/lib/api'

function buildUser(overrides: Partial<CurrentUserContext> = {}): CurrentUserContext {
  return {
    id: 1,
    username: 'tester',
    display: 'Test User',
    email: 'tester@example.com',
    avatar_url: '',
    is_workos_managed: false,
    is_directory_managed: false,
    is_superuser: false,
    is_staff: false,
    is_active: true,
    groups: [],
    resource_groups: [],
    permissions: [],
    ...overrides,
  }
}

describe('access helpers', () => {
  it('treats superusers as having every permission', () => {
    expect(
      hasPermission(
        buildUser({ is_superuser: true }),
        'sql.menu_instance',
      ),
    ).toBe(true)
  })

  it('returns false for missing permissions on non-superusers', () => {
    expect(
      hasPermission(
        buildUser({ is_superuser: false }),
        'some.missing_permission',
      ),
    ).toBe(false)
  })

  it('requires all required permissions and one matching any-permission', () => {
    const user = buildUser({
      permissions: ['sql.menu_system', 'auth.view_group'],
    })

    expect(
      canAccessRequirement(user, {
        requiredPermissions: ['sql.menu_system'],
        anyPermissions: ['auth.view_group', 'sql.view_resourcegroup'],
      }),
    ).toBe(true)

    expect(
      canAccessRequirement(user, {
        requiredPermissions: ['sql.menu_system', 'sql.view_resourcegroup'],
      }),
    ).toBe(false)
  })

  it('allows requiresSuperuser routes only for superusers', () => {
    expect(
      canAccessRequirement(buildUser({ is_superuser: true }), {
        requiresSuperuser: true,
      }),
    ).toBe(true)

    expect(
      canAccessRequirement(buildUser(), {
        requiresSuperuser: true,
      }),
    ).toBe(false)
  })

  it('supports anyPermissions without requiredPermissions', () => {
    expect(
      canAccessRequirement(
        buildUser({ permissions: ['sql.menu_archive'] }),
        { anyPermissions: ['sql.menu_archive', 'sql.menu_instance'] },
      ),
    ).toBe(true)

    expect(
      canAccessRequirement(
        buildUser({ permissions: ['sql.menu_query'] }),
        { anyPermissions: ['sql.menu_archive', 'sql.menu_instance'] },
      ),
    ).toBe(false)
  })

  it('allows empty or undefined access requirements', () => {
    const user = buildUser()

    expect(canAccessRequirement(user, undefined)).toBe(true)
    expect(canAccessRequirement(user, {})).toBe(true)
  })

  it('allows staff-admin routes only for staff admins or superusers', () => {
    expect(
      canAccessRequirement(buildUser({ is_staff: true }), {
        requiresStaffAdmin: true,
      }),
    ).toBe(true)

    expect(
      canAccessRequirement(buildUser(), {
        requiresStaffAdmin: true,
      }),
    ).toBe(false)
  })
})
