import type { CurrentUserContext } from '@/lib/api'

export type AccessRequirement = {
  public?: boolean
  requiresSuperuser?: boolean
  requiresStaffAdmin?: boolean
  requiredPermissions?: string[]
  anyPermissions?: string[]
}

export function hasPermission(
  currentUser: CurrentUserContext | null,
  permission: string,
): boolean {
  if (currentUser?.is_superuser) {
    return true
  }

  if (!Array.isArray(currentUser?.permissions)) {
    return false
  }

  return currentUser.permissions.includes(permission)
}

export function hasAllPermissions(
  currentUser: CurrentUserContext | null,
  permissions: string[] = [],
): boolean {
  return permissions.every((permission) => hasPermission(currentUser, permission))
}

export function hasAnyPermission(
  currentUser: CurrentUserContext | null,
  permissions: string[] = [],
): boolean {
  return permissions.length === 0 || permissions.some((permission) => hasPermission(currentUser, permission))
}

export function canAccessStaffAdminSettings(
  currentUser: CurrentUserContext | null,
): boolean {
  return currentUser?.is_superuser === true || currentUser?.is_staff === true
}

export function canAccessRequirement(
  currentUser: CurrentUserContext | null,
  requirement?: AccessRequirement,
): boolean {
  if (!requirement || requirement.public === true) {
    return true
  }

  if (requirement.requiresSuperuser === true && currentUser?.is_superuser !== true) {
    return false
  }

  if (
    requirement.requiresStaffAdmin === true
    && !canAccessStaffAdminSettings(currentUser)
  ) {
    return false
  }

  if (!hasAllPermissions(currentUser, requirement.requiredPermissions)) {
    return false
  }

  if (!hasAnyPermission(currentUser, requirement.anyPermissions)) {
    return false
  }

  return true
}
