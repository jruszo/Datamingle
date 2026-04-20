import type { CurrentUserContext } from '@/lib/api'
import authModule from '@/features/auth/manifest'
import dashboardModule from '@/features/dashboard/manifest'
import reportsModule from '@/features/reports/manifest'
import inventoryModule from '@/features/inventory/manifest'
import workflowsModule from '@/features/workflows/manifest'
import archivesModule from '@/features/archives/manifest'
import queriesModule from '@/features/queries/manifest'
import permissionsModule from '@/features/permissions/manifest'
import settingsModule from '@/features/settings/manifest'
import type { FeatureModule, FeatureNavigationItem, NavigationSection } from '@/app/feature-contract'
import { canAccessRequirement } from '@/shared/auth/access'
import enterpriseFeatureModules from '@enterprise-feature-modules'

const builtInFeatureModules: FeatureModule[] = [
  authModule,
  dashboardModule,
  reportsModule,
  inventoryModule,
  workflowsModule,
  archivesModule,
  queriesModule,
  permissionsModule,
  settingsModule,
]

export function getFeatureModules() {
  return [...builtInFeatureModules, ...enterpriseFeatureModules]
}

export function getFeatureRoutes() {
  return getFeatureModules().flatMap((featureModule) => featureModule.routes)
}

export function getNavigationItems(section: NavigationSection) {
  return getFeatureModules()
    .flatMap((featureModule) => featureModule.navigation ?? [])
    .filter((item) => item.section === section)
    .sort((left, right) => (left.order ?? 0) - (right.order ?? 0))
}

export function getVisibleNavigationItems(
  section: NavigationSection,
  currentUser: CurrentUserContext | null,
) {
  return getNavigationItems(section).filter((item) =>
    canAccessRequirement(currentUser, item.access),
  )
}

export function getFirstVisibleSettingsItem(currentUser: CurrentUserContext | null) {
  return getVisibleNavigationItems('settings', currentUser)[0] ?? null
}

export function matchesNavigationItem(
  item: FeatureNavigationItem,
  currentPath: string,
) {
  const targetPath = item.matchPrefix ?? item.to

  if (targetPath === '/') {
    return currentPath === '/'
  }

  return currentPath === targetPath || currentPath.startsWith(`${targetPath}/`)
}
