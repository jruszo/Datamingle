import type { CurrentUserContext } from '@/lib/api'
import authModule from '@/features/auth/manifest'
import dashboardModule from '@/features/dashboard/manifest'
import reportsModule from '@/features/reports/manifest'
import infrastructureModule from '@/features/infrastructure/manifest'
import inventoryModule from '@/features/inventory/manifest'
import agentsModule from '@/features/agents/manifest'
import instanceOperationsModule from '@/features/instance-operations/manifest'
import workflowsModule from '@/features/workflows/manifest'
import archivesModule from '@/features/archives/manifest'
import queriesModule from '@/features/queries/manifest'
import permissionsModule from '@/features/permissions/manifest'
import auditModule from '@/features/audit/manifest'
import settingsModule from '@/features/settings/manifest'
import mailboxModule from '@/features/mailbox/manifest'
import type { FeatureModule, FeatureNavigationItem, NavigationSection } from '@/app/feature-contract'
import { canAccessRequirement } from '@/shared/auth/access'
import enterpriseFeatureModules from '@enterprise-feature-modules'

const builtInFeatureModules: FeatureModule[] = [
  authModule,
  dashboardModule,
  reportsModule,
  infrastructureModule,
  inventoryModule,
  agentsModule,
  instanceOperationsModule,
  workflowsModule,
  archivesModule,
  queriesModule,
  permissionsModule,
  auditModule,
  mailboxModule,
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
    .map(sortNavigationChildren)
    .sort((left, right) => (left.order ?? 0) - (right.order ?? 0))
}

export function getVisibleNavigationItems(
  section: NavigationSection,
  currentUser: CurrentUserContext | null,
) {
  return getNavigationItems(section)
    .map((item) => filterNavigationItem(item, currentUser))
    .filter((item): item is FeatureNavigationItem => item !== null)
}

export function getFirstVisibleSettingsItem(currentUser: CurrentUserContext | null) {
  return getVisibleNavigationItems('settings', currentUser)[0] ?? null
}

export function matchesNavigationItem(
  item: FeatureNavigationItem,
  currentPath: string,
) {
  if (item.children?.some((child) => matchesNavigationItem(child, currentPath))) {
    return true
  }

  const targetPath = item.matchPrefix ?? item.to

  if (targetPath === '/') {
    return currentPath === '/'
  }

  return currentPath === targetPath || currentPath.startsWith(`${targetPath}/`)
}

function sortNavigationChildren(item: FeatureNavigationItem): FeatureNavigationItem {
  if (!item.children?.length) {
    return item
  }

  return {
    ...item,
    children: [...item.children]
      .map(sortNavigationChildren)
      .sort((left, right) => (left.order ?? 0) - (right.order ?? 0)),
  }
}

function filterNavigationItem(
  item: FeatureNavigationItem,
  currentUser: CurrentUserContext | null,
): FeatureNavigationItem | null {
  if (!canAccessRequirement(currentUser, item.access)) {
    return null
  }

  if (!item.children?.length) {
    return item
  }

  const children = item.children
    .map((child) => filterNavigationItem(child, currentUser))
    .filter((child): child is FeatureNavigationItem => child !== null)

  if (children.length === 0) {
    return null
  }

  return { ...item, children }
}
