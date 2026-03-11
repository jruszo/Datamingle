import { createRouter, createWebHistory } from 'vue-router'

import {
  ACCESS_TOKEN_KEY,
  AuthSessionExpiredError,
  REFRESH_TOKEN_KEY,
  clearStoredTokens,
  getUsableAccessToken,
} from '@/lib/auth'
import type { CurrentUserContext } from '@/lib/api'
import { useAuthStore } from '@/stores/auth'
import HomeView from '@/views/HomeView.vue'
import InventoryCreateView from '@/views/InventoryCreateView.vue'
import InventoryView from '@/views/InventoryView.vue'
import LoginView from '@/views/LoginView.vue'
import PermissionManagementView from '@/views/PermissionManagementView.vue'
import ProfileView from '@/views/ProfileView.vue'
import QueriesView from '@/views/QueriesView.vue'
import ReportsView from '@/views/ReportsView.vue'
import SettingsGroupsView from '@/views/SettingsGroupsView.vue'
import SettingsGroupDetailView from '@/views/SettingsGroupDetailView.vue'
import SettingsView from '@/views/SettingsView.vue'
import SettingsResourceGroupsView from '@/views/SettingsResourceGroupsView.vue'
import SettingsResourceGroupDetailView from '@/views/SettingsResourceGroupDetailView.vue'
import SettingsInstanceTagDetailView from '@/views/SettingsInstanceTagDetailView.vue'
import SettingsInstanceTagsView from '@/views/SettingsInstanceTagsView.vue'
import SettingsUserDetailView from '@/views/SettingsUserDetailView.vue'
import SettingsUsersView from '@/views/SettingsUsersView.vue'
import WorkflowsView from '@/views/WorkflowsView.vue'

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    { path: '/', name: 'home', component: HomeView, meta: { title: 'Dashboard' } },
    { path: '/login', name: 'login', component: LoginView, meta: { public: true, title: 'Login' } },
    { path: '/inventory', name: 'inventory', component: InventoryView, meta: { title: 'Inventory' } },
    { path: '/inventory/new', name: 'inventory-new', component: InventoryCreateView, meta: { title: 'Add Instance' } },
    { path: '/inventory/:instanceId', name: 'inventory-detail', component: InventoryCreateView, meta: { title: 'Edit Instance' } },
    { path: '/workflows', name: 'workflows', component: WorkflowsView, meta: { title: 'Workflows' } },
    { path: '/queries', name: 'queries', component: QueriesView, meta: { title: 'Queries' } },
    { path: '/permission-management', name: 'permission-management', component: PermissionManagementView, meta: { title: 'Permission Management' } },
    { path: '/reports', name: 'reports', component: ReportsView, meta: { title: 'Reports' } },
    { path: '/profile', name: 'profile', component: ProfileView, meta: { title: 'Profile' } },
    { path: '/settings', name: 'settings', component: SettingsView, meta: { title: 'Settings' } },
    { path: '/settings/instance-tags', name: 'settings-instance-tags', component: SettingsInstanceTagsView, meta: { title: 'Instance Tags', requiresInventoryAdmin: true } },
    { path: '/settings/instance-tags/new', name: 'settings-instance-tags-new', component: SettingsInstanceTagDetailView, meta: { title: 'Instance Tags', requiresInventoryAdmin: true } },
    { path: '/settings/instance-tags/:tagId', name: 'settings-instance-tags-detail', component: SettingsInstanceTagDetailView, meta: { title: 'Instance Tags', requiresInventoryAdmin: true } },
    { path: '/settings/users', name: 'settings-users', component: SettingsUsersView, meta: { title: 'User Management', requiresSuperuser: true } },
    { path: '/settings/users/new', name: 'settings-users-new', component: SettingsUserDetailView, meta: { title: 'User Management', requiresSuperuser: true } },
    { path: '/settings/users/:userId', name: 'settings-users-detail', component: SettingsUserDetailView, meta: { title: 'User Management', requiresSuperuser: true } },
    { path: '/settings/groups', name: 'settings-groups', component: SettingsGroupsView, meta: { title: 'Permission Groups' } },
    { path: '/settings/groups/new', name: 'settings-groups-new', component: SettingsGroupDetailView, meta: { title: 'Permission Groups' } },
    { path: '/settings/groups/:groupId', name: 'settings-groups-detail', component: SettingsGroupDetailView, meta: { title: 'Permission Groups' } },
    { path: '/settings/resource-groups', name: 'settings-resource-groups', component: SettingsResourceGroupsView, meta: { title: 'Resource Groups' } },
    { path: '/settings/resource-groups/new', name: 'settings-resource-groups-new', component: SettingsResourceGroupDetailView, meta: { title: 'Resource Groups' } },
    { path: '/settings/resource-groups/:groupId', name: 'settings-resource-groups-detail', component: SettingsResourceGroupDetailView, meta: { title: 'Resource Groups' } },
    { path: '/groups/management', redirect: { name: 'settings-groups' } },
    { path: '/groups/management/new', redirect: { name: 'settings-groups-new' } },
    { path: '/groups/management/:groupId', redirect: (to) => ({ name: 'settings-groups-detail', params: { groupId: to.params.groupId } }) },
  ],
})

function hasCurrentUserPermission(
  currentUser: CurrentUserContext | null,
  permission: string,
) {
  if (currentUser?.is_superuser) {
    return true
  }
  return currentUser?.permissions.includes(permission) ?? false
}

router.beforeEach(async (to) => {
  const authStore = useAuthStore()
  const accessToken = localStorage.getItem(ACCESS_TOKEN_KEY)
  const refreshToken = localStorage.getItem(REFRESH_TOKEN_KEY)
  const isPublicRoute = to.meta.public === true

  if (!accessToken && !refreshToken) {
    if (isPublicRoute) {
      return true
    }

    clearStoredTokens()
    authStore.clearTokens()
    return { name: 'login' }
  }

  try {
    await getUsableAccessToken(accessToken ?? '')
  } catch (error) {
    if (!isPublicRoute || error instanceof AuthSessionExpiredError) {
      clearStoredTokens()
      authStore.clearTokens()
      return { name: 'login', query: { reason: 'expired' } }
    }

    return true
  }

  if (to.name === 'login') {
    return { name: 'home' }
  }

  let currentUser: CurrentUserContext | null = null

  async function ensureCurrentUser() {
    if (currentUser) {
      return currentUser
    }
    currentUser = await authStore.loadCurrentUser()
    return currentUser
  }

  if (to.name === 'settings') {
    try {
      const resolvedUser = await ensureCurrentUser()
      if (hasCurrentUserPermission(resolvedUser, 'sql.menu_system')) {
        return { name: 'settings-groups' }
      }
      if (hasCurrentUserPermission(resolvedUser, 'sql.menu_instance')) {
        return { name: 'settings-instance-tags' }
      }
      return { name: 'home' }
    } catch {
      clearStoredTokens()
      authStore.clearTokens()
      return { name: 'login', query: { reason: 'expired' } }
    }
  }

  if (to.meta.requiresSuperuser === true || to.meta.requiresInventoryAdmin === true) {
    try {
      const resolvedUser = await ensureCurrentUser()
      if (to.meta.requiresSuperuser === true && !resolvedUser?.is_superuser) {
        return { name: 'home' }
      }
      if (
        to.meta.requiresInventoryAdmin === true &&
        !hasCurrentUserPermission(resolvedUser, 'sql.menu_instance')
      ) {
        return { name: 'home' }
      }
    } catch {
      clearStoredTokens()
      authStore.clearTokens()
      return { name: 'login', query: { reason: 'expired' } }
    }
  }

  return true
})

export default router
