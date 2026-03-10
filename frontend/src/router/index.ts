import { createRouter, createWebHistory } from 'vue-router'

import {
  ACCESS_TOKEN_KEY,
  AuthSessionExpiredError,
  REFRESH_TOKEN_KEY,
  clearStoredTokens,
  getUsableAccessToken,
} from '@/lib/auth'
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
import SettingsResourceGroupsView from '@/views/SettingsResourceGroupsView.vue'
import SettingsResourceGroupDetailView from '@/views/SettingsResourceGroupDetailView.vue'
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
    { path: '/settings', redirect: { name: 'settings-groups' } },
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

  return true
})

export default router
