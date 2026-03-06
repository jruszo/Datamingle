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
import LoginView from '@/views/LoginView.vue'
import ProfileView from '@/views/ProfileView.vue'
import QueriesView from '@/views/QueriesView.vue'
import ReportsView from '@/views/ReportsView.vue'
import SettingsView from '@/views/SettingsView.vue'
import WorkflowsView from '@/views/WorkflowsView.vue'

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    { path: '/', name: 'home', component: HomeView },
    { path: '/login', name: 'login', component: LoginView, meta: { public: true } },
    { path: '/workflows', name: 'workflows', component: WorkflowsView },
    { path: '/queries', name: 'queries', component: QueriesView },
    { path: '/reports', name: 'reports', component: ReportsView },
    { path: '/profile', name: 'profile', component: ProfileView },
    { path: '/settings', name: 'settings', component: SettingsView },
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
