import { createRouter, createWebHistory } from 'vue-router'
import HomeView from '@/views/HomeView.vue'
import LoginView from '@/views/LoginView.vue'
import QueriesView from '@/views/QueriesView.vue'
import ReportsView from '@/views/ReportsView.vue'
import SettingsView from '@/views/SettingsView.vue'
import WorkflowsView from '@/views/WorkflowsView.vue'

const ACCESS_TOKEN_KEY = 'archery.access_token'

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    { path: '/', name: 'home', component: HomeView },
    { path: '/login', name: 'login', component: LoginView, meta: { public: true } },
    { path: '/workflows', name: 'workflows', component: WorkflowsView },
    { path: '/queries', name: 'queries', component: QueriesView },
    { path: '/reports', name: 'reports', component: ReportsView },
    { path: '/settings', name: 'settings', component: SettingsView },
  ],
})

router.beforeEach((to) => {
  const isAuthenticated = Boolean(localStorage.getItem(ACCESS_TOKEN_KEY))
  const isPublicRoute = to.meta.public === true

  if (!isAuthenticated && !isPublicRoute) {
    return { name: 'login' }
  }

  if (isAuthenticated && to.name === 'login') {
    return { name: 'home' }
  }

  return true
})

export default router
