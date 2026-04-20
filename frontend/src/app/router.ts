import { createRouter, createWebHistory } from 'vue-router'

import {
  ACCESS_TOKEN_KEY,
  AuthSessionExpiredError,
  REFRESH_TOKEN_KEY,
  clearStoredTokens,
  getUsableAccessToken,
} from '@/shared/auth/auth'
import { getFeatureRoutes, getFirstVisibleSettingsItem } from '@/app/feature-registry'
import { canAccessRequirement } from '@/shared/auth/access'
import { useAuthStore } from '@/stores/auth'
import type { CurrentUserContext } from '@/lib/api'

function isSessionExpiredError(error: unknown) {
  if (error instanceof AuthSessionExpiredError) {
    return true
  }

  if (
    typeof error === 'object'
    && error !== null
    && 'status' in error
    && error.status === 401
  ) {
    return true
  }

  return error instanceof Error && error.message.includes('(401)')
}

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: getFeatureRoutes(),
})

router.beforeEach(async (to) => {
  const authStore = useAuthStore()
  const accessToken = localStorage.getItem(ACCESS_TOKEN_KEY)
  const refreshToken = localStorage.getItem(REFRESH_TOKEN_KEY)
  const isPublicRoute = to.meta.access?.public === true

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
      const nextSettingsItem = getFirstVisibleSettingsItem(resolvedUser)
      if (nextSettingsItem) {
        return { path: nextSettingsItem.to }
      }

      return { name: 'home' }
    } catch (error) {
      if (!isSessionExpiredError(error)) {
        throw error
      }

      clearStoredTokens()
      authStore.clearTokens()
      return { name: 'login', query: { reason: 'expired' } }
    }
  }

  if (to.meta.access) {
    try {
      const resolvedUser = await ensureCurrentUser()
      if (!canAccessRequirement(resolvedUser, to.meta.access)) {
        return { name: 'home' }
      }
    } catch (error) {
      if (!isSessionExpiredError(error)) {
        throw error
      }

      clearStoredTokens()
      authStore.clearTokens()
      return { name: 'login', query: { reason: 'expired' } }
    }
  }

  return true
})

export default router
