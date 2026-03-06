import { computed, ref } from 'vue'
import { defineStore } from 'pinia'

import { ACCESS_TOKEN_KEY, REFRESH_TOKEN_KEY, setStoredTokens } from '@/lib/auth'
import { fetchCurrentUserContext, type CurrentUserContext } from '@/lib/api'

export const useAuthStore = defineStore('auth', () => {
  const accessToken = ref(localStorage.getItem(ACCESS_TOKEN_KEY) || '')
  const refreshToken = ref(localStorage.getItem(REFRESH_TOKEN_KEY) || '')
  const currentUser = ref<CurrentUserContext | null>(null)
  const currentUserRequest = ref<Promise<CurrentUserContext> | null>(null)

  const isAuthenticated = computed(() => Boolean(accessToken.value))

  function setTokens(access: string, refresh: string) {
    syncTokens(access, refresh)
    currentUser.value = null
    currentUserRequest.value = null
    setStoredTokens(access, refresh)
  }

  function syncTokens(access: string, refresh: string) {
    accessToken.value = access
    refreshToken.value = refresh
  }

  function clearTokens() {
    accessToken.value = ''
    refreshToken.value = ''
    currentUser.value = null
    currentUserRequest.value = null
    localStorage.removeItem(ACCESS_TOKEN_KEY)
    localStorage.removeItem(REFRESH_TOKEN_KEY)
  }

  function setCurrentUser(user: CurrentUserContext | null) {
    currentUser.value = user
  }

  async function loadCurrentUser(force = false) {
    if (!accessToken.value) {
      currentUser.value = null
      currentUserRequest.value = null
      return null
    }

    if (currentUser.value && !force) {
      return currentUser.value
    }

    if (currentUserRequest.value && !force) {
      return currentUserRequest.value
    }

    const request = fetchCurrentUserContext(accessToken.value)
      .then((user) => {
        currentUser.value = user
        return user
      })
      .finally(() => {
        currentUserRequest.value = null
      })

    currentUserRequest.value = request
    return request
  }

  return {
    accessToken,
    refreshToken,
    currentUser,
    isAuthenticated,
    setTokens,
    syncTokens,
    clearTokens,
    setCurrentUser,
    loadCurrentUser,
  }
})
