import { computed } from 'vue'

import { useAuthStore } from '@/stores/auth'

export function useAuthToken() {
  const authStore = useAuthStore()
  const accessToken = computed(() => authStore.accessToken)

  function requireAccessToken() {
    if (!authStore.accessToken) {
      throw new Error('Missing access token. Please login again.')
    }

    return authStore.accessToken
  }

  return {
    accessToken,
    requireAccessToken,
  }
}
