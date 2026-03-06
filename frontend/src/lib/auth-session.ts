import type { Pinia } from 'pinia'
import type { Router } from 'vue-router'

import { AUTH_TOKENS_UPDATED_EVENT, AUTH_UNAUTHORIZED_EVENT } from '@/lib/auth'
import { useAuthStore } from '@/stores/auth'

let authSessionHandlingInstalled = false

export function installAuthSessionHandling(pinia: Pinia, router: Router) {
  if (authSessionHandlingInstalled) {
    return
  }

  authSessionHandlingInstalled = true

  window.addEventListener(AUTH_TOKENS_UPDATED_EVENT, (event) => {
    const authStore = useAuthStore(pinia)
    const detail = (event as CustomEvent<{ access: string; refresh: string }>).detail

    authStore.syncTokens(detail.access, detail.refresh)
  })

  window.addEventListener(AUTH_UNAUTHORIZED_EVENT, () => {
    const authStore = useAuthStore(pinia)

    authStore.clearTokens()

    if (router.currentRoute.value.name !== 'login') {
      void router.replace({ name: 'login', query: { reason: 'expired' } })
    }
  })
}
