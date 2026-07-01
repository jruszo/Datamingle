<script setup lang="ts">
import { computed, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'

import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { loginWithPassword } from '../api'
import { useAuthStore } from '@/stores/auth'

const route = useRoute()
const router = useRouter()
const authStore = useAuthStore()

const email = ref('')
const password = ref('')
const isSubmitting = ref(false)
const formError = ref('')

const sessionMessage = computed(() => {
  return route.query.reason === 'expired' ? 'Your session expired. Sign in again.' : ''
})

const loginError = computed(() => {
  const errorValue = route.query.error
  return typeof errorValue === 'string' ? errorValue : ''
})

function toUserFacingMessage(error: unknown, fallback: string) {
  if (!(error instanceof Error)) {
    return fallback
  }

  const separator = '): '
  const separatorIndex = error.message.indexOf(separator)
  if (separatorIndex === -1) {
    return error.message
  }

  return error.message.slice(separatorIndex + separator.length)
}

async function submitLogin() {
  if (isSubmitting.value) {
    return
  }

  formError.value = ''
  isSubmitting.value = true

  try {
    const tokens = await loginWithPassword(email.value.trim(), password.value)
    authStore.setTokens(tokens.access, tokens.refresh)
    await authStore.loadCurrentUser(true)
    await router.replace('/')
  } catch (error) {
    formError.value = toUserFacingMessage(error, 'Unable to sign in with those credentials.')
  } finally {
    isSubmitting.value = false
  }
}
</script>

<template>
  <div class="mx-auto max-w-md">
    <Card class="border-slate-200">
      <CardHeader>
        <CardTitle>Sign In</CardTitle>
        <CardDescription>Use your Datamingle email and password.</CardDescription>
      </CardHeader>
      <form @submit.prevent="submitLogin">
        <CardContent class="space-y-4">
        <p v-if="sessionMessage" class="rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
          {{ sessionMessage }}
        </p>
        <p
          v-if="loginError || formError"
          class="rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700"
          data-testid="login-error"
        >
          {{ formError || loginError }}
        </p>
          <div class="grid gap-2">
            <label for="login-email" class="text-sm font-medium text-slate-900">Email</label>
            <Input
              id="login-email"
              v-model="email"
              autocomplete="email"
              data-testid="login-email"
              :disabled="isSubmitting"
              required
              type="email"
            />
          </div>
          <div class="grid gap-2">
            <label for="login-password" class="text-sm font-medium text-slate-900">Password</label>
            <Input
              id="login-password"
              v-model="password"
              autocomplete="current-password"
              data-testid="login-password"
              :disabled="isSubmitting"
              required
              type="password"
            />
          </div>
        </CardContent>
        <CardFooter>
          <Button class="w-full" data-testid="login-submit" :disabled="isSubmitting" type="submit">
            {{ isSubmitting ? 'Signing in...' : 'Sign in' }}
          </Button>
        </CardFooter>
      </form>
    </Card>
  </div>
</template>
