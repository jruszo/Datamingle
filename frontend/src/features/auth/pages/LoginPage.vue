<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'

import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { login, publicApiUrl } from '../api'
import { useAuthStore } from '@/stores/auth'

const router = useRouter()
const route = useRoute()
const authStore = useAuthStore()

const form = reactive({
  username: '',
  password: '',
})

const loading = ref(false)
const error = ref('')
const configLoading = ref(true)

const sessionMessage = computed(() => {
  return route.query.reason === 'expired' ? 'Your session expired. Sign in again.' : ''
})

const loginError = computed(() => {
  const errorValue = route.query.error
  return typeof errorValue === 'string' ? errorValue : ''
})

const isWorkosMode = computed(() => authStore.authMode === 'workos')

async function submit() {
  loading.value = true
  error.value = ''
  try {
    const tokens = await login(form.username, form.password)
    authStore.setTokens(tokens.access, tokens.refresh)
    await router.push('/')
  } catch (e) {
    error.value = e instanceof Error ? e.message : 'Login failed'
  } finally {
    loading.value = false
  }
}

async function loadAuthMode() {
  configLoading.value = true
  error.value = ''
  try {
    await authStore.loadAuthConfig(true)
  } catch (e) {
    error.value = e instanceof Error ? e.message : 'Failed to load login configuration'
  } finally {
    configLoading.value = false
  }
}

function continueWithWorkos() {
  window.location.assign(publicApiUrl('/auth/workos/authorize/'))
}

onMounted(() => {
  void loadAuthMode()
})
</script>

<template>
  <div class="mx-auto max-w-md">
    <Card>
      <CardHeader>
        <CardTitle>Sign In</CardTitle>
        <CardDescription>Sign in to access Datamingle.</CardDescription>
      </CardHeader>
      <form @submit.prevent="submit">
        <CardContent class="space-y-3">
          <p v-if="configLoading" class="text-sm text-slate-600">
            Loading sign-in configuration...
          </p>
          <p v-if="sessionMessage" class="rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
            {{ sessionMessage }}
          </p>
          <p v-if="loginError" class="rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">
            {{ loginError }}
          </p>
          <template v-if="!configLoading && isWorkosMode">
            <p class="text-sm text-slate-600">
              Sign in through your organization’s WorkOS connection.
            </p>
            <Button
              class="w-full"
              data-testid="login-workos"
              type="button"
              :disabled="loading || configLoading"
              @click="continueWithWorkos"
            >
              Continue with your organization
            </Button>
          </template>
          <template v-else-if="!configLoading">
            <Input id="login-username" v-model="form.username" data-testid="login-username" placeholder="Username" />
            <Input id="login-password" v-model="form.password" data-testid="login-password" type="password" placeholder="Password" />
          </template>
          <p v-if="error" class="text-sm text-destructive">{{ error }}</p>
        </CardContent>
        <CardFooter v-if="!isWorkosMode">
          <Button class="w-full" data-testid="login-submit" type="submit" :disabled="loading || configLoading">
            {{ loading ? 'Signing in...' : 'Sign in' }}
          </Button>
        </CardFooter>
      </form>
    </Card>
  </div>
</template>
