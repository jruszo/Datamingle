<script setup lang="ts">
import { computed, reactive, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'

import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { login } from '@/lib/api'
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

const sessionMessage = computed(() => {
  return route.query.reason === 'expired' ? 'Your session expired. Sign in again.' : ''
})

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
          <p v-if="sessionMessage" class="rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
            {{ sessionMessage }}
          </p>
          <Input v-model="form.username" placeholder="Username" />
          <Input v-model="form.password" type="password" placeholder="Password" />
          <p v-if="error" class="text-sm text-destructive">{{ error }}</p>
        </CardContent>
        <CardFooter>
          <Button class="w-full" type="submit" :disabled="loading">
            {{ loading ? 'Signing in...' : 'Sign in' }}
          </Button>
        </CardFooter>
      </form>
    </Card>
  </div>
</template>
