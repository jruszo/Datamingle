<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'

import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from '@/components/ui/card'
import { exchangeWorkosCode } from '@/lib/api'
import { useAuthStore } from '@/stores/auth'

const authStore = useAuthStore()
const route = useRoute()
const router = useRouter()

const error = ref('')
const isExchanging = ref(true)

async function finishLogin() {
  const code = typeof route.query.code === 'string' ? route.query.code.trim() : ''
  if (!code) {
    error.value = 'Missing WorkOS exchange code.'
    isExchanging.value = false
    return
  }

  try {
    const tokens = await exchangeWorkosCode(code)
    authStore.setTokens(tokens.access, tokens.refresh)
    await authStore.loadCurrentUser(true)
    await router.replace('/')
  } catch (errorValue) {
    error.value =
      errorValue instanceof Error ? errorValue.message : 'Failed to complete WorkOS login.'
    isExchanging.value = false
  }
}

onMounted(() => {
  void finishLogin()
})
</script>

<template>
  <div class="mx-auto max-w-md">
    <Card>
      <CardHeader>
        <CardTitle>Completing sign in</CardTitle>
        <CardDescription>
          Datamingle is finishing your organization login.
        </CardDescription>
      </CardHeader>
      <CardContent class="space-y-3">
        <p v-if="isExchanging" class="text-sm text-slate-600">
          Exchanging your WorkOS login for a Datamingle session...
        </p>
        <p v-else-if="error" class="rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">
          {{ error }}
        </p>
      </CardContent>
      <CardFooter v-if="error">
        <Button class="w-full" type="button" @click="router.replace('/login')">
          Back to sign in
        </Button>
      </CardFooter>
    </Card>
  </div>
</template>
