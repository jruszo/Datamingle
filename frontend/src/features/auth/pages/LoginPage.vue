<script setup lang="ts">
import { computed } from 'vue'
import { useRoute } from 'vue-router'

import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from '@/components/ui/card'
import { publicApiUrl } from '../api'

const route = useRoute()

const sessionMessage = computed(() => {
  return route.query.reason === 'expired' ? 'Your session expired. Sign in again.' : ''
})

const loginError = computed(() => {
  const errorValue = route.query.error
  return typeof errorValue === 'string' ? errorValue : ''
})

function continueWithWorkos() {
  window.location.assign(publicApiUrl('/auth/workos/authorize/'))
}
</script>

<template>
  <div class="mx-auto max-w-md">
    <Card>
      <CardHeader>
        <CardTitle>Sign In</CardTitle>
        <CardDescription>Sign in through your organization.</CardDescription>
      </CardHeader>
      <CardContent class="space-y-3">
        <p v-if="sessionMessage" class="rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
          {{ sessionMessage }}
        </p>
        <p v-if="loginError" class="rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">
          {{ loginError }}
        </p>
      </CardContent>
      <CardFooter>
        <Button class="w-full" data-testid="login-workos" type="button" @click="continueWithWorkos">
          Continue with your organization
        </Button>
      </CardFooter>
    </Card>
  </div>
</template>
