<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { RouterLink } from 'vue-router'
import { Plus, RefreshCw } from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { fetchPermissionLevels, type PermissionLevelRecord } from '../api'
import { useAuthStore } from '@/stores/auth'

const authStore = useAuthStore()
const levels = ref<PermissionLevelRecord[]>([])
const isLoading = ref(false)
const error = ref('')

function requireToken() {
  if (!authStore.accessToken) {
    throw new Error('Missing access token. Please login again.')
  }
  return authStore.accessToken
}

async function loadLevels() {
  isLoading.value = true
  error.value = ''
  try {
    levels.value = await fetchPermissionLevels(requireToken())
  } catch (errorValue) {
    error.value = errorValue instanceof Error ? errorValue.message : 'Failed to load permission levels.'
  } finally {
    isLoading.value = false
  }
}

onMounted(() => void loadLevels())
</script>

<template>
  <section class="grid gap-6">
    <div class="flex flex-wrap items-start justify-between gap-3">
      <div>
        <h2 class="text-2xl font-semibold text-slate-900">Permission Levels</h2>
        <p class="mt-1 text-sm text-slate-600">
          Define reusable permission sets that team owners can assign to members.
        </p>
      </div>
      <div class="flex gap-2">
        <Button variant="outline" :disabled="isLoading" @click="loadLevels">
          <RefreshCw class="h-4 w-4" />
          Refresh
        </Button>
        <Button as-child>
          <RouterLink to="/settings/permission-levels/new">
            <Plus class="h-4 w-4" />
            Add level
          </RouterLink>
        </Button>
      </div>
    </div>

    <p v-if="error" class="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
      {{ error }}
    </p>

    <Card class="border-slate-200">
      <CardHeader>
        <CardTitle>Levels</CardTitle>
      </CardHeader>
      <CardContent>
        <div class="divide-y divide-slate-200 rounded-md border border-slate-200">
          <RouterLink
            v-for="level in levels"
            :key="level.id"
            :to="`/settings/permission-levels/${level.id}`"
            class="flex flex-wrap items-center justify-between gap-3 px-4 py-4 hover:bg-slate-50"
          >
            <div>
              <p class="font-medium text-slate-900">{{ level.name }}</p>
              <p class="mt-1 text-xs text-slate-500">
                {{ level.permissions.length }} permissions
              </p>
            </div>
            <Badge variant="secondary">{{ level.membership_count }} members</Badge>
          </RouterLink>
          <p v-if="!isLoading && levels.length === 0" class="px-4 py-10 text-center text-sm text-slate-500">
            No permission levels are configured.
          </p>
        </div>
      </CardContent>
    </Card>
  </section>
</template>
