<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { RefreshCw, ShieldCheck, UserRound } from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { useAuthStore } from '@/stores/auth'

const authStore = useAuthStore()

const isLoading = ref(false)
const pageError = ref('')

const currentUser = computed(() => authStore.currentUser)
const currentUserAvatarUrl = computed(() => currentUser.value?.avatar_url?.trim() || '')

const currentUserInitials = computed(() => {
  const source = currentUser.value?.display || currentUser.value?.username || 'U'
  const initials = source
    .split(/\s+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((segment) => segment[0]?.toUpperCase() ?? '')
    .join('')

  return initials || 'U'
})

const accountFacts = computed(() => {
  if (!currentUser.value) {
    return []
  }

  const groups = currentUser.value.groups ?? []
  const teams = currentUser.value.teams ?? []

  return [
    { label: 'Username', value: currentUser.value.username },
    { label: 'Email', value: currentUser.value.email || '-' },
    {
      label: 'Groups',
      value: groups.length > 0 ? groups.map((group) => group.name).join(', ') : '-',
    },
    {
      label: 'Teams',
      value: teams.length > 0 ? teams.map((team) => team.team_name).join(', ') : '-',
    },
  ]
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

async function loadProfile(force = false) {
  if (!authStore.accessToken) {
    pageError.value = 'Missing access token. Please login again.'
    return
  }

  isLoading.value = true
  pageError.value = ''

  try {
    await authStore.loadCurrentUser(force)
  } catch (error) {
    pageError.value = toUserFacingMessage(error, 'Failed to load your profile.')
  } finally {
    isLoading.value = false
  }
}

onMounted(() => {
  void loadProfile()
})
</script>

<template>
  <section class="grid gap-6">
    <div v-if="isLoading" class="flex items-center justify-center py-20 text-sm text-slate-500">
      Loading profile...
    </div>

    <template v-else>
      <Card class="overflow-hidden border-slate-200">
        <CardContent class="p-0">
          <div class="grid gap-0 lg:grid-cols-[minmax(0,1.3fr)_minmax(0,0.9fr)]">
            <div class="bg-slate-50 p-6 lg:p-8">
              <Badge variant="outline" class="border-slate-300 bg-white text-slate-700">Profile</Badge>
              <div class="mt-5 flex flex-col gap-5 sm:flex-row sm:items-center">
                <div class="flex h-20 w-20 items-center justify-center overflow-hidden rounded-lg bg-slate-900 text-xl font-semibold text-white shadow-sm">
                  <img
                    v-if="currentUserAvatarUrl"
                    :src="currentUserAvatarUrl"
                    :alt="`${currentUser?.display || currentUser?.username || 'User'} avatar`"
                    class="h-full w-full object-cover"
                    referrerpolicy="no-referrer"
                  />
                  <span v-else>{{ currentUserInitials }}</span>
                </div>
                <div>
                  <h2 class="text-2xl font-semibold text-slate-900">
                    {{ currentUser?.display || currentUser?.username || 'User profile' }}
                  </h2>
                  <p class="mt-1 text-sm text-slate-600">
                    This account uses Datamingle email and password authentication.
                  </p>
                </div>
              </div>

              <div class="mt-6 flex flex-wrap gap-2">
                <Badge variant="secondary" class="bg-white text-slate-800">
                  <UserRound class="h-3.5 w-3.5" />
                  {{ currentUser?.username || 'Account' }}
                </Badge>
                <Badge variant="secondary" class="bg-white text-slate-800">
                  <ShieldCheck class="h-3.5 w-3.5" />
                  {{ currentUser?.is_superuser ? 'Admin access' : 'Standard access' }}
                </Badge>
              </div>
            </div>

            <div class="border-t border-slate-200 bg-white p-6 lg:border-l lg:border-t-0 lg:p-8">
              <div class="grid gap-4 sm:grid-cols-2 lg:grid-cols-1">
                <div v-for="fact in accountFacts" :key="fact.label" class="rounded-lg border border-slate-200 bg-slate-50 p-4">
                  <p class="text-xs font-medium uppercase text-slate-500">{{ fact.label }}</p>
                  <p class="mt-2 break-words text-sm font-medium text-slate-900">{{ fact.value }}</p>
                </div>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      <div v-if="pageError" class="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
        {{ pageError }}
      </div>

      <Card class="border-slate-200">
        <CardHeader>
          <CardTitle>Account Access</CardTitle>
          <CardDescription>
            Team memberships and permission levels are managed by Datamingle administrators.
          </CardDescription>
        </CardHeader>
        <CardContent class="flex flex-wrap gap-3">
          <Button type="button" variant="outline" class="gap-2" :disabled="isLoading" @click="loadProfile(true)">
            <RefreshCw class="h-4 w-4" />
            Refresh
          </Button>
        </CardContent>
      </Card>
    </template>
  </section>
</template>
