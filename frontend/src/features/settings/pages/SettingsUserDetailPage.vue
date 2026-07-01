<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { RouterLink, useRoute, useRouter } from 'vue-router'
import { ArrowLeft, Trash2 } from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardFooter, CardHeader, CardTitle } from '@/components/ui/card'
import { deleteUser, fetchUser, updateUser, type UserManagementDetailRecord } from '../api'
import { useAuthStore } from '@/stores/auth'

const authStore = useAuthStore()
const route = useRoute()
const router = useRouter()

const user = ref<UserManagementDetailRecord | null>(null)
const isLoading = ref(false)
const isDeleting = ref(false)
const isTogglingStatus = ref(false)
const pageError = ref('')
const feedback = ref('')

const userId = computed(() => {
  const value = Number(route.params.userId)
  return Number.isFinite(value) ? value : null
})
const canManageUsers = computed(() => authStore.currentUser?.is_superuser ?? false)

function requireToken() {
  if (!authStore.accessToken) {
    throw new Error('Missing access token. Please login again.')
  }
  return authStore.accessToken
}

function message(errorValue: unknown, fallback: string) {
  return errorValue instanceof Error ? errorValue.message : fallback
}

async function loadPage() {
  isLoading.value = true
  pageError.value = ''
  feedback.value = ''
  try {
    await authStore.loadCurrentUser()
    if (!canManageUsers.value || !userId.value) {
      pageError.value = 'Only superusers can access Datamingle user management.'
      return
    }
    user.value = await fetchUser(userId.value, requireToken())
  } catch (errorValue) {
    pageError.value = message(errorValue, 'Failed to load the user.')
  } finally {
    isLoading.value = false
  }
}

async function toggleUserStatus() {
  if (!user.value || !userId.value) {
    return
  }
  const nextActive = !user.value.is_active
  if (!window.confirm(`${nextActive ? 'Reactivate' : 'Deactivate'} "${user.value.display || user.value.username}"?`)) {
    return
  }
  isTogglingStatus.value = true
  pageError.value = ''
  try {
    user.value = await updateUser(userId.value, { is_active: nextActive }, requireToken())
    feedback.value = nextActive ? 'User reactivated successfully.' : 'User deactivated successfully.'
  } catch (errorValue) {
    pageError.value = message(errorValue, 'Failed to update the user.')
  } finally {
    isTogglingStatus.value = false
  }
}

async function removeUser() {
  if (!user.value || !userId.value) {
    return
  }
  if (!window.confirm(`Delete "${user.value.display || user.value.username}" from Datamingle? This cannot be undone.`)) {
    return
  }
  isDeleting.value = true
  pageError.value = ''
  try {
    await deleteUser(userId.value, requireToken())
    await router.push('/settings/users')
  } catch (errorValue) {
    pageError.value = message(errorValue, 'Failed to delete the user.')
  } finally {
    isDeleting.value = false
  }
}

onMounted(() => void loadPage())
watch(() => route.fullPath, () => void loadPage())
</script>

<template>
  <section class="grid gap-6">
    <Button as-child variant="ghost" class="w-fit">
      <RouterLink to="/settings/users">
        <ArrowLeft class="h-4 w-4" />
        Back to users
      </RouterLink>
    </Button>

    <Card class="border-slate-200" data-testid="user-management-detail">
      <CardHeader>
        <CardTitle>{{ user?.display || user?.username || 'User' }}</CardTitle>
      </CardHeader>
      <CardContent class="space-y-6">
        <p
          v-if="pageError"
          class="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700"
          data-testid="user-management-detail-error"
        >
          {{ pageError }}
        </p>
        <p
          v-else-if="feedback"
          class="rounded-lg border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700"
          data-testid="user-management-detail-feedback"
        >
          {{ feedback }}
        </p>

        <div v-if="user" class="grid gap-4 md:grid-cols-3">
          <div>
            <p class="text-xs font-medium uppercase text-slate-500">Username</p>
            <p class="mt-1 text-sm text-slate-900">{{ user.username }}</p>
          </div>
          <div>
            <p class="text-xs font-medium uppercase text-slate-500">Email</p>
            <p class="mt-1 text-sm text-slate-900">{{ user.email || 'No email address' }}</p>
          </div>
          <div>
            <p class="text-xs font-medium uppercase text-slate-500">Status</p>
            <Badge
              class="mt-1"
              :variant="user.is_active ? 'secondary' : 'outline'"
              data-testid="user-management-detail-status"
            >
              {{ user.is_active ? 'Active' : 'Inactive' }}
            </Badge>
          </div>
        </div>

        <div class="rounded-lg border border-slate-200 p-5">
          <h3 class="font-semibold text-slate-900">Team memberships</h3>
          <p class="mt-1 text-sm text-slate-500">
            Membership and permission levels are managed from each team.
          </p>
          <div v-if="user?.teams.length" class="mt-4 divide-y divide-slate-200 rounded-md border border-slate-200">
            <RouterLink
              v-for="team in user.teams"
              :key="team.team_id"
              :to="`/settings/teams/${team.team_id}`"
              class="flex items-center justify-between gap-4 px-4 py-3 hover:bg-slate-50"
              :data-testid="`user-management-detail-team-${team.team_id}`"
            >
              <span class="font-medium text-slate-900">{{ team.team_name }}</span>
              <Badge variant="secondary">{{ team.permission_level_name }}</Badge>
            </RouterLink>
          </div>
          <p v-else class="mt-4 text-sm text-slate-500">No teams assigned.</p>
        </div>
      </CardContent>
      <CardFooter v-if="user" class="justify-between border-t border-slate-200 pt-6">
        <Button
          variant="outline"
          data-testid="user-management-detail-toggle-active"
          :disabled="isTogglingStatus || isLoading"
          @click="toggleUserStatus"
        >
          {{ user.is_active ? 'Deactivate user' : 'Reactivate user' }}
        </Button>
        <Button
          variant="destructive"
          data-testid="user-management-detail-delete"
          :disabled="isDeleting || isLoading"
          @click="removeUser"
        >
          <Trash2 class="h-4 w-4" />
          Delete user
        </Button>
      </CardFooter>
    </Card>
  </section>
</template>
