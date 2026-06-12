<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { RouterLink, useRoute, useRouter } from 'vue-router'
import { ArrowLeft, Save, Trash2 } from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import {
  deleteUser,
  fetchPermissionGroups,
  fetchTeams,
  fetchUser,
  updateUser,
  type PermissionGroupRecord,
  type TeamPermissionGroupCode,
  type TeamRecord,
  type UserManagementDetailRecord,
} from '../api'
import { useAuthStore } from '@/stores/auth'

type GroupAccessState = {
  permission_group_id: TeamPermissionGroupCode | ''
}

const authStore = useAuthStore()
const route = useRoute()
const router = useRouter()

const accessRoles = ref<PermissionGroupRecord[]>([])
const resourceGroups = ref<TeamRecord[]>([])
const loadedUser = ref<UserManagementDetailRecord | null>(null)
const username = ref('')
const displayName = ref('')
const email = ref('')
const groupAccessById = ref<Record<number, GroupAccessState>>({})
const groupFilter = ref('')
const isLoading = ref(false)
const isSaving = ref(false)
const isDeleting = ref(false)
const isTogglingStatus = ref(false)
const pageError = ref('')
const formError = ref('')
const formSuccess = ref('')

const userId = computed(() => {
  const value = Number(route.params.userId)
  return Number.isFinite(value) ? value : null
})
const canManageUsers = computed(() => authStore.currentUser?.is_superuser ?? false)
const normalizedGroupFilter = computed(() => groupFilter.value.trim().toLowerCase())
const assignedGroupCount = computed(
  () => Object.values(groupAccessById.value).filter((row) => row.permission_group_id).length,
)

function toUserFacingMessage(errorValue: unknown, fallback: string) {
  if (!(errorValue instanceof Error)) {
    return fallback
  }

  const separator = '): '
  const separatorIndex = errorValue.message.indexOf(separator)
  if (separatorIndex === -1) {
    return errorValue.message
  }

  return errorValue.message.slice(separatorIndex + separator.length)
}

function requireToken() {
  if (!authStore.accessToken) {
    throw new Error('Missing access token. Please login again.')
  }
  return authStore.accessToken
}

function permissionGroupId(value: string | number | '') {
  const groupId = Number(value)
  return accessRoles.value.some((role) => role.id === groupId) ? groupId : null
}

function applyUser(user: UserManagementDetailRecord) {
  loadedUser.value = user
  username.value = user.username
  displayName.value = user.display
  email.value = user.email

  const nextAccess: Record<number, GroupAccessState> = {}
  const rows = user.team_access?.length ? user.team_access : user.teams
  for (const row of rows) {
    nextAccess[row.team_id] = {
      permission_group_id: row.permission_group_id,
    }
  }
  groupAccessById.value = nextAccess
}

function sortGroups(values: TeamRecord[]) {
  return [...values].sort((left, right) =>
    left.team_name.localeCompare(right.team_name, undefined, {
      sensitivity: 'base',
      numeric: true,
    }),
  )
}

function groupMatches(group: TeamRecord, filterValue: string) {
  if (!filterValue) {
    return true
  }

  const haystack = `${group.team_name} ${group.team_id}`.toLowerCase()
  return haystack.includes(filterValue)
}

const filteredTeams = computed(() =>
  sortGroups(
    resourceGroups.value.filter((group) => groupMatches(group, normalizedGroupFilter.value)),
  ),
)

function roleForGroup(groupId: number) {
  return groupAccessById.value[groupId]?.permission_group_id ?? ''
}

function updateGroupRole(groupId: number, event: Event) {
  const value = (event.target as HTMLSelectElement).value
  const permissionId = permissionGroupId(value)

  const nextAccess = { ...groupAccessById.value }
  if (!value) {
    delete nextAccess[groupId]
  } else if (permissionId !== null) {
    nextAccess[groupId] = { permission_group_id: permissionId }
  }
  groupAccessById.value = nextAccess
  formSuccess.value = ''
}

function resourceAccessRows() {
  return Object.entries(groupAccessById.value)
    .map(([resourceGroupId, row]) => ({
      team_id: Number(resourceGroupId),
      permission_group_id: row.permission_group_id,
    }))
    .filter(
      (row): row is { team_id: number; permission_group_id: TeamPermissionGroupCode } =>
        Number.isFinite(row.team_id) && permissionGroupId(row.permission_group_id) !== null,
    )
    .sort((left, right) => left.team_id - right.team_id)
}

async function loadAllGroups() {
  const collectedGroups: TeamRecord[] = []
  let page = 1
  let totalCount = 0

  while (page === 1 || collectedGroups.length < totalCount) {
    const response = await fetchTeams(requireToken(), {
      page,
      size: 100,
      ordering: 'team_name',
    })
    collectedGroups.push(...response.results)
    totalCount = response.count

    if (!response.next || response.results.length === 0) {
      break
    }

    page += 1
  }

  resourceGroups.value = sortGroups(collectedGroups)
}

async function loadPage() {
  isLoading.value = true
  pageError.value = ''
  formError.value = ''
  formSuccess.value = ''
  loadedUser.value = null
  username.value = ''
  displayName.value = ''
  email.value = ''
  groupAccessById.value = {}

  try {
    await authStore.loadCurrentUser()

    if (!canManageUsers.value) {
      pageError.value = 'Only superusers can access Datamingle user management.'
      return
    }

    if (!userId.value) {
      pageError.value = 'Invalid user identifier.'
      return
    }

    const [roles, user] = await Promise.all([
      fetchPermissionGroups(requireToken()),
      fetchUser(userId.value, requireToken()),
      loadAllGroups(),
    ])
    accessRoles.value = roles
    applyUser(user)
  } catch (errorValue) {
    pageError.value = toUserFacingMessage(errorValue, 'Failed to load the user editor.')
  } finally {
    isLoading.value = false
  }
}

async function saveUser() {
  if (!canManageUsers.value) {
    formError.value = 'Only superusers can save Datamingle users.'
    return
  }

  isSaving.value = true
  formError.value = ''
  formSuccess.value = ''

  try {
    if (!userId.value) {
      throw new Error('Missing user identifier.')
    }

    const updatedUser = await updateUser(
      userId.value,
      {
        team_access: resourceAccessRows(),
        is_active: loadedUser.value?.is_active ?? true,
      },
      requireToken(),
    )
    applyUser(updatedUser)
    formSuccess.value = 'User updated successfully.'
  } catch (errorValue) {
    formError.value = toUserFacingMessage(errorValue, 'Failed to save the user.')
  } finally {
    isSaving.value = false
  }
}

async function toggleUserStatus() {
  if (!userId.value || !loadedUser.value) {
    return
  }

  const nextIsActive = !loadedUser.value.is_active
  const actionLabel = nextIsActive ? 'reactivate' : 'deactivate'

  if (
    !window.confirm(
      `${actionLabel[0]?.toUpperCase() ?? ''}${actionLabel.slice(1)} "${loadedUser.value.display || loadedUser.value.username}"?`,
    )
  ) {
    return
  }

  isTogglingStatus.value = true
  formError.value = ''
  formSuccess.value = ''

  try {
    const updatedUser = await updateUser(
      userId.value,
      { is_active: nextIsActive },
      requireToken(),
    )
    applyUser(updatedUser)
    formSuccess.value = nextIsActive
      ? 'User reactivated successfully.'
      : 'User deactivated successfully.'
  } catch (errorValue) {
    formError.value = toUserFacingMessage(errorValue, `Failed to ${actionLabel} the user.`)
  } finally {
    isTogglingStatus.value = false
  }
}

async function removeUserAccount() {
  if (!userId.value || !loadedUser.value) {
    return
  }

  if (
    !window.confirm(
      `Delete "${loadedUser.value.display || loadedUser.value.username}" from Datamingle? This cannot be undone.`,
    )
  ) {
    return
  }

  isDeleting.value = true
  formError.value = ''
  formSuccess.value = ''

  try {
    await deleteUser(userId.value, requireToken())
    await router.push('/settings/users')
  } catch (errorValue) {
    formError.value = toUserFacingMessage(errorValue, 'Failed to delete the user.')
  } finally {
    isDeleting.value = false
  }
}

onMounted(() => {
  void loadPage()
})

watch(
  () => route.fullPath,
  (currentPath, previousPath) => {
    if (currentPath !== previousPath) {
      void loadPage()
    }
  },
)
</script>

<template>
  <section class="grid gap-6">
    <div class="flex flex-wrap items-center justify-between gap-3">
      <Button as-child variant="ghost">
        <RouterLink to="/settings/users">
          <ArrowLeft class="h-4 w-4" />
          Back to users
        </RouterLink>
      </Button>
      <div v-if="loadedUser" class="flex flex-wrap gap-2">
        <Badge
          :variant="loadedUser.is_active ? 'secondary' : 'outline'"
          :class="loadedUser.is_active ? 'bg-emerald-100 text-emerald-800' : 'text-slate-600'"
        >
          {{ loadedUser.is_active ? 'Active' : 'Inactive' }}
        </Badge>
        <Badge
          v-if="loadedUser.is_superuser"
          variant="secondary"
          class="bg-amber-100 text-amber-800"
        >
          Superuser
        </Badge>
        <Badge v-if="loadedUser.is_staff" variant="secondary" class="bg-sky-100 text-sky-800">
          Staff
        </Badge>
      </div>
    </div>

    <Card class="border-slate-200">
      <CardHeader>
        <CardTitle>Edit User</CardTitle>
        <CardDescription>Maintain the user account and resource access assignments.</CardDescription>
      </CardHeader>
      <CardContent class="space-y-6">
        <div class="grid gap-4 md:grid-cols-2">
          <div class="space-y-2">
            <label for="user-username" class="text-sm font-medium text-slate-900">Username</label>
            <Input id="user-username" v-model="username" :disabled="true" placeholder="e.g. jdoe" />
          </div>

          <div class="space-y-2">
            <label for="user-display" class="text-sm font-medium text-slate-900">Display name</label>
            <Input
              id="user-display"
              v-model="displayName"
              :disabled="true"
              placeholder="e.g. Jane Doe"
            />
          </div>

          <div class="space-y-2">
            <label for="user-email" class="text-sm font-medium text-slate-900">Email</label>
            <Input
              id="user-email"
              v-model="email"
              :disabled="true"
              placeholder="jane.doe@example.com"
            />
          </div>
        </div>

        <p
          v-if="pageError"
          class="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700"
        >
          {{ pageError }}
        </p>
        <p
          v-else-if="formError"
          class="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700"
        >
          {{ formError }}
        </p>
        <p
          v-else-if="formSuccess"
          class="rounded-lg border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700"
        >
          {{ formSuccess }}
        </p>

        <div class="space-y-4 rounded-lg border border-slate-200 p-5">
          <div class="flex flex-wrap items-center justify-between gap-3">
            <h3 class="text-base font-semibold text-slate-900">Resource Access</h3>
            <div class="flex flex-wrap items-center gap-2">
              <Badge variant="secondary" class="bg-slate-100 text-slate-700">
                {{ assignedGroupCount }} assigned
              </Badge>
              <Badge variant="secondary" class="bg-slate-100 text-slate-700">
                {{ resourceGroups.length }} groups
              </Badge>
            </div>
          </div>

          <Input
            v-model="groupFilter"
            :disabled="isLoading"
            placeholder="Filter teams"
            aria-label="Filter teams"
          />

          <div class="overflow-x-auto rounded-md border border-slate-200">
            <table class="min-w-full divide-y divide-slate-200 text-sm">
              <thead class="bg-slate-50 text-left text-xs font-medium uppercase text-slate-500">
                <tr>
                  <th class="px-4 py-3">Team</th>
                  <th class="px-4 py-3">Permission group</th>
                </tr>
              </thead>
              <tbody class="divide-y divide-slate-200 bg-white">
                <tr v-for="group in filteredTeams" :key="group.team_id">
                  <td class="px-4 py-3">
                    <div class="font-medium text-slate-900">{{ group.team_name }}</div>
                    <div class="mt-1 text-xs text-slate-500">Group ID {{ group.team_id }}</div>
                  </td>
                  <td class="px-4 py-3">
                    <select
                      class="w-full min-w-52 rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm focus:outline-none focus:ring-1 focus:ring-ring disabled:cursor-not-allowed disabled:opacity-50"
                      :value="roleForGroup(group.team_id)"
                      :disabled="!canManageUsers"
                      @change="updateGroupRole(group.team_id, $event)"
                    >
                      <option value="">No access</option>
                      <option v-for="role in accessRoles" :key="role.id" :value="role.id">
                        {{ role.name }}
                      </option>
                    </select>
                  </td>
                </tr>
                <tr v-if="filteredTeams.length === 0">
                  <td colspan="2" class="px-4 py-8 text-center text-sm text-slate-500">
                    No teams match the current filter.
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </CardContent>
      <CardFooter class="justify-between border-t border-slate-200 pt-6">
        <div class="flex flex-wrap gap-2">
          <Button
            variant="outline"
            :disabled="isTogglingStatus || !loadedUser"
            @click="toggleUserStatus"
          >
            {{ loadedUser?.is_active ? 'Deactivate user' : 'Reactivate user' }}
          </Button>
          <Button variant="destructive" :disabled="isDeleting" @click="removeUserAccount">
            <Trash2 class="h-4 w-4" />
            Delete user
          </Button>
        </div>
        <Button
          :disabled="isLoading || isSaving || !canManageUsers"
          @click="saveUser"
        >
          <Save class="h-4 w-4" />
          Save
        </Button>
      </CardFooter>
    </Card>
  </section>
</template>
