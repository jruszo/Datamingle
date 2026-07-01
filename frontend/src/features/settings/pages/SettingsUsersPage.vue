<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { RouterLink } from 'vue-router'
import { useDebounceFn } from '@vueuse/core'
import { RefreshCw, UserPlus, X } from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { DataTable, type DataTableColumn } from '@/components/ui/data-table'
import { Input } from '@/components/ui/input'
import {
  createUser,
  deleteUser,
  fetchUsers,
  updateUser,
  type UserManagementRecord,
} from '../api'
import { useAuthStore } from '@/stores/auth'

const authStore = useAuthStore()

const users = ref<UserManagementRecord[]>([])
const isLoading = ref(false)
const error = ref('')
const feedback = ref('')
const totalCount = ref(0)
const searchQuery = ref('')
const currentPage = ref(1)
const pageSize = ref(20)
const sortKey = ref('display')
const sortDirection = ref<'asc' | 'desc'>('asc')
const latestRequestId = ref(0)
const isInviteDialogOpen = ref(false)
const inviteEmail = ref('')
const inviteDisplay = ref('')
const invitePassword = ref('')
const inviteSubmitting = ref(false)
const inviteError = ref('')

const columns: DataTableColumn[] = [
  {
    key: 'display',
    label: 'User',
    sortable: true,
    hideable: false,
  },
  {
    key: 'username',
    label: 'Username',
    sortable: true,
  },
  {
    key: 'email',
    label: 'Email',
    sortable: true,
  },
  {
    key: 'teams',
    label: 'Teams',
    sortable: false,
  },
  {
    key: 'status',
    label: 'Status',
    sortable: false,
  },
  {
    key: 'actions',
    label: 'Actions',
    hideable: false,
    headerClass: 'w-[18rem]',
  },
]

const canManageUsers = computed(() => authStore.currentUser?.is_superuser ?? false)

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

function groupSummary(user: UserManagementRecord) {
  if (user.teams.length === 0) {
    return 'No teams assigned'
  }

  return user.teams
    .map((team) => `${team.team_name}: ${team.permission_level_name}`)
    .join(', ')
}

async function loadUsers() {
  const requestId = latestRequestId.value + 1
  latestRequestId.value = requestId
  isLoading.value = true
  error.value = ''

  try {
    await authStore.loadCurrentUser()

    if (!canManageUsers.value) {
      users.value = []
      totalCount.value = 0
      error.value = 'Only superusers can manage Datamingle users.'
      return
    }

    const ordering = sortKey.value
      ? `${sortDirection.value === 'desc' ? '-' : ''}${sortKey.value}`
      : undefined
    const response = await fetchUsers(requireToken(), {
      page: currentPage.value,
      size: pageSize.value,
      search: searchQuery.value,
      ordering,
    })

    if (requestId !== latestRequestId.value) {
      return
    }

    users.value = response.results
    totalCount.value = response.count
  } catch (errorValue) {
    if (requestId !== latestRequestId.value) {
      return
    }
    error.value = toUserFacingMessage(errorValue, 'Failed to load users.')
  } finally {
    if (requestId === latestRequestId.value) {
      isLoading.value = false
    }
  }
}

async function toggleUserActiveState(user: UserManagementRecord) {
  if (!canManageUsers.value) {
    return
  }

  const nextIsActive = !user.is_active
  const actionLabel = nextIsActive ? 'reactivate' : 'deactivate'

  if (
    !window.confirm(
      `${actionLabel[0]?.toUpperCase() ?? ''}${actionLabel.slice(1)} "${user.display || user.username}"?`,
    )
  ) {
    return
  }

  try {
    const updatedUser = await updateUser(
      user.id,
      { is_active: nextIsActive },
      requireToken(),
    )
    feedback.value = nextIsActive
      ? 'User reactivated successfully.'
      : 'User deactivated successfully.'
    users.value = users.value.map((item) => (item.id === updatedUser.id ? updatedUser : item))
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, `Failed to ${actionLabel} the user.`)
  }
}

async function removeUser(user: UserManagementRecord) {
  if (!canManageUsers.value) {
    return
  }

  if (
    !window.confirm(
      `Delete "${user.display || user.username}" from Datamingle? This cannot be undone.`,
    )
  ) {
    return
  }

  try {
    const detail = await deleteUser(user.id, requireToken())
    feedback.value = detail
    await loadUsers()
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to delete the user.')
  }
}

function openInviteDialog() {
  if (!canManageUsers.value) {
    return
  }

  inviteEmail.value = ''
  inviteDisplay.value = ''
  invitePassword.value = ''
  inviteError.value = ''
  isInviteDialogOpen.value = true
}

function closeInviteDialog() {
  if (inviteSubmitting.value) {
    return
  }

  isInviteDialogOpen.value = false
}

async function submitInvite() {
  if (!canManageUsers.value || inviteSubmitting.value) {
    return
  }

  const email = inviteEmail.value.trim()
  if (!email) {
    inviteError.value = 'Email is required.'
    return
  }
  if (!invitePassword.value) {
    inviteError.value = 'Password is required.'
    return
  }

  inviteSubmitting.value = true
  inviteError.value = ''

  try {
    const user = await createUser(
      {
        email,
        display: inviteDisplay.value.trim(),
        password: invitePassword.value,
      },
      requireToken(),
    )
    feedback.value = `User ${user.email || email} created.`
    isInviteDialogOpen.value = false
    await loadUsers()
  } catch (errorValue) {
    inviteError.value = toUserFacingMessage(errorValue, 'Failed to create the user.')
  } finally {
    inviteSubmitting.value = false
  }
}

function handleSearchQueryChange(value: string) {
  searchQuery.value = value
  currentPage.value = 1
}

function handlePageSizeChange(value: number) {
  pageSize.value = value
  currentPage.value = 1
}

onMounted(() => {
  void loadUsers()
})

const debouncedLoadUsers = useDebounceFn(() => {
  feedback.value = ''
  void loadUsers()
}, 250)

watch([currentPage, pageSize, sortKey, sortDirection], () => {
  feedback.value = ''
  void loadUsers()
})

watch(searchQuery, () => {
  feedback.value = ''
  debouncedLoadUsers()
})
</script>

<template>
  <section class="grid gap-6">
    <div class="space-y-1">
      <h2 class="text-2xl font-semibold text-slate-900">User Management</h2>
      <p class="text-sm text-slate-600">
        Superusers can create users and manage team access.
      </p>
    </div>

    <Card class="border-slate-200">
      <CardHeader>
        <CardTitle>Users</CardTitle>
        <CardDescription>
          Search, sort, and maintain Datamingle access records without leaving the SPA.
        </CardDescription>
      </CardHeader>
      <CardContent class="space-y-5">
        <p
          v-if="error"
          class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700"
          data-testid="user-management-error"
        >
          {{ error }}
        </p>
        <p
          v-else-if="feedback"
          class="rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700"
          data-testid="user-management-feedback"
        >
          {{ feedback }}
        </p>

        <DataTable
          :columns="columns"
          :empty-text="'No Datamingle users are available.'"
          :manual-pagination="true"
          :manual-search="true"
          :manual-sort="true"
          :rows="users"
          :loading="isLoading"
          :page="currentPage"
          :page-size="pageSize"
          :search-query="searchQuery"
          :sort-key="sortKey"
          :sort-direction="sortDirection"
          :total-rows="totalCount"
          row-key="id"
          search-placeholder="Filter users by name, username, email, or ID"
          :search-keys="['display', 'username', 'email', 'teams']"
          @update:page="currentPage = $event"
          @update:page-size="handlePageSizeChange"
          @update:search-query="handleSearchQueryChange"
          @update:sort-key="sortKey = $event"
          @update:sort-direction="sortDirection = $event"
        >
          <template #toolbar-actions>
            <Button data-testid="user-management-create-open" @click="openInviteDialog">
              <UserPlus class="h-4 w-4" />
              Create user
            </Button>
            <Button variant="outline" @click="loadUsers">
              <RefreshCw class="h-4 w-4" />
              Refresh
            </Button>
          </template>

          <template #cell-display="{ row }">
            <div class="font-medium text-slate-900" data-testid="user-management-user">
              {{ row.display || row.username }}
            </div>
            <div class="mt-1 text-xs text-slate-500">User ID {{ row.id }}</div>
          </template>

          <template #cell-username="{ value }">
            <code class="rounded bg-slate-100 px-1.5 py-0.5 text-xs text-slate-700">{{
              value
            }}</code>
          </template>

          <template #cell-email="{ value }">
            <span class="text-sm text-slate-700">{{ value || 'No email address' }}</span>
          </template>

          <template #cell-teams="{ row }">
            <div class="space-y-2">
              <div class="flex flex-wrap gap-2">
                <Badge
                  v-for="group in (row as UserManagementRecord).teams.slice(0, 2)"
                  :key="group.team_id"
                  variant="secondary"
                  class="bg-slate-100 text-slate-700"
                >
                  {{ group.team_name }} · {{ group.permission_level_name }}
                </Badge>
                <Badge
                  v-if="(row as UserManagementRecord).teams.length > 2"
                  variant="secondary"
                  class="bg-slate-100 text-slate-700"
                >
                  +{{ (row as UserManagementRecord).teams.length - 2 }} more
                </Badge>
                <span
                  v-if="(row as UserManagementRecord).teams.length === 0"
                  class="text-xs text-slate-500"
                >
                  No teams assigned
                </span>
              </div>
              <p class="text-xs text-slate-500">{{ groupSummary(row as UserManagementRecord) }}</p>
            </div>
          </template>

          <template #cell-status="{ row }">
            <div class="flex flex-wrap gap-2">
              <Badge
                :variant="row.is_active ? 'secondary' : 'outline'"
                :class="row.is_active ? 'bg-emerald-100 text-emerald-800' : 'text-slate-600'"
                data-testid="user-management-status"
              >
                {{ row.is_active ? 'Active' : 'Inactive' }}
              </Badge>
              <Badge
                v-if="row.is_superuser"
                variant="secondary"
                class="bg-amber-100 text-amber-800"
              >
                Superuser
              </Badge>
              <Badge v-if="row.is_staff" variant="secondary" class="bg-sky-100 text-sky-800">
                Staff
              </Badge>
            </div>
          </template>

          <template #cell-actions="{ row }">
            <div class="flex flex-wrap gap-2">
              <Button as-child variant="outline" size="sm">
                <RouterLink
                  :to="`/settings/users/${row.id}`"
                  data-testid="user-management-open"
                >
                  Open
                </RouterLink>
              </Button>
              <Button
                variant="outline"
                size="sm"
                data-testid="user-management-toggle-active"
                @click="toggleUserActiveState(row as UserManagementRecord)"
              >
                {{ row.is_active ? 'Deactivate' : 'Reactivate' }}
              </Button>
              <Button
                variant="destructive"
                size="sm"
                data-testid="user-management-delete"
                @click="removeUser(row as UserManagementRecord)"
              >
                Delete
              </Button>
            </div>
          </template>
        </DataTable>
      </CardContent>
    </Card>

    <div
      v-if="isInviteDialogOpen"
      class="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/40 p-4"
      data-testid="user-management-create-dialog"
      @click.self="closeInviteDialog"
    >
      <div class="w-full max-w-2xl rounded-lg bg-white shadow-xl">
        <div class="flex items-start justify-between border-b border-slate-200 px-6 py-4">
          <div>
            <h3 class="text-lg font-semibold text-slate-900">Create user</h3>
            <p class="mt-1 text-sm text-slate-600">
              Create a Datamingle email/password account. Team access is assigned separately.
            </p>
          </div>
          <Button variant="ghost" size="icon" type="button" @click="closeInviteDialog">
            <X class="h-4 w-4" />
          </Button>
        </div>

        <form class="grid gap-5 px-6 py-5" @submit.prevent="submitInvite">
          <p
            v-if="inviteError"
            class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700"
            data-testid="user-management-create-error"
          >
            {{ inviteError }}
          </p>

          <div class="grid gap-2">
            <label for="invite-email" class="text-sm font-medium text-slate-900">Email</label>
            <Input
              id="invite-email"
              v-model="inviteEmail"
              type="email"
              autocomplete="email"
              data-testid="user-management-create-email"
              :disabled="inviteSubmitting"
              placeholder="person@example.com"
            />
          </div>

          <div class="grid gap-2">
            <label for="invite-display" class="text-sm font-medium text-slate-900"
              >Display name</label
            >
            <Input
              id="invite-display"
              v-model="inviteDisplay"
              data-testid="user-management-create-display"
              :disabled="inviteSubmitting"
              placeholder="Optional"
            />
          </div>

          <div class="grid gap-2">
            <label for="invite-password" class="text-sm font-medium text-slate-900">
              Initial password
            </label>
            <Input
              id="invite-password"
              v-model="invitePassword"
              autocomplete="new-password"
              data-testid="user-management-create-password"
              :disabled="inviteSubmitting"
              placeholder="At least 9 characters"
              type="password"
            />
          </div>

          <div class="flex justify-end gap-3 border-t border-slate-200 pt-4">
            <Button
              variant="outline"
              type="button"
              :disabled="inviteSubmitting"
              @click="closeInviteDialog"
            >
              Cancel
            </Button>
            <Button
              type="submit"
              class="gap-2"
              data-testid="user-management-create-submit"
              :disabled="inviteSubmitting"
            >
              <UserPlus class="h-4 w-4" />
              {{ inviteSubmitting ? 'Creating...' : 'Create user' }}
            </Button>
          </div>
        </form>
      </div>
    </div>
  </section>
</template>
