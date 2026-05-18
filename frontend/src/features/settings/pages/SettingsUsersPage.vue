<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { RouterLink } from 'vue-router'
import { useDebounceFn } from '@vueuse/core'
import { MailPlus, RefreshCw, X } from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { DataTable, type DataTableColumn } from '@/components/ui/data-table'
import { Input } from '@/components/ui/input'
import {
  deleteUser,
  fetchGroups,
  fetchUsers,
  inviteWorkosUser,
  updateUser,
  type GroupRecord,
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
const inviteGroupIds = ref<number[]>([])
const inviteGroups = ref<GroupRecord[]>([])
const inviteGroupsLoading = ref(false)
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
    key: 'groups',
    label: 'Groups',
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
  if (user.groups.length === 0) {
    return 'No groups assigned'
  }

  return user.groups.map((group) => group.name).join(', ')
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

  if (!window.confirm(`${actionLabel[0]?.toUpperCase() ?? ''}${actionLabel.slice(1)} "${user.display || user.username}"?`)) {
    return
  }

  try {
    const updatedUser = await updateUser(
      user.id,
      {
        group_ids: user.groups.map((group) => group.id),
        is_active: nextIsActive,
      },
      requireToken(),
    )
    feedback.value = nextIsActive ? 'User reactivated successfully.' : 'User deactivated successfully.'
    users.value = users.value.map((item) => (item.id === updatedUser.id ? updatedUser : item))
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, `Failed to ${actionLabel} the user.`)
  }
}

async function removeUser(user: UserManagementRecord) {
  if (!canManageUsers.value) {
    return
  }

  if (!window.confirm(`Delete "${user.display || user.username}" from Datamingle? This cannot be undone.`)) {
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

async function loadInviteGroups() {
  inviteGroupsLoading.value = true
  inviteError.value = ''

  try {
    const collectedGroups: GroupRecord[] = []
    let page = 1
    let hasMore = true

    while (hasMore) {
      const response = await fetchGroups(requireToken(), {
        page,
        size: 100,
        ordering: 'name',
      })
      collectedGroups.push(...response.results)
      hasMore = response.next !== null
      page += 1
    }

    inviteGroups.value = collectedGroups.sort((left, right) => left.name.localeCompare(right.name))
  } catch (errorValue) {
    inviteError.value = toUserFacingMessage(errorValue, 'Failed to load groups for invitation.')
  } finally {
    inviteGroupsLoading.value = false
  }
}

function openInviteDialog() {
  if (!canManageUsers.value) {
    return
  }

  inviteEmail.value = ''
  inviteDisplay.value = ''
  inviteGroupIds.value = []
  inviteError.value = ''
  isInviteDialogOpen.value = true
  void loadInviteGroups()
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

  inviteSubmitting.value = true
  inviteError.value = ''

  try {
    const response = await inviteWorkosUser(
      {
        email,
        display: inviteDisplay.value.trim(),
        group_ids: inviteGroupIds.value,
      },
      requireToken(),
    )
    feedback.value = `Invitation sent to ${response.invitation.email || email}.`
    isInviteDialogOpen.value = false
    await loadUsers()
  } catch (errorValue) {
    inviteError.value = toUserFacingMessage(errorValue, 'Failed to send WorkOS invitation.')
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
        Superusers can invite WorkOS users, assign Django auth groups, and control local lifecycle state.
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
        <p v-if="error" class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
          {{ error }}
        </p>
        <p
          v-else-if="feedback"
          class="rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700"
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
          :search-keys="['display', 'username', 'email', 'groups']"
          @update:page="currentPage = $event"
          @update:page-size="handlePageSizeChange"
          @update:search-query="handleSearchQueryChange"
          @update:sort-key="sortKey = $event"
          @update:sort-direction="sortDirection = $event"
        >
          <template #toolbar-actions>
            <Button @click="openInviteDialog">
              <MailPlus class="h-4 w-4" />
              Invite user
            </Button>
            <Button variant="outline" @click="loadUsers">
              <RefreshCw class="h-4 w-4" />
              Refresh
            </Button>
          </template>

          <template #cell-display="{ row }">
            <div class="font-medium text-slate-900">{{ row.display || row.username }}</div>
            <div class="mt-1 text-xs text-slate-500">User ID {{ row.id }}</div>
          </template>

          <template #cell-username="{ value }">
            <code class="rounded bg-slate-100 px-1.5 py-0.5 text-xs text-slate-700">{{ value }}</code>
          </template>

          <template #cell-email="{ value }">
            <span class="text-sm text-slate-700">{{ value || 'No email address' }}</span>
          </template>

          <template #cell-groups="{ row }">
            <div class="space-y-2">
              <div class="flex flex-wrap gap-2">
                <Badge
                  v-for="group in (row as UserManagementRecord).groups.slice(0, 2)"
                  :key="group.id"
                  variant="secondary"
                  class="bg-slate-100 text-slate-700"
                >
                  {{ group.name }}
                </Badge>
                <Badge
                  v-if="(row as UserManagementRecord).groups.length > 2"
                  variant="secondary"
                  class="bg-slate-100 text-slate-700"
                >
                  +{{ (row as UserManagementRecord).groups.length - 2 }} more
                </Badge>
                <span
                  v-if="(row as UserManagementRecord).groups.length === 0"
                  class="text-xs text-slate-500"
                >
                  No groups assigned
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
              <Badge
                v-if="row.is_staff"
                variant="secondary"
                class="bg-sky-100 text-sky-800"
              >
                Staff
              </Badge>
              <Badge
                :variant="row.is_workos_managed ? 'secondary' : 'outline'"
                :class="row.is_workos_managed ? 'bg-violet-100 text-violet-800' : 'text-slate-600'"
              >
                {{ row.is_workos_managed ? 'WorkOS linked' : 'WorkOS pending' }}
              </Badge>
            </div>
          </template>

          <template #cell-actions="{ row }">
            <div class="flex flex-wrap gap-2">
              <Button as-child variant="outline" size="sm">
                <RouterLink :to="`/settings/users/${row.id}`">Open</RouterLink>
              </Button>
              <Button variant="outline" size="sm" @click="toggleUserActiveState(row as UserManagementRecord)">
                {{ row.is_active ? 'Deactivate' : 'Reactivate' }}
              </Button>
              <Button variant="destructive" size="sm" @click="removeUser(row as UserManagementRecord)">
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
      @click.self="closeInviteDialog"
    >
      <div class="w-full max-w-xl rounded-lg bg-white shadow-xl">
        <div class="flex items-start justify-between border-b border-slate-200 px-6 py-4">
          <div>
            <h3 class="text-lg font-semibold text-slate-900">Invite WorkOS user</h3>
            <p class="mt-1 text-sm text-slate-600">
              Send a WorkOS invitation and prepare the matching Datamingle access record.
            </p>
          </div>
          <Button variant="ghost" size="icon" type="button" @click="closeInviteDialog">
            <X class="h-4 w-4" />
          </Button>
        </div>

        <form class="grid gap-5 px-6 py-5" @submit.prevent="submitInvite">
          <p v-if="inviteError" class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
            {{ inviteError }}
          </p>

          <div class="grid gap-2">
            <label for="invite-email" class="text-sm font-medium text-slate-900">Email</label>
            <Input
              id="invite-email"
              v-model="inviteEmail"
              type="email"
              autocomplete="email"
              :disabled="inviteSubmitting"
              placeholder="person@example.com"
            />
          </div>

          <div class="grid gap-2">
            <label for="invite-display" class="text-sm font-medium text-slate-900">Display name</label>
            <Input
              id="invite-display"
              v-model="inviteDisplay"
              :disabled="inviteSubmitting"
              placeholder="Optional"
            />
          </div>

          <div class="grid gap-2">
            <label for="invite-groups" class="text-sm font-medium text-slate-900">Initial Datamingle groups</label>
            <select
              id="invite-groups"
              v-model="inviteGroupIds"
              multiple
              class="min-h-36 rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-slate-400 disabled:cursor-not-allowed disabled:opacity-50"
              :disabled="inviteSubmitting || inviteGroupsLoading"
            >
              <option v-for="group in inviteGroups" :key="group.id" :value="group.id">
                {{ group.name }}
              </option>
            </select>
            <p class="text-xs text-slate-500">
              The invited user is linked to this local record when they accept the WorkOS invitation and sign in.
            </p>
          </div>

          <div class="flex justify-end gap-3 border-t border-slate-200 pt-4">
            <Button variant="outline" type="button" :disabled="inviteSubmitting" @click="closeInviteDialog">
              Cancel
            </Button>
            <Button type="submit" class="gap-2" :disabled="inviteSubmitting || inviteGroupsLoading">
              <MailPlus class="h-4 w-4" />
              {{ inviteSubmitting ? 'Sending...' : 'Send invitation' }}
            </Button>
          </div>
        </form>
      </div>
    </div>
  </section>
</template>
