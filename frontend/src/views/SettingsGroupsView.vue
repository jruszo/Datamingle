<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { RouterLink } from 'vue-router'
import { useDebounceFn } from '@vueuse/core'
import { Plus, RefreshCw } from 'lucide-vue-next'

import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { deleteGroup, fetchGroups, type GroupRecord } from '@/lib/api'
import { DataTable, type DataTableColumn } from '@/components/ui/data-table'
import { useAuthStore } from '@/stores/auth'

const authStore = useAuthStore()

const groups = ref<GroupRecord[]>([])
const isLoading = ref(false)
const error = ref('')
const feedback = ref('')
const totalCount = ref(0)
const searchQuery = ref('')
const currentPage = ref(1)
const pageSize = ref(20)
const sortKey = ref('name')
const sortDirection = ref<'asc' | 'desc'>('asc')
const latestRequestId = ref(0)
const columns: DataTableColumn[] = [
  {
    key: 'name',
    label: 'Group',
    sortable: true,
    hideable: false,
  },
  {
    key: 'permission_count',
    label: 'Permissions',
    sortable: false,
    defaultVisible: true,
  },
  {
    key: 'id',
    label: 'Group ID',
    sortable: true,
    defaultVisible: false,
  },
  {
    key: 'actions',
    label: 'Actions',
    hideable: false,
    headerClass: 'w-[12rem]',
  },
]

function hasPermission(permission: string) {
  if (authStore.currentUser?.is_superuser) {
    return true
  }
  return authStore.currentUser?.permissions.includes(permission) ?? false
}

const canAccessSettings = computed(() => hasPermission('sql.menu_system'))
const canViewGroups = computed(() => canAccessSettings.value && hasPermission('auth.view_group'))
const canCreateGroups = computed(() => hasPermission('sql.menu_system') || hasPermission('auth.add_group'))
const canEditGroups = computed(() => hasPermission('sql.menu_system') || hasPermission('auth.change_group'))
const canDeleteGroups = computed(() => hasPermission('sql.menu_system') || hasPermission('auth.delete_group'))
const tableRows = computed(() =>
  groups.value.map((group) => ({
    ...group,
    permission_count: group.permissions.length,
  })),
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

async function loadGroups() {
  const requestId = latestRequestId.value + 1
  latestRequestId.value = requestId
  isLoading.value = true
  error.value = ''

  try {
    await authStore.loadCurrentUser()

    if (!canViewGroups.value) {
      groups.value = []
      totalCount.value = 0
      error.value = 'You do not have permission to manage Datamingle groups.'
      return
    }

    const ordering = sortKey.value ? `${sortDirection.value === 'desc' ? '-' : ''}${sortKey.value}` : undefined
    const response = await fetchGroups(requireToken(), {
      page: currentPage.value,
      size: pageSize.value,
      search: searchQuery.value,
      ordering,
    })

    if (requestId !== latestRequestId.value) {
      return
    }

    groups.value = response.results
    totalCount.value = response.count
  } catch (errorValue) {
    if (requestId !== latestRequestId.value) {
      return
    }
    error.value = toUserFacingMessage(errorValue, 'Failed to load groups.')
  } finally {
    if (requestId === latestRequestId.value) {
      isLoading.value = false
    }
  }
}

async function removeGroup(group: GroupRecord) {
  if (!canDeleteGroups.value) {
    return
  }

  if (!window.confirm(`Delete the "${group.name}" group from Datamingle?`)) {
    return
  }

  try {
    const detail = await deleteGroup(group.id, requireToken())
    feedback.value = detail
    await loadGroups()
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to delete the group.')
  }
}

function removeGroupById(groupId: number) {
  const group = groups.value.find((item) => item.id === groupId)
  if (!group) {
    error.value = 'Unable to locate the selected group.'
    return
  }

  void removeGroup(group)
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
  void loadGroups()
})

const debouncedLoadGroups = useDebounceFn(() => {
  feedback.value = ''
  void loadGroups()
}, 250)

watch([currentPage, pageSize, sortKey, sortDirection], () => {
  feedback.value = ''
  void loadGroups()
})

watch(searchQuery, () => {
  feedback.value = ''
  debouncedLoadGroups()
})
</script>

<template>
  <section class="grid gap-6">
    <div class="space-y-1">
      <h2 class="text-2xl font-semibold text-slate-900">Permission Groups</h2>
      <p class="text-sm text-slate-600">
        Manage Django auth groups and their permission assignments in Datamingle.
      </p>
    </div>

    <Card class="border-slate-200">
      <CardHeader>
        <CardTitle>Groups</CardTitle>
        <CardDescription>
          Search, sort, and tailor visible columns while maintaining Datamingle permission groups.
        </CardDescription>
      </CardHeader>
      <CardContent class="space-y-5">
        <p v-if="error" class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
          {{ error }}
        </p>
        <p v-else-if="feedback" class="rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700">
          {{ feedback }}
        </p>

        <DataTable
          :columns="columns"
          :empty-text="'No Datamingle groups are available.'"
          :manual-pagination="true"
          :manual-search="true"
          :manual-sort="true"
          :rows="tableRows"
          :loading="isLoading"
          :page="currentPage"
          :page-size="pageSize"
          :search-query="searchQuery"
          :sort-key="sortKey"
          :sort-direction="sortDirection"
          :total-rows="totalCount"
          row-key="id"
          search-placeholder="Filter groups by name or ID"
          :search-keys="['name', 'id', 'permission_count']"
          @update:page="currentPage = $event"
          @update:page-size="handlePageSizeChange"
          @update:search-query="handleSearchQueryChange"
          @update:sort-key="sortKey = $event"
          @update:sort-direction="sortDirection = $event"
        >
          <template #toolbar-actions>
            <Button variant="outline" @click="loadGroups">
              <RefreshCw class="h-4 w-4" />
              Refresh
            </Button>
            <Button v-if="canCreateGroups" as-child>
              <RouterLink to="/settings/groups/new">
                <Plus class="h-4 w-4" />
                Create group
              </RouterLink>
            </Button>
          </template>

          <template #cell-name="{ row }">
            <div class="font-medium text-slate-900">{{ row.name }}</div>
            <div class="mt-1 text-xs text-slate-500">Group ID {{ row.id }}</div>
          </template>

          <template #cell-permission_count="{ value }">
            <Badge variant="secondary" class="bg-slate-100 text-slate-700">
              {{ value }} assigned
            </Badge>
          </template>

          <template #cell-actions="{ row }">
            <div class="flex flex-wrap gap-2">
              <Button v-if="canEditGroups || canViewGroups" as-child variant="outline" size="sm">
                <RouterLink :to="`/settings/groups/${row.id}`">Open</RouterLink>
              </Button>
              <Button
                v-if="canDeleteGroups"
                variant="destructive"
                size="sm"
                @click="removeGroupById(Number(row.id))"
              >
                Delete
              </Button>
            </div>
          </template>
        </DataTable>
      </CardContent>
    </Card>
  </section>
</template>
