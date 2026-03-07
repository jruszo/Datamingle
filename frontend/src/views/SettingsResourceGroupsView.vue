<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { RouterLink } from 'vue-router'
import { useDebounceFn } from '@vueuse/core'
import { Plus, RefreshCw } from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { DataTable, type DataTableColumn } from '@/components/ui/data-table'
import { deleteResourceGroup, fetchResourceGroups, type ResourceGroupRecord } from '@/lib/api'
import { useAuthStore } from '@/stores/auth'

const authStore = useAuthStore()

const resourceGroups = ref<ResourceGroupRecord[]>([])
const isLoading = ref(false)
const error = ref('')
const feedback = ref('')
const totalCount = ref(0)
const searchQuery = ref('')
const currentPage = ref(1)
const pageSize = ref(20)
const sortKey = ref('group_name')
const sortDirection = ref<'asc' | 'desc'>('asc')
const latestRequestId = ref(0)

const columns: DataTableColumn[] = [
  {
    key: 'group_name',
    label: 'Group',
    sortable: true,
    hideable: false,
  },
  {
    key: 'user_count',
    label: 'Users',
    sortable: true,
  },
  {
    key: 'instance_count',
    label: 'Servers',
    sortable: true,
  },
  {
    key: 'group_id',
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
const canViewResourceGroups = computed(() => canAccessSettings.value && hasPermission('sql.view_resourcegroup'))
const canCreateResourceGroups = computed(() => hasPermission('sql.menu_system') || hasPermission('sql.add_resourcegroup'))
const canEditResourceGroups = computed(() => hasPermission('sql.menu_system') || hasPermission('sql.change_resourcegroup'))
const canDeleteResourceGroups = computed(() => hasPermission('sql.menu_system') || hasPermission('sql.delete_resourcegroup'))

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

async function loadResourceGroups() {
  const requestId = latestRequestId.value + 1
  latestRequestId.value = requestId
  isLoading.value = true
  error.value = ''

  try {
    await authStore.loadCurrentUser()

    if (!canViewResourceGroups.value) {
      resourceGroups.value = []
      totalCount.value = 0
      error.value = 'You do not have permission to manage Datamingle resource groups.'
      return
    }

    const ordering = sortKey.value
      ? `${sortDirection.value === 'desc' ? '-' : ''}${sortKey.value}`
      : undefined
    const response = await fetchResourceGroups(requireToken(), {
      page: currentPage.value,
      size: pageSize.value,
      search: searchQuery.value,
      ordering,
    })

    if (requestId !== latestRequestId.value) {
      return
    }

    resourceGroups.value = response.results
    totalCount.value = response.count
  } catch (errorValue) {
    if (requestId !== latestRequestId.value) {
      return
    }
    error.value = toUserFacingMessage(errorValue, 'Failed to load resource groups.')
  } finally {
    if (requestId === latestRequestId.value) {
      isLoading.value = false
    }
  }
}

async function removeResourceGroup(resourceGroup: ResourceGroupRecord) {
  if (!canDeleteResourceGroups.value) {
    return
  }

  if (!window.confirm(`Delete the "${resourceGroup.group_name}" resource group from Datamingle?`)) {
    return
  }

  try {
    const detail = await deleteResourceGroup(resourceGroup.group_id, requireToken())
    feedback.value = detail
    await loadResourceGroups()
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to delete the resource group.')
  }
}

function removeResourceGroupById(groupId: number) {
  const resourceGroup = resourceGroups.value.find((item) => item.group_id === groupId)
  if (!resourceGroup) {
    error.value = 'Unable to locate the selected resource group.'
    return
  }

  void removeResourceGroup(resourceGroup)
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
  void loadResourceGroups()
})

const debouncedLoadResourceGroups = useDebounceFn(() => {
  feedback.value = ''
  void loadResourceGroups()
}, 250)

watch([currentPage, pageSize, sortKey, sortDirection], () => {
  feedback.value = ''
  void loadResourceGroups()
})

watch(searchQuery, () => {
  feedback.value = ''
  debouncedLoadResourceGroups()
})
</script>

<template>
  <section class="grid gap-6">
    <div class="space-y-1">
      <h2 class="text-2xl font-semibold text-slate-900">Resource Groups</h2>
      <p class="text-sm text-slate-600">
        Manage the Datamingle resource groups that bundle users and servers together.
      </p>
    </div>

    <Card class="border-slate-200">
      <CardHeader>
        <CardTitle>Groups</CardTitle>
        <CardDescription>
          Search, sort, and maintain resource groups with the same filterable workflow used in Permission Groups.
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
          :empty-text="'No Datamingle resource groups are available.'"
          :manual-pagination="true"
          :manual-search="true"
          :manual-sort="true"
          :rows="resourceGroups"
          :loading="isLoading"
          :page="currentPage"
          :page-size="pageSize"
          :search-query="searchQuery"
          :sort-key="sortKey"
          :sort-direction="sortDirection"
          :total-rows="totalCount"
          row-key="group_id"
          search-placeholder="Filter resource groups by name or ID"
          :search-keys="['group_name', 'group_id', 'user_count', 'instance_count']"
          @update:page="currentPage = $event"
          @update:page-size="handlePageSizeChange"
          @update:search-query="handleSearchQueryChange"
          @update:sort-key="sortKey = $event"
          @update:sort-direction="sortDirection = $event"
        >
          <template #toolbar-actions>
            <Button variant="outline" @click="loadResourceGroups">
              <RefreshCw class="h-4 w-4" />
              Refresh
            </Button>
            <Button v-if="canCreateResourceGroups" as-child>
              <RouterLink to="/settings/resource-groups/new">
                <Plus class="h-4 w-4" />
                Create resource group
              </RouterLink>
            </Button>
          </template>

          <template #cell-group_name="{ row }">
            <div class="font-medium text-slate-900">{{ row.group_name }}</div>
            <div class="mt-1 text-xs text-slate-500">Group ID {{ row.group_id }}</div>
          </template>

          <template #cell-user_count="{ value }">
            <Badge variant="secondary" class="bg-slate-100 text-slate-700">
              {{ value }} users
            </Badge>
          </template>

          <template #cell-instance_count="{ value }">
            <Badge variant="secondary" class="bg-slate-100 text-slate-700">
              {{ value }} servers
            </Badge>
          </template>

          <template #cell-actions="{ row }">
            <div class="flex items-center gap-2">
              <Button
                v-if="canViewResourceGroups || canEditResourceGroups"
                as-child
                size="sm"
                variant="outline"
              >
                <RouterLink :to="`/settings/resource-groups/${row.group_id}`">Open</RouterLink>
              </Button>
              <Button
                v-if="canDeleteResourceGroups"
                size="sm"
                variant="ghost"
                @click="removeResourceGroupById(Number(row.group_id))"
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
