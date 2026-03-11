<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { RouterLink } from 'vue-router'
import { useDebounceFn } from '@vueuse/core'
import { Plus, RefreshCw } from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { DataTable, type DataTableColumn } from '@/components/ui/data-table'
import { fetchInstanceTags, updateInstanceTag, type InstanceTagRecord } from '@/lib/api'
import { useAuthStore } from '@/stores/auth'

const authStore = useAuthStore()

const tags = ref<InstanceTagRecord[]>([])
const isLoading = ref(false)
const error = ref('')
const feedback = ref('')
const totalCount = ref(0)
const searchQuery = ref('')
const currentPage = ref(1)
const pageSize = ref(20)
const sortKey = ref('tag_name')
const sortDirection = ref<'asc' | 'desc'>('asc')
const latestRequestId = ref(0)

const columns: DataTableColumn[] = [
  { key: 'tag_name', label: 'Tag', sortable: true, hideable: false },
  { key: 'tag_code', label: 'Code', sortable: true },
  { key: 'active', label: 'Status', sortable: true },
  { key: 'id', label: 'Tag ID', sortable: true, defaultVisible: false },
  { key: 'actions', label: 'Actions', hideable: false, headerClass: 'w-[14rem]' },
]

function hasPermission(permission: string) {
  if (authStore.currentUser?.is_superuser) {
    return true
  }
  return authStore.currentUser?.permissions.includes(permission) ?? false
}

const canManageInstanceTags = computed(() => hasPermission('sql.menu_instance'))

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

async function loadTags() {
  const requestId = latestRequestId.value + 1
  latestRequestId.value = requestId
  isLoading.value = true
  error.value = ''

  try {
    await authStore.loadCurrentUser()

    if (!canManageInstanceTags.value) {
      tags.value = []
      totalCount.value = 0
      error.value = 'You do not have permission to manage Datamingle instance tags.'
      return
    }

    const ordering = sortKey.value
      ? `${sortDirection.value === 'desc' ? '-' : ''}${sortKey.value}`
      : undefined
    const response = await fetchInstanceTags(requireToken(), {
      page: currentPage.value,
      size: pageSize.value,
      search: searchQuery.value,
      ordering,
    })

    if (requestId !== latestRequestId.value) {
      return
    }

    tags.value = response.results
    totalCount.value = response.count
  } catch (errorValue) {
    if (requestId !== latestRequestId.value) {
      return
    }
    error.value = toUserFacingMessage(errorValue, 'Failed to load instance tags.')
  } finally {
    if (requestId === latestRequestId.value) {
      isLoading.value = false
    }
  }
}

async function toggleTagActiveState(tag: InstanceTagRecord) {
  if (!canManageInstanceTags.value) {
    return
  }

  const nextIsActive = !tag.active
  const actionLabel = nextIsActive ? 'reactivate' : 'deactivate'
  if (!window.confirm(`${actionLabel[0]?.toUpperCase() ?? ''}${actionLabel.slice(1)} "${tag.tag_name}"?`)) {
    return
  }

  try {
    const updatedTag = await updateInstanceTag(
      tag.id,
      {
        tag_name: tag.tag_name,
        active: nextIsActive,
      },
      requireToken(),
    )
    feedback.value = nextIsActive
      ? 'Instance tag reactivated successfully.'
      : 'Instance tag deactivated successfully.'
    tags.value = tags.value.map((item) => (item.id === updatedTag.id ? updatedTag : item))
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, `Failed to ${actionLabel} the tag.`)
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
  void loadTags()
})

const debouncedLoadTags = useDebounceFn(() => {
  feedback.value = ''
  void loadTags()
}, 250)

watch([currentPage, pageSize, sortKey, sortDirection], () => {
  feedback.value = ''
  void loadTags()
})

watch(searchQuery, () => {
  feedback.value = ''
  debouncedLoadTags()
})
</script>

<template>
  <section class="grid gap-6">
    <div class="space-y-1">
      <h2 class="text-2xl font-semibold text-slate-900">Instance Tags</h2>
      <p class="text-sm text-slate-600">
        Create and maintain the tags used by inventory filters and query access rules.
      </p>
    </div>

    <Card class="border-slate-200">
      <CardHeader>
        <CardTitle>Tags</CardTitle>
        <CardDescription>
          Search, sort, and manage reusable tags such as <code>can_read</code> and <code>can_write</code>.
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
          :empty-text="'No instance tags are available.'"
          :manual-pagination="true"
          :manual-search="true"
          :manual-sort="true"
          :rows="tags"
          :loading="isLoading"
          :page="currentPage"
          :page-size="pageSize"
          :search-query="searchQuery"
          :sort-key="sortKey"
          :sort-direction="sortDirection"
          :total-rows="totalCount"
          row-key="id"
          search-placeholder="Filter tags by code, name, or ID"
          :search-keys="['tag_name', 'tag_code', 'id', 'active']"
          @update:page="currentPage = $event"
          @update:page-size="handlePageSizeChange"
          @update:search-query="handleSearchQueryChange"
          @update:sort-key="sortKey = $event"
          @update:sort-direction="sortDirection = $event"
        >
          <template #toolbar-actions>
            <Button variant="outline" @click="loadTags">
              <RefreshCw class="h-4 w-4" />
              Refresh
            </Button>
            <Button v-if="canManageInstanceTags" as-child>
              <RouterLink to="/settings/instance-tags/new">
                <Plus class="h-4 w-4" />
                Create tag
              </RouterLink>
            </Button>
          </template>

          <template #cell-tag_name="{ row }">
            <div class="font-medium text-slate-900">{{ row.tag_name }}</div>
            <div class="mt-1 text-xs text-slate-500">Tag ID {{ row.id }}</div>
          </template>

          <template #cell-tag_code="{ value }">
            <code class="rounded bg-slate-100 px-1.5 py-0.5 text-xs text-slate-700">{{ value }}</code>
          </template>

          <template #cell-active="{ value }">
            <Badge
              :variant="value ? 'secondary' : 'outline'"
              :class="value ? 'bg-emerald-100 text-emerald-800' : 'text-slate-600'"
            >
              {{ value ? 'Active' : 'Inactive' }}
            </Badge>
          </template>

          <template #cell-actions="{ row }">
            <div class="flex flex-wrap gap-2">
              <Button as-child variant="outline" size="sm">
                <RouterLink :to="`/settings/instance-tags/${row.id}`">Open</RouterLink>
              </Button>
              <Button variant="outline" size="sm" @click="toggleTagActiveState(row as InstanceTagRecord)">
                {{ row.active ? 'Deactivate' : 'Reactivate' }}
              </Button>
            </div>
          </template>
        </DataTable>
      </CardContent>
    </Card>
  </section>
</template>
