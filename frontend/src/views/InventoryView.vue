<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { RouterLink, useRoute } from 'vue-router'
import { useDebounceFn } from '@vueuse/core'
import { Pencil, Plus, RefreshCw } from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { DataTable, type DataTableColumn } from '@/components/ui/data-table'
import {
  fetchInstanceInventory,
  fetchInstanceInventoryMetadata,
  testInstanceConnection,
  type InstanceInventoryMetadata,
  type InstanceInventoryRecord,
} from '@/lib/api'
import { useAuthStore } from '@/stores/auth'

const authStore = useAuthStore()
const route = useRoute()

const metadata = ref<InstanceInventoryMetadata | null>(null)
const instances = ref<InstanceInventoryRecord[]>([])
const isLoading = ref(false)
const isMetadataLoading = ref(false)
const error = ref('')
const feedback = ref('')
const totalCount = ref(0)
const searchQuery = ref('')
const currentPage = ref(1)
const pageSize = ref(20)
const sortKey = ref('id')
const sortDirection = ref<'asc' | 'desc'>('asc')
const selectedType = ref('')
const selectedDbType = ref('')
const selectedTagIds = ref<number[]>([])
const latestRequestId = ref(0)
const testingInstanceId = ref<number | null>(null)

const columns: DataTableColumn[] = [
  { key: 'id', label: 'ID', sortable: true, defaultVisible: false },
  { key: 'instance_name', label: 'Instance', sortable: true, hideable: false },
  { key: 'type', label: 'Type', sortable: true },
  { key: 'db_type', label: 'Database', sortable: true },
  { key: 'host', label: 'Host', sortable: true },
  { key: 'port', label: 'Port', sortable: true },
  { key: 'user', label: 'User', sortable: true },
  { key: 'actions', label: 'Actions', hideable: false, headerClass: 'w-[12rem]' },
]

const selectClass =
  'block h-10 w-full rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-700 shadow-sm outline-none transition focus:border-slate-400'
const multiSelectClass =
  'block min-h-[7rem] w-full rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700 shadow-sm outline-none transition focus:border-slate-400'

function hasPermission(permission: string) {
  if (authStore.currentUser?.is_superuser) {
    return true
  }
  return authStore.currentUser?.permissions.includes(permission) ?? false
}

const canAccessInventory = computed(() => hasPermission('sql.menu_instance'))
const canCreateInstances = computed(() => canAccessInventory.value)
const canEditInstances = computed(() => canAccessInventory.value)
const canRunConnectionTest = computed(() => authStore.currentUser?.is_superuser ?? false)

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

async function loadMetadata() {
  isMetadataLoading.value = true

  try {
    metadata.value = await fetchInstanceInventoryMetadata(requireToken())
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to load instance metadata.')
  } finally {
    isMetadataLoading.value = false
  }
}

async function loadInstances() {
  const requestId = latestRequestId.value + 1
  latestRequestId.value = requestId
  isLoading.value = true
  error.value = ''

  try {
    await authStore.loadCurrentUser()

    if (!canAccessInventory.value) {
      instances.value = []
      totalCount.value = 0
      error.value = 'You do not have permission to access Datamingle inventory.'
      return
    }

    const ordering = sortKey.value ? `${sortDirection.value === 'desc' ? '-' : ''}${sortKey.value}` : undefined
    const response = await fetchInstanceInventory(requireToken(), {
      page: currentPage.value,
      size: pageSize.value,
      search: searchQuery.value,
      type: selectedType.value,
      db_type: selectedDbType.value,
      tag_ids: selectedTagIds.value,
      ordering,
    })

    if (requestId !== latestRequestId.value) {
      return
    }

    instances.value = response.results
    totalCount.value = response.count
  } catch (errorValue) {
    if (requestId !== latestRequestId.value) {
      return
    }
    error.value = toUserFacingMessage(errorValue, 'Failed to load instances.')
  } finally {
    if (requestId === latestRequestId.value) {
      isLoading.value = false
    }
  }
}

async function refreshInventory() {
  feedback.value = ''
  await Promise.all([loadMetadata(), loadInstances()])
}

async function runConnectionTest(instance: InstanceInventoryRecord) {
  if (!canRunConnectionTest.value) {
    return
  }

  testingInstanceId.value = instance.id
  error.value = ''
  feedback.value = ''

  try {
    const detail = await testInstanceConnection(instance.id, requireToken())
    feedback.value = `${instance.instance_name}: ${detail}`
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, `Failed to test ${instance.instance_name}.`)
  } finally {
    testingInstanceId.value = null
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

function handleTagChange(event: Event) {
  const target = event.target as HTMLSelectElement
  selectedTagIds.value = Array.from(target.selectedOptions)
    .map((option) => Number(option.value))
    .filter((value) => Number.isFinite(value))
  currentPage.value = 1
}

onMounted(async () => {
  await authStore.loadCurrentUser()

  if (typeof route.query.created === 'string' && route.query.created) {
    feedback.value = `Instance "${route.query.created}" created successfully.`
  } else if (typeof route.query.edited === 'string' && route.query.edited) {
    feedback.value = `Instance "${route.query.edited}" updated successfully.`
  }

  if (!canAccessInventory.value) {
    error.value = 'You do not have permission to access Datamingle inventory.'
    return
  }

  await Promise.all([loadMetadata(), loadInstances()])
})

const debouncedLoadInstances = useDebounceFn(() => {
  feedback.value = ''
  void loadInstances()
}, 250)

watch([currentPage, pageSize, sortKey, sortDirection, selectedType, selectedDbType], () => {
  feedback.value = ''
  void loadInstances()
})

watch(selectedTagIds, () => {
  feedback.value = ''
  void loadInstances()
})

watch(searchQuery, () => {
  feedback.value = ''
  debouncedLoadInstances()
})
</script>

<template>
  <section class="grid gap-6">
    <div class="space-y-1">
      <h2 class="text-2xl font-semibold text-slate-900">Inventory</h2>
      <p class="text-sm text-slate-600">
        Browse Datamingle instances, filter them like the legacy frontend, and add new inventory records in the SPA.
      </p>
    </div>

    <Card class="border-slate-200">
      <CardHeader>
        <CardTitle>Instances</CardTitle>
        <CardDescription>
          Filter by role, engine, and tags before opening a new connection test or adding more infrastructure.
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

        <div class="grid gap-4 xl:grid-cols-[minmax(0,0.7fr)_minmax(0,0.9fr)_minmax(0,1.2fr)]">
          <div class="grid min-w-0 gap-2">
            <span class="text-xs font-semibold uppercase tracking-[0.24em] text-slate-500">Instance Type</span>
            <select v-model="selectedType" :class="selectClass">
              <option value="">All types</option>
              <option
                v-for="item in metadata?.instance_types ?? []"
                :key="item.value"
                :value="item.value"
              >
                {{ item.label }}
              </option>
            </select>
          </div>

          <div class="grid min-w-0 gap-2">
            <span class="text-xs font-semibold uppercase tracking-[0.24em] text-slate-500">Database Type</span>
            <select v-model="selectedDbType" :class="selectClass">
              <option value="">All databases</option>
              <option
                v-for="item in metadata?.db_types ?? []"
                :key="item.value"
                :value="item.value"
              >
                {{ item.label }}
              </option>
            </select>
          </div>

          <div class="grid min-w-0 gap-2">
            <span class="text-xs font-semibold uppercase tracking-[0.24em] text-slate-500">Tag Filter</span>
            <select :class="multiSelectClass" multiple @change="handleTagChange">
              <option
                v-for="tag in metadata?.tags ?? []"
                :key="tag.id"
                :selected="selectedTagIds.includes(tag.id)"
                :value="tag.id"
              >
                {{ tag.tag_name }}
              </option>
            </select>
            <span class="text-xs text-slate-500">Hold Command/Ctrl to select multiple tags.</span>
          </div>
        </div>

        <DataTable
          :columns="columns"
          :empty-text="'No Datamingle instances are available.'"
          :manual-pagination="true"
          :manual-search="true"
          :manual-sort="true"
          :rows="instances"
          :loading="isLoading || isMetadataLoading"
          :page="currentPage"
          :page-size="pageSize"
          :search-query="searchQuery"
          :sort-key="sortKey"
          :sort-direction="sortDirection"
          :total-rows="totalCount"
          row-key="id"
          search-placeholder="Filter instances by name, host, user, or ID"
          :search-keys="['instance_name', 'host', 'user', 'id']"
          @update:page="currentPage = $event"
          @update:page-size="handlePageSizeChange"
          @update:search-query="handleSearchQueryChange"
          @update:sort-key="sortKey = $event"
          @update:sort-direction="sortDirection = $event"
        >
          <template #toolbar-actions>
            <Button variant="outline" @click="void refreshInventory()">
              <RefreshCw class="h-4 w-4" />
              Refresh
            </Button>
            <Button v-if="canCreateInstances" as-child>
              <RouterLink to="/inventory/new">
                <Plus class="h-4 w-4" />
                Add instance
              </RouterLink>
            </Button>
          </template>

          <template #cell-instance_name="{ row }">
            <div class="font-medium text-slate-900">{{ row.instance_name }}</div>
            <div class="mt-1 text-xs text-slate-500">{{ row.host }}:{{ row.port }}</div>
          </template>

          <template #cell-type="{ value }">
            <Badge variant="secondary" class="bg-slate-100 text-slate-700">
              {{ `${value}`.toUpperCase() }}
            </Badge>
          </template>

          <template #cell-db_type="{ value }">
            <Badge variant="outline" class="border-slate-300 text-slate-700">
              {{ value }}
            </Badge>
          </template>

          <template #cell-actions="{ row }">
            <div class="flex items-center gap-2">
              <Button
                v-if="canEditInstances"
                size="sm"
                variant="outline"
                as-child
              >
                <RouterLink :to="`/inventory/${row.id}`">
                  <Pencil class="h-4 w-4" />
                  Edit
                </RouterLink>
              </Button>
              <Button
                v-if="canRunConnectionTest"
                size="sm"
                variant="outline"
                :disabled="testingInstanceId === row.id"
                @click="void runConnectionTest(row as InstanceInventoryRecord)"
              >
                {{ testingInstanceId === row.id ? 'Testing…' : 'Test connection' }}
              </Button>
              <span v-else class="text-xs text-slate-400">Superuser only</span>
            </div>
          </template>
        </DataTable>
      </CardContent>
    </Card>
  </section>
</template>
