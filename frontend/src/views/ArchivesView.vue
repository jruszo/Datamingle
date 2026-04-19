<script setup lang="ts">
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { RefreshCw, Send } from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import {
  fetchArchiveMetadata,
  fetchArchives,
  type ArchiveListRecord,
  type ArchiveMetadataRecord,
  type PaginatedResponse,
} from '@/lib/api'
import { useAuthStore } from '@/stores/auth'

const authStore = useAuthStore()
const route = useRoute()
const router = useRouter()

const pageError = ref('')
const feedback = ref('')
const listLoading = ref(false)
const metadataLoading = ref(false)

const metadata = ref<ArchiveMetadataRecord | null>(null)
const archivesPage = ref<PaginatedResponse<ArchiveListRecord>>({
  count: 0,
  next: null,
  previous: null,
  results: [],
})

const filters = reactive({
  search: '',
  status: '',
  executionMode: '',
  groupId: '',
  instanceId: '',
  page: 1,
  size: 8,
})

const selectClass =
  'block h-10 w-full rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-700 shadow-sm outline-none transition focus:border-slate-400 disabled:cursor-not-allowed disabled:bg-slate-100'

let isSyncingFromRoute = false

function requireToken() {
  if (!authStore.accessToken) {
    throw new Error('Missing access token. Please login again.')
  }
  return authStore.accessToken
}

function hasPermission(permission: string) {
  if (authStore.currentUser?.is_superuser) {
    return true
  }
  return authStore.currentUser?.permissions.includes(permission) ?? false
}

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

function formatDateTime(value: string | null) {
  if (!value) {
    return 'Not set'
  }
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return value
  }
  return date.toLocaleString()
}

function routeQueryValue(value: unknown) {
  return typeof value === 'string' ? value : ''
}

function parsePositiveInteger(value: string, fallback: number) {
  const parsedValue = Number(value)
  return Number.isInteger(parsedValue) && parsedValue > 0 ? parsedValue : fallback
}

function syncFiltersFromRoute() {
  isSyncingFromRoute = true
  filters.search = routeQueryValue(route.query.search)
  filters.status = routeQueryValue(route.query.status)
  filters.executionMode = routeQueryValue(route.query.executionMode)
  filters.groupId = routeQueryValue(route.query.groupId)
  filters.instanceId = routeQueryValue(route.query.instanceId)
  filters.page = parsePositiveInteger(routeQueryValue(route.query.page), 1)
  isSyncingFromRoute = false
}

function buildListQuery() {
  return {
    ...(filters.search ? { search: filters.search } : {}),
    ...(filters.status ? { status: filters.status } : {}),
    ...(filters.executionMode ? { executionMode: filters.executionMode } : {}),
    ...(filters.groupId ? { groupId: filters.groupId } : {}),
    ...(filters.instanceId ? { instanceId: filters.instanceId } : {}),
    ...(filters.page > 1 ? { page: `${filters.page}` } : {}),
  }
}

function routeMatchesCurrentFilters() {
  const currentQuery = buildListQuery()
  const routeQuery = {
    ...(routeQueryValue(route.query.search) ? { search: routeQueryValue(route.query.search) } : {}),
    ...(routeQueryValue(route.query.status) ? { status: routeQueryValue(route.query.status) } : {}),
    ...(routeQueryValue(route.query.executionMode) ? { executionMode: routeQueryValue(route.query.executionMode) } : {}),
    ...(routeQueryValue(route.query.groupId) ? { groupId: routeQueryValue(route.query.groupId) } : {}),
    ...(routeQueryValue(route.query.instanceId) ? { instanceId: routeQueryValue(route.query.instanceId) } : {}),
    ...(routeQueryValue(route.query.page) ? { page: routeQueryValue(route.query.page) } : {}),
  }
  return JSON.stringify(currentQuery) === JSON.stringify(routeQuery)
}

async function updateListRoute() {
  if (routeMatchesCurrentFilters()) {
    await loadArchives()
    return
  }

  await router.replace({
    name: 'archives',
    query: buildListQuery(),
  })
}

const canViewArchives = computed(() => hasPermission('sql.menu_archive'))
const canCreateArchives = computed(() => hasPermission('sql.archive_apply'))

const filterGroups = computed(() => metadata.value?.resource_groups ?? [])

const filteredInstances = computed(() => {
  const instances = metadata.value?.instances ?? []
  const groupId = Number(filters.groupId)
  if (!groupId) {
    return instances
  }
  return instances.filter((instance) => instance.group_ids.includes(groupId))
})

const canMoveBackward = computed(() => archivesPage.value.previous !== null && filters.page > 1)
const canMoveForward = computed(() => archivesPage.value.next !== null)

function statusBadgeClass(status: number) {
  switch (status) {
    case 1:
      return 'border-emerald-200 bg-emerald-50 text-emerald-700'
    case 0:
      return 'border-amber-200 bg-amber-50 text-amber-700'
    case 2:
    case 3:
      return 'border-rose-200 bg-rose-50 text-rose-700'
    default:
      return 'border-slate-200 bg-slate-100 text-slate-600'
  }
}

function archiveMethodLabel(method: string) {
  if (method === 'pt_archiver') {
    return 'pt-archiver'
  }
  return 'DML delete'
}

function executionModeLabel(mode: string) {
  if (mode === 'scheduled') {
    return 'Scheduled'
  }
  return 'One time'
}

watch(
  () => filters.groupId,
  () => {
    if (isSyncingFromRoute || !filters.instanceId) {
      return
    }
    const selectedInstanceId = Number(filters.instanceId)
    const stillAvailable = filteredInstances.value.some((instance) => instance.id === selectedInstanceId)
    if (!stillAvailable) {
      filters.instanceId = ''
    }
  },
)

watch(
  () => route.query,
  () => {
    if (route.name !== 'archives') {
      return
    }
    syncFiltersFromRoute()
    void loadArchives()
  },
)

async function loadMetadata() {
  if (!canViewArchives.value) {
    return
  }
  metadataLoading.value = true
  try {
    metadata.value = await fetchArchiveMetadata(requireToken())
  } catch (errorValue) {
    pageError.value = toUserFacingMessage(errorValue, 'Failed to load archive metadata.')
  } finally {
    metadataLoading.value = false
  }
}

async function loadArchives() {
  if (!canViewArchives.value) {
    return
  }
  listLoading.value = true
  pageError.value = ''

  try {
    archivesPage.value = await fetchArchives(requireToken(), {
      page: filters.page,
      size: filters.size,
      search: filters.search,
      status: filters.status ? Number(filters.status) : undefined,
      execution_mode: filters.executionMode as 'one_time' | 'scheduled' | '',
      group_id: filters.groupId ? Number(filters.groupId) : undefined,
      instance_id: filters.instanceId ? Number(filters.instanceId) : undefined,
    })
  } catch (errorValue) {
    pageError.value = toUserFacingMessage(errorValue, 'Failed to load archives.')
  } finally {
    listLoading.value = false
  }
}

async function applyFilters() {
  filters.page = 1
  await updateListRoute()
}

async function clearFilters() {
  filters.search = ''
  filters.status = ''
  filters.executionMode = ''
  filters.groupId = ''
  filters.instanceId = ''
  filters.page = 1
  await updateListRoute()
}

async function goToPreviousPage() {
  if (!canMoveBackward.value) {
    return
  }
  filters.page -= 1
  await updateListRoute()
}

async function goToNextPage() {
  if (!canMoveForward.value) {
    return
  }
  filters.page += 1
  await updateListRoute()
}

async function openArchiveDetail(archiveId: number) {
  feedback.value = ''
  await router.push({
    name: 'archive-detail',
    params: { archiveId: `${archiveId}` },
    query: {
      returnTo: route.fullPath,
    },
  })
}

onMounted(async () => {
  await authStore.loadCurrentUser()
  if (!canViewArchives.value) {
    return
  }

  syncFiltersFromRoute()
  await Promise.all([loadMetadata(), loadArchives()])
})
</script>

<template>
  <section class="grid gap-6">
    <div class="flex flex-col gap-4 rounded-2xl border border-slate-200 bg-white p-5 shadow-sm lg:flex-row lg:items-center lg:justify-between">
      <div class="space-y-1">
        <h1 class="text-2xl font-semibold text-slate-900">Archives</h1>
        <p class="text-sm text-slate-500">
          Manage delete-only archive workflows with approval, one-time execution, and recurring schedules.
        </p>
      </div>

      <div class="flex flex-wrap items-center gap-2">
        <Button
          v-if="canCreateArchives"
          type="button"
          class="gap-2"
          @click="void router.push({ name: 'archive-new' })"
        >
          <Send class="h-4 w-4" />
          New archive
        </Button>
        <Button variant="outline" type="button" class="gap-2" @click="void loadArchives()">
          <RefreshCw class="h-4 w-4" />
          Refresh
        </Button>
      </div>
    </div>

    <Card v-if="!canViewArchives" class="border-red-200">
      <CardHeader>
        <CardTitle>Access denied</CardTitle>
        <CardDescription>
          Archive access requires the archive menu permission.
        </CardDescription>
      </CardHeader>
    </Card>

    <template v-else>
      <Card class="border-slate-200">
        <CardHeader class="gap-4">
          <div class="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
            <div>
              <CardTitle>Archive List</CardTitle>
              <CardDescription>
                Browse archive workflows, then open one to review approval, schedule state, and execution logs.
              </CardDescription>
            </div>
            <Badge variant="outline" class="border-slate-200 bg-slate-50 text-slate-600">
              {{ archivesPage.count }} total
            </Badge>
          </div>
          <div class="grid gap-3 lg:grid-cols-3">
            <Input
              v-model="filters.search"
              data-testid="archive-filter-search"
              placeholder="Search archive, requester, database, table, instance, or group"
            />
            <select v-model="filters.status" data-testid="archive-filter-status" :class="selectClass">
              <option value="">All statuses</option>
              <option value="0">Pending review</option>
              <option value="1">Approved</option>
              <option value="2">Rejected</option>
              <option value="3">Canceled</option>
            </select>
            <select v-model="filters.executionMode" data-testid="archive-filter-mode" :class="selectClass">
              <option value="">All execution modes</option>
              <option value="one_time">One time</option>
              <option value="scheduled">Scheduled</option>
            </select>
            <select v-model="filters.groupId" data-testid="archive-filter-group" :class="selectClass" :disabled="metadataLoading">
              <option value="">All groups</option>
              <option
                v-for="group in filterGroups"
                :key="group.group_id"
                :value="`${group.group_id}`"
              >
                {{ group.group_name }}
              </option>
            </select>
            <select v-model="filters.instanceId" data-testid="archive-filter-instance" :class="selectClass" :disabled="metadataLoading">
              <option value="">All instances</option>
              <option
                v-for="instance in filteredInstances"
                :key="instance.id"
                :value="`${instance.id}`"
              >
                {{ instance.instance_name }} · {{ instance.db_type }}
              </option>
            </select>
          </div>
          <div class="flex flex-wrap gap-2">
            <Button type="button" class="gap-2" @click="void applyFilters()">
              Apply filters
            </Button>
            <Button variant="outline" type="button" @click="void clearFilters()">
              Clear
            </Button>
          </div>
        </CardHeader>
        <CardContent class="space-y-4">
          <p
            v-if="pageError"
            class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700"
          >
            {{ pageError }}
          </p>

          <div
            v-if="listLoading"
            class="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-10 text-sm text-slate-500"
          >
            Loading archives...
          </div>

          <div
            v-else-if="archivesPage.results.length === 0"
            class="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-10 text-sm text-slate-500"
          >
            No archives match the current filters.
          </div>

          <div v-else class="grid gap-3">
            <button
              v-for="archive in archivesPage.results"
              :key="archive.id"
              :data-testid="`archive-list-item-${archive.id}`"
              type="button"
              class="grid gap-3 rounded-2xl border border-slate-200 bg-white p-4 text-left transition hover:border-slate-300 hover:bg-slate-50"
              @click="void openArchiveDetail(archive.id)"
            >
              <div class="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
                <div class="space-y-2">
                  <div class="flex flex-wrap items-center gap-2">
                    <p class="font-medium text-slate-900">{{ archive.title }}</p>
                    <Badge variant="outline" class="border-slate-200 bg-slate-50 text-slate-700">
                      {{ executionModeLabel(archive.execution_mode) }}
                    </Badge>
                    <Badge variant="outline" class="border-slate-200 bg-slate-50 text-slate-700">
                      {{ archiveMethodLabel(archive.archive_method) }}
                    </Badge>
                    <Badge variant="outline" :class="statusBadgeClass(archive.status)">
                      {{ archive.status_label }}
                    </Badge>
                  </div>
                  <p class="text-sm text-slate-500">
                    {{ archive.resource_group_name }} / {{ archive.src_instance_name }} / {{ archive.src_db_name }} / {{ archive.src_table_name }}
                  </p>
                  <p class="text-xs text-slate-500">
                    Next run: {{ formatDateTime(archive.next_run_at) }}
                  </p>
                </div>
                <div class="text-sm text-slate-500">
                  <p>{{ archive.user_display }}</p>
                  <p>{{ formatDateTime(archive.create_time) }}</p>
                </div>
              </div>
            </button>
          </div>

          <div class="flex flex-wrap items-center justify-between gap-3 border-t border-slate-100 pt-2">
            <p class="text-sm text-slate-500">
              Page {{ filters.page }}
            </p>
            <div class="flex gap-2">
              <Button variant="outline" type="button" :disabled="!canMoveBackward" @click="void goToPreviousPage()">
                Previous
              </Button>
              <Button variant="outline" type="button" :disabled="!canMoveForward" @click="void goToNextPage()">
                Next
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>
    </template>
  </section>
</template>
