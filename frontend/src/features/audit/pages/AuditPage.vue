<script setup lang="ts">
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { RouterLink, useRoute } from 'vue-router'
import { useDebounceFn } from '@vueuse/core'
import { RefreshCw, Search } from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import {
  fetchGeneralAuditLogs,
  fetchQueryAuditLogs,
  fetchSqlWorkflowAuditLogs,
  type GeneralAuditLogRecord,
  type PaginatedResponse,
  type QueryAuditLogRecord,
  type SqlWorkflowAuditLogRecord,
} from '../api'
import { useAuthStore } from '@/stores/auth'

type AuditView = 'general' | 'query' | 'workflow'

const authStore = useAuthStore()
const route = useRoute()

const activeView = ref<AuditView>('general')
const loading = ref(false)
const error = ref('')
const pageSize = 20
let initializing = true

const filters = reactive({
  search: '',
  startDate: '',
  endDate: '',
  action: '',
  status: '',
  syntaxType: '',
  page: 1,
})

const generalLogs = ref<PaginatedResponse<GeneralAuditLogRecord>>({
  count: 0,
  next: null,
  previous: null,
  results: [],
})
const queryLogs = ref<PaginatedResponse<QueryAuditLogRecord>>({
  count: 0,
  next: null,
  previous: null,
  results: [],
})
const workflowLogs = ref<PaginatedResponse<SqlWorkflowAuditLogRecord>>({
  count: 0,
  next: null,
  previous: null,
  results: [],
})

const selectClass =
  'block h-10 w-full rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-700 shadow-sm outline-none transition focus:border-slate-400'

function hasPermission(permission: string) {
  if (authStore.currentUser?.is_superuser) {
    return true
  }
  return authStore.currentUser?.permissions?.includes(permission) ?? false
}

function requireToken() {
  if (!authStore.accessToken) {
    throw new Error('Missing access token. Please login again.')
  }
  return authStore.accessToken
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

const canViewAudit = computed(() => hasPermission('sql.audit_user'))

const activePage = computed(() => {
  if (activeView.value === 'general') {
    return generalLogs.value
  }
  if (activeView.value === 'query') {
    return queryLogs.value
  }
  return workflowLogs.value
})

function formatDateTime(value: string | null | undefined) {
  if (!value) {
    return '-'
  }
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return value
  }
  return date.toLocaleString()
}

function baseFilters() {
  return {
    page: filters.page,
    size: pageSize,
    search: filters.search.trim(),
    start_date: filters.startDate,
    end_date: filters.endDate,
  }
}

async function loadAuditLogs() {
  if (!canViewAudit.value) {
    error.value = 'You do not have permission to access audit logs.'
    return
  }

  loading.value = true
  error.value = ''

  try {
    if (activeView.value === 'general') {
      generalLogs.value = await fetchGeneralAuditLogs(
        { ...baseFilters(), action: filters.action },
        requireToken(),
      )
    } else if (activeView.value === 'query') {
      queryLogs.value = await fetchQueryAuditLogs(baseFilters(), requireToken())
    } else {
      workflowLogs.value = await fetchSqlWorkflowAuditLogs(
        {
          ...baseFilters(),
          status: filters.status,
          syntax_type: filters.syntaxType,
        },
        requireToken(),
      )
    }
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to load audit logs.')
  } finally {
    loading.value = false
  }
}

function setView(view: AuditView) {
  activeView.value = view
  filters.page = 1
  filters.action = ''
  filters.status = ''
  filters.syntaxType = ''
}

function movePage(direction: -1 | 1) {
  const nextPage = filters.page + direction
  if (nextPage < 1 || (direction === 1 && activePage.value.next === null)) {
    return
  }
  filters.page = nextPage
  void loadAuditLogs()
}

const debouncedLoadAuditLogs = useDebounceFn(() => {
  filters.page = 1
  void loadAuditLogs()
}, 250)

watch(
  () => [
    activeView.value,
    filters.search,
    filters.action,
    filters.status,
    filters.syntaxType,
    filters.startDate,
    filters.endDate,
  ],
  () => {
    if (initializing) {
      return
    }
    debouncedLoadAuditLogs()
  },
)

onMounted(async () => {
  try {
    await authStore.loadCurrentUser()
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to load the current user.')
    return
  }

  const tab = Array.isArray(route.query.tab) ? route.query.tab[0] : route.query.tab
  if (tab === 'query') {
    activeView.value = 'query'
  } else if (tab === 'workflow' || tab === 'sql-workflow') {
    activeView.value = 'workflow'
  } else {
    activeView.value = 'general'
  }

  initializing = false
  await loadAuditLogs()
})
</script>

<template>
  <section class="grid gap-6">
    <div class="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
      <div class="space-y-1">
        <h2 class="text-2xl font-semibold text-slate-900">Audit</h2>
        <p class="text-sm text-slate-600">
          Review user actions, online query activity, and SQL workflow history.
        </p>
      </div>
      <Button variant="outline" type="button" class="gap-2" :disabled="loading" @click="void loadAuditLogs()">
        <RefreshCw class="h-4 w-4" />
        Refresh
      </Button>
    </div>

    <p v-if="error" class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
      {{ error }}
    </p>

    <div class="flex flex-wrap gap-2">
      <Button type="button" :variant="activeView === 'general' ? 'default' : 'outline'" @click="setView('general')">
        General
      </Button>
      <Button type="button" :variant="activeView === 'query' ? 'default' : 'outline'" @click="setView('query')">
        Query
      </Button>
      <Button type="button" :variant="activeView === 'workflow' ? 'default' : 'outline'" @click="setView('workflow')">
        SQL Workflows
      </Button>
    </div>

    <Card class="border-slate-200">
      <CardHeader>
        <CardTitle>Filters</CardTitle>
        <CardDescription>{{ activePage.count }} matching records</CardDescription>
      </CardHeader>
      <CardContent>
        <div class="grid gap-4 lg:grid-cols-[minmax(0,1.2fr)_repeat(4,minmax(0,0.7fr))]">
          <label class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">Search</span>
            <div class="relative">
              <Search class="pointer-events-none absolute left-3 top-2.5 h-4 w-4 text-slate-400" />
              <Input v-model="filters.search" class="pl-9" placeholder="Search audit records" />
            </div>
          </label>
          <label class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">Start</span>
            <Input v-model="filters.startDate" type="date" />
          </label>
          <label class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">End</span>
            <Input v-model="filters.endDate" type="date" />
          </label>
          <label v-if="activeView === 'general'" class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">Action</span>
            <Input v-model="filters.action" placeholder="Login" />
          </label>
          <label v-if="activeView === 'workflow'" class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">Status</span>
            <select v-model="filters.status" :class="selectClass">
              <option value="">All statuses</option>
              <option value="workflow_manreviewing">Pending review</option>
              <option value="workflow_review_pass">Approved</option>
              <option value="workflow_timingtask">Scheduled</option>
              <option value="workflow_executing">Executing</option>
              <option value="workflow_finish">Finished</option>
              <option value="workflow_exception">Failed</option>
              <option value="workflow_abort">Canceled</option>
            </select>
          </label>
          <label v-if="activeView === 'workflow'" class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">Type</span>
            <select v-model="filters.syntaxType" :class="selectClass">
              <option value="">All types</option>
              <option value="1">DDL</option>
              <option value="2">DML</option>
              <option value="3">Export</option>
            </select>
          </label>
        </div>
      </CardContent>
    </Card>

    <Card class="border-slate-200">
      <CardContent class="p-0">
        <div v-if="loading" class="p-6 text-sm text-slate-500">Loading audit records...</div>

        <div v-else-if="activeView === 'general'" class="overflow-x-auto">
          <table class="min-w-full divide-y divide-slate-200 text-sm">
            <thead class="bg-slate-50">
              <tr>
                <th class="px-4 py-3 text-left font-medium text-slate-600">Action</th>
                <th class="px-4 py-3 text-left font-medium text-slate-600">User</th>
                <th class="px-4 py-3 text-left font-medium text-slate-600">Info</th>
                <th class="px-4 py-3 text-left font-medium text-slate-600">Time</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-slate-100 bg-white">
              <tr v-for="(record, index) in generalLogs.results" :key="record.id ?? `${record.user_id}-${record.action_time}-${index}`">
                <td class="px-4 py-3"><Badge variant="outline">{{ record.action }}</Badge></td>
                <td class="px-4 py-3">
                  <RouterLink v-if="record.user_id" class="font-medium text-slate-900 hover:underline" :to="`/settings/users/${record.user_id}`">
                    {{ record.user_display || record.user_name }}
                  </RouterLink>
                  <span v-else>{{ record.user_display || record.user_name || '-' }}</span>
                </td>
                <td class="max-w-[32rem] truncate px-4 py-3 text-slate-600">{{ record.extra_info || '-' }}</td>
                <td class="px-4 py-3 text-slate-500">{{ formatDateTime(record.action_time) }}</td>
              </tr>
            </tbody>
          </table>
        </div>

        <div v-else-if="activeView === 'query'" class="overflow-x-auto">
          <table class="min-w-full divide-y divide-slate-200 text-sm">
            <thead class="bg-slate-50">
              <tr>
                <th class="px-4 py-3 text-left font-medium text-slate-600">Query</th>
                <th class="px-4 py-3 text-left font-medium text-slate-600">Target</th>
                <th class="px-4 py-3 text-left font-medium text-slate-600">User</th>
                <th class="px-4 py-3 text-left font-medium text-slate-600">Rows</th>
                <th class="px-4 py-3 text-left font-medium text-slate-600">Time</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-slate-100 bg-white">
              <tr v-for="record in queryLogs.results" :key="record.id">
                <td class="max-w-[34rem] px-4 py-3">
                  <pre class="max-h-24 overflow-auto whitespace-pre-wrap text-xs text-slate-700">{{ record.sqllog }}</pre>
                </td>
                <td class="px-4 py-3 text-slate-600">{{ record.instance_name }} / {{ record.db_name }}</td>
                <td class="px-4 py-3 text-slate-600">{{ record.user_display || record.username }}</td>
                <td class="px-4 py-3 text-slate-600">{{ record.effect_row }}</td>
                <td class="px-4 py-3 text-slate-500">{{ formatDateTime(record.create_time) }}</td>
              </tr>
            </tbody>
          </table>
        </div>

        <div v-else class="overflow-x-auto">
          <table class="min-w-full divide-y divide-slate-200 text-sm">
            <thead class="bg-slate-50">
              <tr>
                <th class="px-4 py-3 text-left font-medium text-slate-600">Workflow</th>
                <th class="px-4 py-3 text-left font-medium text-slate-600">Target</th>
                <th class="px-4 py-3 text-left font-medium text-slate-600">Submitter</th>
                <th class="px-4 py-3 text-left font-medium text-slate-600">Status</th>
                <th class="px-4 py-3 text-left font-medium text-slate-600">Created</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-slate-100 bg-white">
              <tr v-for="record in workflowLogs.results" :key="record.id">
                <td class="px-4 py-3">
                  <RouterLink class="font-medium text-slate-900 hover:underline" :to="`/workflows/${record.id}`">
                    {{ record.workflow_name }}
                  </RouterLink>
                  <p class="text-xs text-slate-500">{{ record.syntax_type_label }}</p>
                </td>
                <td class="px-4 py-3 text-slate-600">{{ record.group_name }} / {{ record.instance_name }} / {{ record.db_name }}</td>
                <td class="px-4 py-3 text-slate-600">{{ record.engineer_display || record.engineer }}</td>
                <td class="px-4 py-3"><Badge variant="outline">{{ record.status_label || record.status }}</Badge></td>
                <td class="px-4 py-3 text-slate-500">{{ formatDateTime(record.create_time) }}</td>
              </tr>
            </tbody>
          </table>
        </div>

        <div v-if="!loading && activePage.results.length === 0" class="border-t border-slate-100 p-6 text-sm text-slate-500">
          No audit records match the current filters.
        </div>

        <div class="flex items-center justify-between border-t border-slate-200 px-4 py-3 text-sm text-slate-600">
          <span>Page {{ filters.page }}</span>
          <div class="flex gap-2">
            <Button variant="outline" type="button" :disabled="activePage.previous === null || filters.page <= 1 || loading" @click="movePage(-1)">
              Previous
            </Button>
            <Button variant="outline" type="button" :disabled="activePage.next === null || loading" @click="movePage(1)">
              Next
            </Button>
          </div>
        </div>
      </CardContent>
    </Card>
  </section>
</template>
