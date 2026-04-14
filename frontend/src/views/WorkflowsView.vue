<script setup lang="ts">
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ExternalLink, RefreshCw, Send } from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import {
  fetchWorkflowExportSubmissionMetadata,
  fetchWorkflowSubmissionMetadata,
  fetchWorkflows,
  type PaginatedResponse,
  type WorkflowSubmissionMetadata,
  type WorkflowSummaryRecord,
} from '@/lib/api'
import { useAuthStore } from '@/stores/auth'

const authStore = useAuthStore()
const route = useRoute()
const router = useRouter()

const listLoading = ref(false)
const metadataLoading = ref(false)
const pageError = ref('')
const feedback = ref('')

const metadata = ref<WorkflowSubmissionMetadata | null>(null)
const exportMetadata = ref<WorkflowSubmissionMetadata | null>(null)

const workflowsPage = ref<PaginatedResponse<WorkflowSummaryRecord>>({
  count: 0,
  next: null,
  previous: null,
  results: [],
})

const filters = reactive({
  search: '',
  status: '',
  syntaxType: '',
  groupId: '',
  instanceId: '',
  startDate: '',
  endDate: '',
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

function syntaxBadgeClass(syntaxType: number) {
  switch (syntaxType) {
    case 1:
      return 'border-sky-200 bg-sky-50 text-sky-700'
    case 2:
      return 'border-amber-200 bg-amber-50 text-amber-700'
    case 3:
      return 'border-violet-200 bg-violet-50 text-violet-700'
    default:
      return 'border-slate-200 bg-slate-100 text-slate-600'
  }
}

function statusBadgeClass(status: string) {
  switch (status) {
    case 'workflow_finish':
      return 'border-emerald-200 bg-emerald-50 text-emerald-700'
    case 'workflow_review_pass':
      return 'border-blue-200 bg-blue-50 text-blue-700'
    case 'workflow_manreviewing':
    case 'workflow_timingtask':
    case 'workflow_queuing':
    case 'workflow_executing':
      return 'border-amber-200 bg-amber-50 text-amber-700'
    case 'workflow_exception':
    case 'workflow_abort':
    case 'workflow_autoreviewwrong':
      return 'border-rose-200 bg-rose-50 text-rose-700'
    default:
      return 'border-slate-200 bg-slate-100 text-slate-600'
  }
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
  filters.syntaxType = routeQueryValue(route.query.syntaxType)
  filters.groupId = routeQueryValue(route.query.groupId)
  filters.instanceId = routeQueryValue(route.query.instanceId)
  filters.startDate = routeQueryValue(route.query.startDate)
  filters.endDate = routeQueryValue(route.query.endDate)
  filters.page = parsePositiveInteger(routeQueryValue(route.query.page), 1)
  isSyncingFromRoute = false
}

function buildListQuery() {
  return {
    ...(filters.search ? { search: filters.search } : {}),
    ...(filters.status ? { status: filters.status } : {}),
    ...(filters.syntaxType ? { syntaxType: filters.syntaxType } : {}),
    ...(filters.groupId ? { groupId: filters.groupId } : {}),
    ...(filters.instanceId ? { instanceId: filters.instanceId } : {}),
    ...(filters.startDate ? { startDate: filters.startDate } : {}),
    ...(filters.endDate ? { endDate: filters.endDate } : {}),
    ...(filters.page > 1 ? { page: `${filters.page}` } : {}),
  }
}

function routeMatchesCurrentFilters() {
  const currentQuery = buildListQuery()
  const routeQuery = {
    ...(routeQueryValue(route.query.search) ? { search: routeQueryValue(route.query.search) } : {}),
    ...(routeQueryValue(route.query.status) ? { status: routeQueryValue(route.query.status) } : {}),
    ...(routeQueryValue(route.query.syntaxType) ? { syntaxType: routeQueryValue(route.query.syntaxType) } : {}),
    ...(routeQueryValue(route.query.groupId) ? { groupId: routeQueryValue(route.query.groupId) } : {}),
    ...(routeQueryValue(route.query.instanceId) ? { instanceId: routeQueryValue(route.query.instanceId) } : {}),
    ...(routeQueryValue(route.query.startDate) ? { startDate: routeQueryValue(route.query.startDate) } : {}),
    ...(routeQueryValue(route.query.endDate) ? { endDate: routeQueryValue(route.query.endDate) } : {}),
    ...(routeQueryValue(route.query.page) ? { page: routeQueryValue(route.query.page) } : {}),
  }

  return JSON.stringify(currentQuery) === JSON.stringify(routeQuery)
}

async function updateListRoute() {
  if (routeMatchesCurrentFilters()) {
    await loadWorkflows()
    return
  }

  await router.replace({
    name: 'workflows',
    query: buildListQuery(),
  })
}

const canViewWorkflows = computed(() => (
  hasPermission('sql.menu_sqlworkflow')
  || hasPermission('sql.menu_sqlexportworkflow')
  || hasPermission('sql.sql_submit')
  || hasPermission('sql.sqlexport_submit')
  || hasPermission('sql.offline_download')
  || hasPermission('sql.audit_user')
))

const canCreateDdl = computed(() => {
  return (metadata.value?.instances ?? []).some((instance) =>
    instance.allowed_syntax_types.includes(1),
  )
})

const canCreateDml = computed(() => {
  return (metadata.value?.instances ?? []).some((instance) =>
    instance.allowed_syntax_types.includes(2),
  )
})

const canCreateExport = computed(() => (exportMetadata.value?.instances ?? []).length > 0)

const filterGroups = computed(() => {
  const groups = new Map<number, WorkflowSubmissionMetadata['resource_groups'][number]>()
  for (const group of metadata.value?.resource_groups ?? []) {
    groups.set(group.group_id, group)
  }
  for (const group of exportMetadata.value?.resource_groups ?? []) {
    groups.set(group.group_id, group)
  }
  return Array.from(groups.values()).sort((left, right) => left.group_name.localeCompare(right.group_name))
})

const allFilterInstances = computed(() => {
  const instances = new Map<number, WorkflowSubmissionMetadata['instances'][number]>()
  for (const instance of metadata.value?.instances ?? []) {
    instances.set(instance.id, instance)
  }
  for (const instance of exportMetadata.value?.instances ?? []) {
    const current = instances.get(instance.id)
    if (!current) {
      instances.set(instance.id, instance)
      continue
    }

    const mergedGroupIds = Array.from(new Set([...current.group_ids, ...instance.group_ids]))
    const mergedGroupNames = Array.from(new Set([...current.group_names, ...instance.group_names]))
    const mergedSyntaxTypes = Array.from(
      new Set([...current.allowed_syntax_types, ...instance.allowed_syntax_types]),
    ).sort((left, right) => left - right)
    instances.set(instance.id, {
      ...current,
      group_ids: mergedGroupIds,
      group_names: mergedGroupNames,
      allowed_syntax_types: mergedSyntaxTypes,
    })
  }

  return Array.from(instances.values()).sort((left, right) =>
    left.instance_name.localeCompare(right.instance_name),
  )
})

const filteredInstances = computed(() => {
  const groupId = Number(filters.groupId)
  const instances = allFilterInstances.value
  if (!groupId) {
    return instances
  }
  return instances.filter((instance) =>
    instance.group_ids.includes(groupId),
  )
})

const canMoveBackward = computed(() => workflowsPage.value.previous !== null && filters.page > 1)
const canMoveForward = computed(() => workflowsPage.value.next !== null)

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
    if (route.name !== 'workflows') {
      return
    }
    syncFiltersFromRoute()
    void loadWorkflows()
  },
)

async function loadMetadata() {
  if (!canViewWorkflows.value) {
    return
  }

  metadataLoading.value = true
  try {
    const token = requireToken()
    const [workflowResult, exportResult] = await Promise.allSettled([
      fetchWorkflowSubmissionMetadata(token),
      fetchWorkflowExportSubmissionMetadata(token),
    ])

    if (workflowResult.status === 'fulfilled') {
      metadata.value = workflowResult.value
    } else {
      metadata.value = null
    }

    if (exportResult.status === 'fulfilled') {
      exportMetadata.value = exportResult.value
    } else {
      exportMetadata.value = null
    }

    if (workflowResult.status === 'rejected' && exportResult.status === 'rejected') {
      throw workflowResult.reason
    }
  } catch (errorValue) {
    pageError.value = toUserFacingMessage(errorValue, 'Failed to load workflow metadata.')
  } finally {
    metadataLoading.value = false
  }
}

async function loadWorkflows() {
  if (!canViewWorkflows.value) {
    return
  }

  listLoading.value = true
  pageError.value = ''

  try {
    workflowsPage.value = await fetchWorkflows(requireToken(), {
      page: filters.page,
      size: filters.size,
      search: filters.search,
      status: filters.status || undefined,
      syntax_type: filters.syntaxType ? Number(filters.syntaxType) as 1 | 2 | 3 : undefined,
      group_id: filters.groupId ? Number(filters.groupId) : undefined,
      instance_id: filters.instanceId ? Number(filters.instanceId) : undefined,
      start_date: filters.startDate || undefined,
      end_date: filters.endDate || undefined,
    })
  } catch (errorValue) {
    pageError.value = toUserFacingMessage(errorValue, 'Failed to load workflows.')
  } finally {
    listLoading.value = false
  }
}

async function openWorkflowDetail(workflowId: number) {
  feedback.value = ''
  await router.push({
    name: 'workflow-detail',
    params: { workflowId: `${workflowId}` },
    query: {
      returnTo: route.fullPath,
    },
  })
}

async function applyFilters() {
  filters.page = 1
  await updateListRoute()
}

async function clearFilters() {
  filters.search = ''
  filters.status = ''
  filters.syntaxType = ''
  filters.groupId = ''
  filters.instanceId = ''
  filters.startDate = ''
  filters.endDate = ''
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

onMounted(async () => {
  await authStore.loadCurrentUser()
  if (!canViewWorkflows.value) {
    return
  }

  syncFiltersFromRoute()
  await Promise.all([
    loadMetadata(),
    loadWorkflows(),
  ])
})
</script>

<template>
  <section class="grid gap-6">
    <div class="flex flex-col gap-4 rounded-2xl border border-slate-200 bg-white p-5 shadow-sm lg:flex-row lg:items-center lg:justify-between">
      <div class="space-y-1">
        <h1 class="text-2xl font-semibold text-slate-900">SQL Workflows</h1>
        <p class="text-sm text-slate-500">
          Submit SQL change and export requests, review approvals, and track execution or file delivery from one place.
        </p>
      </div>

      <div class="flex flex-wrap items-center gap-2">
        <Button
          v-if="canCreateDdl"
          variant="outline"
          type="button"
          class="gap-2"
          @click="void router.push({ name: 'workflow-ddl-new' })"
        >
          <Send class="h-4 w-4" />
          New DDL request
        </Button>
        <Button
          v-if="canCreateDml"
          type="button"
          class="gap-2"
          @click="void router.push({ name: 'workflow-dml-new' })"
        >
          <Send class="h-4 w-4" />
          New DML request
        </Button>
        <Button
          v-if="canCreateExport"
          variant="outline"
          type="button"
          class="gap-2"
          @click="void router.push({ name: 'workflow-export-new' })"
        >
          <Send class="h-4 w-4" />
          New export request
        </Button>
        <Button variant="outline" type="button" class="gap-2" @click="void loadWorkflows()">
          <RefreshCw class="h-4 w-4" />
          Refresh
        </Button>
      </div>
    </div>

    <p
      v-if="feedback"
      class="rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700"
    >
      {{ feedback }}
    </p>

    <Card v-if="!canViewWorkflows" class="border-red-200">
      <CardHeader>
        <CardTitle>Access denied</CardTitle>
        <CardDescription>
          Workflow access requires SQL workflow, export workflow, submit, download, or audit permissions.
        </CardDescription>
      </CardHeader>
    </Card>

    <template v-else>
      <Card class="border-slate-200">
        <CardHeader class="gap-4">
          <div class="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
            <div>
              <CardTitle>Workflow List</CardTitle>
              <CardDescription>
                Browse visible workflows, then open one to review the audit chain and execution state.
              </CardDescription>
            </div>
            <Badge variant="outline" class="border-slate-200 bg-slate-50 text-slate-600">
              {{ workflowsPage.count }} total
            </Badge>
          </div>
          <div class="grid gap-3 lg:grid-cols-3">
            <Input
              v-model="filters.search"
              data-testid="workflow-filter-search"
              placeholder="Search workflow, requester, database, instance, or group"
            />
            <select v-model="filters.status" data-testid="workflow-filter-status" :class="selectClass">
              <option value="">All statuses</option>
              <option value="workflow_manreviewing">Pending review</option>
              <option value="workflow_review_pass">Approved</option>
              <option value="workflow_timingtask">Scheduled</option>
              <option value="workflow_queuing">Queued</option>
              <option value="workflow_executing">Executing</option>
              <option value="workflow_finish">Finished</option>
              <option value="workflow_exception">Execution failed</option>
              <option value="workflow_abort">Canceled</option>
              <option value="workflow_autoreviewwrong">Auto-rejected</option>
            </select>
            <select v-model="filters.syntaxType" data-testid="workflow-filter-syntax-type" :class="selectClass">
              <option value="">All syntax types</option>
              <option value="1">DDL</option>
              <option value="2">DML</option>
              <option value="3">Export</option>
            </select>
            <select v-model="filters.groupId" data-testid="workflow-filter-group" :class="selectClass" :disabled="metadataLoading">
              <option value="">All groups</option>
              <option
                v-for="group in filterGroups"
                :key="group.group_id"
                :value="`${group.group_id}`"
              >
                {{ group.group_name }}
              </option>
            </select>
            <select v-model="filters.instanceId" data-testid="workflow-filter-instance" :class="selectClass" :disabled="metadataLoading">
              <option value="">All instances</option>
              <option
                v-for="instance in filteredInstances"
                :key="instance.id"
                :value="`${instance.id}`"
              >
                {{ instance.instance_name }} · {{ instance.db_type }}
              </option>
            </select>
            <div class="grid gap-2 sm:grid-cols-2">
              <input
                v-model="filters.startDate"
                class="block h-10 w-full rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-700 shadow-sm outline-none transition focus:border-slate-400"
                type="date"
              >
              <input
                v-model="filters.endDate"
                class="block h-10 w-full rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-700 shadow-sm outline-none transition focus:border-slate-400"
                type="date"
              >
            </div>
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
            Loading workflows...
          </div>

          <div
            v-else-if="workflowsPage.results.length === 0"
            class="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-10 text-sm text-slate-500"
          >
            No workflows match the current filters.
          </div>

          <div v-else class="grid gap-3">
            <button
              v-for="workflow in workflowsPage.results"
              :key="workflow.id"
              :data-testid="`workflow-list-item-${workflow.id}`"
              type="button"
              class="grid gap-3 rounded-2xl border border-slate-200 bg-white p-4 text-left transition hover:border-slate-300 hover:bg-slate-50"
              @click="void openWorkflowDetail(workflow.id)"
            >
              <div class="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
                <div class="space-y-2">
                  <div class="flex flex-wrap items-center gap-2">
                    <p class="font-medium text-slate-900">{{ workflow.workflow_name }}</p>
                    <Badge variant="outline" :class="syntaxBadgeClass(workflow.syntax_type)">
                      {{ workflow.syntax_type_label }}
                    </Badge>
                    <Badge variant="outline" :class="statusBadgeClass(workflow.status)">
                      {{ workflow.status_label }}
                    </Badge>
                    <a
                      v-if="workflow.demand_url"
                      :href="workflow.demand_url"
                      class="inline-flex items-center gap-1 text-xs font-medium text-sky-700 hover:text-sky-800"
                      target="_blank"
                      rel="noreferrer"
                      @click.stop
                    >
                      Demand
                      <ExternalLink class="h-3 w-3" />
                    </a>
                  </div>
                  <p class="text-sm text-slate-500">
                    {{ workflow.group_name }} / {{ workflow.instance_name }} / {{ workflow.db_name }}
                  </p>
                </div>
                <div class="text-sm text-slate-500">
                  <p>{{ workflow.engineer_display }}</p>
                  <p>{{ formatDateTime(workflow.create_time) }}</p>
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
