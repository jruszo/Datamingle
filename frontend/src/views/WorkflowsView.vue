<script setup lang="ts">
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import {
  CalendarClock,
  CheckCircle2,
  ExternalLink,
  Play,
  RefreshCw,
  ShieldCheck,
} from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import {
  executeWorkflow,
  fetchWorkflowDetail,
  fetchWorkflowMetadata,
  fetchWorkflows,
  reviewWorkflow,
  scheduleWorkflow,
  updateWorkflowExecutionWindow,
  type PaginatedResponse,
  type WorkflowDetailRecord,
  type WorkflowMetadataRecord,
  type WorkflowResultRow,
  type WorkflowSummaryRecord,
} from '@/lib/api'
import { useAuthStore } from '@/stores/auth'

const authStore = useAuthStore()
const route = useRoute()
const router = useRouter()

const listLoading = ref(false)
const detailLoading = ref(false)
const metadataLoading = ref(false)
const reviewSubmitting = ref(false)
const executeSubmitting = ref(false)
const scheduleSubmitting = ref(false)
const windowSubmitting = ref(false)

const pageError = ref('')
const detailError = ref('')
const feedback = ref('')

const metadata = ref<WorkflowMetadataRecord | null>(null)
const selectedWorkflowId = ref<number | null>(null)
const selectedWorkflow = ref<WorkflowDetailRecord | null>(null)

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

const reviewForm = reactive({
  auditRemark: '',
})

const scheduleForm = reactive({
  runDate: '',
})

const windowForm = reactive({
  runDateStart: '',
  runDateEnd: '',
})

const selectClass =
  'block h-10 w-full rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-700 shadow-sm outline-none transition focus:border-slate-400 disabled:cursor-not-allowed disabled:bg-slate-100'
const textareaClass =
  'block min-h-[7rem] w-full rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700 shadow-sm outline-none transition focus:border-slate-400 disabled:cursor-not-allowed disabled:bg-slate-100'

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

function formatDateTimeLocalValue(value: string | null) {
  if (!value) {
    return ''
  }
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return value.slice(0, 16)
  }

  const year = date.getFullYear()
  const month = `${date.getMonth() + 1}`.padStart(2, '0')
  const day = `${date.getDate()}`.padStart(2, '0')
  const hours = `${date.getHours()}`.padStart(2, '0')
  const minutes = `${date.getMinutes()}`.padStart(2, '0')
  return `${year}-${month}-${day}T${hours}:${minutes}`
}

function stringifyCellValue(value: unknown) {
  if (value === null || value === undefined || value === '') {
    return '—'
  }
  if (Array.isArray(value) || typeof value === 'object') {
    return JSON.stringify(value)
  }
  return `${value}`
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

function resultColumns(rows: WorkflowResultRow[]) {
  const firstRow = rows[0]
  if (!firstRow) {
    return []
  }
  return Object.keys(firstRow)
}

function syncDetailForms(detail: WorkflowDetailRecord | null) {
  reviewForm.auditRemark = ''
  scheduleForm.runDate = ''
  windowForm.runDateStart = formatDateTimeLocalValue(detail?.run_date_start ?? null)
  windowForm.runDateEnd = formatDateTimeLocalValue(detail?.run_date_end ?? null)
}

const canViewWorkflows = computed(() => (
  hasPermission('sql.menu_sqlworkflow')
  || hasPermission('sql.sql_submit')
  || hasPermission('sql.audit_user')
))

const canSubmitWorkflow = computed(() => hasPermission('sql.sql_submit'))
const filteredInstances = computed(() => {
  const rows = metadata.value?.instances ?? []
  const groupId = Number(filters.groupId)
  if (!groupId) {
    return rows
  }
  return rows.filter((instance) =>
    instance.resource_groups.some((group) => group.group_id === groupId),
  )
})

const reviewResultColumns = computed(() => resultColumns(selectedWorkflow.value?.review_rows ?? []))
const executeResultColumns = computed(() => resultColumns(selectedWorkflow.value?.execute_rows ?? []))
const canMoveBackward = computed(() => workflowsPage.value.previous !== null && filters.page > 1)
const canMoveForward = computed(() => workflowsPage.value.next !== null)

watch(
  () => filters.groupId,
  () => {
    if (!filters.instanceId) {
      return
    }
    const selectedInstanceId = Number(filters.instanceId)
    const stillAvailable = filteredInstances.value.some((instance) => instance.id === selectedInstanceId)
    if (!stillAvailable) {
      filters.instanceId = ''
    }
  },
)

async function loadMetadata() {
  if (!canViewWorkflows.value) {
    return
  }

  metadataLoading.value = true
  try {
    metadata.value = await fetchWorkflowMetadata(requireToken())
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
      syntax_type: filters.syntaxType || undefined,
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

async function loadWorkflowDetail(workflowId: number) {
  detailLoading.value = true
  detailError.value = ''
  selectedWorkflowId.value = workflowId

  try {
    const detail = await fetchWorkflowDetail(workflowId, requireToken())
    selectedWorkflow.value = detail
    syncDetailForms(detail)
    await router.replace({
      name: 'workflow-detail',
      params: { workflowId: `${workflowId}` },
      query: Object.fromEntries(
        Object.entries(route.query).filter(([key]) => key !== 'workflowId'),
      ),
    })
  } catch (errorValue) {
    detailError.value = toUserFacingMessage(errorValue, 'Failed to load workflow detail.')
    selectedWorkflow.value = null
  } finally {
    detailLoading.value = false
  }
}

async function refreshSelectedWorkflow() {
  if (!selectedWorkflowId.value) {
    return
  }
  await Promise.all([loadWorkflows(), loadWorkflowDetail(selectedWorkflowId.value)])
}

async function submitReviewAction(auditType: 'pass' | 'reject' | 'cancel') {
  if (!selectedWorkflowId.value) {
    return
  }

  reviewSubmitting.value = true
  detailError.value = ''
  feedback.value = ''

  try {
    await reviewWorkflow(
      selectedWorkflowId.value,
      {
        workflow_type: 2,
        audit_type: auditType,
        audit_remark: reviewForm.auditRemark.trim(),
      },
      requireToken(),
    )
    if (auditType === 'pass') {
      feedback.value = 'Workflow approved.'
    } else if (auditType === 'reject') {
      feedback.value = 'Workflow rejected.'
    } else {
      feedback.value = 'Workflow canceled.'
    }
    reviewForm.auditRemark = ''
    await refreshSelectedWorkflow()
  } catch (errorValue) {
    detailError.value = toUserFacingMessage(errorValue, 'Failed to submit the workflow review action.')
  } finally {
    reviewSubmitting.value = false
  }
}

async function executeSelectedWorkflow(mode: 'auto' | 'manual') {
  if (!selectedWorkflowId.value) {
    return
  }

  executeSubmitting.value = true
  detailError.value = ''
  feedback.value = ''

  try {
    feedback.value = await executeWorkflow(
      selectedWorkflowId.value,
      {
        workflow_type: 2,
        mode,
      },
      requireToken(),
    )
    await refreshSelectedWorkflow()
  } catch (errorValue) {
    detailError.value = toUserFacingMessage(errorValue, 'Failed to start workflow execution.')
  } finally {
    executeSubmitting.value = false
  }
}

async function saveExecutionWindow() {
  if (!selectedWorkflowId.value) {
    return
  }

  windowSubmitting.value = true
  detailError.value = ''
  feedback.value = ''

  try {
    feedback.value = await updateWorkflowExecutionWindow(
      selectedWorkflowId.value,
      {
        run_date_start: windowForm.runDateStart || null,
        run_date_end: windowForm.runDateEnd || null,
      },
      requireToken(),
    )
    await refreshSelectedWorkflow()
  } catch (errorValue) {
    detailError.value = toUserFacingMessage(errorValue, 'Failed to update the execution window.')
  } finally {
    windowSubmitting.value = false
  }
}

async function saveSchedule() {
  if (!selectedWorkflowId.value) {
    return
  }
  if (!scheduleForm.runDate) {
    detailError.value = 'Choose a schedule time first.'
    return
  }

  scheduleSubmitting.value = true
  detailError.value = ''
  feedback.value = ''

  try {
    feedback.value = await scheduleWorkflow(
      selectedWorkflowId.value,
      {
        run_date: scheduleForm.runDate,
      },
      requireToken(),
    )
    await refreshSelectedWorkflow()
  } catch (errorValue) {
    detailError.value = toUserFacingMessage(errorValue, 'Failed to schedule this workflow.')
  } finally {
    scheduleSubmitting.value = false
  }
}

async function applyFilters() {
  filters.page = 1
  await loadWorkflows()
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
  await loadWorkflows()
}

async function openWorkflowFromQuery() {
  const routeValue = route.params.workflowId
  const rawValue = (
    typeof routeValue === 'string' && routeValue
      ? routeValue
      : typeof route.query.workflowId === 'string'
        ? route.query.workflowId
        : ''
  )
  if (!rawValue) {
    return
  }
  const workflowId = Number(rawValue)
  if (!workflowId) {
    return
  }
  await loadWorkflowDetail(workflowId)
}

async function goToPreviousPage() {
  if (!canMoveBackward.value) {
    return
  }
  filters.page -= 1
  await loadWorkflows()
}

async function goToNextPage() {
  if (!canMoveForward.value) {
    return
  }
  filters.page += 1
  await loadWorkflows()
}

onMounted(async () => {
  await authStore.loadCurrentUser()
  if (!canViewWorkflows.value) {
    return
  }

  await Promise.all([
    loadMetadata(),
    loadWorkflows(),
  ])
  await openWorkflowFromQuery()
})
</script>

<template>
  <section class="grid gap-6">
    <div class="flex flex-col gap-4 rounded-2xl border border-slate-200 bg-white p-5 shadow-sm lg:flex-row lg:items-center lg:justify-between">
      <div class="space-y-1">
        <h1 class="text-2xl font-semibold text-slate-900">SQL Workflows</h1>
        <p class="text-sm text-slate-500">
          Submit SQL change tickets, review approvals, and execute approved DDL or DML workflows.
        </p>
      </div>

      <div class="flex flex-wrap items-center gap-2">
        <Button
          v-if="canSubmitWorkflow"
          type="button"
          class="gap-2"
          @click="void router.push({ name: 'workflows-new' })"
        >
          <ShieldCheck class="h-4 w-4" />
          New workflow
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
          `sql.menu_sqlworkflow` or `sql.sql_submit` is required to access the workflow module.
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
              placeholder="Search workflow, requester, database, instance, or group"
            />
            <select v-model="filters.status" :class="selectClass">
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
            <select v-model="filters.syntaxType" :class="selectClass">
              <option value="">All syntax types</option>
              <option value="1">DDL</option>
              <option value="2">DML</option>
            </select>
            <select v-model="filters.groupId" :class="selectClass" :disabled="metadataLoading">
              <option value="">All groups</option>
              <option
                v-for="group in metadata?.resource_groups ?? []"
                :key="group.group_id"
                :value="`${group.group_id}`"
              >
                {{ group.group_name }}
              </option>
            </select>
            <select v-model="filters.instanceId" :class="selectClass" :disabled="metadataLoading">
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
              type="button"
              :class="selectedWorkflowId === workflow.id ? 'border-slate-400 bg-slate-50' : 'border-slate-200 bg-white hover:border-slate-300 hover:bg-slate-50'"
              class="grid gap-3 rounded-2xl border p-4 text-left transition"
              @click="void loadWorkflowDetail(workflow.id)"
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

      <Card v-if="!selectedWorkflowId" class="border-slate-200">
        <CardHeader>
          <CardTitle>Workflow Detail</CardTitle>
          <CardDescription>
            Select a workflow to inspect the approval flow, SQL review output, and execution controls.
          </CardDescription>
        </CardHeader>
      </Card>

      <Card v-else class="border-slate-200">
        <CardHeader class="gap-4">
          <div class="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
            <div>
              <CardTitle>{{ selectedWorkflow?.workflow_name || 'Workflow detail' }}</CardTitle>
              <CardDescription>
                Review approvals, time windows, execution controls, and result logs for the selected workflow.
              </CardDescription>
            </div>
            <Button variant="outline" type="button" class="gap-2" @click="void refreshSelectedWorkflow()">
              <RefreshCw class="h-4 w-4" />
              Refresh detail
            </Button>
          </div>
          <div v-if="selectedWorkflow" class="flex flex-wrap gap-2">
            <Badge variant="outline" :class="syntaxBadgeClass(selectedWorkflow.syntax_type)">
              {{ selectedWorkflow.syntax_type_label }}
            </Badge>
            <Badge variant="outline" :class="statusBadgeClass(selectedWorkflow.status)">
              {{ selectedWorkflow.status_label }}
            </Badge>
          </div>
        </CardHeader>
        <CardContent class="space-y-6">
          <p
            v-if="detailError"
            class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700"
          >
            {{ detailError }}
          </p>

          <div
            v-if="detailLoading"
            class="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-10 text-sm text-slate-500"
          >
            Loading workflow detail...
          </div>

          <template v-else-if="selectedWorkflow">
            <div class="grid gap-4 lg:grid-cols-2 xl:grid-cols-4">
              <div class="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                <p class="text-xs uppercase tracking-wide text-slate-500">Target</p>
                <p class="mt-2 text-sm font-medium text-slate-900">
                  {{ selectedWorkflow.instance_name }}
                </p>
                <p class="text-sm text-slate-500">{{ selectedWorkflow.db_name }}</p>
              </div>
              <div class="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                <p class="text-xs uppercase tracking-wide text-slate-500">Requester</p>
                <p class="mt-2 text-sm font-medium text-slate-900">
                  {{ selectedWorkflow.engineer_display }}
                </p>
                <p class="text-sm text-slate-500">{{ selectedWorkflow.group_name }}</p>
              </div>
              <div class="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                <p class="text-xs uppercase tracking-wide text-slate-500">Created</p>
                <p class="mt-2 text-sm font-medium text-slate-900">
                  {{ formatDateTime(selectedWorkflow.create_time) }}
                </p>
                <p class="text-sm text-slate-500">
                  Finished: {{ formatDateTime(selectedWorkflow.finish_time) }}
                </p>
              </div>
              <div class="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                <p class="text-xs uppercase tracking-wide text-slate-500">Execution window</p>
                <p class="mt-2 text-sm font-medium text-slate-900">
                  {{ formatDateTime(selectedWorkflow.run_date_start) }}
                </p>
                <p class="text-sm text-slate-500">
                  Ends {{ formatDateTime(selectedWorkflow.run_date_end) }}
                </p>
              </div>
            </div>

            <div class="grid gap-6 xl:grid-cols-[minmax(0,1.4fr)_minmax(22rem,1fr)]">
              <div class="space-y-6">
                <div class="space-y-3">
                  <div class="flex items-center gap-2">
                    <CheckCircle2 class="h-4 w-4 text-slate-500" />
                    <h2 class="text-base font-semibold text-slate-900">Approval flow</h2>
                  </div>
                  <div class="flex flex-wrap items-center gap-2">
                    <template v-for="(node, index) in selectedWorkflow.review_info" :key="`${node.group_name}-${index}`">
                      <Badge
                        variant="outline"
                        :class="node.is_current_node
                          ? 'border-blue-200 bg-blue-50 text-blue-700'
                          : node.is_passed_node
                            ? 'border-emerald-200 bg-emerald-50 text-emerald-700'
                            : 'border-slate-200 bg-slate-100 text-slate-600'"
                      >
                        {{ node.group_name }}
                      </Badge>
                      <span
                        v-if="index < selectedWorkflow.review_info.length - 1"
                        class="text-xs text-slate-400"
                      >
                        →
                      </span>
                    </template>
                  </div>
                  <p
                    v-if="selectedWorkflow.current_reviewers.length > 0"
                    class="text-sm text-slate-500"
                  >
                    Current reviewers:
                    {{ selectedWorkflow.current_reviewers.map((reviewer) => reviewer.display || reviewer.username).join(', ') }}
                  </p>
                </div>

                <div class="space-y-3">
                  <div class="flex items-center gap-2">
                    <Play class="h-4 w-4 text-slate-500" />
                    <h2 class="text-base font-semibold text-slate-900">SQL content</h2>
                  </div>
                  <pre class="overflow-x-auto rounded-2xl border border-slate-200 bg-slate-950 p-4 text-xs text-slate-100">{{ selectedWorkflow.sql_content }}</pre>
                </div>

                <div class="space-y-3">
                  <h2 class="text-base font-semibold text-slate-900">Review result</h2>
                  <div
                    v-if="selectedWorkflow.review_rows.length === 0"
                    class="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-8 text-sm text-slate-500"
                  >
                    No review rows recorded.
                  </div>
                  <div v-else class="overflow-x-auto rounded-2xl border border-slate-200">
                    <table class="min-w-full divide-y divide-slate-200 text-sm">
                      <thead class="bg-slate-50 text-left text-xs uppercase tracking-wide text-slate-500">
                        <tr>
                          <th
                            v-for="column in reviewResultColumns"
                            :key="column"
                            class="px-3 py-2 font-medium"
                          >
                            {{ column }}
                          </th>
                        </tr>
                      </thead>
                      <tbody class="divide-y divide-slate-100 bg-white">
                        <tr
                          v-for="(row, rowIndex) in selectedWorkflow.review_rows"
                          :key="`review-${rowIndex}`"
                        >
                          <td
                            v-for="column in reviewResultColumns"
                            :key="`${rowIndex}-${column}`"
                            class="max-w-[24rem] px-3 py-2 align-top text-slate-700"
                          >
                            {{ stringifyCellValue(row[column]) }}
                          </td>
                        </tr>
                      </tbody>
                    </table>
                  </div>
                </div>

                <div class="space-y-3">
                  <h2 class="text-base font-semibold text-slate-900">Execution result</h2>
                  <div
                    v-if="selectedWorkflow.execute_rows.length === 0"
                    class="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-8 text-sm text-slate-500"
                  >
                    No execution rows recorded yet.
                  </div>
                  <div v-else class="overflow-x-auto rounded-2xl border border-slate-200">
                    <table class="min-w-full divide-y divide-slate-200 text-sm">
                      <thead class="bg-slate-50 text-left text-xs uppercase tracking-wide text-slate-500">
                        <tr>
                          <th
                            v-for="column in executeResultColumns"
                            :key="column"
                            class="px-3 py-2 font-medium"
                          >
                            {{ column }}
                          </th>
                        </tr>
                      </thead>
                      <tbody class="divide-y divide-slate-100 bg-white">
                        <tr
                          v-for="(row, rowIndex) in selectedWorkflow.execute_rows"
                          :key="`execute-${rowIndex}`"
                        >
                          <td
                            v-for="column in executeResultColumns"
                            :key="`${rowIndex}-${column}`"
                            class="max-w-[24rem] px-3 py-2 align-top text-slate-700"
                          >
                            {{ stringifyCellValue(row[column]) }}
                          </td>
                        </tr>
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>

              <div class="space-y-6">
                <div class="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                  <div class="flex items-center gap-2">
                    <CalendarClock class="h-4 w-4 text-slate-500" />
                    <h2 class="text-base font-semibold text-slate-900">Actions</h2>
                  </div>
                  <p class="mt-2 text-sm text-slate-500">
                    {{ selectedWorkflow.last_operation_info || 'No workflow operations recorded yet.' }}
                  </p>

                  <div
                    v-if="selectedWorkflow.is_can_review || selectedWorkflow.is_can_cancel"
                    class="mt-4 space-y-2"
                  >
                    <label class="text-sm font-medium text-slate-700" for="workflow-review-remark">
                      Remark
                    </label>
                    <textarea
                      id="workflow-review-remark"
                      v-model="reviewForm.auditRemark"
                      :class="textareaClass"
                      placeholder="Enter approval remark, rejection reason, or cancellation context."
                    />
                  </div>

                  <div class="mt-4 flex flex-wrap gap-2">
                    <Button
                      v-if="selectedWorkflow.is_can_review"
                      type="button"
                      :disabled="reviewSubmitting"
                      @click="void submitReviewAction('pass')"
                    >
                      Approve
                    </Button>
                    <Button
                      v-if="selectedWorkflow.is_can_reject"
                      variant="outline"
                      type="button"
                      :disabled="reviewSubmitting"
                      @click="void submitReviewAction('reject')"
                    >
                      Reject
                    </Button>
                    <Button
                      v-if="selectedWorkflow.is_can_cancel"
                      variant="outline"
                      type="button"
                      :disabled="reviewSubmitting"
                      @click="void submitReviewAction('cancel')"
                    >
                      Cancel workflow
                    </Button>
                    <Button
                      v-if="selectedWorkflow.is_can_execute"
                      type="button"
                      :disabled="executeSubmitting"
                      @click="void executeSelectedWorkflow('auto')"
                    >
                      Execute now
                    </Button>
                    <Button
                      v-if="selectedWorkflow.is_can_manual_execute"
                      variant="outline"
                      type="button"
                      :disabled="executeSubmitting"
                      @click="void executeSelectedWorkflow('manual')"
                    >
                      Mark manual complete
                    </Button>
                  </div>
                </div>

                <div
                  v-if="selectedWorkflow.is_can_edit_execution_window"
                  class="rounded-2xl border border-slate-200 bg-white p-4"
                >
                  <div class="flex items-center gap-2">
                    <CheckCircle2 class="h-4 w-4 text-slate-500" />
                    <h2 class="text-base font-semibold text-slate-900">Execution window</h2>
                  </div>
                  <div class="mt-4 grid gap-3">
                    <input
                      v-model="windowForm.runDateStart"
                      class="block h-10 w-full rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-700 shadow-sm outline-none transition focus:border-slate-400"
                      type="datetime-local"
                    >
                    <input
                      v-model="windowForm.runDateEnd"
                      class="block h-10 w-full rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-700 shadow-sm outline-none transition focus:border-slate-400"
                      type="datetime-local"
                    >
                    <Button type="button" :disabled="windowSubmitting" @click="void saveExecutionWindow()">
                      Save window
                    </Button>
                  </div>
                </div>

                <div
                  v-if="selectedWorkflow.is_can_schedule"
                  class="rounded-2xl border border-slate-200 bg-white p-4"
                >
                  <div class="flex items-center gap-2">
                    <CalendarClock class="h-4 w-4 text-slate-500" />
                    <h2 class="text-base font-semibold text-slate-900">Schedule execution</h2>
                  </div>
                  <p class="mt-2 text-sm text-slate-500">
                    Scheduled run: {{ formatDateTime(selectedWorkflow.scheduled_run_date) }}
                  </p>
                  <div class="mt-4 grid gap-3">
                    <input
                      v-model="scheduleForm.runDate"
                      class="block h-10 w-full rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-700 shadow-sm outline-none transition focus:border-slate-400"
                      type="datetime-local"
                    >
                    <Button type="button" :disabled="scheduleSubmitting" @click="void saveSchedule()">
                      Save schedule
                    </Button>
                  </div>
                </div>

                <div class="rounded-2xl border border-slate-200 bg-white p-4">
                  <div class="flex items-center gap-2">
                    <RefreshCw class="h-4 w-4 text-slate-500" />
                    <h2 class="text-base font-semibold text-slate-900">Workflow log</h2>
                  </div>
                  <div
                    v-if="selectedWorkflow.logs.length === 0"
                    class="mt-4 rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-8 text-sm text-slate-500"
                  >
                    No log entries yet.
                  </div>
                  <ol v-else class="mt-4 space-y-3">
                    <li
                      v-for="(log, index) in selectedWorkflow.logs"
                      :key="`${log.operation_time}-${index}`"
                      class="rounded-xl border border-slate-200 bg-slate-50 px-4 py-3"
                    >
                      <div class="flex items-start justify-between gap-3">
                        <div>
                          <p class="text-sm font-medium text-slate-900">{{ log.operation_type_desc }}</p>
                          <p class="mt-1 text-sm text-slate-600">{{ log.operation_info }}</p>
                        </div>
                        <div class="text-right text-xs text-slate-500">
                          <p>{{ log.operator_display || 'System' }}</p>
                          <p>{{ formatDateTime(log.operation_time) }}</p>
                        </div>
                      </div>
                    </li>
                  </ol>
                </div>
              </div>
            </div>
          </template>
        </CardContent>
      </Card>
    </template>
  </section>
</template>
