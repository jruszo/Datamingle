<script setup lang="ts">
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import {
  ArrowLeft,
  CalendarClock,
  CheckCircle2,
  Play,
  RefreshCw,
} from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import {
  executeWorkflow,
  downloadWorkflowExport,
  fetchWorkflowDetail,
  reviewWorkflow,
  scheduleWorkflow,
  updateWorkflowExecutionWindow,
  type WorkflowDetailRecord,
  type WorkflowResultRow,
} from '../api'
import { useAuthStore } from '@/stores/auth'
import { useMailboxStore } from '@/stores/mailbox'

const authStore = useAuthStore()
const mailboxStore = useMailboxStore()
const route = useRoute()
const router = useRouter()

const detailLoading = ref(false)
const reviewSubmitting = ref(false)
const executeSubmitting = ref(false)
const scheduleSubmitting = ref(false)
const windowSubmitting = ref(false)
const downloadSubmitting = ref(false)

const detailError = ref('')
const feedback = ref('')
const selectedWorkflow = ref<WorkflowDetailRecord | null>(null)
const selectedExecutor = ref('')

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

const textareaClass =
  'block min-h-[7rem] w-full rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700 shadow-sm outline-none transition focus:border-slate-400 disabled:cursor-not-allowed disabled:bg-slate-100'
const selectClass =
  'block h-10 w-full rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-700 shadow-sm outline-none transition focus:border-slate-400 disabled:cursor-not-allowed disabled:bg-slate-100'

let authInitialized = false

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
  return authStore.currentUser?.permissions?.includes(permission) ?? false
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

function humanExportFormat(value: string | null) {
  switch (value) {
    case 'csv':
      return 'CSV'
    case 'tsv':
      return 'TSV'
    case 'sql':
      return 'SQL'
    case 'xlsx':
      return 'Excel (.xlsx)'
    case 'json':
      return 'JSON'
    case 'xml':
      return 'XML'
    default:
      return 'Not generated'
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
  if (!detail) {
    selectedExecutor.value = ''
    return
  }
  if (detail.scheduled_executor) {
    selectedExecutor.value = detail.scheduled_executor
    return
  }
  if (detail.available_executors.length === 1) {
    selectedExecutor.value = detail.available_executors[0]?.id ?? ''
    return
  }
  selectedExecutor.value = ''
}

function routeParamWorkflowId() {
  const routeValue = route.params.workflowId
  const workflowId = typeof routeValue === 'string' ? Number(routeValue) : Number.NaN
  return Number.isInteger(workflowId) && workflowId > 0 ? workflowId : null
}

const selectedWorkflowId = computed(() => routeParamWorkflowId())
const reviewResultColumns = computed(() => resultColumns(selectedWorkflow.value?.review_rows ?? []))
const executeResultColumns = computed(() => resultColumns(selectedWorkflow.value?.execute_rows ?? []))
const availableExecutors = computed(() => selectedWorkflow.value?.available_executors ?? [])
const executorBlockerEntries = computed(() => Object.entries(selectedWorkflow.value?.executor_blockers ?? {}))
const isMysqlDdlWorkflow = computed(() => (
  selectedWorkflow.value?.syntax_type === 1
  && selectedWorkflow.value?.instance_db_type === 'mysql'
  && !selectedWorkflow.value?.is_offline_export
))
const requiresExecutorSelection = computed(() => (
  isMysqlDdlWorkflow.value
  && availableExecutors.value.length > 1
  && !selectedExecutor.value
))
const noCompatibleExecutors = computed(() => (
  isMysqlDdlWorkflow.value && availableExecutors.value.length === 0
))

function executorLabel(executorId: string | null) {
  if (!executorId) {
    return 'Not selected'
  }
  const executor = availableExecutors.value.find((option) => option.id === executorId)
  return executor?.label || executorId
}

const canViewWorkflows = computed(() => (
  hasPermission('sql.menu_sqlworkflow')
  || hasPermission('sql.menu_sqlexportworkflow')
  || hasPermission('sql.sql_submit')
  || hasPermission('sql.sqlexport_submit')
  || hasPermission('sql.offline_download')
  || hasPermission('sql.audit_user')
))

const canDownloadSelectedExport = computed(() => {
  return Boolean(selectedWorkflow.value?.download_available) && hasPermission('sql.offline_download')
})

async function loadWorkflowDetail() {
  if (!canViewWorkflows.value) {
    return
  }

  const workflowId = selectedWorkflowId.value
  if (!workflowId) {
    selectedWorkflow.value = null
    detailError.value = 'Invalid workflow id.'
    return
  }

  detailLoading.value = true
  detailError.value = ''

  try {
    const detail = await fetchWorkflowDetail(workflowId, requireToken())
    selectedWorkflow.value = detail
    syncDetailForms(detail)
  } catch (errorValue) {
    detailError.value = toUserFacingMessage(errorValue, 'Failed to load workflow detail.')
    selectedWorkflow.value = null
  } finally {
    detailLoading.value = false
  }
}

async function refreshSelectedWorkflow() {
  await loadWorkflowDetail()
}

async function refreshMailboxSummaryBestEffort() {
  try {
    await mailboxStore.refreshSummary()
  } catch (errorValue) {
    console.error('Failed to refresh mailbox summary.', errorValue)
  }
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
    await refreshMailboxSummaryBestEffort()
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
  if (mode === 'auto' && requiresExecutorSelection.value) {
    detailError.value = 'Select a compatible DDL executor before starting online execution.'
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
        executor: mode === 'auto' && selectedExecutor.value ? selectedExecutor.value as 'direct' | 'gh-ost' | 'pt-osc' : undefined,
      },
      requireToken(),
    )
    await refreshSelectedWorkflow()
    await refreshMailboxSummaryBestEffort()
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
  if (requiresExecutorSelection.value) {
    detailError.value = 'Select a compatible DDL executor before scheduling online execution.'
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
        executor: selectedExecutor.value ? selectedExecutor.value as 'direct' | 'gh-ost' | 'pt-osc' : undefined,
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

async function downloadSelectedExport() {
  if (!selectedWorkflowId.value) {
    return
  }

  downloadSubmitting.value = true
  detailError.value = ''

  try {
    const result = await downloadWorkflowExport(selectedWorkflowId.value, requireToken())
    if (result.mode === 'redirect') {
      window.location.href = result.url
      return
    }

    const objectUrl = window.URL.createObjectURL(result.data)
    const anchor = document.createElement('a')
    anchor.href = objectUrl
    anchor.download = result.filename
    anchor.style.display = 'none'
    document.body.appendChild(anchor)
    anchor.click()
    anchor.remove()
    window.URL.revokeObjectURL(objectUrl)
  } catch (errorValue) {
    detailError.value = toUserFacingMessage(errorValue, 'Failed to download the export artifact.')
  } finally {
    downloadSubmitting.value = false
  }
}

async function goBackToWorkflows() {
  const returnTo = typeof route.query.returnTo === 'string' ? route.query.returnTo : ''
  if (returnTo.startsWith('/workflows')) {
    await router.replace(returnTo)
    return
  }
  await router.push({ name: 'workflows' })
}

watch(
  () => route.params.workflowId,
  () => {
    if (!authInitialized) {
      return
    }
    feedback.value = ''
    void loadWorkflowDetail()
  },
)

onMounted(async () => {
  await authStore.loadCurrentUser()
  authInitialized = true
  if (!canViewWorkflows.value) {
    return
  }

  await loadWorkflowDetail()
})
</script>

<template>
  <section class="grid gap-6">
    <div class="flex flex-wrap items-center justify-between gap-3">
      <Button
        variant="ghost"
        type="button"
        class="gap-2 px-0"
        data-testid="workflow-detail-back"
        @click="void goBackToWorkflows()"
      >
        <ArrowLeft class="h-4 w-4" />
        Back to workflows
      </Button>
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

    <Card v-else class="border-slate-200">
      <CardHeader class="gap-4">
        <div class="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <CardTitle>{{ selectedWorkflow?.workflow_name || 'Workflow detail' }}</CardTitle>
            <CardDescription>
              Review approvals, time windows, execution controls, and result logs for the selected workflow.
            </CardDescription>
          </div>
          <Button variant="outline" type="button" class="gap-2" data-testid="workflow-detail-refresh" @click="void refreshSelectedWorkflow()">
            <RefreshCw class="h-4 w-4" />
            Refresh detail
          </Button>
        </div>
        <div v-if="selectedWorkflow" class="flex flex-wrap gap-2">
          <Badge variant="outline" :class="syntaxBadgeClass(selectedWorkflow.syntax_type)">
            {{ selectedWorkflow.syntax_type_label }}
          </Badge>
          <Badge variant="outline" data-testid="workflow-detail-status" :class="statusBadgeClass(selectedWorkflow.status)">
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

        <div
          v-else-if="!selectedWorkflow"
          class="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-10 text-sm text-slate-500"
        >
          The requested workflow could not be loaded.
        </div>

        <template v-else>
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
              <p class="text-sm text-slate-500">{{ selectedWorkflow.team_name }}</p>
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
              <p class="text-xs uppercase tracking-wide text-slate-500">
                {{ selectedWorkflow.is_offline_export ? 'Export file' : 'Execution window' }}
              </p>
              <p class="mt-2 text-sm font-medium text-slate-900">
                {{
                  selectedWorkflow.is_offline_export
                    ? humanExportFormat(selectedWorkflow.export_format)
                    : formatDateTime(selectedWorkflow.run_date_start)
                }}
              </p>
              <p class="text-sm text-slate-500">
                {{
                  selectedWorkflow.is_offline_export
                    ? selectedWorkflow.file_name || 'No file generated yet'
                    : `Ends ${formatDateTime(selectedWorkflow.run_date_end)}`
                }}
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
                  <template v-for="(node, index) in selectedWorkflow.review_info" :key="`${node.team_name}-${index}`">
                    <Badge
                      variant="outline"
                      :class="node.is_current_node
                        ? 'border-blue-200 bg-blue-50 text-blue-700'
                        : node.is_passed_node
                          ? 'border-emerald-200 bg-emerald-50 text-emerald-700'
                          : 'border-slate-200 bg-slate-100 text-slate-600'"
                    >
                      {{ node.team_name }}
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
                  v-if="isMysqlDdlWorkflow"
                  class="mt-4 space-y-3 rounded-xl border border-slate-200 bg-white px-4 py-3"
                >
                  <div>
                    <p class="text-sm font-medium text-slate-900">DDL executor</p>
                    <p class="mt-1 text-sm text-slate-500">
                      Choose how Datamingle should run this MySQL schema change. Only compatible executors are shown.
                    </p>
                  </div>

                  <div v-if="availableExecutors.length > 0" class="space-y-2">
                    <select
                      v-model="selectedExecutor"
                      data-testid="workflow-ddl-executor"
                      :class="selectClass"
                      :disabled="executeSubmitting || scheduleSubmitting"
                    >
                      <option value="">
                        {{ availableExecutors.length > 1 ? 'Select executor' : 'Auto-selected executor' }}
                      </option>
                      <option
                        v-for="executor in availableExecutors"
                        :key="executor.id"
                        :value="executor.id"
                      >
                        {{ executor.label }}
                      </option>
                    </select>
                    <p v-if="selectedWorkflow.scheduled_executor" class="text-xs text-slate-500">
                      Scheduled executor: {{ executorLabel(selectedWorkflow.scheduled_executor) }}
                    </p>
                  </div>

                  <div
                    v-else
                    class="rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800"
                  >
                    No compatible online executor is currently available for this workflow.
                  </div>

                  <div
                    v-if="executorBlockerEntries.length > 0"
                    class="space-y-2 rounded-lg border border-slate-200 bg-slate-50 px-3 py-3 text-sm text-slate-600"
                  >
                    <p class="font-medium text-slate-700">Why other executors are unavailable</p>
                    <ul class="space-y-1">
                      <li
                        v-for="[executorId, reason] in executorBlockerEntries"
                        :key="executorId"
                      >
                        {{ executorId }}: {{ reason }}
                      </li>
                    </ul>
                  </div>
                </div>

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
                    data-testid="workflow-approve"
                    type="button"
                    :disabled="reviewSubmitting"
                    @click="void submitReviewAction('pass')"
                  >
                    Approve
                  </Button>
                  <Button
                    v-if="selectedWorkflow.is_can_reject"
                    data-testid="workflow-reject"
                    variant="outline"
                    type="button"
                    :disabled="reviewSubmitting"
                    @click="void submitReviewAction('reject')"
                  >
                    Reject
                  </Button>
                  <Button
                    v-if="selectedWorkflow.is_can_cancel"
                    data-testid="workflow-cancel"
                    variant="outline"
                    type="button"
                    :disabled="reviewSubmitting"
                    @click="void submitReviewAction('cancel')"
                  >
                    Cancel workflow
                  </Button>
                  <Button
                    v-if="selectedWorkflow.is_can_execute"
                    data-testid="workflow-execute-now"
                    type="button"
                    :disabled="executeSubmitting || requiresExecutorSelection"
                    @click="void executeSelectedWorkflow('auto')"
                  >
                    Execute now
                  </Button>
                  <Button
                    v-if="selectedWorkflow.is_can_manual_execute"
                    data-testid="workflow-execute-manual"
                    variant="outline"
                    type="button"
                    :disabled="executeSubmitting"
                    @click="void executeSelectedWorkflow('manual')"
                  >
                    Mark manual complete
                  </Button>
                  <Button
                    v-if="canDownloadSelectedExport"
                    data-testid="workflow-download-export"
                    variant="outline"
                    type="button"
                    :disabled="downloadSubmitting"
                    @click="void downloadSelectedExport()"
                  >
                    {{ downloadSubmitting ? 'Preparing download...' : 'Download export' }}
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
                <p
                  v-if="isMysqlDdlWorkflow && selectedWorkflow.scheduled_executor"
                  class="mt-1 text-sm text-slate-500"
                >
                  Scheduled executor: {{ executorLabel(selectedWorkflow.scheduled_executor) }}
                </p>
                <div class="mt-4 grid gap-3">
                  <input
                    v-model="scheduleForm.runDate"
                    class="block h-10 w-full rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-700 shadow-sm outline-none transition focus:border-slate-400"
                    type="datetime-local"
                  >
                  <Button type="button" :disabled="scheduleSubmitting || requiresExecutorSelection || noCompatibleExecutors" @click="void saveSchedule()">
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
  </section>
</template>
