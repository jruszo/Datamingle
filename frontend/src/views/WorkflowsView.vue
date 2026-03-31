<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref, watch } from 'vue'
import { useDebounceFn } from '@vueuse/core'
import { useRoute, useRouter } from 'vue-router'
import {
  CalendarClock,
  CheckCircle2,
  ChevronRight,
  Clock3,
  ExternalLink,
  FileDown,
  Play,
  RefreshCw,
  Send,
  StopCircle,
  X,
  XCircle,
} from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import {
  executeWorkflow,
  fetchWorkflowContent,
  fetchWorkflowDetail,
  fetchWorkflowRollback,
  fetchWorkflows,
  fetchWorkflowSubmissionMetadata,
  reviewWorkflow,
  scheduleWorkflow,
  updateWorkflowExecutionWindow,
  type PaginatedResponse,
  type WorkflowContentRecord,
  type WorkflowDetailRecord,
  type WorkflowRollbackRecord,
  type WorkflowScope,
  type WorkflowSubmissionMetadata,
  type WorkflowSummaryRecord,
  type WorkflowSyntaxType,
} from '@/lib/api'
import { useAuthStore } from '@/stores/auth'

const authStore = useAuthStore()
const route = useRoute()
const router = useRouter()

const WORKFLOW_PAGE_SIZE = 10

const listError = ref('')
const lookupError = ref('')
const detailError = ref('')
const feedback = ref('')

const lookupsLoading = ref(false)
const listLoading = ref(false)
const detailLoading = ref(false)
const contentLoading = ref(false)
const rollbackLoading = ref(false)
const reviewSubmitting = ref(false)
const executeSubmitting = ref(false)
const scheduleSubmitting = ref(false)
const windowSubmitting = ref(false)

const metadata = ref<WorkflowSubmissionMetadata | null>(null)
const workflowsPage = ref<PaginatedResponse<WorkflowSummaryRecord>>({
  count: 0,
  next: null,
  previous: null,
  results: [],
})
const selectedWorkflow = ref<WorkflowDetailRecord | null>(null)
const selectedContent = ref<WorkflowContentRecord | null>(null)
const selectedRollback = ref<WorkflowRollbackRecord | null>(null)

const search = ref('')
const scope = ref<WorkflowScope>('all')
const statusFilter = ref('')
const syntaxFilter = ref<WorkflowSyntaxType | ''>('')
const groupFilter = ref('')
const instanceFilter = ref('')
const startDate = ref('')
const endDate = ref('')
const page = ref(1)

const reviewForm = reactive({
  audit_remark: '',
})

const scheduleForm = reactive({
  run_date: '',
})

const executionWindowForm = reactive({
  run_date_start: '',
  run_date_end: '',
})

let listLoadCounter = 0
let detailLoadCounter = 0
let pollTimer: number | null = null

const selectClass =
  'block h-10 w-full rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-700 shadow-sm outline-none transition focus:border-slate-400 disabled:cursor-not-allowed disabled:bg-slate-100'
const textareaClass =
  'block min-h-[7.5rem] w-full rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700 shadow-sm outline-none transition focus:border-slate-400 disabled:cursor-not-allowed disabled:bg-slate-100'

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

function formatDate(value: string | null) {
  if (!value) {
    return 'Unlimited'
  }
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return value
  }
  return date.toLocaleString()
}

function workflowStatusLabel(status: string) {
  switch (status) {
    case 'workflow_finish':
      return 'Finished'
    case 'workflow_abort':
      return 'Canceled'
    case 'workflow_manreviewing':
      return 'Pending review'
    case 'workflow_review_pass':
      return 'Approved'
    case 'workflow_timingtask':
      return 'Scheduled'
    case 'workflow_queuing':
      return 'Queued'
    case 'workflow_executing':
      return 'Executing'
    case 'workflow_autoreviewwrong':
      return 'Auto review failed'
    case 'workflow_exception':
      return 'Exception'
    case 'workflow_finish_manual':
      return 'Manually completed'
    default:
      return status
  }
}

function workflowStatusClass(status: string) {
  switch (status) {
    case 'workflow_finish':
    case 'workflow_finish_manual':
      return 'border-emerald-200 bg-emerald-50 text-emerald-700'
    case 'workflow_review_pass':
    case 'workflow_timingtask':
    case 'workflow_queuing':
    case 'workflow_executing':
      return 'border-sky-200 bg-sky-50 text-sky-700'
    case 'workflow_abort':
    case 'workflow_exception':
    case 'workflow_autoreviewwrong':
      return 'border-rose-200 bg-rose-50 text-rose-700'
    default:
      return 'border-amber-200 bg-amber-50 text-amber-700'
  }
}

function syntaxLabel(value: WorkflowSyntaxType) {
  return value === 1 ? 'DDL' : 'DML'
}

function syncDetailForms() {
  reviewForm.audit_remark = ''
  scheduleForm.run_date = selectedWorkflow.value?.run_date
    ? selectedWorkflow.value.run_date.slice(0, 16)
    : ''
  executionWindowForm.run_date_start = selectedWorkflow.value?.run_date_start
    ? selectedWorkflow.value.run_date_start.slice(0, 16)
    : ''
  executionWindowForm.run_date_end = selectedWorkflow.value?.run_date_end
    ? selectedWorkflow.value.run_date_end.slice(0, 16)
    : ''
}

function clearPollTimer() {
  if (pollTimer !== null) {
    window.clearInterval(pollTimer)
    pollTimer = null
  }
}

const canBrowseSharedWorkflowList = computed(() => {
  return (
    hasPermission('sql.menu_sqlworkflow') ||
    hasPermission('sql.audit_user') ||
    hasPermission('sql.sql_review') ||
    hasPermission('sql.sql_execute_for_resource_group')
  )
})

const canViewWorkflowList = computed(() => {
  return Boolean(authStore.currentUser)
})

const canCreateDml = computed(() => {
  return hasPermission('sql.sql_submit') || (metadata.value?.instances.length ?? 0) > 0
})

const filteredInstanceOptions = computed(() => {
  const groupId = Number(groupFilter.value)
  const instances = metadata.value?.instances ?? []
  if (!groupId) {
    return instances
  }
  return instances.filter((instance) => instance.group_ids.includes(groupId))
})

const selectedWorkflowId = computed(() => {
  const rawValue = route.params.workflowId
  if (!rawValue) {
    return null
  }
  const parsedValue = Number(rawValue)
  return Number.isFinite(parsedValue) ? parsedValue : null
})

const shouldPollSelectedWorkflow = computed(() => {
  return ['workflow_timingtask', 'workflow_queuing', 'workflow_executing'].includes(
    selectedWorkflow.value?.status ?? '',
  )
})

const detailColumns = computed(() => {
  if (selectedContent.value?.column_list?.length) {
    return selectedContent.value.column_list
  }
  const firstRow = selectedContent.value?.rows[0]
  return firstRow ? Object.keys(firstRow) : []
})

async function loadLookups() {
  lookupsLoading.value = true
  lookupError.value = ''

  try {
    metadata.value = await fetchWorkflowSubmissionMetadata(requireToken())
  } catch (errorValue) {
    lookupError.value = toUserFacingMessage(errorValue, 'Failed to load workflow filters.')
  } finally {
    lookupsLoading.value = false
  }
}

async function loadWorkflows() {
  if (!canViewWorkflowList.value) {
    workflowsPage.value = {
      count: 0,
      next: null,
      previous: null,
      results: [],
    }
    return
  }

  const requestId = ++listLoadCounter
  listLoading.value = true
  listError.value = ''

  try {
    const payload = await fetchWorkflows(requireToken(), {
      page: page.value,
      size: WORKFLOW_PAGE_SIZE,
      search: search.value,
      scope: scope.value,
      status: statusFilter.value,
      syntax_type: syntaxFilter.value,
      instance_id: instanceFilter.value ? Number(instanceFilter.value) : '',
      group_id: groupFilter.value ? Number(groupFilter.value) : '',
      start_date: startDate.value,
      end_date: endDate.value,
    })
    if (requestId === listLoadCounter) {
      workflowsPage.value = payload
    }
  } catch (errorValue) {
    if (requestId === listLoadCounter) {
      listError.value = toUserFacingMessage(errorValue, 'Failed to load workflows.')
    }
  } finally {
    if (requestId === listLoadCounter) {
      listLoading.value = false
    }
  }
}

async function loadSelectedWorkflow(workflowId: number, preserveFeedback = false) {
  const requestId = ++detailLoadCounter
  detailLoading.value = true
  contentLoading.value = true
  rollbackLoading.value = true
  detailError.value = ''
  if (!preserveFeedback) {
    feedback.value = ''
  }

  try {
    const detail = await fetchWorkflowDetail(workflowId, requireToken())
    const content = await fetchWorkflowContent(workflowId, requireToken())
    let rollback: WorkflowRollbackRecord | null = null
    if (detail.is_can_rollback) {
      rollback = await fetchWorkflowRollback(workflowId, requireToken())
    }

    if (requestId === detailLoadCounter) {
      selectedWorkflow.value = detail
      selectedContent.value = content
      selectedRollback.value = rollback
      syncDetailForms()
    }
  } catch (errorValue) {
    if (requestId === detailLoadCounter) {
      selectedWorkflow.value = null
      selectedContent.value = null
      selectedRollback.value = null
      detailError.value = toUserFacingMessage(errorValue, 'Failed to load workflow detail.')
    }
  } finally {
    if (requestId === detailLoadCounter) {
      detailLoading.value = false
      contentLoading.value = false
      rollbackLoading.value = false
    }
  }
}

async function openWorkflow(workflowId: number) {
  await router.push({ name: 'workflow-detail', params: { workflowId } })
}

async function closeDetail() {
  clearPollTimer()
  await router.push({ name: 'workflows' })
}

async function submitReview(auditType: 'pass' | 'cancel') {
  if (!selectedWorkflow.value) {
    return
  }

  reviewSubmitting.value = true
  detailError.value = ''

  try {
    const detail = await reviewWorkflow(
      selectedWorkflow.value.id,
      {
        workflow_type: 2,
        audit_type: auditType,
        audit_remark: reviewForm.audit_remark.trim(),
      },
      requireToken(),
    )
    feedback.value = detail
    await Promise.all([
      loadSelectedWorkflow(selectedWorkflow.value.id, true),
      loadWorkflows(),
    ])
  } catch (errorValue) {
    detailError.value = toUserFacingMessage(errorValue, 'Failed to submit the review action.')
  } finally {
    reviewSubmitting.value = false
  }
}

async function triggerExecution(mode: 'auto' | 'manual') {
  if (!selectedWorkflow.value) {
    return
  }

  executeSubmitting.value = true
  detailError.value = ''

  try {
    const detail = await executeWorkflow(
      selectedWorkflow.value.id,
      { workflow_type: 2, mode },
      requireToken(),
    )
    feedback.value = detail
    await Promise.all([
      loadSelectedWorkflow(selectedWorkflow.value.id, true),
      loadWorkflows(),
    ])
  } catch (errorValue) {
    detailError.value = toUserFacingMessage(errorValue, 'Failed to trigger workflow execution.')
  } finally {
    executeSubmitting.value = false
  }
}

async function submitSchedule() {
  if (!selectedWorkflow.value || !scheduleForm.run_date) {
    detailError.value = 'A scheduled execution time is required.'
    return
  }

  scheduleSubmitting.value = true
  detailError.value = ''

  try {
    const detail = await scheduleWorkflow(
      selectedWorkflow.value.id,
      { run_date: scheduleForm.run_date },
      requireToken(),
    )
    feedback.value = detail
    await Promise.all([
      loadSelectedWorkflow(selectedWorkflow.value.id, true),
      loadWorkflows(),
    ])
  } catch (errorValue) {
    detailError.value = toUserFacingMessage(errorValue, 'Failed to schedule workflow execution.')
  } finally {
    scheduleSubmitting.value = false
  }
}

async function saveExecutionWindow() {
  if (!selectedWorkflow.value) {
    return
  }

  windowSubmitting.value = true
  detailError.value = ''

  try {
    const detail = await updateWorkflowExecutionWindow(
      selectedWorkflow.value.id,
      {
        run_date_start: executionWindowForm.run_date_start || null,
        run_date_end: executionWindowForm.run_date_end || null,
      },
      requireToken(),
    )
    feedback.value = detail
    await Promise.all([
      loadSelectedWorkflow(selectedWorkflow.value.id, true),
      loadWorkflows(),
    ])
  } catch (errorValue) {
    detailError.value = toUserFacingMessage(errorValue, 'Failed to update the execution window.')
  } finally {
    windowSubmitting.value = false
  }
}

async function terminateWorkflow() {
  if (!selectedWorkflow.value) {
    return
  }

  reviewSubmitting.value = true
  detailError.value = ''

  try {
    const detail = await reviewWorkflow(
      selectedWorkflow.value.id,
      {
        workflow_type: 2,
        audit_type: 'cancel',
        audit_remark: reviewForm.audit_remark.trim(),
      },
      requireToken(),
    )
    feedback.value = detail
    await Promise.all([
      loadSelectedWorkflow(selectedWorkflow.value.id, true),
      loadWorkflows(),
    ])
  } catch (errorValue) {
    detailError.value = toUserFacingMessage(errorValue, 'Failed to terminate the workflow.')
  } finally {
    reviewSubmitting.value = false
  }
}

function downloadRollback() {
  if (!selectedWorkflow.value || !selectedRollback.value?.download_content) {
    return
  }

  const blob = new Blob([selectedRollback.value.download_content], { type: 'application/sql' })
  const url = URL.createObjectURL(blob)
  const link = document.createElement('a')
  link.href = url
  link.download = `rollback_${selectedWorkflow.value.id}.sql`
  document.body.appendChild(link)
  link.click()
  document.body.removeChild(link)
  URL.revokeObjectURL(url)
}

const debouncedLoadWorkflows = useDebounceFn(() => {
  page.value = 1
  void loadWorkflows()
}, 250)

watch(search, () => {
  debouncedLoadWorkflows()
})

watch([scope, statusFilter, syntaxFilter, groupFilter, instanceFilter, startDate, endDate], () => {
  page.value = 1
  void loadWorkflows()
})

watch(page, () => {
  void loadWorkflows()
})

watch(
  canBrowseSharedWorkflowList,
  (canBrowseShared) => {
    if (!canBrowseShared && scope.value !== 'mine') {
      scope.value = 'mine'
    }
  },
  { immediate: true },
)

watch(groupFilter, (groupIdValue) => {
  const groupId = Number(groupIdValue)
  if (groupId && !filteredInstanceOptions.value.some((instance) => instance.id === Number(instanceFilter.value))) {
    instanceFilter.value = ''
  }
})

watch(
  selectedWorkflowId,
  (workflowId) => {
    if (!workflowId) {
      selectedWorkflow.value = null
      selectedContent.value = null
      selectedRollback.value = null
      detailError.value = ''
      clearPollTimer()
      return
    }
    void loadSelectedWorkflow(workflowId)
  },
  { immediate: true },
)

watch(
  shouldPollSelectedWorkflow,
  (shouldPoll) => {
    clearPollTimer()
    if (!shouldPoll || !selectedWorkflow.value) {
      return
    }
    pollTimer = window.setInterval(() => {
      if (selectedWorkflow.value) {
        void loadSelectedWorkflow(selectedWorkflow.value.id, true)
        void loadWorkflows()
      }
    }, 5000)
  },
)

onMounted(() => {
  void loadLookups()
  void loadWorkflows()
})

onBeforeUnmount(() => {
  clearPollTimer()
})
</script>

<template>
  <section class="grid gap-6">
    <div class="flex flex-wrap items-start justify-between gap-3">
      <div class="space-y-1">
        <p class="text-sm font-medium uppercase tracking-[0.2em] text-slate-500">Workflow Console</p>
        <h1 class="text-3xl font-semibold tracking-tight text-slate-900">SQL workflows</h1>
        <p class="text-sm text-slate-500">
          Review DDL and DML requests, inspect approval flow, and trigger execution actions from one place.
        </p>
      </div>

      <div class="flex flex-wrap items-center gap-2">
        <Button
          v-if="canCreateDml"
          type="button"
          class="gap-2"
          @click="void router.push({ name: 'workflow-dml-new' })"
        >
          <Send class="h-4 w-4" />
          New DML request
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

    <Card v-if="lookupError" class="border-red-200">
      <CardHeader>
        <CardTitle>Filter lookups failed</CardTitle>
        <CardDescription>{{ lookupError }}</CardDescription>
      </CardHeader>
    </Card>

    <Card v-if="!canViewWorkflowList" class="border-red-200">
      <CardHeader>
        <CardTitle>Workflow list unavailable</CardTitle>
        <CardDescription>
          Sign in again to load workflow data.
        </CardDescription>
      </CardHeader>
    </Card>

    <Card v-else class="border-slate-200">
      <CardHeader class="gap-4">
        <div class="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <CardTitle>Active workflows</CardTitle>
            <CardDescription>
              DDL and DML requests visible to you, including requests currently pending your review.
            </CardDescription>
          </div>
          <Badge variant="outline" class="border-slate-200 bg-slate-50 text-slate-600">
            {{ workflowsPage.count }} total
          </Badge>
        </div>

        <div class="grid gap-3 xl:grid-cols-4">
          <Input v-model="search" placeholder="Search workflow, requester, group, instance, or DB" />
          <select v-model="scope" :class="selectClass" :disabled="!canBrowseSharedWorkflowList">
            <option value="mine">My submissions</option>
            <option v-if="canBrowseSharedWorkflowList" value="all">All visible workflows</option>
            <option v-if="canBrowseSharedWorkflowList" value="pending_review">Pending my review</option>
          </select>
          <select v-model="syntaxFilter" :class="selectClass">
            <option value="">DDL + DML</option>
            <option :value="1">DDL only</option>
            <option :value="2">DML only</option>
          </select>
          <select v-model="statusFilter" :class="selectClass">
            <option value="">All statuses</option>
            <option value="workflow_manreviewing">Pending review</option>
            <option value="workflow_review_pass">Approved</option>
            <option value="workflow_timingtask">Scheduled</option>
            <option value="workflow_queuing">Queued</option>
            <option value="workflow_executing">Executing</option>
            <option value="workflow_finish">Finished</option>
            <option value="workflow_abort">Canceled</option>
            <option value="workflow_autoreviewwrong">Auto review failed</option>
            <option value="workflow_exception">Exception</option>
          </select>
          <select v-model="groupFilter" :class="selectClass" :disabled="lookupsLoading">
            <option value="">All resource groups</option>
            <option
              v-for="group in metadata?.resource_groups ?? []"
              :key="group.group_id"
              :value="group.group_id"
            >
              {{ group.group_name }}
            </option>
          </select>
          <select v-model="instanceFilter" :class="selectClass" :disabled="lookupsLoading">
            <option value="">All instances</option>
            <option
              v-for="instance in filteredInstanceOptions"
              :key="instance.id"
              :value="instance.id"
            >
              {{ instance.instance_name }}
            </option>
          </select>
          <Input v-model="startDate" type="date" />
          <Input v-model="endDate" type="date" />
        </div>
      </CardHeader>

      <CardContent class="space-y-4">
        <p
          v-if="listError"
          class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700"
        >
          {{ listError }}
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
            class="grid gap-3 rounded-2xl border border-slate-200 bg-white p-4 text-left transition hover:border-slate-300 hover:bg-slate-50"
            @click="void openWorkflow(workflow.id)"
          >
            <div class="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
              <div class="space-y-2">
                <div class="flex flex-wrap items-center gap-2">
                  <p class="font-medium text-slate-900">{{ workflow.workflow_name }}</p>
                  <Badge variant="outline" :class="workflowStatusClass(workflow.status)">
                    {{ workflowStatusLabel(workflow.status) }}
                  </Badge>
                  <Badge variant="outline" class="border-slate-200 bg-slate-50 text-slate-600">
                    {{ syntaxLabel(workflow.syntax_type) }}
                  </Badge>
                </div>
                <p class="text-sm text-slate-500">
                  {{ workflow.group_name }} / {{ workflow.instance_name }} / {{ workflow.db_name }}
                </p>
              </div>

              <div class="flex items-center gap-2 text-sm text-slate-500">
                <span>{{ workflow.engineer_display }}</span>
                <span>•</span>
                <span>{{ formatDate(workflow.create_time) }}</span>
                <ChevronRight class="h-4 w-4" />
              </div>
            </div>

            <div class="grid gap-2 text-sm text-slate-600 md:grid-cols-3">
              <p>Execution window: {{ workflow.run_date_start || workflow.run_date_end ? `${formatDate(workflow.run_date_start)} - ${formatDate(workflow.run_date_end)}` : 'Unlimited' }}</p>
              <p>Backup: {{ workflow.is_backup ? 'Enabled' : 'Disabled' }}</p>
              <p>Completed: {{ workflow.finish_time ? formatDate(workflow.finish_time) : 'Pending' }}</p>
            </div>
          </button>
        </div>

        <div class="flex items-center justify-between border-t border-slate-100 pt-4 text-sm text-slate-500">
          <span>
            Page {{ page }}
            <span v-if="workflowsPage.count > 0">
              of {{ Math.max(1, Math.ceil(workflowsPage.count / WORKFLOW_PAGE_SIZE)) }}
            </span>
          </span>
          <div class="flex gap-2">
            <Button
              variant="outline"
              size="sm"
              type="button"
              :disabled="!workflowsPage.previous"
              @click="page = Math.max(1, page - 1)"
            >
              Previous
            </Button>
            <Button
              variant="outline"
              size="sm"
              type="button"
              :disabled="!workflowsPage.next"
              @click="page += 1"
            >
              Next
            </Button>
          </div>
        </div>
      </CardContent>
    </Card>

    <div
      v-if="selectedWorkflowId"
      class="fixed inset-0 z-50 flex justify-end bg-slate-950/45"
      @click.self="void closeDetail()"
    >
      <div class="flex h-full w-full max-w-3xl flex-col bg-white shadow-2xl">
        <div class="flex items-start justify-between gap-4 border-b border-slate-200 px-6 py-5">
          <div>
            <h2 class="text-xl font-semibold text-slate-900">Workflow detail</h2>
            <p class="mt-1 text-sm text-slate-500">
              Inspect approval flow, result rows, and execution actions for the selected workflow.
            </p>
          </div>
          <Button variant="ghost" size="icon" type="button" @click="void closeDetail()">
            <X class="h-4 w-4" />
          </Button>
        </div>

        <div class="flex-1 overflow-y-auto px-6 py-6">
          <p
            v-if="detailError"
            class="mb-4 rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700"
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
            Select a workflow to inspect details.
          </div>

          <div v-else class="space-y-6">
            <div class="space-y-3 rounded-2xl border border-slate-200 bg-slate-50 p-4">
              <div class="flex flex-wrap items-center gap-2">
                <p class="text-lg font-semibold text-slate-900">{{ selectedWorkflow.workflow_name }}</p>
                <Badge variant="outline" :class="workflowStatusClass(selectedWorkflow.status)">
                  {{ workflowStatusLabel(selectedWorkflow.status) }}
                </Badge>
                <Badge variant="outline" class="border-slate-200 bg-white text-slate-600">
                  {{ syntaxLabel(selectedWorkflow.syntax_type) }}
                </Badge>
                <a
                  v-if="selectedWorkflow.demand_url"
                  :href="selectedWorkflow.demand_url"
                  class="inline-flex items-center gap-1 text-sm text-sky-700 underline-offset-4 hover:underline"
                  target="_blank"
                  rel="noreferrer"
                >
                  Requirement
                  <ExternalLink class="h-4 w-4" />
                </a>
              </div>

              <div class="grid gap-3 text-sm text-slate-600 md:grid-cols-2">
                <div>
                  <p class="text-slate-400">Requester</p>
                  <p>{{ selectedWorkflow.engineer_display }}</p>
                </div>
                <div>
                  <p class="text-slate-400">Target</p>
                  <p>{{ selectedWorkflow.group_name }} / {{ selectedWorkflow.instance_name }} / {{ selectedWorkflow.db_name }}</p>
                </div>
                <div>
                  <p class="text-slate-400">Created</p>
                  <p>{{ formatDate(selectedWorkflow.create_time) }}</p>
                </div>
                <div>
                  <p class="text-slate-400">Execution window</p>
                  <p>
                    {{ selectedWorkflow.run_date_start || selectedWorkflow.run_date_end
                      ? `${formatDate(selectedWorkflow.run_date_start)} - ${formatDate(selectedWorkflow.run_date_end)}`
                      : 'Unlimited' }}
                  </p>
                </div>
                <div>
                  <p class="text-slate-400">Scheduled execution</p>
                  <p>{{ selectedWorkflow.run_date ? formatDate(selectedWorkflow.run_date) : 'Not scheduled' }}</p>
                </div>
                <div>
                  <p class="text-slate-400">Backup</p>
                  <p>{{ selectedWorkflow.is_backup ? 'Enabled' : 'Disabled' }}</p>
                </div>
              </div>
            </div>

            <div class="space-y-3">
              <div class="flex items-center gap-2">
                <Clock3 class="h-4 w-4 text-slate-500" />
                <h3 class="font-medium text-slate-900">Approval flow</h3>
              </div>
              <div class="space-y-3">
                <div
                  v-for="(node, index) in selectedWorkflow.review_info"
                  :key="`${node.group_name}-${index}`"
                  class="rounded-2xl border p-4"
                  :class="
                    node.is_current_node
                      ? 'border-amber-200 bg-amber-50'
                      : node.is_passed_node
                        ? 'border-emerald-200 bg-emerald-50'
                        : 'border-slate-200 bg-white'
                  "
                >
                  <div class="flex items-center justify-between gap-3">
                    <p class="font-medium text-slate-900">{{ node.group_name }}</p>
                    <Badge
                      variant="outline"
                      :class="
                        node.is_auto_pass
                          ? 'border-slate-200 bg-slate-50 text-slate-600'
                          : node.is_current_node
                            ? 'border-amber-200 bg-amber-100 text-amber-700'
                            : node.is_passed_node
                              ? 'border-emerald-200 bg-emerald-100 text-emerald-700'
                              : 'border-slate-200 bg-slate-50 text-slate-600'
                      "
                    >
                      {{
                        node.is_auto_pass
                          ? 'Auto pass'
                          : node.is_current_node
                            ? 'Current'
                            : node.is_passed_node
                              ? 'Passed'
                              : 'Pending'
                      }}
                    </Badge>
                  </div>
                </div>
              </div>
              <p
                v-if="selectedWorkflow.current_reviewers.length > 0"
                class="text-sm text-slate-500"
              >
                Current reviewers:
                {{ selectedWorkflow.current_reviewers.map((item) => item.display || item.username).join(', ') }}
              </p>
            </div>

            <div class="space-y-3">
              <div class="flex items-center gap-2">
                <CheckCircle2 class="h-4 w-4 text-slate-500" />
                <h3 class="font-medium text-slate-900">
                  {{ selectedContent?.source === 'execution' ? 'Execution result' : 'Review result' }}
                </h3>
              </div>
              <div
                v-if="contentLoading"
                class="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-8 text-sm text-slate-500"
              >
                Loading workflow result...
              </div>
              <div v-else class="space-y-3">
                <pre class="max-h-72 overflow-auto rounded-2xl border border-slate-200 bg-slate-950 p-4 text-xs text-slate-100">{{ selectedWorkflow.sql_content }}</pre>
                <div
                  v-if="!selectedContent || selectedContent.rows.length === 0"
                  class="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-8 text-sm text-slate-500"
                >
                  No row-level result is currently available for this workflow.
                </div>
                <div v-else class="overflow-x-auto rounded-2xl border border-slate-200">
                  <table class="min-w-full divide-y divide-slate-200 text-left text-sm">
                    <thead class="bg-slate-50">
                      <tr>
                        <th
                          v-for="column in detailColumns"
                          :key="column"
                          class="px-4 py-3 font-medium text-slate-600"
                        >
                          {{ column }}
                        </th>
                      </tr>
                    </thead>
                    <tbody class="divide-y divide-slate-100 bg-white">
                      <tr v-for="(row, rowIndex) in selectedContent.rows" :key="rowIndex">
                        <td
                          v-for="column in detailColumns"
                          :key="`${rowIndex}-${column}`"
                          class="max-w-[18rem] whitespace-pre-wrap break-words px-4 py-3 text-slate-700"
                        >
                          {{ row[column] ?? '' }}
                        </td>
                      </tr>
                    </tbody>
                  </table>
                </div>
                <p
                  v-if="selectedWorkflow.last_operation_info"
                  class="rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-600"
                >
                  {{ selectedWorkflow.last_operation_info }}
                </p>
              </div>
            </div>

            <div class="space-y-3">
              <div class="flex items-center gap-2">
                <FileDown class="h-4 w-4 text-slate-500" />
                <h3 class="font-medium text-slate-900">Audit history</h3>
              </div>
              <div
                v-if="selectedWorkflow.logs.length === 0"
                class="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-8 text-sm text-slate-500"
              >
                No workflow log entries yet.
              </div>
              <div v-else class="space-y-3">
                <div
                  v-for="(log, index) in selectedWorkflow.logs"
                  :key="`${log.operation_time}-${index}`"
                  class="rounded-2xl border border-slate-200 bg-white p-4"
                >
                  <div class="flex flex-wrap items-center justify-between gap-2">
                    <p class="font-medium text-slate-900">{{ log.operation_type_desc }}</p>
                    <span class="text-xs text-slate-400">{{ formatDate(log.operation_time) }}</span>
                  </div>
                  <p class="mt-2 text-sm text-slate-600">{{ log.operation_info }}</p>
                  <p class="mt-2 text-xs uppercase tracking-wide text-slate-400">{{ log.operator_display }}</p>
                </div>
              </div>
            </div>

            <div
              v-if="selectedWorkflow.is_can_review"
              class="space-y-3 rounded-2xl border border-slate-200 bg-slate-50 p-4"
            >
              <h3 class="font-medium text-slate-900">Review action</h3>
              <textarea
                v-model="reviewForm.audit_remark"
                :class="textareaClass"
                :disabled="reviewSubmitting"
                placeholder="Add a review note"
              />
              <div class="flex flex-wrap gap-2">
                <Button
                  type="button"
                  class="gap-2"
                  :disabled="reviewSubmitting"
                  @click="void submitReview('pass')"
                >
                  <CheckCircle2 class="h-4 w-4" />
                  Approve
                </Button>
                <Button
                  variant="outline"
                  type="button"
                  class="gap-2 border-rose-200 text-rose-700 hover:bg-rose-50 hover:text-rose-700"
                  :disabled="reviewSubmitting"
                  @click="void submitReview('cancel')"
                >
                  <XCircle class="h-4 w-4" />
                  Reject
                </Button>
              </div>
            </div>

            <div
              v-if="selectedWorkflow.is_can_review"
              class="space-y-3 rounded-2xl border border-slate-200 bg-slate-50 p-4"
            >
              <h3 class="font-medium text-slate-900">Execution window</h3>
              <div class="grid gap-3 md:grid-cols-2">
                <Input v-model="executionWindowForm.run_date_start" type="datetime-local" :disabled="windowSubmitting" />
                <Input v-model="executionWindowForm.run_date_end" type="datetime-local" :disabled="windowSubmitting" />
              </div>
              <Button type="button" variant="outline" :disabled="windowSubmitting" @click="void saveExecutionWindow()">
                Save execution window
              </Button>
            </div>

            <div
              v-if="selectedWorkflow.is_can_execute || selectedWorkflow.is_can_timingtask"
              class="space-y-4 rounded-2xl border border-slate-200 bg-slate-50 p-4"
            >
              <h3 class="font-medium text-slate-900">Execution actions</h3>
              <div class="flex flex-wrap gap-2">
                <Button
                  v-if="selectedWorkflow.is_can_execute"
                  type="button"
                  class="gap-2"
                  :disabled="executeSubmitting"
                  @click="void triggerExecution('auto')"
                >
                  <Play class="h-4 w-4" />
                  Execute now
                </Button>
                <Button
                  v-if="selectedWorkflow.is_can_execute && selectedWorkflow.manual_execution_enabled"
                  variant="outline"
                  type="button"
                  class="gap-2"
                  :disabled="executeSubmitting"
                  @click="void triggerExecution('manual')"
                >
                  Manual execute
                </Button>
              </div>

              <div v-if="selectedWorkflow.is_can_timingtask" class="space-y-3">
                <div class="flex items-center gap-2">
                  <CalendarClock class="h-4 w-4 text-slate-500" />
                  <p class="text-sm font-medium text-slate-900">Scheduled execution</p>
                </div>
                <Input v-model="scheduleForm.run_date" type="datetime-local" :disabled="scheduleSubmitting" />
                <Button type="button" variant="outline" :disabled="scheduleSubmitting" @click="void submitSchedule()">
                  {{ selectedWorkflow.status === 'workflow_timingtask' ? 'Update schedule' : 'Schedule execution' }}
                </Button>
              </div>
            </div>

            <div
              v-if="selectedWorkflow.is_can_cancel"
              class="space-y-3 rounded-2xl border border-slate-200 bg-slate-50 p-4"
            >
              <h3 class="font-medium text-slate-900">Terminate workflow</h3>
              <textarea
                v-model="reviewForm.audit_remark"
                :class="textareaClass"
                :disabled="reviewSubmitting"
                placeholder="Add a termination reason"
              />
              <Button
                variant="outline"
                type="button"
                class="gap-2 border-rose-200 text-rose-700 hover:bg-rose-50 hover:text-rose-700"
                :disabled="reviewSubmitting"
                @click="void terminateWorkflow()"
              >
                <StopCircle class="h-4 w-4" />
                Terminate workflow
              </Button>
            </div>

            <div
              v-if="selectedWorkflow.is_can_rollback"
              class="space-y-3 rounded-2xl border border-slate-200 bg-slate-50 p-4"
            >
              <div class="flex items-center justify-between gap-3">
                <h3 class="font-medium text-slate-900">Rollback SQL</h3>
                <Button variant="outline" type="button" class="gap-2" @click="downloadRollback">
                  <FileDown class="h-4 w-4" />
                  Download rollback SQL
                </Button>
              </div>
              <div
                v-if="rollbackLoading"
                class="rounded-xl border border-dashed border-slate-200 bg-white px-4 py-8 text-sm text-slate-500"
              >
                Loading rollback SQL...
              </div>
              <div
                v-else-if="!selectedRollback || selectedRollback.rows.length === 0"
                class="rounded-xl border border-dashed border-slate-200 bg-white px-4 py-8 text-sm text-slate-500"
              >
                No rollback SQL is currently available for this workflow.
              </div>
              <div v-else class="space-y-3">
                <div
                  v-for="(pair, index) in selectedRollback.rows"
                  :key="index"
                  class="rounded-2xl border border-slate-200 bg-white p-4"
                >
                  <p class="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-400">Original SQL</p>
                  <pre class="overflow-auto whitespace-pre-wrap rounded-xl bg-slate-950 p-3 text-xs text-slate-100">{{ pair[0] }}</pre>
                  <p class="mb-2 mt-4 text-xs font-semibold uppercase tracking-wide text-slate-400">Rollback SQL</p>
                  <pre class="overflow-auto whitespace-pre-wrap rounded-xl bg-slate-950 p-3 text-xs text-slate-100">{{ pair[1] }}</pre>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </section>
</template>
