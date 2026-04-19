<script setup lang="ts">
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import {
  ArrowLeft,
  CheckCircle2,
  PauseCircle,
  Play,
  RefreshCw,
  XCircle,
} from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import {
  fetchArchiveDetail,
  fetchArchiveLogs,
  reviewArchive,
  runArchiveNow,
  updateArchiveState,
  type ArchiveDetailRecord,
  type ArchiveLogRecord,
  type PaginatedResponse,
} from '@/lib/api'
import { useAuthStore } from '@/stores/auth'

const authStore = useAuthStore()
const route = useRoute()
const router = useRouter()

const detailLoading = ref(false)
const logsLoading = ref(false)
const reviewSubmitting = ref(false)
const runSubmitting = ref(false)
const stateSubmitting = ref(false)

const detailError = ref('')
const feedback = ref('')

const archiveDetail = ref<ArchiveDetailRecord | null>(null)
const archiveLogsPage = ref<PaginatedResponse<ArchiveLogRecord>>({
  count: 0,
  next: null,
  previous: null,
  results: [],
})
const logPage = ref(1)
const logPageSize = 20

const reviewForm = reactive({
  auditRemark: '',
})

const textareaClass =
  'block min-h-[7rem] w-full rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700 shadow-sm outline-none transition focus:border-slate-400 disabled:cursor-not-allowed disabled:bg-slate-100'

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

function archiveMethodLabel(method: string) {
  if (method === 'pt_archiver') {
    return 'pt-archiver'
  }
  return 'Rendered DML delete'
}

function executionModeLabel(mode: string) {
  if (mode === 'scheduled') {
    return 'Scheduled'
  }
  return 'One time'
}

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

function executionStateBadgeClass(value: string) {
  if (value === 'Enabled' || value === 'Ready' || value === 'Completed') {
    return 'border-emerald-200 bg-emerald-50 text-emerald-700'
  }
  if (value === 'Pending' || value === 'Queued' || value === 'Running') {
    return 'border-amber-200 bg-amber-50 text-amber-700'
  }
  return 'border-slate-200 bg-slate-100 text-slate-600'
}

function routeParamArchiveId() {
  const routeValue = route.params.archiveId
  const archiveId = typeof routeValue === 'string' ? Number(routeValue) : Number.NaN
  return Number.isInteger(archiveId) && archiveId > 0 ? archiveId : null
}

const selectedArchiveId = computed(() => routeParamArchiveId())
const canViewArchives = computed(() => hasPermission('sql.menu_archive'))
const scheduleWeekdaysLabel = computed(() => {
  const weekdays = archiveDetail.value?.schedule_weekdays ?? []
  if (weekdays.length === 0) {
    return 'Not set'
  }
  return weekdays.map((value) => value.toUpperCase()).join(', ')
})

async function loadArchiveDetail() {
  if (!canViewArchives.value) {
    return
  }
  const archiveId = selectedArchiveId.value
  if (!archiveId) {
    archiveDetail.value = null
    detailError.value = 'Invalid archive id.'
    return
  }

  detailLoading.value = true
  detailError.value = ''

  try {
    archiveDetail.value = await fetchArchiveDetail(archiveId, requireToken())
  } catch (errorValue) {
    archiveDetail.value = null
    detailError.value = toUserFacingMessage(errorValue, 'Failed to load archive detail.')
  } finally {
    detailLoading.value = false
  }
}

async function loadArchiveLogs(page = logPage.value) {
  if (!canViewArchives.value || !selectedArchiveId.value) {
    return
  }

  logsLoading.value = true
  try {
    archiveLogsPage.value = await fetchArchiveLogs(
      selectedArchiveId.value,
      requireToken(),
      page,
      logPageSize,
    )
    logPage.value = page
  } catch (errorValue) {
    detailError.value = toUserFacingMessage(errorValue, 'Failed to load archive logs.')
  } finally {
    logsLoading.value = false
  }
}

async function refreshArchiveDetail() {
  await Promise.all([loadArchiveDetail(), loadArchiveLogs(logPage.value)])
}

async function submitReviewAction(auditType: 'pass' | 'reject' | 'cancel') {
  if (!selectedArchiveId.value) {
    return
  }

  reviewSubmitting.value = true
  feedback.value = ''
  detailError.value = ''

  try {
    await reviewArchive(
      selectedArchiveId.value,
      {
        audit_type: auditType,
        audit_remark: reviewForm.auditRemark.trim(),
      },
      requireToken(),
    )
    feedback.value = auditType === 'pass'
      ? 'Archive approved.'
      : auditType === 'reject'
        ? 'Archive rejected.'
        : 'Archive canceled.'
    reviewForm.auditRemark = ''
    await refreshArchiveDetail()
  } catch (errorValue) {
    detailError.value = toUserFacingMessage(errorValue, 'Failed to submit the archive review action.')
  } finally {
    reviewSubmitting.value = false
  }
}

async function queueArchiveRunNow() {
  if (!selectedArchiveId.value) {
    return
  }

  runSubmitting.value = true
  feedback.value = ''
  detailError.value = ''

  try {
    feedback.value = await runArchiveNow(selectedArchiveId.value, requireToken())
    await refreshArchiveDetail()
  } catch (errorValue) {
    detailError.value = toUserFacingMessage(errorValue, 'Failed to queue archive execution.')
  } finally {
    runSubmitting.value = false
  }
}

async function setArchiveEnabled(enabled: boolean) {
  if (!selectedArchiveId.value) {
    return
  }

  stateSubmitting.value = true
  feedback.value = ''
  detailError.value = ''

  try {
    feedback.value = await updateArchiveState(
      selectedArchiveId.value,
      { enabled },
      requireToken(),
    )
    await refreshArchiveDetail()
  } catch (errorValue) {
    detailError.value = toUserFacingMessage(
      errorValue,
      enabled ? 'Failed to enable the archive schedule.' : 'Failed to disable the archive schedule.',
    )
  } finally {
    stateSubmitting.value = false
  }
}

async function goBackToArchives() {
  const returnTo = typeof route.query.returnTo === 'string' ? route.query.returnTo : ''
  if (returnTo.startsWith('/archives')) {
    await router.replace(returnTo)
    return
  }
  await router.push({ name: 'archives' })
}

watch(
  () => route.params.archiveId,
  () => {
    if (!authInitialized) {
      return
    }
    feedback.value = ''
    void refreshArchiveDetail()
  },
)

onMounted(async () => {
  await authStore.loadCurrentUser()
  authInitialized = true
  if (!canViewArchives.value) {
    return
  }
  await refreshArchiveDetail()
})
</script>

<template>
  <section class="grid gap-6">
    <div class="flex flex-wrap items-center justify-between gap-3">
      <Button
        variant="ghost"
        type="button"
        class="gap-2 px-0"
        data-testid="archive-detail-back"
        @click="void goBackToArchives()"
      >
        <ArrowLeft class="h-4 w-4" />
        Back to archives
      </Button>
    </div>

    <p
      v-if="feedback"
      class="rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700"
    >
      {{ feedback }}
    </p>

    <Card v-if="!canViewArchives" class="border-red-200">
      <CardHeader>
        <CardTitle>Access denied</CardTitle>
        <CardDescription>
          Archive access requires the archive menu permission.
        </CardDescription>
      </CardHeader>
    </Card>

    <Card v-else class="border-slate-200">
      <CardHeader class="gap-4">
        <div class="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <CardTitle>{{ archiveDetail?.title || 'Archive detail' }}</CardTitle>
            <CardDescription>
              Review approval progress, schedule state, and execution logs for the selected delete-only archive workflow.
            </CardDescription>
          </div>
          <Button
            variant="outline"
            type="button"
            class="gap-2"
            data-testid="archive-detail-refresh"
            @click="void refreshArchiveDetail()"
          >
            <RefreshCw class="h-4 w-4" />
            Refresh detail
          </Button>
        </div>

        <div v-if="archiveDetail" class="flex flex-wrap gap-2">
          <Badge variant="outline" class="border-slate-200 bg-slate-50 text-slate-700">
            {{ executionModeLabel(archiveDetail.execution_mode) }}
          </Badge>
          <Badge variant="outline" class="border-slate-200 bg-slate-50 text-slate-700">
            {{ archiveMethodLabel(archiveDetail.archive_method) }}
          </Badge>
          <Badge variant="outline" data-testid="archive-detail-status" :class="statusBadgeClass(archiveDetail.status)">
            {{ archiveDetail.status_label }}
          </Badge>
          <Badge
            variant="outline"
            data-testid="archive-execution-state"
            :class="executionStateBadgeClass(archiveDetail.execution_state_label)"
          >
            {{ archiveDetail.execution_state_label }}
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
          Loading archive detail...
        </div>

        <div
          v-else-if="!archiveDetail"
          class="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-10 text-sm text-slate-500"
        >
          The requested archive workflow could not be loaded.
        </div>

        <template v-else>
          <div class="grid gap-4 lg:grid-cols-2 xl:grid-cols-4">
            <div class="rounded-2xl border border-slate-200 bg-slate-50 p-4">
              <p class="text-xs uppercase tracking-wide text-slate-500">Target</p>
              <p class="mt-2 text-sm font-medium text-slate-900">
                {{ archiveDetail.src_instance.instance_name }}
              </p>
              <p class="text-sm text-slate-500">
                {{ archiveDetail.src_db_name }} / {{ archiveDetail.src_table_name }}
              </p>
            </div>
            <div class="rounded-2xl border border-slate-200 bg-slate-50 p-4">
              <p class="text-xs uppercase tracking-wide text-slate-500">Requester</p>
              <p class="mt-2 text-sm font-medium text-slate-900">
                {{ archiveDetail.user_display || archiveDetail.user_name }}
              </p>
              <p class="text-sm text-slate-500">{{ archiveDetail.resource_group.group_name }}</p>
            </div>
            <div class="rounded-2xl border border-slate-200 bg-slate-50 p-4">
              <p class="text-xs uppercase tracking-wide text-slate-500">Next run</p>
              <p class="mt-2 text-sm font-medium text-slate-900">
                {{ formatDateTime(archiveDetail.next_run_at) }}
              </p>
              <p class="text-sm text-slate-500">
                Last run: {{ formatDateTime(archiveDetail.last_archive_time) }}
              </p>
            </div>
            <div class="rounded-2xl border border-rose-200 bg-rose-50 p-4">
              <p class="text-xs uppercase tracking-wide text-rose-600">Safety</p>
              <p class="mt-2 text-sm font-medium text-rose-800">
                Delete only
              </p>
              <p class="text-sm text-rose-700">
                No backup artifact is created for this workflow.
              </p>
            </div>
          </div>

          <div class="grid gap-6 xl:grid-cols-[2fr_1fr]">
            <div class="space-y-6">
              <Card class="border-slate-200">
                <CardHeader>
                  <CardTitle>Archive Definition</CardTitle>
                  <CardDescription>
                    The server renders any supported variables when the delete is executed.
                  </CardDescription>
                </CardHeader>
                <CardContent class="grid gap-4 lg:grid-cols-2">
                  <div class="space-y-1">
                    <p class="text-xs uppercase tracking-wide text-slate-500">Method</p>
                    <p class="text-sm text-slate-900">{{ archiveMethodLabel(archiveDetail.archive_method) }}</p>
                  </div>
                  <div class="space-y-1">
                    <p class="text-xs uppercase tracking-wide text-slate-500">Execution mode</p>
                    <p class="text-sm text-slate-900">{{ executionModeLabel(archiveDetail.execution_mode) }}</p>
                  </div>
                  <div class="space-y-1">
                    <p class="text-xs uppercase tracking-wide text-slate-500">Schedule</p>
                    <p class="text-sm text-slate-900">
                      {{ archiveDetail.schedule_frequency ? archiveDetail.schedule_frequency : 'One-time execution' }}
                    </p>
                  </div>
                  <div class="space-y-1">
                    <p class="text-xs uppercase tracking-wide text-slate-500">Schedule time</p>
                    <p class="text-sm text-slate-900">{{ archiveDetail.schedule_time || 'Not scheduled' }}</p>
                  </div>
                  <div
                    v-if="archiveDetail.execution_mode === 'scheduled' && archiveDetail.schedule_frequency === 'weekly'"
                    class="space-y-1 lg:col-span-2"
                  >
                    <p class="text-xs uppercase tracking-wide text-slate-500">Weekdays</p>
                    <p class="text-sm text-slate-900">{{ scheduleWeekdaysLabel }}</p>
                  </div>
                  <div class="space-y-2 lg:col-span-2">
                    <p class="text-xs uppercase tracking-wide text-slate-500">Delete condition</p>
                    <pre class="overflow-x-auto rounded-xl border border-slate-200 bg-slate-950 p-4 text-sm text-slate-100">{{ archiveDetail.condition }}</pre>
                  </div>
                </CardContent>
              </Card>

              <Card class="border-slate-200">
                <CardHeader>
                  <CardTitle>Approval & Activity</CardTitle>
                  <CardDescription>
                    Approval status and workflow activity for this archive request.
                  </CardDescription>
                </CardHeader>
                <CardContent class="space-y-5">
                  <div class="grid gap-3 md:grid-cols-2">
                    <div class="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                      <p class="text-xs uppercase tracking-wide text-slate-500">Approval flow</p>
                      <div class="mt-3 flex flex-wrap gap-2">
                        <Badge
                          v-for="node in archiveDetail.review_info"
                          :key="`${archiveDetail.id}-${node.group_name}-${node.is_current_node}`"
                          variant="outline"
                          :class="node.is_current_node ? 'border-amber-200 bg-amber-50 text-amber-700' : node.is_passed_node ? 'border-emerald-200 bg-emerald-50 text-emerald-700' : 'border-slate-200 bg-white text-slate-700'"
                        >
                          {{ node.group_name }}
                        </Badge>
                      </div>
                    </div>
                    <div class="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                      <p class="text-xs uppercase tracking-wide text-slate-500">Current reviewers</p>
                      <div class="mt-3 space-y-2 text-sm text-slate-700">
                        <p v-if="archiveDetail.current_reviewers.length === 0">No active reviewers.</p>
                        <p
                          v-for="reviewer in archiveDetail.current_reviewers"
                          :key="reviewer.id"
                        >
                          {{ reviewer.display || reviewer.username }}
                        </p>
                      </div>
                    </div>
                  </div>

                  <div class="space-y-3">
                    <div class="flex items-center justify-between gap-3">
                      <p class="text-sm font-medium text-slate-700">Workflow log</p>
                      <p class="text-xs text-slate-500">
                        Last operation: {{ archiveDetail.last_operation_info || 'Not recorded yet' }}
                      </p>
                    </div>
                    <div
                      v-if="archiveDetail.logs.length === 0"
                      class="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-sm text-slate-500"
                    >
                      No workflow activity recorded yet.
                    </div>
                    <div v-else class="space-y-3">
                      <div
                        v-for="(log, index) in archiveDetail.logs"
                        :key="`${log.operation_time}-${index}`"
                        class="rounded-xl border border-slate-200 bg-slate-50 p-4"
                      >
                        <div class="flex flex-wrap items-center justify-between gap-2">
                          <p class="text-sm font-medium text-slate-900">{{ log.operation_type_desc }}</p>
                          <p class="text-xs text-slate-500">{{ formatDateTime(log.operation_time) }}</p>
                        </div>
                        <p class="mt-2 text-sm text-slate-700">{{ log.operation_info }}</p>
                        <p class="mt-1 text-xs text-slate-500">{{ log.operator_display }}</p>
                      </div>
                    </div>
                  </div>
                </CardContent>
              </Card>

              <Card class="border-slate-200">
                <CardHeader>
                  <div class="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
                    <div>
                      <CardTitle>Archive Logs</CardTitle>
                      <CardDescription>
                        Execution logs from the delete operation path, including rendered conditions and row counts.
                      </CardDescription>
                    </div>
                    <Button variant="outline" type="button" class="gap-2" @click="void loadArchiveLogs(logPage)">
                      <RefreshCw class="h-4 w-4" />
                      Refresh logs
                    </Button>
                  </div>
                </CardHeader>
                <CardContent class="space-y-4">
                  <div
                    v-if="logsLoading"
                    class="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-10 text-sm text-slate-500"
                  >
                    Loading archive logs...
                  </div>

                  <div
                    v-else-if="archiveLogsPage.results.length === 0"
                    class="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-10 text-sm text-slate-500"
                  >
                    No archive execution logs recorded yet.
                  </div>

                  <div v-else class="space-y-3">
                    <div
                      v-for="log in archiveLogsPage.results"
                      :key="log.id"
                      :data-testid="`archive-log-item-${log.id}`"
                      class="rounded-2xl border border-slate-200 bg-slate-50 p-4"
                    >
                      <div class="flex flex-wrap items-center justify-between gap-2">
                        <div class="flex flex-wrap items-center gap-2">
                          <Badge variant="outline" class="border-slate-200 bg-white text-slate-700">
                            {{ archiveMethodLabel(log.archive_method) }}
                          </Badge>
                          <Badge
                            variant="outline"
                            :class="log.success ? 'border-emerald-200 bg-emerald-50 text-emerald-700' : 'border-rose-200 bg-rose-50 text-rose-700'"
                          >
                            {{ log.success ? 'Succeeded' : 'Failed' }}
                          </Badge>
                        </div>
                        <p class="text-xs text-slate-500">
                          {{ formatDateTime(log.start_time) }}
                        </p>
                      </div>

                      <div class="mt-3 grid gap-3 md:grid-cols-3">
                        <div class="rounded-xl border border-slate-200 bg-white p-3">
                          <p class="text-xs uppercase tracking-wide text-slate-500">Selected</p>
                          <p class="mt-2 text-sm font-medium text-slate-900">{{ log.select_cnt }}</p>
                        </div>
                        <div class="rounded-xl border border-slate-200 bg-white p-3">
                          <p class="text-xs uppercase tracking-wide text-slate-500">Deleted</p>
                          <p class="mt-2 text-sm font-medium text-slate-900">{{ log.delete_cnt }}</p>
                        </div>
                        <div class="rounded-xl border border-slate-200 bg-white p-3">
                          <p class="text-xs uppercase tracking-wide text-slate-500">Inserted</p>
                          <p class="mt-2 text-sm font-medium text-slate-900">{{ log.insert_cnt }}</p>
                        </div>
                      </div>

                      <div class="mt-3 space-y-2">
                        <p class="text-xs uppercase tracking-wide text-slate-500">Rendered condition</p>
                        <pre class="overflow-x-auto rounded-xl border border-slate-200 bg-slate-950 p-3 text-xs text-slate-100">{{ log.condition }}</pre>
                      </div>

                      <div class="mt-3 space-y-2">
                        <p class="text-xs uppercase tracking-wide text-slate-500">Command / SQL</p>
                        <pre class="overflow-x-auto rounded-xl border border-slate-200 bg-slate-950 p-3 text-xs text-slate-100">{{ log.cmd }}</pre>
                      </div>

                      <p v-if="log.error_info" class="mt-3 text-sm text-rose-700">{{ log.error_info }}</p>
                    </div>
                  </div>

                  <div class="flex flex-wrap items-center justify-between gap-3 border-t border-slate-100 pt-2">
                    <p class="text-sm text-slate-500">
                      Page {{ logPage }}
                    </p>
                    <div class="flex gap-2">
                      <Button
                        variant="outline"
                        type="button"
                        :disabled="archiveLogsPage.previous === null || logPage <= 1 || logsLoading"
                        @click="void loadArchiveLogs(logPage - 1)"
                      >
                        Previous
                      </Button>
                      <Button
                        variant="outline"
                        type="button"
                        :disabled="archiveLogsPage.next === null || logsLoading"
                        @click="void loadArchiveLogs(logPage + 1)"
                      >
                        Next
                      </Button>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </div>

            <div class="space-y-6">
              <Card class="border-slate-200">
                <CardHeader>
                  <CardTitle>Actions</CardTitle>
                  <CardDescription>
                    Approval and management actions update this archive definition directly.
                  </CardDescription>
                </CardHeader>
                <CardContent class="space-y-4">
                  <div
                    v-if="archiveDetail.is_can_review || archiveDetail.is_can_cancel"
                    class="space-y-3"
                  >
                    <div class="space-y-2">
                      <label class="text-sm font-medium text-slate-700">Remark</label>
                      <textarea
                        v-model="reviewForm.auditRemark"
                        :class="textareaClass"
                        placeholder="Optional review context"
                      />
                    </div>
                    <div class="flex flex-wrap gap-2">
                      <Button
                        v-if="archiveDetail.is_can_review"
                        type="button"
                        class="gap-2"
                        data-testid="archive-approve"
                        :disabled="reviewSubmitting"
                        @click="void submitReviewAction('pass')"
                      >
                        <CheckCircle2 class="h-4 w-4" />
                        Approve
                      </Button>
                      <Button
                        v-if="archiveDetail.is_can_review"
                        variant="outline"
                        type="button"
                        class="gap-2"
                        data-testid="archive-reject"
                        :disabled="reviewSubmitting"
                        @click="void submitReviewAction('reject')"
                      >
                        <XCircle class="h-4 w-4" />
                        Reject
                      </Button>
                      <Button
                        v-if="archiveDetail.is_can_cancel"
                        variant="outline"
                        type="button"
                        class="gap-2"
                        data-testid="archive-cancel"
                        :disabled="reviewSubmitting"
                        @click="void submitReviewAction('cancel')"
                      >
                        Cancel request
                      </Button>
                    </div>
                  </div>

                  <div class="space-y-2">
                    <Button
                      v-if="archiveDetail.is_can_run_now"
                      type="button"
                      class="w-full gap-2"
                      data-testid="archive-run-now"
                      :disabled="runSubmitting"
                      @click="void queueArchiveRunNow()"
                    >
                      <Play class="h-4 w-4" />
                      Run now
                    </Button>
                    <Button
                      v-if="archiveDetail.is_can_enable"
                      type="button"
                      class="w-full gap-2"
                      data-testid="archive-enable"
                      :disabled="stateSubmitting"
                      @click="void setArchiveEnabled(true)"
                    >
                      <Play class="h-4 w-4" />
                      Enable schedule
                    </Button>
                    <Button
                      v-if="archiveDetail.is_can_disable"
                      variant="outline"
                      type="button"
                      class="w-full gap-2"
                      data-testid="archive-disable"
                      :disabled="stateSubmitting"
                      @click="void setArchiveEnabled(false)"
                    >
                      <PauseCircle class="h-4 w-4" />
                      Disable schedule
                    </Button>
                  </div>
                </CardContent>
              </Card>
            </div>
          </div>
        </template>
      </CardContent>
    </Card>
  </section>
</template>
