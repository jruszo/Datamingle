<script setup lang="ts">
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { format as formatSqlText } from 'sql-formatter'
import { ArrowLeft, RefreshCw, Send } from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { SqlCodeEditor } from '@/features/queries/components'
import {
  checkWorkflowExportSql,
  createWorkflow,
  fetchInstanceResources,
  fetchWorkflowApprovalPreview,
  fetchWorkflowExportSubmissionMetadata,
  type WorkflowApprovalPreview,
  type WorkflowCheckResult,
  type WorkflowSubmissionMetadata,
  type WorkflowSubmitInstanceRecord,
} from '../api'
import { useAuthStore } from '@/stores/auth'

type ExportDraft = {
  instanceId?: number
  instanceName?: string
  dbName?: string
  schemaName?: string
  sqlContent?: string
}

const EXPORT_DRAFT_KEY = 'exportWorkflowDraft'

const authStore = useAuthStore()
const router = useRouter()

const pageLoading = ref(false)
const approvalLoading = ref(false)
const databasesLoading = ref(false)
const checking = ref(false)
const submitting = ref(false)

const pageError = ref('')
const formError = ref('')
const checkError = ref('')
const approvalError = ref('')
const databasesError = ref('')

const submissionMetadata = ref<WorkflowSubmissionMetadata | null>(null)
const approvalPreview = ref<WorkflowApprovalPreview | null>(null)
const availableDatabases = ref<string[]>([])
const checkResult = ref<WorkflowCheckResult | null>(null)
const checkedFingerprint = ref('')

const form = reactive({
  workflowName: '',
  groupId: '',
  instanceId: '',
  dbName: '',
  schemaName: '',
  exportFormat: 'csv' as 'csv' | 'tsv' | 'sql' | 'xlsx',
  runDateStart: '',
  runDateEnd: '',
  sqlContent: '',
})

const selectClass =
  'block h-10 w-full rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-700 shadow-sm outline-none transition focus:border-slate-400 disabled:cursor-not-allowed disabled:bg-slate-100'

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

function workflowFingerprint() {
  return JSON.stringify({
    instanceId: form.instanceId,
    dbName: form.dbName.trim(),
    schemaName: form.schemaName.trim(),
    sqlContent: form.sqlContent.trim(),
    exportFormat: form.exportFormat,
  })
}

function invalidateCheck() {
  checkError.value = ''
  checkedFingerprint.value = ''
}

const eligibleGroups = computed(() => submissionMetadata.value?.resource_groups ?? [])
const filteredInstances = computed(() => {
  const instances = submissionMetadata.value?.instances ?? []
  const groupId = Number(form.groupId)
  if (!groupId) {
    return instances
  }
  return instances.filter((instance) => instance.group_ids.includes(groupId))
})

const selectedInstance = computed<WorkflowSubmitInstanceRecord | null>(() => {
  const instanceId = Number(form.instanceId)
  return filteredInstances.value.find((instance) => instance.id === instanceId) ?? null
})

const canCreateExport = computed(() => (submissionMetadata.value?.instances ?? []).length > 0)
const isCheckFresh = computed(() => checkedFingerprint.value === workflowFingerprint())
const canSubmit = computed(() => {
  return (
    Boolean(form.workflowName.trim()) &&
    Boolean(form.groupId) &&
    Boolean(form.instanceId) &&
    Boolean(form.dbName) &&
    Boolean(form.sqlContent.trim()) &&
    checkResult.value?.syntax_type === 3 &&
    !checkResult.value?.error_count &&
    isCheckFresh.value
  )
})

async function loadSubmissionMetadata() {
  pageLoading.value = true
  pageError.value = ''

  try {
    submissionMetadata.value = await fetchWorkflowExportSubmissionMetadata(requireToken())
  } catch (errorValue) {
    pageError.value = toUserFacingMessage(errorValue, 'Failed to load the export submission form.')
  } finally {
    pageLoading.value = false
  }
}

async function loadApprovalPreview(groupId: number) {
  approvalLoading.value = true
  approvalError.value = ''

  try {
    approvalPreview.value = await fetchWorkflowApprovalPreview(groupId, requireToken())
  } catch (errorValue) {
    approvalPreview.value = null
    approvalError.value = toUserFacingMessage(errorValue, 'Failed to load the approval flow.')
  } finally {
    approvalLoading.value = false
  }
}

async function loadDatabases(instanceId: number) {
  databasesLoading.value = true
  databasesError.value = ''

  try {
    const payload = await fetchInstanceResources(instanceId, 'database', requireToken())
    availableDatabases.value = payload.result.map((item) => `${item}`)
    if (!availableDatabases.value.includes(form.dbName)) {
      form.dbName = ''
    }
  } catch (errorValue) {
    availableDatabases.value = []
    form.dbName = ''
    databasesError.value = toUserFacingMessage(
      errorValue,
      'Failed to load databases for the selected instance.',
    )
  } finally {
    databasesLoading.value = false
  }
}

function restoreDraft() {
  const rawDraft = window.sessionStorage.getItem(EXPORT_DRAFT_KEY)
  if (!rawDraft) {
    return
  }

  let draft: ExportDraft | null = null
  try {
    draft = JSON.parse(rawDraft) as ExportDraft
  } catch {
    window.sessionStorage.removeItem(EXPORT_DRAFT_KEY)
    return
  }

  form.sqlContent = draft.sqlContent?.trim() || ''
  if (!submissionMetadata.value) {
    return
  }

  const matchedInstance = submissionMetadata.value.instances.find((instance) => (
    (draft?.instanceId && instance.id === draft.instanceId)
    || (draft?.instanceName && instance.instance_name === draft.instanceName)
  ))

  if (!matchedInstance) {
    return
  }

  if (matchedInstance.group_ids.length === 1) {
    form.groupId = `${matchedInstance.group_ids[0]}`
  }
  form.instanceId = `${matchedInstance.id}`
  form.dbName = draft.dbName?.trim() || ''
  form.schemaName = draft.schemaName?.trim() || ''
}

async function runCheck() {
  formError.value = ''
  checkError.value = ''

  if (!form.instanceId || !form.dbName || !form.sqlContent.trim()) {
    checkError.value = 'Instance, database, and SQL content are required before running export validation.'
    return false
  }

  checking.value = true
  try {
    const result = await checkWorkflowExportSql(
      {
        instance_id: Number(form.instanceId),
        db_name: form.dbName,
        schema_name: form.schemaName.trim() || undefined,
        full_sql: form.sqlContent,
      },
      requireToken(),
    )
    checkResult.value = result
    checkedFingerprint.value = workflowFingerprint()
    if (result.syntax_type !== 3) {
      checkError.value = 'Only SELECT or WITH statements can be submitted for export.'
      return false
    }
    return result.error_count === 0
  } catch (errorValue) {
    checkedFingerprint.value = ''
    checkResult.value = null
    checkError.value = toUserFacingMessage(errorValue, 'Export validation failed.')
    return false
  } finally {
    checking.value = false
  }
}

function formatSql() {
  if (!form.sqlContent.trim()) {
    return
  }

  const language = selectedInstance.value?.db_type === 'mysql' ? 'mysql' : 'sql'
  form.sqlContent = formatSqlText(form.sqlContent, { language })
}

async function submitWorkflow() {
  formError.value = ''

  if (!form.workflowName.trim()) {
    formError.value = 'Workflow name is required.'
    return
  }
  if (!form.groupId || !form.instanceId || !form.dbName || !form.sqlContent.trim()) {
    formError.value = 'Group, instance, database, and SQL content are required.'
    return
  }
  if (!isCheckFresh.value || !checkResult.value) {
    const checkPassed = await runCheck()
    if (!checkPassed) {
      return
    }
  }
  if (checkResult.value && checkResult.value.error_count > 0) {
    formError.value = 'Resolve the export validation errors before submitting.'
    return
  }

  submitting.value = true
  try {
    const payload = await createWorkflow(
      {
        workflow: {
          workflow_name: form.workflowName.trim(),
          group_id: Number(form.groupId),
          db_name: form.dbName,
          schema_name: form.schemaName.trim() || undefined,
          instance: Number(form.instanceId),
          export_format: form.exportFormat,
          is_backup: false,
          is_offline_export: 1,
          run_date_start: form.runDateStart || null,
          run_date_end: form.runDateEnd || null,
        },
        sql_content: form.sqlContent,
      },
      requireToken(),
    )
    window.sessionStorage.removeItem(EXPORT_DRAFT_KEY)
    await router.replace({
      name: 'workflow-detail',
      params: { workflowId: payload.workflow.id },
    })
  } catch (errorValue) {
    formError.value = toUserFacingMessage(errorValue, 'Failed to submit the export workflow.')
  } finally {
    submitting.value = false
  }
}

watch(
  () => form.groupId,
  (groupIdValue) => {
    const retainedInstanceId = Number(form.instanceId)
    const keepsCurrentInstance = filteredInstances.value.some((instance) => instance.id === retainedInstanceId)

    approvalPreview.value = null
    approvalError.value = ''
    form.instanceId = keepsCurrentInstance ? form.instanceId : ''
    form.dbName = ''
    form.schemaName = ''
    availableDatabases.value = []
    databasesError.value = ''
    invalidateCheck()

    const groupId = Number(groupIdValue)
    if (groupId) {
      void loadApprovalPreview(groupId)
    }
    if (groupId && keepsCurrentInstance && retainedInstanceId) {
      void loadDatabases(retainedInstanceId)
    }
  },
)

watch(
  () => form.instanceId,
  (instanceIdValue) => {
    form.dbName = ''
    form.schemaName = ''
    availableDatabases.value = []
    databasesError.value = ''
    invalidateCheck()

    const instanceId = Number(instanceIdValue)
    if (instanceId) {
      void loadDatabases(instanceId)
    }
  },
)

watch(
  () => form.dbName,
  () => {
    form.schemaName = ''
    invalidateCheck()
  },
)

watch(
  () => form.sqlContent,
  () => {
    invalidateCheck()
  },
)

watch(
  () => form.exportFormat,
  () => {
    invalidateCheck()
  },
)

onMounted(async () => {
  await loadSubmissionMetadata()
  restoreDraft()
  if (form.groupId) {
    await loadApprovalPreview(Number(form.groupId))
  }
  if (form.instanceId) {
    await loadDatabases(Number(form.instanceId))
  }
})
</script>

<template>
  <section class="grid gap-6">
    <div class="flex flex-wrap items-start justify-between gap-3">
      <div class="space-y-1">
        <p class="text-sm font-medium uppercase tracking-[0.2em] text-slate-500">Workflow Submission</p>
        <h1 class="text-3xl font-semibold tracking-tight text-slate-900">New Export Request</h1>
        <p class="text-sm text-slate-500">
          Submit a SELECT export for approval, then download the generated file after execution finishes.
        </p>
      </div>
      <Button variant="outline" type="button" class="gap-2" @click="void router.push({ name: 'workflows' })">
        <ArrowLeft class="h-4 w-4" />
        Back to workflows
      </Button>
    </div>

    <Card v-if="pageError" class="border-red-200">
      <CardHeader>
        <CardTitle>Unable to load submission page</CardTitle>
        <CardDescription>{{ pageError }}</CardDescription>
      </CardHeader>
    </Card>

    <Card v-else-if="pageLoading">
      <CardHeader>
        <CardTitle>Loading submission form</CardTitle>
        <CardDescription>Fetching groups, instances, and export settings.</CardDescription>
      </CardHeader>
    </Card>

    <Card v-else-if="!canCreateExport" class="border-red-200">
      <CardHeader>
        <CardTitle>Access denied</CardTitle>
        <CardDescription>
          Your account does not currently have access to submit export workflows.
        </CardDescription>
      </CardHeader>
    </Card>

    <template v-else>
      <div class="grid gap-6 xl:grid-cols-[minmax(0,1fr)_22rem]">
        <Card class="border-slate-200">
          <CardHeader>
            <CardTitle>Export request</CardTitle>
            <CardDescription>
              Use this flow for large approved exports that should be downloaded as files instead of viewed inline.
            </CardDescription>
          </CardHeader>
          <CardContent class="space-y-5">
            <div class="flex flex-wrap items-center gap-2">
              <Button variant="outline" type="button" :disabled="checking || submitting" @click="formatSql">
                Format SQL
              </Button>
              <Button type="button" variant="outline" class="gap-2" :disabled="checking || submitting" @click="void runCheck()">
                <RefreshCw class="h-4 w-4" />
                Validate export
              </Button>
              <Button
                type="button"
                class="gap-2"
                data-testid="export-submit"
                :disabled="submitting || checking || !canSubmit"
                @click="void submitWorkflow()"
              >
                <Send class="h-4 w-4" />
                Submit export request
              </Button>
            </div>

            <p v-if="formError" class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
              {{ formError }}
            </p>

            <p v-if="checkError" class="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-700">
              {{ checkError }}
            </p>

            <p
              v-else-if="checkResult && !isCheckFresh"
              class="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-700"
            >
              SQL, instance, database, schema, or format changed after the last validation. Run export validation again before submitting.
            </p>

            <div
              v-if="checkResult && isCheckFresh"
              class="rounded-2xl border border-slate-200 bg-slate-50 p-4"
            >
              <div class="flex flex-wrap items-center justify-between gap-3">
                <div>
                  <p class="text-sm font-medium text-slate-900">Validation summary</p>
                  <p class="text-sm text-slate-500">
                    {{ checkResult.affected_rows }} row{{ checkResult.affected_rows === 1 ? '' : 's' }} expected for export.
                  </p>
                </div>
                <Badge
                  variant="outline"
                  :class="checkResult.error_count > 0 ? 'border-red-200 bg-red-50 text-red-700' : 'border-emerald-200 bg-emerald-50 text-emerald-700'"
                >
                  {{ checkResult.error_count > 0 ? 'Blocked' : 'Ready' }}
                </Badge>
              </div>
            </div>

            <SqlCodeEditor
              v-model="form.sqlContent"
              :db-type="selectedInstance?.db_type || 'mysql'"
              :disabled="checking || submitting"
              :min-height="420"
              placeholder="Paste a SELECT or WITH query for export."
              test-id="export-sql-editor"
            />
          </CardContent>
        </Card>

        <div class="grid gap-6">
          <Card class="border-slate-200">
            <CardHeader>
              <CardTitle>Request context</CardTitle>
              <CardDescription>
                Choose where the export runs and what file format should be produced.
              </CardDescription>
            </CardHeader>
            <CardContent class="space-y-4">
              <div class="space-y-2">
                <label class="text-sm font-medium text-slate-700" for="export-workflow-name">Workflow name</label>
                <Input id="export-workflow-name" v-model="form.workflowName" data-testid="export-workflow-name" placeholder="Short summary of the export request" :disabled="submitting" />
              </div>

              <div class="space-y-2">
                <label class="text-sm font-medium text-slate-700" for="export-workflow-group">Resource group</label>
                <select id="export-workflow-group" v-model="form.groupId" data-testid="export-workflow-group" :class="selectClass" :disabled="submitting">
                  <option value="">Select a resource group</option>
                  <option v-for="group in eligibleGroups" :key="group.group_id" :value="group.group_id">
                    {{ group.group_name }}
                  </option>
                </select>
              </div>

              <div class="space-y-2">
                <label class="text-sm font-medium text-slate-700" for="export-workflow-instance">Instance</label>
                <select id="export-workflow-instance" v-model="form.instanceId" data-testid="export-workflow-instance" :class="selectClass" :disabled="!form.groupId || submitting">
                  <option value="">Select an instance</option>
                  <option v-for="instance in filteredInstances" :key="instance.id" :value="instance.id">
                    {{ instance.instance_name }} / {{ instance.db_type.toUpperCase() }}
                  </option>
                </select>
              </div>

              <div class="space-y-2">
                <label class="text-sm font-medium text-slate-700" for="export-workflow-db">Database</label>
                <select id="export-workflow-db" v-model="form.dbName" data-testid="export-workflow-db" :class="selectClass" :disabled="!form.instanceId || databasesLoading || submitting">
                  <option value="">{{ databasesLoading ? 'Loading databases...' : 'Select a database' }}</option>
                  <option v-for="databaseName in availableDatabases" :key="databaseName" :value="databaseName">
                    {{ databaseName }}
                  </option>
                </select>
                <p v-if="databasesError" class="text-sm text-red-600">{{ databasesError }}</p>
              </div>

              <div v-if="form.schemaName" class="space-y-2">
                <label class="text-sm font-medium text-slate-700">Schema</label>
                <Input :model-value="form.schemaName" disabled />
                <p class="text-xs text-slate-500">
                  Reused from the query workspace so the export runs in the same schema context.
                </p>
              </div>

              <div class="space-y-2">
                <label class="text-sm font-medium text-slate-700" for="export-format">Export format</label>
                <select id="export-format" v-model="form.exportFormat" data-testid="export-format" :class="selectClass" :disabled="submitting">
                  <option value="csv">CSV</option>
                  <option value="tsv">TSV</option>
                  <option value="sql">SQL</option>
                  <option value="xlsx">Excel (.xlsx)</option>
                </select>
              </div>

              <div class="grid gap-3 md:grid-cols-2">
                <div class="space-y-2">
                  <label class="text-sm font-medium text-slate-700">Execution window start</label>
                  <Input v-model="form.runDateStart" type="datetime-local" :disabled="submitting" />
                </div>
                <div class="space-y-2">
                  <label class="text-sm font-medium text-slate-700">Execution window end</label>
                  <Input v-model="form.runDateEnd" type="datetime-local" :disabled="submitting" />
                </div>
              </div>
            </CardContent>
          </Card>

          <Card class="border-slate-200">
            <CardHeader>
              <CardTitle>Approval flow</CardTitle>
              <CardDescription>Preview the configured reviewers for this resource group.</CardDescription>
            </CardHeader>
            <CardContent class="space-y-3">
              <p v-if="approvalError" class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
                {{ approvalError }}
              </p>
              <p v-else-if="approvalLoading" class="text-sm text-slate-500">
                Loading approval flow...
              </p>
              <p v-else-if="!approvalPreview" class="text-sm text-slate-500">
                Select a resource group to preview the approval chain.
              </p>
              <template v-else>
                <p data-testid="export-approval-preview" class="text-sm text-slate-500">{{ approvalPreview.display }}</p>
                <div class="flex flex-wrap gap-2">
                  <Badge
                    v-for="(node, index) in approvalPreview.review_info"
                    :key="`${node.group_name}-${index}`"
                    variant="outline"
                    class="border-slate-200 bg-slate-50 text-slate-700"
                  >
                    {{ node.group_name }}
                  </Badge>
                </div>
              </template>
            </CardContent>
          </Card>
        </div>
      </div>
    </template>
  </section>
</template>
