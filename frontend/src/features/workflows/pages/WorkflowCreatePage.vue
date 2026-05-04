<script setup lang="ts">
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { format as formatSqlText } from 'sql-formatter'
import { ArrowLeft, CheckCircle2, FileUp, RefreshCw, Send } from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { SqlCodeEditor } from '@/features/queries/components'
import {
  checkWorkflowSql,
  createWorkflow,
  fetchInstanceResources,
  fetchWorkflowApprovalPreview,
  fetchWorkflowSubmissionMetadata,
  parseWorkflowSql,
  type WorkflowApprovalPreview,
  type WorkflowCheckResult,
  type WorkflowParseResult,
  type WorkflowSubmissionMetadata,
  type WorkflowSubmitInstanceRecord,
  type WorkflowSyntaxType,
} from '../api'
import { useAuthStore } from '@/stores/auth'

const props = withDefaults(
  defineProps<{
    expectedSyntaxType: WorkflowSyntaxType
    enableFileParse?: boolean
  }>(),
  {
    enableFileParse: false,
  },
)

const authStore = useAuthStore()
const router = useRouter()

const pageError = ref('')
const formError = ref('')
const checkError = ref('')
const approvalError = ref('')
const databasesError = ref('')
const parseError = ref('')

const pageLoading = ref(false)
const approvalLoading = ref(false)
const databasesLoading = ref(false)
const checking = ref(false)
const submitting = ref(false)
const parsingUpload = ref(false)

const submissionMetadata = ref<WorkflowSubmissionMetadata | null>(null)
const approvalPreview = ref<WorkflowApprovalPreview | null>(null)
const availableDatabases = ref<string[]>([])
const checkResult = ref<WorkflowCheckResult | null>(null)
const parsedUpload = ref<WorkflowParseResult | null>(null)
const parsedUploadFileName = ref('')
const checkedFingerprint = ref('')
const sqlFileInput = ref<HTMLInputElement | null>(null)

const form = reactive({
  workflowName: '',
  demandUrl: '',
  groupId: '',
  instanceId: '',
  dbName: '',
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
    sqlContent: form.sqlContent.trim(),
  })
}

function syntaxTypeLabel(value: number | null | undefined) {
  if (value === 1) {
    return 'DDL'
  }
  if (value === 2) {
    return 'DML'
  }
  return 'unknown'
}

function expectedSyntaxLabel() {
  return syntaxTypeLabel(props.expectedSyntaxType)
}

function expectedSyntaxDescription() {
  if (props.expectedSyntaxType === 1) {
    return 'schema-change statements such as CREATE, ALTER, and DROP'
  }
  return 'data-change statements such as INSERT, UPDATE, and DELETE'
}

function uploadSyntaxMismatchMessage(actualSyntaxType: number | null | undefined) {
  const actualLabel = syntaxTypeLabel(actualSyntaxType)
  if (actualLabel === 'unknown') {
    return `The uploaded file contains statements that could not be classified as ${expectedSyntaxLabel()}.`
  }
  return `The uploaded file resolved to ${actualLabel} statements. This page only accepts ${expectedSyntaxLabel()} SQL.`
}

const eligibleInstances = computed(() => {
  const instances = submissionMetadata.value?.instances ?? []
  return instances.filter((instance) =>
    instance.allowed_syntax_types.includes(props.expectedSyntaxType),
  )
})

const eligibleGroups = computed(() => {
  const eligibleGroupIds = new Set<number>()
  for (const instance of eligibleInstances.value) {
    for (const groupId of instance.group_ids) {
      eligibleGroupIds.add(groupId)
    }
  }
  return (submissionMetadata.value?.resource_groups ?? []).filter((group) =>
    eligibleGroupIds.has(group.group_id),
  )
})

const selectedInstance = computed<WorkflowSubmitInstanceRecord | null>(() => {
  const instanceId = Number(form.instanceId)
  return eligibleInstances.value.find((instance) => instance.id === instanceId) ?? null
})

const filteredInstances = computed(() => {
  const groupId = Number(form.groupId)
  if (!groupId) {
    return eligibleInstances.value
  }
  return eligibleInstances.value.filter((instance) => instance.group_ids.includes(groupId))
})

const canCreateWorkflow = computed(() => eligibleInstances.value.length > 0)
const isCheckFresh = computed(() => checkedFingerprint.value === workflowFingerprint())

const canSubmit = computed(() => {
  return (
    Boolean(form.workflowName.trim()) &&
    Boolean(form.groupId) &&
    Boolean(form.instanceId) &&
    Boolean(form.dbName) &&
    Boolean(form.sqlContent.trim()) &&
    checkResult.value?.syntax_type === props.expectedSyntaxType &&
    isCheckFresh.value
  )
})

const checkColumns = computed(() => {
  if (checkResult.value?.column_list?.length) {
    return checkResult.value.column_list
  }
  const firstRow = checkResult.value?.rows[0]
  return firstRow ? Object.keys(firstRow) : []
})

const parsedPreviewRows = computed(() => {
  return parsedUpload.value?.rows.slice(0, 5) ?? []
})

const pageTitle = computed(() => `New ${expectedSyntaxLabel()} request`)
const requestTitle = computed(() => `${expectedSyntaxLabel()} request`)
const submitButtonLabel = computed(() => `Submit ${expectedSyntaxLabel()} request`)
const workflowNamePlaceholder = computed(() => {
  if (props.expectedSyntaxType === 1) {
    return 'Short summary of the schema change'
  }
  return 'Short summary of the data change'
})
const pageDescription = computed(() => {
  return `Submit ${expectedSyntaxLabel()} for review with the same SQL-check and approval workflow used by the legacy deployment flow.`
})
const accessDeniedDescription = computed(() => {
  return `Your account does not currently have access to submit ${expectedSyntaxLabel()} workflows.`
})
const editorPlaceholder = computed(() => {
  return `Paste ${expectedSyntaxLabel()} here. This page is intended for ${expectedSyntaxDescription()}.`
})
const successCheckMessage = computed(() => {
  return `This check is current and the SQL is classified as ${expectedSyntaxLabel()}.`
})
const syntaxMismatchMessage = computed(() => {
  return `This page only accepts ${expectedSyntaxLabel()} SQL. The checked SQL did not resolve to a ${expectedSyntaxLabel()} workflow.`
})

async function loadSubmissionMetadata() {
  pageLoading.value = true
  pageError.value = ''

  try {
    submissionMetadata.value = await fetchWorkflowSubmissionMetadata(requireToken())
  } catch (errorValue) {
    pageError.value = toUserFacingMessage(
      errorValue,
      `Failed to load ${expectedSyntaxLabel()} submission form.`,
    )
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

function invalidateCheck() {
  checkError.value = ''
  checkedFingerprint.value = ''
}

async function runCheck(showSuccessOnWrongSyntax = false) {
  formError.value = ''
  checkError.value = ''

  if (!form.instanceId || !form.dbName || !form.sqlContent.trim()) {
    checkError.value = 'Instance, database, and SQL content are required before running SQL check.'
    return false
  }

  checking.value = true
  try {
    const result = await checkWorkflowSql(
      {
        instance_id: Number(form.instanceId),
        db_name: form.dbName,
        full_sql: form.sqlContent,
      },
      requireToken(),
    )
    checkResult.value = result
    checkedFingerprint.value = workflowFingerprint()

    if (result.syntax_type !== props.expectedSyntaxType) {
      checkError.value = syntaxMismatchMessage.value
      if (!showSuccessOnWrongSyntax) {
        return false
      }
    }

    return result.syntax_type === props.expectedSyntaxType
  } catch (errorValue) {
    checkedFingerprint.value = ''
    checkResult.value = null
    checkError.value = toUserFacingMessage(errorValue, 'SQL check failed.')
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

function resetFileInput() {
  if (sqlFileInput.value) {
    sqlFileInput.value.value = ''
  }
}

async function onSqlFileSelected(event: Event) {
  const target = event.target as HTMLInputElement | null
  const file = target?.files?.[0]
  if (!file) {
    return
  }

  parseError.value = ''
  formError.value = ''

  if (!file.name.toLowerCase().endsWith('.sql')) {
    parseError.value = 'Only .sql files are supported for workflow upload.'
    resetFileInput()
    return
  }

  const reader = new FileReader()
  reader.onload = async () => {
    const fileText = typeof reader.result === 'string' ? reader.result : ''
    parsingUpload.value = true

    try {
      const payload = await parseWorkflowSql(
        {
          text: fileText,
          db_type: selectedInstance.value?.db_type,
        },
        requireToken(),
      )

      if (payload.total === 0) {
        parseError.value = 'The uploaded file did not contain any SQL statements.'
        parsedUpload.value = null
        parsedUploadFileName.value = ''
        return
      }

      if (payload.summary.has_mixed_syntax) {
        parseError.value =
          'The uploaded file mixes DDL and DML statements. Split the file by workflow type before uploading.'
        parsedUpload.value = null
        parsedUploadFileName.value = ''
        return
      }

      if (
        payload.summary.has_unknown_syntax ||
        payload.summary.syntax_type !== props.expectedSyntaxType
      ) {
        parseError.value = uploadSyntaxMismatchMessage(payload.summary.syntax_type)
        parsedUpload.value = null
        parsedUploadFileName.value = ''
        return
      }

      parsedUpload.value = payload
      parsedUploadFileName.value = file.name
      form.sqlContent = payload.rows.map((row) => row.sql.trim()).join('\n\n')
      invalidateCheck()
    } catch (errorValue) {
      parseError.value = toUserFacingMessage(errorValue, 'Failed to parse the selected SQL file.')
    } finally {
      parsingUpload.value = false
      resetFileInput()
    }
  }
  reader.onerror = () => {
    parseError.value = 'Failed to read the selected SQL file.'
    resetFileInput()
  }
  reader.readAsText(file)
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
    const checkPassed = await runCheck(true)
    if (!checkPassed) {
      return
    }
  }

  if (checkResult.value && (checkResult.value.warning_count > 0 || checkResult.value.error_count > 0)) {
    const confirmed = window.confirm(
      `The SQL check returned ${checkResult.value.warning_count} warning(s) and ${checkResult.value.error_count} error(s). Submit anyway?`,
    )
    if (!confirmed) {
      return
    }
  }

  submitting.value = true
  try {
    const payload = await createWorkflow(
      {
        workflow: {
          workflow_name: form.workflowName.trim(),
          demand_url: form.demandUrl.trim() || undefined,
          group_id: Number(form.groupId),
          db_name: form.dbName,
          instance: Number(form.instanceId),
          is_offline_export: 0,
          run_date_start: form.runDateStart || null,
          run_date_end: form.runDateEnd || null,
        },
        sql_content: form.sqlContent,
      },
      requireToken(),
    )
    await router.replace({
      name: 'workflow-detail',
      params: { workflowId: payload.workflow.id },
    })
  } catch (errorValue) {
    formError.value = toUserFacingMessage(
      errorValue,
      `Failed to submit the ${expectedSyntaxLabel()} workflow.`,
    )
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
    invalidateCheck()
  },
)

watch(
  () => form.sqlContent,
  () => {
    invalidateCheck()
  },
)

onMounted(() => {
  void loadSubmissionMetadata()
})
</script>

<template>
  <section class="grid gap-6">
    <div class="flex flex-wrap items-start justify-between gap-3">
      <div class="space-y-1">
        <p class="text-sm font-medium uppercase tracking-[0.2em] text-slate-500">Workflow Submission</p>
        <h1 class="text-3xl font-semibold tracking-tight text-slate-900">{{ pageTitle }}</h1>
        <p class="text-sm text-slate-500">
          {{ pageDescription }}
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
        <CardDescription>Fetching groups, instances, and workflow settings.</CardDescription>
      </CardHeader>
    </Card>

    <Card v-else-if="!canCreateWorkflow" class="border-red-200">
      <CardHeader>
        <CardTitle>Access denied</CardTitle>
        <CardDescription>
          {{ accessDeniedDescription }}
        </CardDescription>
      </CardHeader>
    </Card>

    <template v-else>
      <div class="grid gap-6 xl:grid-cols-[minmax(0,1fr)_22rem]">
        <Card class="border-slate-200">
          <CardHeader>
            <CardTitle>{{ requestTitle }}</CardTitle>
            <CardDescription>
              Provide the workflow context on the right, then run SQL check before submitting.
            </CardDescription>
          </CardHeader>
          <CardContent class="space-y-5">
            <div class="flex flex-wrap items-center gap-2">
              <Button variant="outline" type="button" class="gap-2" :disabled="checking || submitting" @click="formatSql">
                Format SQL
              </Button>
              <label
                v-if="enableFileParse"
                class="inline-flex cursor-pointer items-center gap-2 rounded-md border border-slate-200 px-3 py-2 text-sm text-slate-700 transition hover:bg-slate-50"
              >
                <FileUp class="h-4 w-4" />
                {{ parsingUpload ? 'Parsing upload…' : 'Upload `.sql`' }}
                <input
                  ref="sqlFileInput"
                  accept=".sql"
                  class="hidden"
                  type="file"
                  :disabled="checking || submitting || parsingUpload"
                  @change="onSqlFileSelected"
                />
              </label>
              <Button type="button" variant="outline" class="gap-2" :disabled="checking || submitting" @click="void runCheck()">
                <RefreshCw class="h-4 w-4" />
                SQL check
              </Button>
              <Button
                type="button"
                class="gap-2"
                data-testid="workflow-submit"
                :disabled="submitting || checking || parsingUpload || !canSubmit"
                @click="void submitWorkflow()"
              >
                <Send class="h-4 w-4" />
                {{ submitButtonLabel }}
              </Button>
            </div>

            <p
              v-if="formError"
              class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700"
            >
              {{ formError }}
            </p>

            <p
              v-if="parseError"
              class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700"
            >
              {{ parseError }}
            </p>

            <div
              v-if="enableFileParse && parsedUpload"
              class="rounded-2xl border border-slate-200 bg-slate-50 p-4"
            >
              <div class="flex flex-wrap items-center justify-between gap-3">
                <div>
                  <p class="text-sm font-medium text-slate-900">Parsed upload ready</p>
                  <p class="text-sm text-slate-500">
                    {{ parsedUploadFileName }} loaded as {{ parsedUpload.total }} {{ expectedSyntaxLabel() }} statement{{ parsedUpload.total === 1 ? '' : 's' }}.
                  </p>
                </div>
                <Badge variant="outline" class="border-slate-200 bg-white text-slate-600">
                  {{ expectedSyntaxLabel() }} only
                </Badge>
              </div>
              <div class="mt-3 space-y-2">
                <div
                  v-for="statement in parsedPreviewRows"
                  :key="statement.sql_id"
                  class="rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700"
                >
                  <span class="font-medium text-slate-900">#{{ statement.sql_id }}</span>
                  {{ statement.sql.slice(0, 180) }}{{ statement.sql.length > 180 ? '…' : '' }}
                </div>
              </div>
            </div>

            <p
              v-if="checkError"
              class="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-700"
            >
              {{ checkError }}
            </p>

            <p
              v-else-if="checkResult && !isCheckFresh"
              class="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-700"
            >
              SQL, instance, or database changed after the last check. Run SQL check again before submitting.
            </p>

            <SqlCodeEditor
              v-model="form.sqlContent"
              :db-type="selectedInstance?.db_type || 'mysql'"
              :disabled="checking || submitting || parsingUpload"
              :min-height="420"
              :placeholder="editorPlaceholder"
              test-id="workflow-sql-editor"
            />
          </CardContent>
        </Card>

        <div class="grid gap-6">
          <Card class="border-slate-200">
            <CardHeader>
              <CardTitle>Request context</CardTitle>
              <CardDescription>
                Choose the resource group, instance, and database before running SQL check.
              </CardDescription>
            </CardHeader>
            <CardContent class="space-y-4">
              <div class="space-y-2">
                <label class="text-sm font-medium text-slate-700" for="workflow-name">Workflow name</label>
                <Input id="workflow-name" v-model="form.workflowName" data-testid="workflow-name" :placeholder="workflowNamePlaceholder" :disabled="submitting" />
              </div>

              <div class="space-y-2">
                <label class="text-sm font-medium text-slate-700" for="workflow-demand-url">Requirement URL</label>
                <Input id="workflow-demand-url" v-model="form.demandUrl" data-testid="workflow-demand-url" placeholder="Optional ticket or requirement link" :disabled="submitting" />
              </div>

              <div class="space-y-2">
                <label class="text-sm font-medium text-slate-700" for="workflow-group">Resource group</label>
                <select id="workflow-group" v-model="form.groupId" data-testid="workflow-group" :class="selectClass" :disabled="submitting">
                  <option value="">Select a resource group</option>
                  <option
                    v-for="group in eligibleGroups"
                    :key="group.group_id"
                    :value="group.group_id"
                  >
                    {{ group.group_name }}
                  </option>
                </select>
              </div>

              <div class="space-y-2">
                <label class="text-sm font-medium text-slate-700" for="workflow-instance">Instance</label>
                <select id="workflow-instance" v-model="form.instanceId" data-testid="workflow-instance" :class="selectClass" :disabled="!form.groupId || submitting">
                  <option value="">Select an instance</option>
                  <option
                    v-for="instance in filteredInstances"
                    :key="instance.id"
                    :value="instance.id"
                  >
                    {{ instance.instance_name }} / {{ instance.db_type.toUpperCase() }}
                  </option>
                </select>
              </div>

              <div class="space-y-2">
                <label class="text-sm font-medium text-slate-700" for="workflow-db">Database</label>
                <select id="workflow-db" v-model="form.dbName" data-testid="workflow-db" :class="selectClass" :disabled="!form.instanceId || databasesLoading || submitting">
                  <option value="">{{ databasesLoading ? 'Loading databases...' : 'Select a database' }}</option>
                  <option v-for="databaseName in availableDatabases" :key="databaseName" :value="databaseName">
                    {{ databaseName }}
                  </option>
                </select>
                <p v-if="databasesError" class="text-sm text-red-600">{{ databasesError }}</p>
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
              <div
                v-else-if="approvalLoading"
                class="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-8 text-sm text-slate-500"
              >
                Loading approval flow...
              </div>
              <div
                v-else-if="!approvalPreview"
                class="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-8 text-sm text-slate-500"
              >
                Select a resource group to preview its approval chain.
              </div>
              <template v-else>
                <p data-testid="workflow-approval-preview" class="text-sm text-slate-600">{{ approvalPreview.display }}</p>
                <div class="space-y-2">
                  <div
                    v-for="(node, index) in approvalPreview.review_info"
                    :key="`${node.group_name}-${index}`"
                    class="flex items-center justify-between rounded-2xl border border-slate-200 bg-white px-4 py-3"
                  >
                    <span class="text-sm font-medium text-slate-900">{{ node.group_name }}</span>
                    <Badge variant="outline" class="border-slate-200 bg-slate-50 text-slate-600">
                      {{ node.is_auto_pass ? 'Auto pass' : 'Approval step' }}
                    </Badge>
                  </div>
                </div>
              </template>
            </CardContent>
          </Card>
        </div>
      </div>

      <Card v-if="checkResult" class="border-slate-200">
        <CardHeader>
          <div class="flex flex-wrap items-center justify-between gap-3">
            <div>
              <CardTitle>SQL check result</CardTitle>
              <CardDescription>Review warnings, errors, and affected statements before submission.</CardDescription>
            </div>
            <div class="flex flex-wrap gap-2">
              <Badge variant="outline" class="border-slate-200 bg-slate-50 text-slate-600">
                Type: {{ syntaxTypeLabel(checkResult.syntax_type) }}
              </Badge>
              <Badge variant="outline" class="border-amber-200 bg-amber-50 text-amber-700">
                Warnings: {{ checkResult.warning_count }}
              </Badge>
              <Badge variant="outline" class="border-red-200 bg-red-50 text-red-700">
                Errors: {{ checkResult.error_count }}
              </Badge>
            </div>
          </div>
        </CardHeader>
        <CardContent class="space-y-4">
          <div
            v-if="checkResult.rows.length === 0"
            class="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-8 text-sm text-slate-500"
          >
            The SQL check did not return row-level details.
          </div>
          <div v-else class="overflow-x-auto rounded-2xl border border-slate-200">
            <table class="min-w-full divide-y divide-slate-200 text-left text-sm">
              <thead class="bg-slate-50">
                <tr>
                  <th
                    v-for="column in checkColumns"
                    :key="column"
                    class="px-4 py-3 font-medium text-slate-600"
                  >
                    {{ column }}
                  </th>
                </tr>
              </thead>
              <tbody class="divide-y divide-slate-100 bg-white">
                <tr v-for="(row, index) in checkResult.rows" :key="index">
                  <td
                    v-for="column in checkColumns"
                    :key="`${index}-${column}`"
                    class="max-w-[20rem] whitespace-pre-wrap break-words px-4 py-3 text-slate-700"
                  >
                    {{ row[column] ?? '' }}
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
          <p
            v-if="checkResult.syntax_type === expectedSyntaxType && isCheckFresh"
            class="rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700"
          >
            <CheckCircle2 class="mr-2 inline h-4 w-4" />
            {{ successCheckMessage }}
          </p>
        </CardContent>
      </Card>
    </template>
  </section>
</template>
