<script setup lang="ts">
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { ArrowLeft, RefreshCw, ShieldCheck } from 'lucide-vue-next'

import SqlCodeEditor from '@/components/queries/SqlCodeEditor.vue'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import {
  checkWorkflowSql,
  createWorkflow,
  fetchInstanceResources,
  fetchWorkflowMetadata,
  type WorkflowCheckResult,
  type WorkflowMetadataRecord,
  type WorkflowResultRow,
} from '@/lib/api'
import { useAuthStore } from '@/stores/auth'

const authStore = useAuthStore()
const router = useRouter()

const metadataLoading = ref(false)
const databasesLoading = ref(false)
const checkLoading = ref(false)
const submitting = ref(false)

const pageError = ref('')
const formError = ref('')

const metadata = ref<WorkflowMetadataRecord | null>(null)
const databaseOptions = ref<string[]>([])
const checkResult = ref<WorkflowCheckResult | null>(null)
const lastCheckedSignature = ref('')

const form = reactive({
  workflowName: '',
  demandUrl: '',
  groupId: '',
  instanceId: '',
  dbName: '',
  isBackup: true,
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
    default:
      return 'border-slate-200 bg-slate-100 text-slate-600'
  }
}

function buildCheckSignature() {
  return JSON.stringify([
    form.instanceId,
    form.dbName,
    form.sqlContent,
  ])
}

function clearCheckState() {
  checkResult.value = null
  lastCheckedSignature.value = ''
}

const canCreateWorkflow = computed(() => hasPermission('sql.sql_submit'))
const filteredInstances = computed(() => {
  const rows = metadata.value?.instances ?? []
  const groupId = Number(form.groupId)
  if (!groupId) {
    return rows
  }
  return rows.filter((instance) =>
    instance.resource_groups.some((group) => group.group_id === groupId),
  )
})

const selectedInstance = computed(() =>
  filteredInstances.value.find((instance) => instance.id === Number(form.instanceId)) ?? null,
)
const checkColumns = computed(() => checkResult.value?.column_list ?? [])

watch(
  () => [form.groupId, form.instanceId, form.dbName, form.sqlContent],
  () => {
    clearCheckState()
  },
)

watch(
  () => form.groupId,
  () => {
    if (!form.instanceId) {
      return
    }
    const selectedInstanceId = Number(form.instanceId)
    const stillAvailable = filteredInstances.value.some((instance) => instance.id === selectedInstanceId)
    if (!stillAvailable) {
      form.instanceId = ''
      form.dbName = ''
      databaseOptions.value = []
    }
  },
)

watch(
  () => form.instanceId,
  async (instanceId) => {
    form.dbName = ''
    databaseOptions.value = []

    if (!instanceId) {
      return
    }

    databasesLoading.value = true
    formError.value = ''

    try {
      const payload = await fetchInstanceResources(
        Number(instanceId),
        'database',
        requireToken(),
      )
      databaseOptions.value = payload.result
    } catch (errorValue) {
      formError.value = toUserFacingMessage(errorValue, 'Failed to load databases for the selected instance.')
    } finally {
      databasesLoading.value = false
    }
  },
)

async function loadMetadata() {
  if (!canCreateWorkflow.value) {
    return
  }

  metadataLoading.value = true
  pageError.value = ''

  try {
    metadata.value = await fetchWorkflowMetadata(requireToken())
    if (metadata.value.allow_backup_toggle === false) {
      form.isBackup = true
    }
  } catch (errorValue) {
    pageError.value = toUserFacingMessage(errorValue, 'Failed to load workflow submission metadata.')
  } finally {
    metadataLoading.value = false
  }
}

async function runSqlCheck() {
  formError.value = ''

  if (!form.instanceId) {
    formError.value = 'Choose a target instance first.'
    return
  }
  if (!form.dbName.trim()) {
    formError.value = 'Choose a target database first.'
    return
  }
  if (!form.sqlContent.trim()) {
    formError.value = 'SQL content cannot be empty.'
    return
  }

  checkLoading.value = true

  try {
    checkResult.value = await checkWorkflowSql(
      {
        instance_id: Number(form.instanceId),
        db_name: form.dbName.trim(),
        full_sql: form.sqlContent,
      },
      requireToken(),
    )
    lastCheckedSignature.value = buildCheckSignature()
  } catch (errorValue) {
    formError.value = toUserFacingMessage(errorValue, 'Failed to run SQL check.')
  } finally {
    checkLoading.value = false
  }
}

async function submitWorkflowForm() {
  formError.value = ''

  if (!form.workflowName.trim()) {
    formError.value = 'A workflow name is required.'
    return
  }
  if (!form.groupId) {
    formError.value = 'Choose a resource group first.'
    return
  }
  if (!form.instanceId) {
    formError.value = 'Choose a target instance first.'
    return
  }
  if (!form.dbName.trim()) {
    formError.value = 'Choose a target database first.'
    return
  }
  if (!form.sqlContent.trim()) {
    formError.value = 'SQL content cannot be empty.'
    return
  }
  if (!checkResult.value || lastCheckedSignature.value !== buildCheckSignature()) {
    formError.value = 'Run SQL check after the latest SQL or target change before submitting.'
    return
  }

  submitting.value = true

  try {
    const createdWorkflow = await createWorkflow(
      {
        workflow: {
          workflow_name: form.workflowName.trim(),
          demand_url: form.demandUrl.trim() || undefined,
          group_id: Number(form.groupId),
          db_name: form.dbName.trim(),
          instance: Number(form.instanceId),
          is_offline_export: 0,
          is_backup: metadata.value?.allow_backup_toggle ? form.isBackup : undefined,
          run_date_start: form.runDateStart || null,
          run_date_end: form.runDateEnd || null,
        },
        sql_content: form.sqlContent,
      },
      requireToken(),
    )

    await router.push({
      name: 'workflows',
      query: {
        workflowId: `${createdWorkflow.workflow.id}`,
      },
    })
  } catch (errorValue) {
    formError.value = toUserFacingMessage(errorValue, 'Failed to submit the workflow.')
  } finally {
    submitting.value = false
  }
}

onMounted(async () => {
  await authStore.loadCurrentUser()
  await loadMetadata()
})
</script>

<template>
  <section class="grid gap-6">
    <div class="flex flex-col gap-4 rounded-2xl border border-slate-200 bg-white p-5 shadow-sm lg:flex-row lg:items-center lg:justify-between">
      <div class="space-y-1">
        <div class="flex items-center gap-2">
          <Button variant="ghost" type="button" class="gap-2 px-0" @click="void router.push({ name: 'workflows' })">
            <ArrowLeft class="h-4 w-4" />
            Back to workflows
          </Button>
        </div>
        <h1 class="text-2xl font-semibold text-slate-900">New SQL Workflow</h1>
        <p class="text-sm text-slate-500">
          Create a DDL or DML ticket, run SQL review, and submit it into the existing approval flow.
        </p>
      </div>
    </div>

    <Card v-if="!canCreateWorkflow" class="border-red-200">
      <CardHeader>
        <CardTitle>Access denied</CardTitle>
        <CardDescription>
          `sql.sql_submit` is required to submit SQL workflows from the SPA.
        </CardDescription>
      </CardHeader>
    </Card>

    <template v-else>
      <p
        v-if="pageError"
        class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700"
      >
        {{ pageError }}
      </p>

      <div class="grid gap-6 xl:grid-cols-[minmax(0,1.6fr)_minmax(22rem,1fr)]">
        <Card class="border-slate-200">
          <CardHeader>
            <CardTitle>SQL editor</CardTitle>
            <CardDescription>
              Paste the statement set you want reviewed. The detected syntax type will come from the SQL check result.
            </CardDescription>
          </CardHeader>
          <CardContent class="space-y-4">
            <SqlCodeEditor
              v-model="form.sqlContent"
              :db-type="selectedInstance?.db_type ?? ''"
              :min-height="460"
              placeholder="Paste DDL or DML statements here. Run SQL check before submitting."
              @submit="void runSqlCheck()"
            />
            <div class="flex flex-wrap gap-2">
              <Button type="button" class="gap-2" :disabled="checkLoading" @click="void runSqlCheck()">
                <ShieldCheck class="h-4 w-4" />
                SQL check
              </Button>
              <Button type="button" variant="outline" :disabled="checkLoading" @click="form.sqlContent = ''">
                Clear editor
              </Button>
            </div>
          </CardContent>
        </Card>

        <Card class="border-slate-200">
          <CardHeader>
            <CardTitle>Workflow details</CardTitle>
            <CardDescription>
              Choose the target, optional execution window, and submission metadata.
            </CardDescription>
          </CardHeader>
          <CardContent class="space-y-4">
            <p
              v-if="formError"
              class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700"
            >
              {{ formError }}
            </p>

            <div class="grid gap-4">
              <div class="space-y-2">
                <label class="text-sm font-medium text-slate-700" for="workflow-name">Workflow name</label>
                <Input id="workflow-name" v-model="form.workflowName" placeholder="Describe the change briefly" />
              </div>

              <div class="space-y-2">
                <label class="text-sm font-medium text-slate-700" for="workflow-demand-url">Demand URL</label>
                <Input id="workflow-demand-url" v-model="form.demandUrl" placeholder="Optional tracking link" />
              </div>

              <div class="space-y-2">
                <label class="text-sm font-medium text-slate-700" for="workflow-group">Resource group</label>
                <select id="workflow-group" v-model="form.groupId" :class="selectClass" :disabled="metadataLoading">
                  <option value="">Select group</option>
                  <option
                    v-for="group in metadata?.resource_groups ?? []"
                    :key="group.group_id"
                    :value="`${group.group_id}`"
                  >
                    {{ group.group_name }}
                  </option>
                </select>
              </div>

              <div class="space-y-2">
                <label class="text-sm font-medium text-slate-700" for="workflow-instance">Target instance</label>
                <select id="workflow-instance" v-model="form.instanceId" :class="selectClass" :disabled="metadataLoading">
                  <option value="">Select instance</option>
                  <option
                    v-for="instance in filteredInstances"
                    :key="instance.id"
                    :value="`${instance.id}`"
                  >
                    {{ instance.instance_name }} · {{ instance.db_type }}
                  </option>
                </select>
              </div>

              <div class="space-y-2">
                <label class="text-sm font-medium text-slate-700" for="workflow-db">Target database</label>
                <select id="workflow-db" v-model="form.dbName" :class="selectClass" :disabled="databasesLoading || !form.instanceId">
                  <option value="">{{ databasesLoading ? 'Loading databases...' : 'Select database' }}</option>
                  <option v-for="dbName in databaseOptions" :key="dbName" :value="dbName">
                    {{ dbName }}
                  </option>
                </select>
              </div>

              <div v-if="metadata?.allow_backup_toggle" class="space-y-2">
                <label class="text-sm font-medium text-slate-700" for="workflow-backup">Backup</label>
                <select id="workflow-backup" v-model="form.isBackup" :class="selectClass">
                  <option :value="true">Backup SQL</option>
                  <option :value="false">Do not create SQL backup</option>
                </select>
              </div>

              <div class="grid gap-4 sm:grid-cols-2">
                <div class="space-y-2">
                  <label class="text-sm font-medium text-slate-700" for="workflow-window-start">Execution window start</label>
                  <input
                    id="workflow-window-start"
                    v-model="form.runDateStart"
                    class="block h-10 w-full rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-700 shadow-sm outline-none transition focus:border-slate-400"
                    type="datetime-local"
                  >
                </div>
                <div class="space-y-2">
                  <label class="text-sm font-medium text-slate-700" for="workflow-window-end">Execution window end</label>
                  <input
                    id="workflow-window-end"
                    v-model="form.runDateEnd"
                    class="block h-10 w-full rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-700 shadow-sm outline-none transition focus:border-slate-400"
                    type="datetime-local"
                  >
                </div>
              </div>
            </div>

            <div class="flex flex-wrap gap-2 border-t border-slate-100 pt-4">
              <Button type="button" :disabled="submitting || checkLoading" @click="void submitWorkflowForm()">
                Submit workflow
              </Button>
              <Button type="button" variant="outline" class="gap-2" :disabled="metadataLoading" @click="void loadMetadata()">
                <RefreshCw class="h-4 w-4" />
                Reload metadata
              </Button>
            </div>
          </CardContent>
        </Card>
      </div>

      <Card class="border-slate-200">
        <CardHeader>
          <CardTitle>Check result</CardTitle>
          <CardDescription>
            Review warnings, errors, and detected syntax type before you submit the workflow.
          </CardDescription>
        </CardHeader>
        <CardContent class="space-y-4">
          <div
            v-if="!checkResult"
            class="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-10 text-sm text-slate-500"
          >
            Run SQL check to see syntax detection and review output.
          </div>

          <template v-else>
            <div class="flex flex-wrap items-center gap-3">
              <Badge variant="outline" :class="syntaxBadgeClass(checkResult.syntax_type)">
                {{ checkResult.syntax_type === 1 ? 'DDL detected' : checkResult.syntax_type === 2 ? 'DML detected' : 'Other syntax type' }}
              </Badge>
              <Badge variant="outline" class="border-slate-200 bg-slate-50 text-slate-600">
                {{ checkResult.warning_count }} warning(s)
              </Badge>
              <Badge variant="outline" class="border-slate-200 bg-slate-50 text-slate-600">
                {{ checkResult.error_count }} error(s)
              </Badge>
            </div>

            <div class="overflow-x-auto rounded-2xl border border-slate-200">
              <table class="min-w-full divide-y divide-slate-200 text-sm">
                <thead class="bg-slate-50 text-left text-xs uppercase tracking-wide text-slate-500">
                  <tr>
                    <th
                      v-for="column in checkColumns"
                      :key="column"
                      class="px-3 py-2 font-medium"
                    >
                      {{ column }}
                    </th>
                  </tr>
                </thead>
                <tbody class="divide-y divide-slate-100 bg-white">
                  <tr
                    v-for="(row, rowIndex) in checkResult.rows"
                    :key="`check-${rowIndex}`"
                  >
                    <td
                      v-for="column in checkColumns"
                      :key="`${rowIndex}-${column}`"
                      class="max-w-[24rem] px-3 py-2 align-top text-slate-700"
                    >
                      {{ stringifyCellValue((row as WorkflowResultRow)[column]) }}
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </template>
        </CardContent>
      </Card>
    </template>
  </section>
</template>
