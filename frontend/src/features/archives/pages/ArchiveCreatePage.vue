<script setup lang="ts">
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { ArrowLeft, RefreshCw, Send } from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import {
  createArchive,
  fetchArchiveApprovalPreview,
  fetchArchiveMetadata,
  fetchInstanceResources,
  type ArchiveApprovalPreview,
  type ArchiveMetadataRecord,
  type ArchiveWeekday,
} from '../api'
import { useAuthStore } from '@/stores/auth'

const authStore = useAuthStore()
const router = useRouter()

const pageLoading = ref(false)
const approvalLoading = ref(false)
const databasesLoading = ref(false)
const tablesLoading = ref(false)
const submitting = ref(false)

const pageError = ref('')
const formError = ref('')
const approvalError = ref('')
const databasesError = ref('')
const tablesError = ref('')

const metadata = ref<ArchiveMetadataRecord | null>(null)
const approvalPreview = ref<ArchiveApprovalPreview | null>(null)
const availableDatabases = ref<string[]>([])
const availableTables = ref<string[]>([])

const form = reactive({
  title: '',
  groupId: '',
  instanceId: '',
  dbName: '',
  tableName: '',
  condition: '',
  archiveMethod: 'dml' as 'dml' | 'pt_archiver',
  executionMode: 'one_time' as 'one_time' | 'scheduled',
  scheduleFrequency: 'daily' as 'daily' | 'weekly',
  scheduleTime: '02:00',
  scheduleWeekdays: ['mon'] as ArchiveWeekday[],
})

const selectClass =
  'block h-10 w-full rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-700 shadow-sm outline-none transition focus:border-slate-400 disabled:cursor-not-allowed disabled:bg-slate-100'
const textareaClass =
  'block min-h-[9rem] w-full rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700 shadow-sm outline-none transition focus:border-slate-400 disabled:cursor-not-allowed disabled:bg-slate-100'

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

const canCreateArchives = computed(() => hasPermission('sql.archive_apply'))

const eligibleGroups = computed(() => {
  const selectedInstanceId = Number(form.instanceId)
  if (!selectedInstanceId) {
    return metadata.value?.resource_groups ?? []
  }
  const instance = metadata.value?.instances.find((record) => record.id === selectedInstanceId)
  if (!instance) {
    return metadata.value?.resource_groups ?? []
  }
  return (metadata.value?.resource_groups ?? []).filter((group) =>
    instance.group_ids.includes(group.group_id),
  )
})

const eligibleInstances = computed(() => {
  const selectedGroupId = Number(form.groupId)
  if (!selectedGroupId) {
    return metadata.value?.instances ?? []
  }
  return (metadata.value?.instances ?? []).filter((instance) =>
    instance.group_ids.includes(selectedGroupId),
  )
})

const selectedInstance = computed(() => {
  const selectedInstanceId = Number(form.instanceId)
  return (metadata.value?.instances ?? []).find((instance) => instance.id === selectedInstanceId) ?? null
})

const availableMethods = computed(() => selectedInstance.value?.available_archive_methods ?? ['dml'])

const weekdayOptions = computed(() => metadata.value?.weekdays ?? [])
const requiresWeeklyWeekdays = computed(() => form.executionMode === 'scheduled' && form.scheduleFrequency === 'weekly')

const canSubmit = computed(() => {
  const hasScheduleFields = form.executionMode === 'one_time'
    || (
      Boolean(form.scheduleTime)
      && Boolean(form.scheduleFrequency)
      && (form.scheduleFrequency !== 'weekly' || form.scheduleWeekdays.length > 0)
    )

  return (
    Boolean(form.title.trim())
    && Boolean(form.groupId)
    && Boolean(form.instanceId)
    && Boolean(form.dbName)
    && Boolean(form.tableName)
    && Boolean(form.condition.trim())
    && hasScheduleFields
  )
})

async function loadMetadata() {
  pageLoading.value = true
  pageError.value = ''

  try {
    metadata.value = await fetchArchiveMetadata(requireToken())
  } catch (errorValue) {
    pageError.value = toUserFacingMessage(errorValue, 'Failed to load archive metadata.')
  } finally {
    pageLoading.value = false
  }
}

async function loadApprovalPreview(groupId: number) {
  approvalLoading.value = true
  approvalError.value = ''

  try {
    approvalPreview.value = await fetchArchiveApprovalPreview(groupId, requireToken())
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
  availableDatabases.value = []

  try {
    const payload = await fetchInstanceResources(instanceId, 'database', requireToken())
    availableDatabases.value = payload.result.map((item) => `${item}`)
  } catch (errorValue) {
    databasesError.value = toUserFacingMessage(errorValue, 'Failed to load databases.')
  } finally {
    databasesLoading.value = false
  }
}

async function loadTables(instanceId: number, dbName: string) {
  tablesLoading.value = true
  tablesError.value = ''
  availableTables.value = []

  try {
    const payload = await fetchInstanceResources(instanceId, 'table', requireToken(), { db_name: dbName })
    availableTables.value = payload.result.map((item) => `${item}`)
  } catch (errorValue) {
    tablesError.value = toUserFacingMessage(errorValue, 'Failed to load tables.')
  } finally {
    tablesLoading.value = false
  }
}

function toggleWeekday(weekday: ArchiveWeekday) {
  if (form.scheduleWeekdays.includes(weekday)) {
    form.scheduleWeekdays = form.scheduleWeekdays.filter((value) => value !== weekday)
    return
  }
  form.scheduleWeekdays = [...form.scheduleWeekdays, weekday]
}

async function submitArchive() {
  if (!canSubmit.value) {
    formError.value = 'Complete the required fields first.'
    return
  }

  submitting.value = true
  formError.value = ''

  try {
    const result = await createArchive(
      {
        title: form.title.trim(),
        group_id: Number(form.groupId),
        instance_id: Number(form.instanceId),
        db_name: form.dbName,
        table_name: form.tableName,
        condition: form.condition.trim(),
        archive_method: form.archiveMethod,
        execution_mode: form.executionMode,
        schedule_frequency: form.executionMode === 'scheduled' ? form.scheduleFrequency : null,
        schedule_time: form.executionMode === 'scheduled' ? form.scheduleTime : null,
        schedule_weekdays:
          form.executionMode === 'scheduled' && form.scheduleFrequency === 'weekly'
            ? form.scheduleWeekdays
            : [],
      },
      requireToken(),
    )

    await router.push({
      name: 'archive-detail',
      params: { archiveId: `${result.id}` },
      query: { returnTo: '/archives' },
    })
  } catch (errorValue) {
    formError.value = toUserFacingMessage(errorValue, 'Failed to submit archive workflow.')
  } finally {
    submitting.value = false
  }
}

watch(
  () => form.groupId,
  (groupId) => {
    approvalPreview.value = null
    formError.value = ''
    if (!groupId) {
      return
    }
    void loadApprovalPreview(Number(groupId))
  },
)

watch(
  () => form.instanceId,
  (instanceId) => {
    form.dbName = ''
    form.tableName = ''
    availableDatabases.value = []
    availableTables.value = []
    databasesError.value = ''
    tablesError.value = ''

    const selectedGroupId = Number(form.groupId)
    if (
      selectedGroupId
      && !eligibleInstances.value.some((instance) => instance.id === Number(instanceId))
    ) {
      form.groupId = ''
    }

    if (!instanceId) {
      return
    }

    const instance = selectedInstance.value
    if (instance && !instance.available_archive_methods.includes(form.archiveMethod)) {
      form.archiveMethod = instance.available_archive_methods[0] ?? 'dml'
    }
    void loadDatabases(Number(instanceId))
  },
)

watch(
  () => form.dbName,
  (dbName) => {
    form.tableName = ''
    availableTables.value = []
    tablesError.value = ''
    if (!dbName || !form.instanceId) {
      return
    }
    void loadTables(Number(form.instanceId), dbName)
  },
)

onMounted(async () => {
  await authStore.loadCurrentUser()
  await loadMetadata()
})
</script>

<template>
  <section class="grid gap-6">
    <div class="flex flex-wrap items-center justify-between gap-3">
      <Button variant="outline" type="button" class="gap-2" @click="void router.push({ name: 'archives' })">
        <ArrowLeft class="h-4 w-4" />
        Back to archives
      </Button>
      <Button variant="outline" type="button" class="gap-2" @click="void loadMetadata()">
        <RefreshCw class="h-4 w-4" />
        Refresh
      </Button>
    </div>

    <Card class="border-slate-200">
      <CardHeader>
        <div class="flex flex-col gap-2 lg:flex-row lg:items-center lg:justify-between">
          <div>
            <CardTitle>New Archive</CardTitle>
            <CardDescription>
              Submit a delete-only archive workflow. No backup artifact is created by this flow.
            </CardDescription>
          </div>
          <Badge variant="outline" class="border-rose-200 bg-rose-50 text-rose-700">
            Archival means deletion
          </Badge>
        </div>
      </CardHeader>
      <CardContent class="space-y-5">
        <p
          v-if="pageError"
          class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700"
        >
          {{ pageError }}
        </p>

        <div
          v-if="!canCreateArchives"
          class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700"
        >
          Your account does not currently have permission to submit archive workflows.
        </div>

        <div
          v-else-if="pageLoading"
          class="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-10 text-sm text-slate-500"
        >
          Loading archive form...
        </div>

        <template v-else>
          <div class="grid gap-4 lg:grid-cols-2">
            <div class="space-y-2">
              <label class="text-sm font-medium text-slate-700">Title</label>
              <Input v-model="form.title" data-testid="archive-title" placeholder="Short summary of the deletion request" />
            </div>
            <div class="space-y-2">
              <label class="text-sm font-medium text-slate-700">Execution mode</label>
              <select v-model="form.executionMode" data-testid="archive-execution-mode" :class="selectClass">
                <option value="one_time">One time</option>
                <option value="scheduled">Scheduled</option>
              </select>
            </div>
            <div class="space-y-2">
              <label class="text-sm font-medium text-slate-700">Resource group</label>
              <select v-model="form.groupId" data-testid="archive-group" :class="selectClass">
                <option value="">Select group</option>
                <option
                  v-for="group in eligibleGroups"
                  :key="group.group_id"
                  :value="`${group.group_id}`"
                >
                  {{ group.group_name }}
                </option>
              </select>
            </div>
            <div class="space-y-2">
              <label class="text-sm font-medium text-slate-700">Instance</label>
              <select v-model="form.instanceId" data-testid="archive-instance" :class="selectClass">
                <option value="">Select instance</option>
                <option
                  v-for="instance in eligibleInstances"
                  :key="instance.id"
                  :value="`${instance.id}`"
                >
                  {{ instance.instance_name }} · {{ instance.db_type }}
                </option>
              </select>
            </div>
            <div class="space-y-2">
              <label class="text-sm font-medium text-slate-700">Database</label>
              <select
                v-model="form.dbName"
                data-testid="archive-db"
                :class="selectClass"
                :disabled="!form.instanceId || databasesLoading"
              >
                <option value="">Select database</option>
                <option v-for="database in availableDatabases" :key="database" :value="database">
                  {{ database }}
                </option>
              </select>
              <p v-if="databasesError" class="text-sm text-red-700">{{ databasesError }}</p>
            </div>
            <div class="space-y-2">
              <label class="text-sm font-medium text-slate-700">Table</label>
              <select
                v-model="form.tableName"
                data-testid="archive-table"
                :class="selectClass"
                :disabled="!form.dbName || tablesLoading"
              >
                <option value="">Select table</option>
                <option v-for="table in availableTables" :key="table" :value="table">
                  {{ table }}
                </option>
              </select>
              <p v-if="tablesError" class="text-sm text-red-700">{{ tablesError }}</p>
            </div>
            <div class="space-y-2">
              <label class="text-sm font-medium text-slate-700">Archive method</label>
              <select v-model="form.archiveMethod" data-testid="archive-method" :class="selectClass">
                <option
                  v-for="method in availableMethods"
                  :key="method"
                  :value="method"
                >
                  {{ method === 'pt_archiver' ? 'pt-archiver' : 'Rendered DML delete' }}
                </option>
              </select>
            </div>
          </div>

          <div
            v-if="form.executionMode === 'scheduled'"
            class="grid gap-4 rounded-2xl border border-slate-200 bg-slate-50 p-4 lg:grid-cols-3"
          >
            <div class="space-y-2">
              <label class="text-sm font-medium text-slate-700">Schedule frequency</label>
              <select v-model="form.scheduleFrequency" data-testid="archive-schedule-frequency" :class="selectClass">
                <option value="daily">Daily</option>
                <option value="weekly">Weekly</option>
              </select>
            </div>
            <div class="space-y-2">
              <label class="text-sm font-medium text-slate-700">Schedule time</label>
              <input
                v-model="form.scheduleTime"
                data-testid="archive-schedule-time"
                class="block h-10 w-full rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-700 shadow-sm outline-none transition focus:border-slate-400"
                type="time"
              >
            </div>
            <div v-if="requiresWeeklyWeekdays" class="space-y-2">
              <label class="text-sm font-medium text-slate-700">Weekdays</label>
              <div class="flex flex-wrap gap-2">
                <label
                  v-for="weekday in weekdayOptions"
                  :key="weekday.value"
                  class="inline-flex items-center gap-2 rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700"
                >
                  <input
                    :checked="form.scheduleWeekdays.includes(weekday.value)"
                    :data-testid="`archive-weekday-${weekday.value}`"
                    class="rounded border-slate-300"
                    type="checkbox"
                    @change="toggleWeekday(weekday.value)"
                  >
                  {{ weekday.label }}
                </label>
              </div>
            </div>
          </div>

          <div class="space-y-2">
            <label class="text-sm font-medium text-slate-700">Delete condition</label>
            <textarea
              v-model="form.condition"
              data-testid="archive-condition"
              :class="textareaClass"
              placeholder="Example: created_at < {{ today }} or status = 'expired'"
            />
            <p class="text-xs text-slate-500">
              Supported variables:
              <code v-pre>{{ today }}</code>,
              <code v-pre>{{ yesterday }}</code>,
              <code v-pre>{{ tomorrow }}</code>,
              <code v-pre>{{ now }}</code>.
            </p>
          </div>

          <div v-if="approvalPreview || approvalLoading || approvalError" class="rounded-2xl border border-slate-200 bg-slate-50 p-4">
            <div class="space-y-2">
              <p class="text-sm font-medium text-slate-700">Approval flow</p>
              <p v-if="approvalLoading" class="text-sm text-slate-500">Loading approval flow...</p>
              <p v-else-if="approvalError" class="text-sm text-red-700">{{ approvalError }}</p>
              <template v-else-if="approvalPreview">
                <p class="text-sm text-slate-700">{{ approvalPreview.display }}</p>
                <div class="flex flex-wrap gap-2">
                  <Badge
                    v-for="node in approvalPreview.review_info"
                    :key="node.group_name"
                    variant="outline"
                    class="border-slate-200 bg-white text-slate-700"
                  >
                    {{ node.group_name }}
                  </Badge>
                </div>
              </template>
            </div>
          </div>

          <div class="rounded-2xl border border-amber-200 bg-amber-50 p-4 text-sm text-amber-800">
            Running this workflow deletes matching data from the source table. Backup generation is intentionally disabled for this flow.
          </div>

          <p
            v-if="formError"
            class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700"
          >
            {{ formError }}
          </p>

          <div class="flex flex-wrap gap-2">
            <Button type="button" class="gap-2" :disabled="submitting || !canSubmit" @click="void submitArchive()">
              <Send class="h-4 w-4" />
              Submit archive
            </Button>
            <Button variant="outline" type="button" @click="void router.push({ name: 'archives' })">
              Cancel
            </Button>
          </div>
        </template>
      </CardContent>
    </Card>
  </section>
</template>
