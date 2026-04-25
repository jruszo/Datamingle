<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { LockKeyhole, RefreshCw, ServerCrash, SquareActivity, Table2, Trash2 } from 'lucide-vue-next'

import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import {
  fetchInstanceOperationDiagnosticInstances,
  fetchInstanceOperationDiagnosticLocks,
  fetchInstanceOperationDiagnosticProcesses,
  fetchInstanceOperationDiagnosticTablespace,
  fetchInstanceOperationDiagnosticTransactions,
  killInstanceOperationDiagnosticSessions,
  previewInstanceOperationDiagnosticKill,
  type InstanceOperationDiagnosticInstance,
  type InstanceOperationDiagnosticRow,
} from '../api'
import { useAuthStore } from '@/stores/auth'

type DiagnosticTab = 'processes' | 'tablespace' | 'transactions' | 'locks'

const authStore = useAuthStore()

const instances = ref<InstanceOperationDiagnosticInstance[]>([])
const selectedInstanceId = ref<number | null>(null)
const activeTab = ref<DiagnosticTab>('processes')
const commandType = ref('All')
const rows = ref<InstanceOperationDiagnosticRow[]>([])
const selectedThreadIds = ref<number[]>([])
const killSql = ref('')

const loadingInstances = ref(false)
const loadingRows = ref(false)
const submitting = ref(false)
const error = ref('')
const feedback = ref('')

const selectClass =
  'block h-10 w-full rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-700 shadow-sm outline-none transition focus:border-slate-400'

const tabs: Array<{ id: DiagnosticTab; label: string }> = [
  { id: 'processes', label: 'Processes' },
  { id: 'tablespace', label: 'Tablespace' },
  { id: 'transactions', label: 'Transactions' },
  { id: 'locks', label: 'Locks' },
]

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

const canOpenDiagnostics = computed(() => hasPermission('sql.menu_dbdiagnostic'))
const canViewProcesses = computed(() => hasPermission('sql.process_view'))
const canKillProcesses = computed(() => hasPermission('sql.process_kill'))
const canViewTablespace = computed(() => hasPermission('sql.tablespace_view'))
const canViewTransactions = computed(() => hasPermission('sql.trx_view'))
const canViewLocks = computed(() => hasPermission('sql.trxandlocks_view'))

const selectedInstance = computed(() =>
  instances.value.find((instance) => instance.id === selectedInstanceId.value) ?? null,
)

const visibleTabs = computed(() =>
  tabs.filter((tab) => {
    if (tab.id === 'processes') {
      return canViewProcesses.value
    }
    if (tab.id === 'tablespace') {
      return canViewTablespace.value
    }
    if (tab.id === 'transactions') {
      return canViewTransactions.value
    }
    return canViewLocks.value
  }),
)

const columns = computed(() => {
  const firstRow = rows.value[0]
  return firstRow ? Object.keys(firstRow).slice(0, 10) : []
})

function rowValue(row: InstanceOperationDiagnosticRow, column: string) {
  const value = row[column]
  if (value === null || value === undefined || value === '') {
    return '-'
  }
  return typeof value === 'object' ? JSON.stringify(value) : `${value}`
}

function processId(row: InstanceOperationDiagnosticRow) {
  const value = row.id ?? row.ID ?? row.thread_id ?? row.ThreadID
  const numericValue = Number(value)
  return Number.isFinite(numericValue) ? numericValue : null
}

function toggleThread(row: InstanceOperationDiagnosticRow) {
  const id = processId(row)
  if (id == null) {
    return
  }
  selectedThreadIds.value = selectedThreadIds.value.includes(id)
    ? selectedThreadIds.value.filter((threadId) => threadId !== id)
    : [...selectedThreadIds.value, id]
}

async function loadInstances() {
  loadingInstances.value = true
  error.value = ''

  try {
    instances.value = await fetchInstanceOperationDiagnosticInstances(requireToken())
    const firstInstance = instances.value[0]
    if (!selectedInstanceId.value && firstInstance) {
      selectedInstanceId.value = firstInstance.id
    }
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to load diagnostic instances.')
  } finally {
    loadingInstances.value = false
  }
}

async function loadRows() {
  if (!selectedInstanceId.value) {
    rows.value = []
    return
  }

  loadingRows.value = true
  error.value = ''
  feedback.value = ''
  selectedThreadIds.value = []
  killSql.value = ''

  try {
    if (activeTab.value === 'processes') {
      rows.value = (await fetchInstanceOperationDiagnosticProcesses(requireToken(), {
        instance_id: selectedInstanceId.value,
        command_type: commandType.value,
      })).results
    } else if (activeTab.value === 'tablespace') {
      rows.value = (await fetchInstanceOperationDiagnosticTablespace(requireToken(), {
        instance_id: selectedInstanceId.value,
        size: 25,
      })).results
    } else if (activeTab.value === 'transactions') {
      rows.value = (await fetchInstanceOperationDiagnosticTransactions(requireToken(), selectedInstanceId.value)).results
    } else {
      rows.value = (await fetchInstanceOperationDiagnosticLocks(requireToken(), selectedInstanceId.value)).results
    }
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to load diagnostics.')
  } finally {
    loadingRows.value = false
  }
}

async function previewKill() {
  if (!selectedInstanceId.value || selectedThreadIds.value.length === 0) {
    error.value = 'Select at least one process.'
    return
  }

  submitting.value = true
  error.value = ''
  feedback.value = ''

  try {
    const result = await previewInstanceOperationDiagnosticKill(
      { instance_id: selectedInstanceId.value, thread_ids: selectedThreadIds.value },
      requireToken(),
    )
    killSql.value = result.kill_sql
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to build kill command.')
  } finally {
    submitting.value = false
  }
}

async function confirmKill() {
  if (!selectedInstanceId.value || selectedThreadIds.value.length === 0 || !killSql.value) {
    return
  }
  if (!window.confirm(`Kill ${selectedThreadIds.value.length} selected session(s)?`)) {
    return
  }

  submitting.value = true
  error.value = ''
  feedback.value = ''

  try {
    await killInstanceOperationDiagnosticSessions(
      { instance_id: selectedInstanceId.value, thread_ids: selectedThreadIds.value },
      requireToken(),
    )
    await loadRows()
    feedback.value = 'Selected sessions terminated.'
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to kill sessions.')
  } finally {
    submitting.value = false
  }
}

onMounted(async () => {
  await authStore.loadCurrentUser()

  if (!canOpenDiagnostics.value) {
    error.value = 'You do not have permission to open session diagnostics.'
    return
  }

  const firstVisibleTab = visibleTabs.value[0]
  if (!firstVisibleTab) {
    error.value = 'You do not have permission to view diagnostic data.'
    return
  }
  activeTab.value = firstVisibleTab.id
  await loadInstances()
})

watch([selectedInstanceId, activeTab, commandType], () => {
  void loadRows()
})
</script>

<template>
  <section class="grid gap-6">
    <div class="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
      <div class="space-y-1">
        <div class="flex flex-wrap items-center gap-2">
          <RouterLink to="/instance-operations/databases" class="text-sm font-medium text-slate-500 hover:text-slate-900">
            Databases
          </RouterLink>
          <span class="text-slate-300">/</span>
          <RouterLink to="/instance-operations/accounts" class="text-sm font-medium text-slate-500 hover:text-slate-900">
            Accounts
          </RouterLink>
          <span class="text-slate-300">/</span>
          <RouterLink to="/instance-operations/parameters" class="text-sm font-medium text-slate-500 hover:text-slate-900">
            Parameters
          </RouterLink>
          <span class="text-slate-300">/</span>
          <span class="text-sm font-semibold text-slate-900">Diagnostics</span>
        </div>
        <h2 class="text-2xl font-semibold text-slate-900">Session Diagnostics</h2>
        <p class="text-sm text-slate-600">
          Inspect active sessions, storage, transactions, and lock waits for operational triage.
        </p>
      </div>
      <Button variant="outline" type="button" class="gap-2" :disabled="loadingRows" @click="void loadRows()">
        <RefreshCw class="h-4 w-4" />
        Refresh
      </Button>
    </div>

    <p v-if="error" class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
      {{ error }}
    </p>
    <p
      v-else-if="feedback"
      class="rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700"
    >
      {{ feedback }}
    </p>

    <Card class="border-slate-200">
      <CardHeader>
        <CardTitle>Scope</CardTitle>
        <CardDescription>Select an instance and diagnostic view.</CardDescription>
      </CardHeader>
      <CardContent>
        <div class="grid gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(0,0.75fr)_minmax(0,0.65fr)]">
          <label class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">Instance</span>
            <select v-model.number="selectedInstanceId" :class="selectClass" :disabled="loadingInstances">
              <option v-if="instances.length === 0" :value="null">No available instances</option>
              <option v-for="instance in instances" :key="instance.id" :value="instance.id">
                {{ instance.label }}
              </option>
            </select>
          </label>
          <label class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">View</span>
            <select v-model="activeTab" :class="selectClass">
              <option v-for="tab in visibleTabs" :key="tab.id" :value="tab.id">
                {{ tab.label }}
              </option>
            </select>
          </label>
          <label v-if="activeTab === 'processes'" class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">Command</span>
            <select v-model="commandType" :class="selectClass">
              <option>All</option>
              <option>Query</option>
              <option>Sleep</option>
              <option>Not Sleep</option>
            </select>
          </label>
        </div>
      </CardContent>
    </Card>

    <div class="grid gap-6 xl:grid-cols-[minmax(0,1fr)_minmax(20rem,0.32fr)]">
      <Card class="border-slate-200">
        <CardHeader>
          <CardTitle class="flex items-center gap-2">
            <SquareActivity v-if="activeTab === 'processes'" class="h-5 w-5" />
            <Table2 v-else-if="activeTab === 'tablespace'" class="h-5 w-5" />
            <ServerCrash v-else-if="activeTab === 'transactions'" class="h-5 w-5" />
            <LockKeyhole v-else class="h-5 w-5" />
            {{ tabs.find((tab) => tab.id === activeTab)?.label }}
          </CardTitle>
          <CardDescription>
            {{ rows.length }} rows for {{ selectedInstance?.instance_name || 'no instance' }}
          </CardDescription>
        </CardHeader>
        <CardContent class="p-0">
          <div v-if="loadingRows" class="p-6 text-sm text-slate-500">
            Loading diagnostics...
          </div>
          <div v-else-if="rows.length === 0" class="p-6 text-sm text-slate-500">
            No diagnostic rows returned.
          </div>
          <div v-else class="overflow-x-auto">
            <table class="min-w-full divide-y divide-slate-200 text-sm">
              <thead class="bg-slate-50">
                <tr>
                  <th v-if="activeTab === 'processes' && canKillProcesses" class="w-12 px-4 py-3"></th>
                  <th v-for="column in columns" :key="column" class="px-4 py-3 text-left font-medium text-slate-600">
                    {{ column }}
                  </th>
                </tr>
              </thead>
              <tbody class="divide-y divide-slate-100 bg-white">
                <tr v-for="(row, index) in rows" :key="index">
                  <td v-if="activeTab === 'processes' && canKillProcesses" class="px-4 py-3">
                    <input
                      type="checkbox"
                      class="h-4 w-4 rounded border-slate-300"
                      :aria-label="`Select process ${processId(row) ?? 'unknown'}`"
                      :checked="processId(row) ? selectedThreadIds.includes(processId(row) as number) : false"
                      :disabled="!processId(row)"
                      @change="toggleThread(row)"
                    >
                  </td>
                  <td v-for="column in columns" :key="column" class="max-w-[18rem] truncate px-4 py-3 text-slate-600">
                    {{ rowValue(row, column) }}
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>

      <Card class="border-slate-200">
        <CardHeader>
          <CardTitle>Actions</CardTitle>
          <CardDescription>Use kill actions only after reviewing the selected process IDs.</CardDescription>
        </CardHeader>
        <CardContent class="grid gap-4">
          <div v-if="activeTab !== 'processes'" class="text-sm text-slate-500">
            Session termination is available from the Processes view.
          </div>
          <template v-else>
            <div class="rounded-md border border-slate-200 p-3 text-sm text-slate-600">
              {{ selectedThreadIds.length }} process{{ selectedThreadIds.length === 1 ? '' : 'es' }} selected.
            </div>
            <Button variant="outline" type="button" class="gap-2" :disabled="!canKillProcesses || selectedThreadIds.length === 0 || submitting" @click="void previewKill()">
              <Trash2 class="h-4 w-4" />
              Preview kill
            </Button>
            <div v-if="killSql" class="rounded-md border border-slate-200 bg-slate-50 p-3 font-mono text-xs text-slate-700">
              {{ killSql }}
            </div>
            <Button variant="destructive" type="button" class="gap-2" :disabled="!killSql || submitting" @click="void confirmKill()">
              <Trash2 class="h-4 w-4" />
              Kill sessions
            </Button>
          </template>
        </CardContent>
      </Card>
    </div>
  </section>
</template>
