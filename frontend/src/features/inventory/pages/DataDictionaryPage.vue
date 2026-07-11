<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref, watch } from 'vue'
import {
  BookOpen,
  Database,
  Download,
  Edit3,
  Plus,
  RefreshCw,
  Save,
  Table2,
  X,
} from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import {
  exportDataDictionary,
  fetchDataDictionaryDatabases,
  fetchDataDictionaryInstances,
  fetchDataDictionaryTableDetail,
  fetchDataDictionaryTables,
  type DataDictionaryInstance,
  type DataDictionaryResultSet,
  type DataDictionaryTableDetail,
  type DataDictionaryTableGroup,
} from '../api'
import {
  createInstanceOperationDatabase,
  fetchInstanceOperationDatabaseInstances,
  fetchInstanceOperationDatabases,
  updateInstanceOperationDatabase,
  type InstanceOperationDatabaseRecord,
} from '@/features/instance-operations/api'
import { useAuthStore } from '@/stores/auth'

type FormMode = 'create' | 'edit'

type ScopedInstance = DataDictionaryInstance & {
  canManageDatabases: boolean
  canViewDictionary: boolean
}

type DatabaseRow = InstanceOperationDatabaseRecord & {
  dictionaryVisible: boolean
}

const authStore = useAuthStore()

const instances = ref<ScopedInstance[]>([])
const databaseRows = ref<DatabaseRow[]>([])
const tableGroups = ref<DataDictionaryTableGroup[]>([])
const tableDetail = ref<DataDictionaryTableDetail | null>(null)

const selectedInstanceId = ref<number | null>(null)
const selectedDbName = ref('')
const selectedTableName = ref('')
const databaseSearch = ref('')
const tableSearch = ref('')
let databaseSearchTimer: ReturnType<typeof setTimeout> | null = null
let tableSearchTimer: ReturnType<typeof setTimeout> | null = null
let databaseRequestSequence = 0
let tableRequestSequence = 0

const loadingInstances = ref(false)
const loadingDatabases = ref(false)
const loadingTables = ref(false)
const loadingDetail = ref(false)
const exporting = ref(false)
const refreshing = ref(false)
const submitting = ref(false)

const error = ref('')
const feedback = ref('')
const activeFormMode = ref<FormMode | null>(null)

const form = reactive({
  dbName: '',
  owner: '',
  remark: '',
})

const selectClass =
  'block h-10 w-full rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-700 shadow-sm outline-none transition focus:border-slate-400'

function hasPermission(permission: string) {
  if (authStore.currentUser?.is_superuser) {
    return true
  }
  return authStore.currentUser?.permissions?.includes(permission) ?? false
}

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

const canViewDataDictionary = computed(() => hasPermission('sql.menu_data_dictionary'))
const canManageDatabases = computed(() => hasPermission('sql.menu_database'))
const canExportDataDictionary = computed(() => hasPermission('sql.data_dictionary_export'))
const canUseMergedWorkspace = computed(
  () => canViewDataDictionary.value || canManageDatabases.value,
)

const selectedInstance = computed(
  () => instances.value.find((instance) => instance.id === selectedInstanceId.value) ?? null,
)

const selectedDatabase = computed(
  () =>
    databaseRows.value.find((databaseRecord) => databaseRecord.db_name === selectedDbName.value) ??
    null,
)

const canBrowseSelectedDatabase = computed(
  () =>
    canViewDataDictionary.value &&
    selectedInstance.value?.canViewDictionary === true &&
    selectedDatabase.value?.dictionaryVisible === true,
)

function normalizeRows(resultSet: DataDictionaryResultSet | null | undefined) {
  const rows = resultSet?.rows
  if (!Array.isArray(rows)) {
    return rows === undefined || rows === null ? [] : [[rows]]
  }

  if (rows.length === 0) {
    return []
  }

  if (Array.isArray(rows[0])) {
    return rows as unknown[][]
  }

  return [rows]
}

function cellValue(value: unknown) {
  if (value === null || value === undefined || value === '') {
    return '-'
  }
  if (Array.isArray(value) || typeof value === 'object') {
    return JSON.stringify(value)
  }
  return `${value}`
}

function createSqlText(detail: DataDictionaryTableDetail) {
  const createSql = detail.create_sql
  if (!Array.isArray(createSql) || createSql.length === 0) {
    return ''
  }

  const firstRow = createSql[0]
  if (Array.isArray(firstRow)) {
    return firstRow.map(cellValue).join('\n\n')
  }
  return cellValue(firstRow)
}

function resetDatabaseSelection() {
  selectedDbName.value = ''
  selectedTableName.value = ''
  tableGroups.value = []
  tableDetail.value = null
}

function resetForm() {
  form.dbName = ''
  form.owner = ''
  form.remark = ''
}

function closeForm() {
  resetForm()
  activeFormMode.value = null
}

function openCreateForm() {
  if (!canManageDatabases.value || selectedInstance.value?.canManageDatabases !== true) {
    return
  }

  resetForm()
  activeFormMode.value = 'create'
  feedback.value = ''
}

function openEditForm(databaseRecord: DatabaseRow) {
  if (!canManageDatabases.value || selectedInstance.value?.canManageDatabases !== true) {
    return
  }

  form.dbName = databaseRecord.db_name
  form.owner = databaseRecord.owner ?? ''
  form.remark = databaseRecord.remark ?? ''
  activeFormMode.value = 'edit'
  feedback.value = ''
}

function mergeInstance(
  instancesById: Map<number, ScopedInstance>,
  instance: DataDictionaryInstance,
  capability: 'canManageDatabases' | 'canViewDictionary',
) {
  const existingInstance = instancesById.get(instance.id)
  if (existingInstance) {
    existingInstance[capability] = true
    return
  }

  instancesById.set(instance.id, {
    ...instance,
    canManageDatabases: capability === 'canManageDatabases',
    canViewDictionary: capability === 'canViewDictionary',
  })
}

async function loadInstances() {
  loadingInstances.value = true
  error.value = ''

  const instancesById = new Map<number, ScopedInstance>()
  const failures: string[] = []

  try {
    if (canViewDataDictionary.value) {
      try {
        const dictionaryInstances = await fetchDataDictionaryInstances(requireToken())
        dictionaryInstances.forEach((instance) =>
          mergeInstance(instancesById, instance, 'canViewDictionary'),
        )
      } catch (errorValue) {
        failures.push(toUserFacingMessage(errorValue, 'Failed to load data dictionary instances.'))
      }
    }

    if (canManageDatabases.value) {
      try {
        const manageableInstances = await fetchInstanceOperationDatabaseInstances(requireToken())
        manageableInstances.forEach((instance) =>
          mergeInstance(instancesById, instance, 'canManageDatabases'),
        )
      } catch (errorValue) {
        failures.push(
          toUserFacingMessage(errorValue, 'Failed to load database-management instances.'),
        )
      }
    }

    instances.value = Array.from(instancesById.values()).sort((left, right) =>
      left.label.localeCompare(right.label),
    )

    if (
      !selectedInstanceId.value ||
      !instances.value.some((instance) => instance.id === selectedInstanceId.value)
    ) {
      selectedInstanceId.value = instances.value[0]?.id ?? null
    }

    if (instances.value.length === 0 && failures.length > 0) {
      error.value = failures[0] ?? 'Failed to load instances.'
    }
  } finally {
    loadingInstances.value = false
  }
}

async function loadDatabases() {
  const instance = selectedInstance.value
  if (!instance) {
    databaseRows.value = []
    resetDatabaseSelection()
    return
  }

  loadingDatabases.value = true
  error.value = ''
  closeForm()
  resetDatabaseSelection()

  const rowsByName = new Map<string, DatabaseRow>()
  const failures: string[] = []
  const requestSequence = ++databaseRequestSequence
  const instanceId = instance.id

  try {
    if (canManageDatabases.value && instance.canManageDatabases) {
      try {
        const response = await fetchInstanceOperationDatabases(requireToken(), {
          instance_id: instance.id,
          search: databaseSearch.value,
        })
        response.results.forEach((databaseRecord) => {
          rowsByName.set(databaseRecord.db_name, {
            ...databaseRecord,
            dictionaryVisible: false,
          })
        })
      } catch (errorValue) {
        failures.push(toUserFacingMessage(errorValue, 'Failed to load database metadata.'))
      }
    }

    if (canViewDataDictionary.value && instance.canViewDictionary) {
      try {
        const response = await fetchDataDictionaryDatabases(
          instance.id,
          requireToken(),
          databaseSearch.value,
        )
        response.result.forEach((databaseName) => {
          const existingDatabase = rowsByName.get(databaseName)
          rowsByName.set(databaseName, {
            ...(existingDatabase ?? {
              db_name: databaseName,
              saved: false,
            }),
            dictionaryVisible: true,
          })
        })
      } catch (errorValue) {
        failures.push(toUserFacingMessage(errorValue, 'Failed to load data dictionary databases.'))
      }
    }

    if (requestSequence !== databaseRequestSequence || selectedInstance.value?.id !== instanceId) {
      return
    }
    databaseRows.value = Array.from(rowsByName.values())
    selectedDbName.value = databaseRows.value[0]?.db_name ?? ''

    if (databaseRows.value.length === 0 && failures.length > 0) {
      error.value = failures[0] ?? 'Failed to load databases.'
    }

    if (canBrowseSelectedDatabase.value) {
      await loadTables()
    }
  } finally {
    if (requestSequence === databaseRequestSequence) loadingDatabases.value = false
  }
}

async function loadTables() {
  if (!selectedInstance.value || !selectedDbName.value || !canBrowseSelectedDatabase.value) {
    tableGroups.value = []
    selectedTableName.value = ''
    tableDetail.value = null
    return
  }

  loadingTables.value = true
  error.value = ''
  selectedTableName.value = ''
  tableDetail.value = null
  const requestSequence = ++tableRequestSequence
  const instanceId = selectedInstance.value.id
  const databaseName = selectedDbName.value

  try {
    const response = await fetchDataDictionaryTables(
      instanceId,
      databaseName,
      requireToken(),
      tableSearch.value,
    )
    if (
      requestSequence === tableRequestSequence &&
      selectedInstance.value?.id === instanceId &&
      selectedDbName.value === databaseName
    ) {
      tableGroups.value = response.result
    }
  } catch (errorValue) {
    if (requestSequence === tableRequestSequence) {
      error.value = toUserFacingMessage(errorValue, 'Failed to load tables.')
    }
  } finally {
    if (requestSequence === tableRequestSequence) loadingTables.value = false
  }
}

async function loadTableDetail(tableName: string) {
  if (!selectedInstance.value || !selectedDbName.value || !canBrowseSelectedDatabase.value) {
    return
  }

  loadingDetail.value = true
  error.value = ''
  selectedTableName.value = tableName

  try {
    tableDetail.value = await fetchDataDictionaryTableDetail(
      selectedInstance.value.id,
      selectedDbName.value,
      tableName,
      requireToken(),
    )
  } catch (errorValue) {
    tableDetail.value = null
    error.value = toUserFacingMessage(errorValue, 'Failed to load table detail.')
  } finally {
    loadingDetail.value = false
  }
}

async function refreshDataDictionary() {
  feedback.value = ''
  refreshing.value = true
  try {
    await loadInstances()
    await loadDatabases()
  } finally {
    refreshing.value = false
  }
}

function selectDatabase(databaseName: string) {
  if (selectedDbName.value === databaseName) {
    return
  }

  selectedDbName.value = databaseName
}

async function exportSelectedDictionary() {
  if (
    !selectedInstance.value ||
    !canExportDataDictionary.value ||
    !canBrowseSelectedDatabase.value
  ) {
    return
  }

  exporting.value = true
  error.value = ''
  feedback.value = ''

  try {
    const result = await exportDataDictionary(
      selectedInstance.value.id,
      selectedDbName.value,
      requireToken(),
    )

    if (result.mode === 'message') {
      feedback.value = result.detail
      return
    }

    const objectUrl = window.URL.createObjectURL(result.data)
    const anchor = document.createElement('a')
    anchor.href = objectUrl
    anchor.download = result.filename
    document.body.appendChild(anchor)
    anchor.click()
    anchor.remove()
    setTimeout(() => {
      window.URL.revokeObjectURL(objectUrl)
    }, 60000)
    feedback.value = 'Data dictionary export prepared.'
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to export data dictionary.')
  } finally {
    exporting.value = false
  }
}

async function submitDatabaseForm() {
  if (!selectedInstance.value || !activeFormMode.value) {
    return
  }
  if (!form.dbName.trim()) {
    error.value = 'Database name is required.'
    return
  }

  submitting.value = true
  error.value = ''
  feedback.value = ''

  const payload = {
    instance_id: selectedInstance.value.id,
    db_name: form.dbName.trim(),
    owner: form.owner.trim(),
    remark: form.remark.trim(),
  }

  try {
    if (activeFormMode.value === 'create') {
      await createInstanceOperationDatabase(payload, requireToken())
      feedback.value = `Database "${payload.db_name}" created.`
    } else {
      await updateInstanceOperationDatabase(payload, requireToken())
      feedback.value = `Database "${payload.db_name}" metadata updated.`
    }
    closeForm()
    await loadDatabases()
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to save database metadata.')
  } finally {
    submitting.value = false
  }
}

onMounted(async () => {
  try {
    await authStore.loadCurrentUser()
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to load the current user.')
    return
  }

  if (!canUseMergedWorkspace.value) {
    error.value = 'You do not have permission to access database metadata.'
    return
  }

  await refreshDataDictionary()
})

watch(selectedInstanceId, () => {
  if (!canUseMergedWorkspace.value || refreshing.value) {
    return
  }
  void loadDatabases()
})

watch(selectedDbName, () => {
  if (!canUseMergedWorkspace.value || loadingDatabases.value || refreshing.value) {
    return
  }
  void loadTables()
})

watch(databaseSearch, () => {
  if (!canUseMergedWorkspace.value || refreshing.value) return
  if (databaseSearchTimer) clearTimeout(databaseSearchTimer)
  databaseSearchTimer = setTimeout(() => void loadDatabases(), 300)
})

watch(tableSearch, () => {
  if (!canUseMergedWorkspace.value || loadingDatabases.value || refreshing.value) return
  if (tableSearchTimer) clearTimeout(tableSearchTimer)
  tableSearchTimer = setTimeout(() => void loadTables(), 300)
})

onBeforeUnmount(() => {
  if (databaseSearchTimer) clearTimeout(databaseSearchTimer)
  if (tableSearchTimer) clearTimeout(tableSearchTimer)
})
</script>

<template>
  <section class="grid gap-6">
    <div class="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
      <div class="space-y-1">
        <h2 class="text-2xl font-semibold text-slate-900">Data Dictionary</h2>
        <p class="text-sm text-slate-600">
          Browse database metadata, table structure, ownership, and dictionary exports in one place.
        </p>
      </div>
      <div class="flex flex-wrap gap-2">
        <Button
          variant="outline"
          type="button"
          class="gap-2"
          :disabled="refreshing || loadingInstances || loadingDatabases"
          @click="void refreshDataDictionary()"
        >
          <RefreshCw class="h-4 w-4" />
          Refresh
        </Button>
        <Button
          v-if="canExportDataDictionary"
          type="button"
          class="gap-2"
          :disabled="exporting || !canBrowseSelectedDatabase"
          @click="void exportSelectedDictionary()"
        >
          <Download class="h-4 w-4" />
          {{ exporting ? 'Exporting...' : 'Export' }}
        </Button>
        <Button
          v-if="canManageDatabases"
          type="button"
          class="gap-2"
          :disabled="!selectedInstance || !selectedInstance.canManageDatabases"
          @click="openCreateForm"
        >
          <Plus class="h-4 w-4" />
          New database
        </Button>
      </div>
    </div>

    <p
      v-if="error"
      class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700"
    >
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
        <CardDescription
          >Select an instance before opening database and table metadata.</CardDescription
        >
      </CardHeader>
      <CardContent>
        <div class="grid gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(0,0.6fr)_minmax(0,1fr)]">
          <label class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">Instance</span>
            <select
              v-model.number="selectedInstanceId"
              :class="selectClass"
              :disabled="loadingInstances"
            >
              <option v-if="instances.length === 0" :value="null">No available instances</option>
              <option v-for="instance in instances" :key="instance.id" :value="instance.id">
                {{ instance.label }}
              </option>
            </select>
          </label>
          <div class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">Engine</span>
            <div
              class="flex h-10 items-center rounded-md border border-slate-200 bg-slate-50 px-3 text-sm text-slate-700"
            >
              {{ selectedInstance?.db_type || '-' }}
            </div>
          </div>
          <label class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">Search</span>
            <Input v-model="databaseSearch" placeholder="Database, owner, or remark" />
          </label>
        </div>
      </CardContent>
    </Card>

    <div
      class="grid gap-6 xl:grid-cols-[minmax(17rem,0.3fr)_minmax(17rem,0.28fr)_minmax(0,0.42fr)]"
    >
      <div class="grid content-start gap-6">
        <Card class="border-slate-200">
          <CardHeader>
            <CardTitle class="flex items-center gap-2">
              <Database class="h-5 w-5" />
              Databases
            </CardTitle>
            <CardDescription>
              {{ databaseRows.length }} shown for
              {{ selectedInstance?.instance_name || 'no instance' }}
            </CardDescription>
          </CardHeader>
          <CardContent class="space-y-3">
            <div
              v-if="loadingDatabases"
              class="rounded-md border border-slate-200 p-4 text-sm text-slate-500"
            >
              Loading databases...
            </div>
            <div
              v-else-if="databaseRows.length === 0"
              class="rounded-md border border-slate-200 p-4 text-sm text-slate-500"
            >
              No databases match the current filters.
            </div>
            <div v-else class="max-h-[44rem] space-y-2 overflow-y-auto pr-1">
              <div
                v-for="databaseRecord in databaseRows"
                :key="databaseRecord.db_name"
                class="grid grid-cols-[minmax(0,1fr)_auto] gap-2 rounded-md border p-2"
                :class="
                  selectedDbName === databaseRecord.db_name
                    ? 'border-slate-400 bg-slate-50'
                    : 'border-slate-200 bg-white'
                "
              >
                <button
                  type="button"
                  class="min-w-0 text-left"
                  @click="selectDatabase(databaseRecord.db_name)"
                >
                  <span class="flex min-w-0 flex-wrap items-center gap-2">
                    <span class="truncate text-sm font-medium text-slate-900">{{
                      databaseRecord.db_name
                    }}</span>
                    <Badge v-if="databaseRecord.saved" variant="outline">Saved</Badge>
                    <Badge
                      v-if="!databaseRecord.dictionaryVisible && canViewDataDictionary"
                      variant="secondary"
                      >No dictionary</Badge
                    >
                  </span>
                  <span class="mt-1 block truncate text-xs text-slate-500">
                    {{ databaseRecord.owner_display || databaseRecord.owner || '-' }}
                    <span v-if="databaseRecord.remark"> - {{ databaseRecord.remark }}</span>
                  </span>
                  <span class="mt-1 block text-xs text-slate-500">
                    Rows {{ cellValue(databaseRecord.table_rows) }} · Data
                    {{ cellValue(databaseRecord.data_total) }}
                  </span>
                </button>
                <Button
                  v-if="canManageDatabases && selectedInstance?.canManageDatabases"
                  variant="outline"
                  type="button"
                  size="sm"
                  class="gap-2 self-start"
                  @click="openEditForm(databaseRecord)"
                >
                  <Edit3 class="h-4 w-4" />
                  Edit
                </Button>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      <Card class="border-slate-200">
        <CardHeader>
          <CardTitle class="flex items-center gap-2">
            <BookOpen class="h-5 w-5" />
            Tables
          </CardTitle>
          <CardDescription
            >{{
              tableGroups.reduce((total, group) => total + group.tables.length, 0)
            }}
            tables</CardDescription
          >
        </CardHeader>
        <CardContent class="space-y-4">
          <Input
            v-model="tableSearch"
            placeholder="Search tables or comments"
            :disabled="!canBrowseSelectedDatabase"
          />

          <div
            v-if="!canViewDataDictionary"
            class="rounded-md border border-slate-200 p-4 text-sm text-slate-500"
          >
            Data dictionary browsing requires Data Dictionary permission.
          </div>
          <div
            v-else-if="selectedDbName && !canBrowseSelectedDatabase"
            class="rounded-md border border-slate-200 p-4 text-sm text-slate-500"
          >
            No table dictionary metadata is available for this database.
          </div>
          <div
            v-else-if="loadingTables"
            class="rounded-md border border-slate-200 p-4 text-sm text-slate-500"
          >
            Loading tables...
          </div>
          <div
            v-else-if="tableGroups.length === 0"
            class="rounded-md border border-slate-200 p-4 text-sm text-slate-500"
          >
            No tables found.
          </div>
          <div v-else class="max-h-[48rem] space-y-4 overflow-y-auto pr-1">
            <div v-for="group in tableGroups" :key="group.group" class="space-y-2">
              <Badge variant="secondary">{{ group.group }}</Badge>
              <div class="grid gap-2">
                <button
                  v-for="[tableName, tableComment] in group.tables"
                  :key="tableName"
                  type="button"
                  class="rounded-md border px-3 py-2 text-left transition hover:border-slate-300 hover:bg-slate-50"
                  :class="
                    selectedTableName === tableName
                      ? 'border-slate-400 bg-slate-50'
                      : 'border-slate-200 bg-white'
                  "
                  @click="void loadTableDetail(tableName)"
                >
                  <span class="block text-sm font-medium text-slate-900">{{ tableName }}</span>
                  <span class="block truncate text-xs text-slate-500">{{
                    tableComment || '-'
                  }}</span>
                </button>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      <Card class="border-slate-200">
        <CardHeader>
          <CardTitle class="flex items-center gap-2">
            <Table2 class="h-5 w-5" />
            {{ selectedTableName || 'Table Detail' }}
          </CardTitle>
          <CardDescription>{{ selectedDbName || 'No database selected' }}</CardDescription>
        </CardHeader>
        <CardContent>
          <div
            v-if="loadingDetail"
            class="rounded-md border border-slate-200 p-4 text-sm text-slate-500"
          >
            Loading table detail...
          </div>
          <div
            v-else-if="!canBrowseSelectedDatabase"
            class="rounded-md border border-slate-200 p-4 text-sm text-slate-500"
          >
            Select a dictionary-visible database to view table metadata.
          </div>
          <div
            v-else-if="!tableDetail"
            class="rounded-md border border-slate-200 p-4 text-sm text-slate-500"
          >
            Select a table to view metadata.
          </div>
          <div v-else class="space-y-6">
            <div class="overflow-x-auto rounded-md border border-slate-200">
              <table class="min-w-full divide-y divide-slate-200 text-sm">
                <tbody class="divide-y divide-slate-100 bg-white">
                  <tr
                    v-for="(column, index) in tableDetail.meta_data.column_list ?? []"
                    :key="column"
                  >
                    <th class="w-52 bg-slate-50 px-3 py-2 text-left font-medium text-slate-600">
                      {{ column }}
                    </th>
                    <td class="px-3 py-2 text-slate-900">
                      {{ cellValue(normalizeRows(tableDetail.meta_data)[0]?.[index]) }}
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>

            <div class="space-y-3">
              <h3 class="text-base font-semibold text-slate-900">Columns</h3>
              <div class="overflow-x-auto rounded-md border border-slate-200">
                <table class="min-w-full divide-y divide-slate-200 text-sm">
                  <thead class="bg-slate-50">
                    <tr>
                      <th
                        v-for="(column, columnIndex) in tableDetail.desc.column_list ?? []"
                        :key="`desc-header-${columnIndex}`"
                        class="px-3 py-2 text-left font-medium text-slate-600"
                      >
                        {{ column }}
                      </th>
                    </tr>
                  </thead>
                  <tbody class="divide-y divide-slate-100 bg-white">
                    <tr
                      v-for="(row, rowIndex) in normalizeRows(tableDetail.desc)"
                      :key="`desc-${rowIndex}`"
                    >
                      <td
                        v-for="(column, columnIndex) in tableDetail.desc.column_list ?? []"
                        :key="`desc-cell-${rowIndex}-${columnIndex}`"
                        class="px-3 py-2 text-slate-900"
                      >
                        {{ cellValue(row[columnIndex]) }}
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>

            <div class="space-y-3">
              <h3 class="text-base font-semibold text-slate-900">Indexes</h3>
              <div class="overflow-x-auto rounded-md border border-slate-200">
                <table class="min-w-full divide-y divide-slate-200 text-sm">
                  <thead class="bg-slate-50">
                    <tr>
                      <th
                        v-for="(column, columnIndex) in tableDetail.index.column_list ?? []"
                        :key="`index-header-${columnIndex}`"
                        class="px-3 py-2 text-left font-medium text-slate-600"
                      >
                        {{ column }}
                      </th>
                    </tr>
                  </thead>
                  <tbody class="divide-y divide-slate-100 bg-white">
                    <tr
                      v-for="(row, rowIndex) in normalizeRows(tableDetail.index)"
                      :key="`index-${rowIndex}`"
                    >
                      <td
                        v-for="(column, columnIndex) in tableDetail.index.column_list ?? []"
                        :key="`index-cell-${rowIndex}-${columnIndex}`"
                        class="px-3 py-2 text-slate-900"
                      >
                        {{ cellValue(row[columnIndex]) }}
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>

            <div v-if="createSqlText(tableDetail)" class="space-y-3">
              <h3 class="text-base font-semibold text-slate-900">Create SQL</h3>
              <pre
                class="overflow-x-auto rounded-md border border-slate-200 bg-slate-950 p-4 text-xs text-slate-100"
                >{{ createSqlText(tableDetail) }}</pre
              >
            </div>
          </div>
        </CardContent>
      </Card>
    </div>

    <Teleport to="body">
      <div
        v-if="activeFormMode"
        class="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/45 px-4 py-6"
        @click.self="closeForm"
      >
        <section
          role="dialog"
          aria-modal="true"
          aria-labelledby="database-form-title"
          class="max-h-full w-full max-w-lg overflow-y-auto rounded-md border border-slate-200 bg-white shadow-xl"
        >
          <div class="flex items-start justify-between gap-4 border-b border-slate-200 px-5 py-4">
            <div class="space-y-1">
              <h3 id="database-form-title" class="text-lg font-semibold text-slate-900">
                {{ activeFormMode === 'create' ? 'Create database' : 'Edit metadata' }}
              </h3>
              <p class="text-sm text-slate-600">
                {{
                  activeFormMode === 'create'
                    ? 'Create a database and save ownership metadata.'
                    : 'Update ownership metadata for an existing database.'
                }}
              </p>
            </div>
            <Button variant="ghost" size="icon" type="button" @click="closeForm">
              <X class="h-4 w-4" />
            </Button>
          </div>
          <form class="grid gap-4 px-5 py-5" @submit.prevent="void submitDatabaseForm()">
            <label class="grid gap-2">
              <span class="text-sm font-medium text-slate-700">Database name</span>
              <Input
                v-model="form.dbName"
                :disabled="activeFormMode === 'edit'"
                placeholder="appdb"
              />
            </label>
            <label class="grid gap-2">
              <span class="text-sm font-medium text-slate-700">Owner username</span>
              <Input v-model="form.owner" placeholder="jane.doe" />
            </label>
            <label class="grid gap-2">
              <span class="text-sm font-medium text-slate-700">Remark</span>
              <Input v-model="form.remark" placeholder="Business owner, lifecycle, or notes" />
            </label>
            <div class="flex flex-wrap justify-end gap-2">
              <Button variant="outline" type="button" class="gap-2" @click="closeForm">
                <X class="h-4 w-4" />
                Cancel
              </Button>
              <Button type="submit" class="gap-2" :disabled="submitting">
                <Save class="h-4 w-4" />
                {{ submitting ? 'Saving...' : 'Save' }}
              </Button>
            </div>
          </form>
        </section>
      </div>
    </Teleport>
  </section>
</template>
