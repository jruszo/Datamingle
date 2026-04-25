<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { BookOpen, Download, RefreshCw, Table2 } from 'lucide-vue-next'

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
import { useAuthStore } from '@/stores/auth'

const authStore = useAuthStore()

const instances = ref<DataDictionaryInstance[]>([])
const databases = ref<string[]>([])
const tableGroups = ref<DataDictionaryTableGroup[]>([])
const tableDetail = ref<DataDictionaryTableDetail | null>(null)

const selectedInstanceId = ref<number | null>(null)
const selectedDbName = ref('')
const selectedTableName = ref('')
const tableSearch = ref('')

const loadingInstances = ref(false)
const loadingDatabases = ref(false)
const loadingTables = ref(false)
const loadingDetail = ref(false)
const exporting = ref(false)
const refreshing = ref(false)

const error = ref('')
const feedback = ref('')

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
const canExportDataDictionary = computed(() => hasPermission('sql.data_dictionary_export'))

const selectedInstance = computed(() =>
  instances.value.find((instance) => instance.id === selectedInstanceId.value) ?? null,
)

const filteredTableGroups = computed(() => {
  const query = tableSearch.value.trim().toLowerCase()
  if (!query) {
    return tableGroups.value
  }

  return tableGroups.value
    .map((group) => ({
      ...group,
      tables: group.tables.filter(([tableName, tableComment]) =>
        `${tableName} ${tableComment ?? ''}`.toLowerCase().includes(query),
      ),
    }))
    .filter((group) => group.tables.length > 0)
})

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

async function loadInstances() {
  loadingInstances.value = true
  error.value = ''

  try {
    instances.value = await fetchDataDictionaryInstances(requireToken())
    const firstInstance = instances.value[0]
    if (!selectedInstanceId.value && firstInstance) {
      selectedInstanceId.value = firstInstance.id
    }
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to load data dictionary instances.')
  } finally {
    loadingInstances.value = false
  }
}

async function loadDatabases() {
  if (!selectedInstanceId.value) {
    databases.value = []
    return
  }

  loadingDatabases.value = true
  error.value = ''
  selectedDbName.value = ''
  selectedTableName.value = ''
  tableGroups.value = []
  tableDetail.value = null

  try {
    const response = await fetchDataDictionaryDatabases(selectedInstanceId.value, requireToken())
    databases.value = response.result
    selectedDbName.value = databases.value[0] ?? ''
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to load databases.')
  } finally {
    loadingDatabases.value = false
  }
}

async function loadTables() {
  if (!selectedInstanceId.value || !selectedDbName.value) {
    tableGroups.value = []
    return
  }

  loadingTables.value = true
  error.value = ''
  selectedTableName.value = ''
  tableDetail.value = null

  try {
    const response = await fetchDataDictionaryTables(
      selectedInstanceId.value,
      selectedDbName.value,
      requireToken(),
    )
    tableGroups.value = response.result
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to load tables.')
  } finally {
    loadingTables.value = false
  }
}

async function loadTableDetail(tableName: string) {
  if (!selectedInstanceId.value || !selectedDbName.value) {
    return
  }

  loadingDetail.value = true
  error.value = ''
  selectedTableName.value = tableName

  try {
    tableDetail.value = await fetchDataDictionaryTableDetail(
      selectedInstanceId.value,
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
    await loadTables()
  } finally {
    refreshing.value = false
  }
}

async function exportSelectedDictionary() {
  if (!selectedInstanceId.value || !canExportDataDictionary.value) {
    return
  }

  exporting.value = true
  error.value = ''
  feedback.value = ''

  try {
    const result = await exportDataDictionary(
      selectedInstanceId.value,
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
    window.URL.revokeObjectURL(objectUrl)
    feedback.value = 'Data dictionary export prepared.'
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to export data dictionary.')
  } finally {
    exporting.value = false
  }
}

onMounted(async () => {
  await authStore.loadCurrentUser()

  if (!canViewDataDictionary.value) {
    error.value = 'You do not have permission to access the data dictionary.'
    return
  }

  await refreshDataDictionary()
})

watch(selectedInstanceId, () => {
  if (!canViewDataDictionary.value || refreshing.value) {
    return
  }
  void loadDatabases()
})

watch(selectedDbName, () => {
  if (!canViewDataDictionary.value || !selectedDbName.value || refreshing.value) {
    return
  }
  void loadTables()
})
</script>

<template>
  <section class="grid gap-6">
    <div class="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
      <div class="space-y-1">
        <h2 class="text-2xl font-semibold text-slate-900">Data Dictionary</h2>
        <p class="text-sm text-slate-600">
          Browse database metadata, table columns, indexes, and exported dictionary artifacts.
        </p>
      </div>
      <div class="flex flex-wrap gap-2">
        <Button variant="outline" type="button" class="gap-2" :disabled="loadingInstances" @click="void refreshDataDictionary()">
          <RefreshCw class="h-4 w-4" />
          Refresh
        </Button>
        <Button
          v-if="canExportDataDictionary"
          type="button"
          class="gap-2"
          :disabled="exporting || !selectedInstanceId"
          @click="void exportSelectedDictionary()"
        >
          <Download class="h-4 w-4" />
          {{ exporting ? 'Exporting...' : 'Export' }}
        </Button>
      </div>
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
        <CardDescription>Select an instance and database before opening table metadata.</CardDescription>
      </CardHeader>
      <CardContent>
        <div class="grid gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_minmax(0,0.8fr)]">
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
            <span class="text-sm font-medium text-slate-700">Database</span>
            <select v-model="selectedDbName" :class="selectClass" :disabled="loadingDatabases || databases.length === 0">
              <option v-if="databases.length === 0" value="">No databases</option>
              <option v-for="database in databases" :key="database" :value="database">
                {{ database }}
              </option>
            </select>
          </label>
          <div class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">Engine</span>
            <div class="flex h-10 items-center rounded-md border border-slate-200 bg-slate-50 px-3 text-sm text-slate-700">
              {{ selectedInstance?.db_type || '-' }}
            </div>
          </div>
        </div>
      </CardContent>
    </Card>

    <div class="grid gap-6 xl:grid-cols-[minmax(18rem,0.36fr)_minmax(0,0.64fr)]">
      <Card class="border-slate-200">
        <CardHeader>
          <CardTitle class="flex items-center gap-2">
            <BookOpen class="h-5 w-5" />
            Tables
          </CardTitle>
          <CardDescription>{{ tableGroups.reduce((total, group) => total + group.tables.length, 0) }} tables</CardDescription>
        </CardHeader>
        <CardContent class="space-y-4">
          <Input v-model="tableSearch" placeholder="Search tables or comments" />

          <div v-if="loadingTables" class="rounded-md border border-slate-200 p-4 text-sm text-slate-500">
            Loading tables...
          </div>
          <div v-else-if="filteredTableGroups.length === 0" class="rounded-md border border-slate-200 p-4 text-sm text-slate-500">
            No tables found.
          </div>
          <div v-else class="max-h-[48rem] space-y-4 overflow-y-auto pr-1">
            <div v-for="group in filteredTableGroups" :key="group.group" class="space-y-2">
              <Badge variant="secondary">{{ group.group }}</Badge>
              <div class="grid gap-2">
                <button
                  v-for="[tableName, tableComment] in group.tables"
                  :key="tableName"
                  type="button"
                  class="rounded-md border px-3 py-2 text-left transition hover:border-slate-300 hover:bg-slate-50"
                  :class="selectedTableName === tableName ? 'border-slate-400 bg-slate-50' : 'border-slate-200 bg-white'"
                  @click="void loadTableDetail(tableName)"
                >
                  <span class="block text-sm font-medium text-slate-900">{{ tableName }}</span>
                  <span class="block truncate text-xs text-slate-500">{{ tableComment || '-' }}</span>
                </button>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      <div class="grid gap-6">
        <Card class="border-slate-200">
          <CardHeader>
            <CardTitle class="flex items-center gap-2">
              <Table2 class="h-5 w-5" />
              {{ selectedTableName || 'Table Detail' }}
            </CardTitle>
            <CardDescription>{{ selectedDbName || 'No database selected' }}</CardDescription>
          </CardHeader>
          <CardContent>
            <div v-if="loadingDetail" class="rounded-md border border-slate-200 p-4 text-sm text-slate-500">
              Loading table detail...
            </div>
            <div v-else-if="!tableDetail" class="rounded-md border border-slate-200 p-4 text-sm text-slate-500">
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
                          v-for="column in tableDetail.desc.column_list ?? []"
                          :key="column"
                          class="px-3 py-2 text-left font-medium text-slate-600"
                        >
                          {{ column }}
                        </th>
                      </tr>
                    </thead>
                    <tbody class="divide-y divide-slate-100 bg-white">
                      <tr v-for="(row, rowIndex) in normalizeRows(tableDetail.desc)" :key="`desc-${rowIndex}`">
                        <td v-for="(column, columnIndex) in tableDetail.desc.column_list ?? []" :key="column" class="px-3 py-2 text-slate-900">
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
                          v-for="column in tableDetail.index.column_list ?? []"
                          :key="column"
                          class="px-3 py-2 text-left font-medium text-slate-600"
                        >
                          {{ column }}
                        </th>
                      </tr>
                    </thead>
                    <tbody class="divide-y divide-slate-100 bg-white">
                      <tr v-for="(row, rowIndex) in normalizeRows(tableDetail.index)" :key="`index-${rowIndex}`">
                        <td v-for="(column, columnIndex) in tableDetail.index.column_list ?? []" :key="column" class="px-3 py-2 text-slate-900">
                          {{ cellValue(row[columnIndex]) }}
                        </td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>

              <div v-if="createSqlText(tableDetail)" class="space-y-3">
                <h3 class="text-base font-semibold text-slate-900">Create SQL</h3>
                <pre class="overflow-x-auto rounded-md border border-slate-200 bg-slate-950 p-4 text-xs text-slate-100">{{ createSqlText(tableDetail) }}</pre>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  </section>
</template>
