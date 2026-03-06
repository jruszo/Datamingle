<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref, watch } from 'vue'
import { format as formatSqlText } from 'sql-formatter'
import {
  Database,
  Minus,
  Play,
  RefreshCcw,
  Search,
  Sparkles,
  Star,
  Wand2,
  Plus,
  X,
} from 'lucide-vue-next'

import SqlCodeEditor from '@/components/queries/SqlCodeEditor.vue'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import {
  describeQueryTable,
  executeQuery,
  fetchFavoriteQueries,
  fetchInstanceResources,
  fetchQueryInstances,
  fetchQueryLogs,
  updateFavoriteQuery,
  type FavoriteQuery,
  type PaginatedResponse,
  type QueryDescribePayload,
  type QueryLogRecord,
  type QueryResultPayload,
  type QueryableInstance,
} from '@/lib/api'
import { useAuthStore } from '@/stores/auth'

type QueryTab = {
  id: string
  title: string
  kind: 'result' | 'describe'
  payload: QueryResultPayload | QueryDescribePayload | null
  error: string
  instanceName: string
  dbName: string
  dbType: string
  sqlCache: string
  tableName: string
}

type QueryEditorHandle = {
  getSelectedText: () => string
  setValue: (value: string) => void
  focus: () => void
}

type QueryToast = {
  id: number
  message: string
  tone: 'success' | 'error' | 'info'
}

const ENGINE_LABELS: Record<string, string> = {
  mysql: 'MySQL',
  pgsql: 'PgSQL',
  oracle: 'Oracle',
  mssql: 'MsSQL',
  mongo: 'Mongo',
  redis: 'Redis',
  clickhouse: 'ClickHouse',
  doris: 'Doris',
  cassandra: 'Cassandra',
  phoenix: 'Phoenix',
}

const EXPLAIN_DISABLED = new Set(['redis', 'phoenix', 'cassandra'])
const FORMAT_DISABLED = new Set(['redis', 'mongo'])

const REDIS_HELP = [
  'exists key: returns 1 if the key exists, otherwise 0.',
  'keys pattern: finds keys matching a pattern.',
  'ttl key: shows the remaining lifetime in seconds.',
  'type key: returns the stored value type.',
]

const authStore = useAuthStore()
const editorRef = ref<QueryEditorHandle | null>(null)

const workspaceLoading = ref(false)
const instancesLoading = ref(false)
const favoritesLoading = ref(false)
const historyLoading = ref(false)
const resourceLoading = ref(false)
const queryRunning = ref(false)

const pageError = ref('')
const sqlText = ref('')
const commonQueryId = ref('')
const activeTab = ref('history')
const toasts = ref<QueryToast[]>([])
const toastCounter = ref(0)

const instances = ref<QueryableInstance[]>([])
const databases = ref<string[]>([])
const schemas = ref<string[]>([])
const tables = ref<string[]>([])
const favorites = ref<FavoriteQuery[]>([])
const queryTabs = ref<QueryTab[]>([])
const resultTabCounter = ref(0)
const historyPage = ref<PaginatedResponse<QueryLogRecord>>({
  count: 0,
  next: null,
  previous: null,
  results: [],
})

const toastTimers = new Map<number, ReturnType<typeof window.setTimeout>>()

const form = reactive({
  instanceId: 0,
  dbName: '',
  schemaName: '',
  tableName: '',
  limitNum: 100,
})

const historyFilters = reactive({
  search: '',
  star: 'all',
  aliasId: 'all',
  page: 1,
  size: 12,
})

function toUserFacingMessage(error: unknown, fallback: string) {
  if (!(error instanceof Error)) {
    return fallback
  }

  const separator = '): '
  const separatorIndex = error.message.indexOf(separator)
  if (separatorIndex === -1) {
    return error.message
  }

  return error.message.slice(separatorIndex + separator.length)
}

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

function engineLabel(dbType: string) {
  return ENGINE_LABELS[dbType] || dbType
}

function selectedQueryText() {
  return editorRef.value?.getSelectedText().trim() || sqlText.value.trim()
}

function formatterLanguage(dbType: string) {
  switch (dbType) {
    case 'mysql':
    case 'doris':
      return 'mysql'
    case 'clickhouse':
      return 'clickhouse'
    case 'pgsql':
      return 'postgresql'
    case 'oracle':
      return 'plsql'
    case 'mssql':
      return 'transactsql'
    default:
      return 'sql'
  }
}

function buildExplainSql(dbType: string, statement: string) {
  switch (dbType) {
    case 'oracle':
      return `explain plan for ${statement}`
    case 'mssql':
      return `SET SHOWPLAN_ALL ON; ${statement}; SET SHOWPLAN_ALL OFF;`
    default:
      return `explain ${statement}`
  }
}

function stringifyValue(value: unknown) {
  if (value === null || value === undefined || value === '') {
    return '(null)'
  }
  if (Array.isArray(value) || typeof value === 'object') {
    return JSON.stringify(value)
  }
  return `${value}`
}

function formatTimestamp(value: string) {
  const timestamp = new Date(value)
  if (Number.isNaN(timestamp.getTime())) {
    return value
  }
  return timestamp.toLocaleString()
}

function dismissToast(id: number) {
  const timer = toastTimers.get(id)
  if (timer) {
    window.clearTimeout(timer)
    toastTimers.delete(id)
  }
  toasts.value = toasts.value.filter((toast) => toast.id !== id)
}

function pushToast(message: string, tone: QueryToast['tone']) {
  toastCounter.value += 1
  const id = toastCounter.value

  toasts.value.push({
    id,
    message,
    tone,
  })

  const timer = window.setTimeout(() => {
    dismissToast(id)
  }, 4200)

  toastTimers.set(id, timer)
}

function selectClass(disabled: boolean) {
  return disabled
    ? 'rounded-xl border border-slate-200 bg-slate-100 px-3 py-2 text-sm text-slate-400 shadow-none transition-colors cursor-not-allowed'
    : 'rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm transition-colors focus:outline-none focus:ring-2 focus:ring-sky-100'
}

function resetResourceLists() {
  databases.value = []
  schemas.value = []
  tables.value = []
}

function createResultTab(title?: string) {
  resultTabCounter.value += 1
  const tab: QueryTab = {
    id: `result-${resultTabCounter.value}`,
    title: title || `Execution Result ${resultTabCounter.value}`,
    kind: 'result',
    payload: null,
    error: '',
    instanceName: '',
    dbName: '',
    dbType: '',
    sqlCache: '',
    tableName: '',
  }
  queryTabs.value.push(tab)
  activeTab.value = tab.id
  return tab
}

function upsertResultTab(
  payload: QueryResultPayload,
  sqlCache: string,
  error = '',
) {
  const activeResultTab = queryTabs.value.find((tab) => tab.id === activeTab.value && tab.kind === 'result')
  const tab = activeResultTab || createResultTab()
  tab.payload = payload
  tab.error = error
  tab.instanceName = selectedInstance.value?.instance_name || ''
  tab.dbName = form.dbName
  tab.dbType = selectedDbType.value
  tab.sqlCache = sqlCache
  tab.tableName = form.tableName
  activeTab.value = tab.id
}

function upsertDescribeTab(payload: QueryDescribePayload) {
  const key = `${selectedInstance.value?.instance_name || ''}:${form.dbName}:${form.tableName}`
  const existing = queryTabs.value.find((tab) => tab.kind === 'describe' && tab.tableName === key)
  const tab =
    existing ||
    (() => {
      const describeTab: QueryTab = {
        id: `describe-${key}`,
        title: form.tableName,
        kind: 'describe',
        payload: null,
        error: '',
        instanceName: selectedInstance.value?.instance_name || '',
        dbName: form.dbName,
        dbType: selectedDbType.value,
        sqlCache: '',
        tableName: key,
      }
      queryTabs.value.push(describeTab)
      return describeTab
    })()

  tab.payload = payload
  tab.error = ''
  tab.instanceName = selectedInstance.value?.instance_name || ''
  tab.dbName = form.dbName
  tab.dbType = selectedDbType.value
  activeTab.value = tab.id
}

function setResultError(message: string, sqlCache: string) {
  const activeResultTab = queryTabs.value.find((tab) => tab.id === activeTab.value && tab.kind === 'result')
  const tab = activeResultTab || createResultTab()
  tab.error = message
  tab.payload = null
  tab.instanceName = selectedInstance.value?.instance_name || ''
  tab.dbName = form.dbName
  tab.dbType = selectedDbType.value
  tab.sqlCache = sqlCache
  tab.tableName = form.tableName
  activeTab.value = tab.id
}

function removeActiveTab() {
  if (activeTab.value === 'history' || activeTab.value === 'redis-help') {
    return
  }

  const tabIndex = queryTabs.value.findIndex((tab) => tab.id === activeTab.value)
  if (tabIndex === -1) {
    return
  }

  queryTabs.value.splice(tabIndex, 1)
  activeTab.value = queryTabs.value[queryTabs.value.length - 1]?.id || 'history'
}

function resultColumns(payload: QueryResultPayload | QueryDescribePayload | null) {
  if (!payload) {
    return []
  }

  if (payload.column_list.length > 0) {
    return payload.column_list
  }

  return Object.keys(payload.rows[0] || {})
}

function tableRows(payload: QueryResultPayload | QueryDescribePayload | null) {
  return payload?.rows || []
}

function ddlContent(payload: QueryDescribePayload | null, dbType: string) {
  if (!payload || payload.display_mode !== 'ddl') {
    return ''
  }

  const firstRow = payload.rows[0]
  if (!firstRow) {
    return ''
  }

  const columnList = payload.column_list
  const ddlColumn = columnList[1] || Object.keys(firstRow)[1]
  if (!ddlColumn) {
    return ''
  }

  const ddl = firstRow[ddlColumn] || Object.values(firstRow)[1]
  if (typeof ddl !== 'string') {
    return stringifyValue(ddl)
  }

  try {
    return formatSqlText(ddl, { language: formatterLanguage(dbType) })
  } catch {
    return ddl
  }
}

function applyEditorValue(value: string) {
  sqlText.value = value
  editorRef.value?.setValue(value)
  editorRef.value?.focus()
}

async function loadInstances() {
  if (!canSeeWorkspace.value) {
    return
  }

  instancesLoading.value = true
  try {
    instances.value = await fetchQueryInstances(requireToken())
  } finally {
    instancesLoading.value = false
  }
}

async function loadFavorites() {
  if (!canManageHistory.value) {
    favorites.value = []
    return
  }

  favoritesLoading.value = true
  try {
    favorites.value = await fetchFavoriteQueries(requireToken())
  } finally {
    favoritesLoading.value = false
  }
}

async function loadHistory() {
  if (!canManageHistory.value) {
    historyPage.value = {
      count: 0,
      next: null,
      previous: null,
      results: [],
    }
    return
  }

  historyLoading.value = true
  try {
    const starFilter =
      historyFilters.star === 'true' || historyFilters.star === 'false'
        ? historyFilters.star
        : undefined

    historyPage.value = await fetchQueryLogs(
      {
        page: historyFilters.page,
        size: historyFilters.size,
        search: historyFilters.search.trim() || undefined,
        star: starFilter,
        query_log_id:
          historyFilters.aliasId === 'all' ? undefined : Number(historyFilters.aliasId),
      },
      requireToken(),
    )
  } finally {
    historyLoading.value = false
  }
}

async function loadDatabases() {
  if (!form.instanceId) {
    resetResourceLists()
    return
  }

  resourceLoading.value = true
  try {
    const payload = await fetchInstanceResources(form.instanceId, 'database', requireToken())
    databases.value = payload.result
  } finally {
    resourceLoading.value = false
  }
}

async function loadSchemas() {
  if (!form.instanceId || !form.dbName) {
    schemas.value = []
    return
  }

  resourceLoading.value = true
  try {
    const payload = await fetchInstanceResources(form.instanceId, 'schema', requireToken(), {
      db_name: form.dbName,
    })
    schemas.value = payload.result
  } finally {
    resourceLoading.value = false
  }
}

async function loadTables() {
  if (!form.instanceId || !form.dbName) {
    tables.value = []
    return
  }

  resourceLoading.value = true
  try {
    const payload = await fetchInstanceResources(form.instanceId, 'table', requireToken(), {
      db_name: form.dbName,
      schema_name: needsSchemaSelection.value ? form.schemaName : undefined,
    })
    tables.value = payload.result
  } finally {
    resourceLoading.value = false
  }
}

async function handleInstanceChange() {
  form.dbName = ''
  form.schemaName = ''
  form.tableName = ''
  resetResourceLists()

  if (!form.instanceId) {
    return
  }

  await loadDatabases()
}

async function handleDatabaseChange() {
  form.schemaName = ''
  form.tableName = ''
  schemas.value = []
  tables.value = []

  if (!form.dbName) {
    return
  }

  if (needsSchemaSelection.value) {
    await loadSchemas()
    return
  }

  await loadTables()
}

async function handleSchemaChange() {
  form.tableName = ''
  tables.value = []

  if (!form.schemaName) {
    return
  }

  await loadTables()
}

async function handleTableChange() {
  if (!form.tableName) {
    return
  }

  await showTableStructure()
}

async function restoreQueryContext(instanceName: string, dbName: string, statement: string) {
  const instance = instances.value.find((item) => item.instance_name === instanceName)
  if (!instance) {
    throw new Error(`Instance "${instanceName}" is not available to this user.`)
  }

  form.instanceId = instance.id
  await loadDatabases()
  form.dbName = dbName
  if (needsSchemaSelection.value) {
    await loadSchemas()
  } else {
    await loadTables()
  }
  applyEditorValue(statement)
}

async function selectCommonQuery() {
  if (!commonQueryId.value) {
    return
  }

  const favorite = favorites.value.find((item) => `${item.id}` === commonQueryId.value)
  if (!favorite) {
    return
  }

  try {
    await restoreQueryContext(favorite.instance_name, favorite.db_name, favorite.sqllog)
    pushToast(`Loaded favorite query "${favorite.alias || favorite.id}".`, 'success')
  } catch (error) {
    pushToast(toUserFacingMessage(error, 'Failed to load the selected common query.'), 'error')
  }
}

async function rerunHistoryItem(item: QueryLogRecord) {
  try {
    await restoreQueryContext(item.instance_name, item.db_name, item.sqllog)
    pushToast(`Loaded query log #${item.id} into the editor.`, 'success')
  } catch (error) {
    pushToast(toUserFacingMessage(error, 'Failed to restore the selected query log.'), 'error')
  }
}

async function toggleFavorite(item: QueryLogRecord) {
  const nextStar = !item.favorite
  let alias = item.alias

  if (nextStar) {
    const promptValue = window.prompt(
      'Favorite alias',
      item.alias || item.sqllog.slice(0, 48),
    )
    if (promptValue === null) {
      return
    }
    alias = promptValue.trim()
  }

  try {
    await updateFavoriteQuery(item.id, nextStar, alias, requireToken())
    await Promise.all([loadFavorites(), loadHistory()])
    pushToast(
      nextStar ? 'Query saved to common queries.' : 'Query removed from common queries.',
      'success',
    )
  } catch (error) {
    pushToast(toUserFacingMessage(error, 'Failed to update the favorite query.'), 'error')
  }
}

async function showTableStructure() {
  if (!selectedInstance.value || !form.dbName || !form.tableName) {
    return
  }

  try {
    const payload = await describeQueryTable(
      {
        instance_id: selectedInstance.value.id,
        db_name: form.dbName,
        schema_name: needsSchemaSelection.value ? form.schemaName : undefined,
        tb_name: form.tableName,
      },
      requireToken(),
    )
    upsertDescribeTab(payload)
  } catch (error) {
    pushToast(toUserFacingMessage(error, 'Failed to load the table structure.'), 'error')
  }
}

async function formatCurrentSql() {
  if (!sqlText.value.trim()) {
    pushToast('SQL content cannot be empty.', 'error')
    return
  }

  try {
    const formatted = formatSqlText(sqlText.value, {
      language: formatterLanguage(selectedDbType.value),
    })
    applyEditorValue(formatted)
    pushToast('SQL formatted.', 'success')
  } catch (error) {
    pushToast(toUserFacingMessage(error, 'Failed to format SQL.'), 'error')
  }
}

async function runQuery(mode: 'query' | 'plan') {
  const selectedSql = selectedQueryText()
  const instance = selectedInstance.value

  if (!instance) {
    pushToast('Please select an instance.', 'error')
    return
  }
  if (!form.dbName) {
    pushToast('Please select a database.', 'error')
    return
  }
  if (!selectedSql) {
    pushToast('SQL content cannot be empty.', 'error')
    return
  }

  if (mode === 'plan' && !canExplain.value) {
    pushToast('Execution plans are not supported for this engine.', 'error')
    return
  }

  const sqlToRun = mode === 'plan' ? buildExplainSql(selectedDbType.value, selectedSql) : selectedSql

  queryRunning.value = true

  try {
    const payload = await executeQuery(
      {
        instance_name: instance.instance_name,
        db_name: form.dbName,
        schema_name: needsSchemaSelection.value ? form.schemaName : undefined,
        tb_name: form.tableName || undefined,
        sql_content: sqlToRun,
        limit_num: form.limitNum,
      },
      requireToken(),
    )
    upsertResultTab(payload, sqlToRun)
    pushToast(mode === 'plan' ? 'Execution plan loaded.' : 'Query executed successfully.', 'success')
    if (canManageHistory.value) {
      historyFilters.page = 1
      await loadHistory()
    }
  } catch (error) {
    const message = toUserFacingMessage(error, 'Query execution failed.')
    pushToast(message, 'error')
    setResultError(message, sqlToRun)
  } finally {
    queryRunning.value = false
  }
}

async function loadWorkspace() {
  workspaceLoading.value = true
  pageError.value = ''

  try {
    await authStore.loadCurrentUser(true)

    if (!canSeeWorkspace.value) {
      return
    }

    await loadInstances()
    await Promise.all([loadFavorites(), loadHistory()])
  } catch (error) {
    pageError.value = toUserFacingMessage(error, 'Failed to load the query workspace.')
  } finally {
    workspaceLoading.value = false
  }
}

function submitHistorySearch() {
  historyFilters.page = 1
  void loadHistory()
}

function previousHistoryPage() {
  if (historyFilters.page === 1) {
    return
  }
  historyFilters.page -= 1
  void loadHistory()
}

function nextHistoryPage() {
  if (!historyPage.value.next) {
    return
  }
  historyFilters.page += 1
  void loadHistory()
}

const selectedInstance = computed(() => {
  return instances.value.find((item) => item.id === form.instanceId) || null
})

const selectedDbType = computed(() => {
  return selectedInstance.value?.db_type || ''
})

const groupedInstances = computed(() => {
  const groups = new Map<string, QueryableInstance[]>()

  for (const instance of instances.value) {
    const label = engineLabel(instance.db_type)
    const group = groups.get(label) || []
    group.push(instance)
    groups.set(label, group)
  }

  return Array.from(groups.entries()).map(([label, items]) => ({ label, items }))
})

const canSeeWorkspace = computed(() => {
  return hasPermission('sql.menu_query') || hasPermission('sql.menu_sqlquery') || hasPermission('sql.query_submit')
})

const canManageHistory = computed(() => hasPermission('sql.menu_sqlquery'))
const canRunQueries = computed(() => hasPermission('sql.query_submit'))
const needsSchemaSelection = computed(() => selectedDbType.value === 'pgsql')
const canExplain = computed(() => !EXPLAIN_DISABLED.has(selectedDbType.value))
const canFormat = computed(() => !FORMAT_DISABLED.has(selectedDbType.value))
const showRedisHelp = computed(() => selectedDbType.value === 'redis')
const currentTab = computed(() => queryTabs.value.find((tab) => tab.id === activeTab.value) || null)
const commonQueryDisabled = computed(() => !canManageHistory.value || favoritesLoading.value || favorites.value.length === 0)
const instanceSelectDisabled = computed(() => instancesLoading.value || workspaceLoading.value)
const databaseSelectDisabled = computed(() => resourceLoading.value || !form.instanceId)
const schemaSelectDisabled = computed(() => resourceLoading.value || !form.dbName)
const tableSelectDisabled = computed(() => {
  return resourceLoading.value || !form.dbName || (needsSchemaSelection.value && !form.schemaName)
})
const commonQueryPlaceholder = computed(() => {
  if (!canManageHistory.value) {
    return 'History access required'
  }
  if (favoritesLoading.value) {
    return 'Loading common queries...'
  }
  if (favorites.value.length === 0) {
    return 'No saved queries'
  }
  return 'Select a saved query'
})
const instancePlaceholder = computed(() => {
  if (instancesLoading.value) {
    return 'Loading instances...'
  }
  return 'Select instance'
})
const databasePlaceholder = computed(() => {
  if (!form.instanceId) {
    return 'Select instance first'
  }
  if (resourceLoading.value) {
    return 'Loading databases...'
  }
  return 'Select database'
})
const schemaPlaceholder = computed(() => {
  if (!form.dbName) {
    return 'Select database first'
  }
  if (resourceLoading.value) {
    return 'Loading schemas...'
  }
  return 'Select schema'
})
const tablePlaceholder = computed(() => {
  if (!form.dbName) {
    return 'Select database first'
  }
  if (needsSchemaSelection.value && !form.schemaName) {
    return 'Select schema first'
  }
  if (resourceLoading.value) {
    return selectedDbType.value === 'mongo' ? 'Loading collections...' : 'Loading tables...'
  }
  return selectedDbType.value === 'mongo' ? 'View collection fields' : 'Show table structure'
})
const historyStart = computed(() => {
  if (historyPage.value.count === 0) {
    return 0
  }
  return (historyFilters.page - 1) * historyFilters.size + 1
})
const historyEnd = computed(() => {
  return Math.min(historyFilters.page * historyFilters.size, historyPage.value.count)
})

watch(showRedisHelp, (enabled) => {
  if (!enabled && activeTab.value === 'redis-help') {
    activeTab.value = 'history'
  }
})

onMounted(() => {
  void loadWorkspace()
})

onBeforeUnmount(() => {
  for (const timer of toastTimers.values()) {
    window.clearTimeout(timer)
  }
  toastTimers.clear()
})
</script>

<template>
  <section class="grid gap-6">
    <Card class="overflow-hidden border-sky-100 shadow-lg shadow-sky-100/40">
      <CardHeader
        class="border-b border-sky-100 bg-[radial-gradient(circle_at_top_left,_rgba(14,165,233,0.18),_transparent_42%),linear-gradient(135deg,#eff6ff_0%,#f8fafc_48%,#fff7ed_100%)]"
      >
        <div class="flex flex-col gap-3 lg:flex-row lg:items-end lg:justify-between">
          <div>
            <CardTitle class="flex items-center gap-2 text-2xl">
              <Database class="h-5 w-5 text-sky-600" />
              SQL Query Center
            </CardTitle>
            <CardDescription class="mt-2 max-w-2xl text-slate-600">
              Run read-only SQL queries, inspect table structure, review common queries, and track execution history.
            </CardDescription>
          </div>
          <div class="flex flex-wrap gap-2">
            <Badge variant="secondary" class="bg-white/80 text-slate-800">
              {{ selectedInstance ? engineLabel(selectedInstance.db_type) : 'No instance selected' }}
            </Badge>
          </div>
        </div>
      </CardHeader>

      <CardContent class="grid gap-6 p-6">
        <div
          v-if="!canSeeWorkspace"
          class="rounded-3xl border border-amber-200 bg-amber-50 px-5 py-4 text-sm text-amber-800"
        >
          Your account does not have access to the SQL query workspace.
        </div>

        <template v-else>
          <div v-if="pageError" class="rounded-3xl border border-red-200 bg-red-50 px-5 py-4 text-sm text-red-700">
            {{ pageError }}
          </div>

          <div class="grid gap-6 xl:grid-cols-[minmax(0,1.6fr)_minmax(320px,0.9fr)]">
            <div class="grid gap-4">
              <div class="flex flex-wrap items-center gap-3">
                <label class="text-xs font-semibold uppercase tracking-[0.24em] text-slate-500">Common Queries</label>
                <select
                  v-model="commonQueryId"
                  class="min-w-[220px]"
                  :class="selectClass(commonQueryDisabled)"
                  :disabled="commonQueryDisabled"
                  @change="void selectCommonQuery()"
                >
                  <option value="">{{ commonQueryPlaceholder }}</option>
                  <option v-for="favorite in favorites" :key="favorite.id" :value="`${favorite.id}`">
                    {{ favorite.alias || `${favorite.instance_name} / ${favorite.db_name}` }}
                  </option>
                </select>
                <span v-if="favoritesLoading" class="text-xs text-slate-500">Loading favorites...</span>
              </div>

              <SqlCodeEditor
                ref="editorRef"
                v-model="sqlText"
                :db-type="selectedDbType"
                :disabled="workspaceLoading"
              />
            </div>

            <div class="grid gap-4 rounded-[1.75rem] border border-slate-200 bg-slate-50/90 p-4">
              <div>
                <p class="text-xs font-semibold uppercase tracking-[0.24em] text-slate-500">Target</p>
                <p class="mt-1 text-sm text-slate-600">Choose an instance, database, and optional table context before running or explaining a query.</p>
              </div>

              <div class="grid gap-3">
                <select
                  v-model.number="form.instanceId"
                  :class="selectClass(instanceSelectDisabled)"
                  :disabled="instanceSelectDisabled"
                  @change="void handleInstanceChange()"
                >
                  <option :value="0">{{ instancePlaceholder }}</option>
                  <optgroup
                    v-for="group in groupedInstances"
                    :key="group.label"
                    :label="group.label"
                  >
                    <option v-for="instance in group.items" :key="instance.id" :value="instance.id">
                      {{ instance.instance_name }}
                    </option>
                  </optgroup>
                </select>

                <select
                  v-model="form.dbName"
                  :class="selectClass(databaseSelectDisabled)"
                  :disabled="databaseSelectDisabled"
                  @change="void handleDatabaseChange()"
                >
                  <option value="">{{ databasePlaceholder }}</option>
                  <option v-for="database in databases" :key="database" :value="database">
                    {{ database }}
                  </option>
                </select>

                <select
                  v-if="needsSchemaSelection"
                  v-model="form.schemaName"
                  :class="selectClass(schemaSelectDisabled)"
                  :disabled="schemaSelectDisabled"
                  @change="void handleSchemaChange()"
                >
                  <option value="">{{ schemaPlaceholder }}</option>
                  <option v-for="schema in schemas" :key="schema" :value="schema">
                    {{ schema }}
                  </option>
                </select>

                <select
                  v-model="form.tableName"
                  :class="selectClass(tableSelectDisabled)"
                  :disabled="tableSelectDisabled"
                  @change="void handleTableChange()"
                >
                  <option value="">{{ tablePlaceholder }}</option>
                  <option v-for="table in tables" :key="table" :value="table">
                    {{ table }}
                  </option>
                </select>

                <select
                  v-model.number="form.limitNum"
                  :class="selectClass(false)"
                >
                  <option :value="100">100 rows</option>
                  <option :value="500">500 rows</option>
                  <option :value="1000">1000 rows</option>
                  <option :value="0">Maximum allowed rows</option>
                </select>
              </div>

              <div class="grid grid-cols-1 gap-2 sm:grid-cols-3 xl:grid-cols-1">
                <Button variant="outline" :disabled="!canFormat || workspaceLoading" @click="void formatCurrentSql()">
                  <Wand2 class="h-4 w-4" />
                  Format
                </Button>
                <Button variant="secondary" :disabled="!canRunQueries || !canExplain || queryRunning" @click="void runQuery('plan')">
                  <Sparkles class="h-4 w-4" />
                  Execution Plan
                </Button>
                <Button :disabled="!canRunQueries || queryRunning" @click="void runQuery('query')">
                  <Play class="h-4 w-4" />
                  {{ queryRunning ? 'Running...' : 'Run Query' }}
                </Button>
              </div>

              <div class="rounded-2xl border border-slate-200 bg-white/70 p-3 text-xs text-slate-600">
                <p>Comment lines are supported. If you select part of the editor, only that selection is executed.</p>
                <p class="mt-2">Table structure opens automatically when you pick a table. Query history is refreshed after each successful run.</p>
              </div>
            </div>
          </div>

          <Card class="border-slate-200 shadow-none">
            <CardHeader class="border-b border-slate-200 bg-slate-50/80">
              <div class="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
                <div>
                  <CardTitle class="text-lg">Query Results</CardTitle>
                  <CardDescription>
                    Switch between query history, dynamic execution results, and table-structure tabs.
                  </CardDescription>
                </div>
                <div class="flex items-center gap-2">
                  <Button variant="outline" size="sm" @click="createResultTab()">
                    <Plus class="h-4 w-4" />
                  </Button>
                  <Button variant="outline" size="sm" @click="removeActiveTab">
                    <Minus class="h-4 w-4" />
                  </Button>
                </div>
              </div>
            </CardHeader>

            <CardContent class="p-0">
              <div class="flex flex-wrap gap-2 border-b border-slate-200 px-4 py-4">
                <button
                  v-if="showRedisHelp"
                  type="button"
                  class="rounded-full px-3 py-1.5 text-sm font-medium transition"
                  :class="activeTab === 'redis-help' ? 'bg-slate-900 text-white' : 'bg-slate-100 text-slate-600 hover:bg-slate-200'"
                  @click="activeTab = 'redis-help'"
                >
                  Redis Help
                </button>
                <button
                  type="button"
                  class="rounded-full px-3 py-1.5 text-sm font-medium transition"
                  :class="activeTab === 'history' ? 'bg-slate-900 text-white' : 'bg-slate-100 text-slate-600 hover:bg-slate-200'"
                  @click="activeTab = 'history'"
                >
                  Query History
                </button>
                <button
                  v-for="tab in queryTabs"
                  :key="tab.id"
                  type="button"
                  class="rounded-full px-3 py-1.5 text-sm font-medium transition"
                  :class="activeTab === tab.id ? 'bg-slate-900 text-white' : 'bg-slate-100 text-slate-600 hover:bg-slate-200'"
                  @click="activeTab = tab.id"
                >
                  {{ tab.title }}
                </button>
              </div>

              <div v-if="activeTab === 'redis-help'" class="grid gap-4 p-6 md:grid-cols-2">
                <div
                  v-for="line in REDIS_HELP"
                  :key="line"
                  class="rounded-3xl border border-sky-100 bg-sky-50/70 p-4 text-sm text-slate-700"
                >
                  {{ line }}
                </div>
              </div>

              <div v-else-if="activeTab === 'history'" class="grid gap-5 p-6">
                <div v-if="!canManageHistory" class="rounded-3xl border border-amber-200 bg-amber-50 px-5 py-4 text-sm text-amber-800">
                  Query history and common queries are disabled for your account.
                </div>

                <template v-else>
                  <div class="flex flex-col gap-3 lg:flex-row lg:items-center">
                    <form class="flex flex-1 gap-3" @submit.prevent="submitHistorySearch">
                      <Input v-model="historyFilters.search" placeholder="Search by database, instance, user, alias, or SQL text" />
                      <Button variant="outline" type="submit" :disabled="historyLoading">
                        <Search class="h-4 w-4" />
                        Search
                      </Button>
                    </form>

                    <div class="flex flex-wrap gap-3">
                      <select
                        v-model="historyFilters.star"
                        class="rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm"
                        @change="submitHistorySearch"
                      >
                        <option value="all">All</option>
                        <option value="true">Starred</option>
                        <option value="false">Unstarred</option>
                      </select>

                      <select
                        v-model="historyFilters.aliasId"
                        class="rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm"
                        @change="submitHistorySearch"
                      >
                        <option value="all">All aliases</option>
                        <option v-for="favorite in favorites" :key="favorite.id" :value="`${favorite.id}`">
                          {{ favorite.alias || favorite.id }}
                        </option>
                      </select>
                    </div>
                  </div>

                  <div class="overflow-hidden rounded-[1.5rem] border border-slate-200 bg-white">
                    <div class="overflow-x-auto">
                      <table class="min-w-full divide-y divide-slate-200 text-left text-sm">
                        <thead class="bg-slate-50 text-slate-600">
                          <tr>
                            <th class="px-4 py-3 font-medium">Actions</th>
                            <th class="px-4 py-3 font-medium">Alias</th>
                            <th class="px-4 py-3 font-medium">Instance</th>
                            <th class="px-4 py-3 font-medium">Database</th>
                            <th class="px-4 py-3 font-medium">User</th>
                            <th class="px-4 py-3 font-medium">Rows</th>
                            <th class="px-4 py-3 font-medium">Duration</th>
                            <th class="px-4 py-3 font-medium">Query Time</th>
                            <th class="px-4 py-3 font-medium">Statement</th>
                          </tr>
                        </thead>
                        <tbody class="divide-y divide-slate-100">
                          <tr v-if="historyLoading">
                            <td colspan="9" class="px-4 py-6 text-center text-slate-500">
                              Loading query history...
                            </td>
                          </tr>
                          <tr v-else-if="historyPage.results.length === 0">
                            <td colspan="9" class="px-4 py-6 text-center text-slate-500">
                              No matching query history.
                            </td>
                          </tr>
                          <tr v-for="item in historyPage.results" :key="item.id" class="align-top">
                            <td class="px-4 py-3">
                              <div class="flex gap-2">
                                <Button variant="outline" size="sm" @click="void rerunHistoryItem(item)">
                                  <RefreshCcw class="h-4 w-4" />
                                </Button>
                                <Button
                                  :variant="item.favorite ? 'default' : 'outline'"
                                  size="sm"
                                  @click="void toggleFavorite(item)"
                                >
                                  <Star class="h-4 w-4" />
                                </Button>
                              </div>
                            </td>
                            <td class="px-4 py-3 font-medium text-slate-800">{{ item.alias || 'Not saved' }}</td>
                            <td class="px-4 py-3 text-slate-600">{{ item.instance_name }}</td>
                            <td class="px-4 py-3 text-slate-600">{{ item.db_name }}</td>
                            <td class="px-4 py-3 text-slate-600">{{ item.user_display }}</td>
                            <td class="px-4 py-3 text-slate-600">{{ item.effect_row }}</td>
                            <td class="px-4 py-3 text-slate-600">{{ item.cost_time }}</td>
                            <td class="px-4 py-3 text-slate-600">{{ formatTimestamp(item.create_time) }}</td>
                            <td class="max-w-xl px-4 py-3 text-slate-600">
                              <pre class="max-h-28 overflow-auto whitespace-pre-wrap rounded-2xl bg-slate-50 p-3 text-xs">{{ item.sqllog }}</pre>
                            </td>
                          </tr>
                        </tbody>
                      </table>
                    </div>
                  </div>

                  <div class="flex flex-col gap-3 text-sm text-slate-500 sm:flex-row sm:items-center sm:justify-between">
                    <p>
                      Showing {{ historyStart }}-{{ historyEnd }} of {{ historyPage.count }}
                    </p>
                    <div class="flex gap-2">
                      <Button variant="outline" size="sm" :disabled="historyFilters.page === 1 || historyLoading" @click="previousHistoryPage">
                        Previous
                      </Button>
                      <Button variant="outline" size="sm" :disabled="!historyPage.next || historyLoading" @click="nextHistoryPage">
                        Next
                      </Button>
                    </div>
                  </div>
                </template>
              </div>

              <div v-else-if="currentTab" class="grid gap-5 p-6">
                <div class="flex flex-col gap-3 rounded-[1.5rem] border border-slate-200 bg-slate-50/60 p-4 lg:flex-row lg:items-center lg:justify-between">
                  <div>
                    <p class="text-xs font-semibold uppercase tracking-[0.24em] text-slate-500">Execution Context</p>
                    <p class="mt-2 text-sm text-slate-700">
                      {{ currentTab.instanceName || 'No instance' }} / {{ currentTab.dbName || 'No database' }}
                    </p>
                  </div>
                  <div class="flex flex-wrap gap-2 text-xs text-slate-600">
                    <Badge variant="secondary" class="bg-white text-slate-700">
                      {{ currentTab.kind === 'describe' ? 'Table Structure' : 'Execution Result' }}
                    </Badge>
                    <Badge
                      v-if="currentTab.payload && 'query_time' in currentTab.payload"
                      variant="secondary"
                      class="bg-white text-slate-700"
                    >
                      Query {{ currentTab.payload.query_time }} sec
                    </Badge>
                    <Badge
                      v-if="currentTab.payload && 'mask_time' in currentTab.payload && currentTab.payload.mask_time"
                      variant="secondary"
                      class="bg-white text-slate-700"
                    >
                      Mask {{ currentTab.payload.mask_time }} sec
                    </Badge>
                  </div>
                </div>

                <div v-if="currentTab.error" class="rounded-3xl border border-red-200 bg-red-50 px-5 py-4 text-sm text-red-700">
                  {{ currentTab.error }}
                </div>

                <div
                  v-else-if="currentTab.kind === 'describe' && currentTab.payload && 'display_mode' in currentTab.payload && currentTab.payload.display_mode === 'ddl'"
                  class="overflow-hidden rounded-[1.5rem] border border-slate-200 bg-white"
                >
                  <div class="border-b border-slate-200 px-5 py-4">
                    <p class="text-sm font-medium text-slate-900">Create Table</p>
                  </div>
                  <pre class="max-h-[32rem] overflow-auto bg-slate-950 px-5 py-5 text-sm leading-7 text-slate-100">{{ ddlContent(currentTab.payload, currentTab.dbType) }}</pre>
                </div>

                <div v-else class="overflow-hidden rounded-[1.5rem] border border-slate-200 bg-white">
                  <div class="border-b border-slate-200 px-5 py-4">
                    <p class="text-sm font-medium text-slate-900">
                      {{ tableRows(currentTab.payload).length }} rows returned
                    </p>
                  </div>
                  <div class="overflow-x-auto">
                    <table class="min-w-full divide-y divide-slate-200 text-left text-sm">
                      <thead class="bg-slate-50 text-slate-600">
                        <tr>
                          <th
                            v-for="column in resultColumns(currentTab.payload)"
                            :key="column"
                            class="px-4 py-3 font-medium"
                          >
                            {{ column }}
                          </th>
                        </tr>
                      </thead>
                      <tbody class="divide-y divide-slate-100">
                        <tr v-if="tableRows(currentTab.payload).length === 0">
                          <td
                            :colspan="Math.max(resultColumns(currentTab.payload).length, 1)"
                            class="px-4 py-6 text-center text-slate-500"
                          >
                            No rows returned.
                          </td>
                        </tr>
                        <tr
                          v-for="(row, index) in tableRows(currentTab.payload)"
                          :key="`${currentTab.id}-${index}`"
                          class="align-top"
                        >
                          <td
                            v-for="column in resultColumns(currentTab.payload)"
                            :key="`${currentTab.id}-${index}-${column}`"
                            class="max-w-sm px-4 py-3 text-slate-700"
                          >
                            <pre class="whitespace-pre-wrap break-words text-xs">{{ stringifyValue(row[column]) }}</pre>
                          </td>
                        </tr>
                      </tbody>
                    </table>
                  </div>
                </div>

                <div
                  v-if="currentTab.sqlCache"
                  class="rounded-[1.5rem] border border-slate-200 bg-slate-50/70 p-4"
                >
                  <p class="text-xs font-semibold uppercase tracking-[0.24em] text-slate-500">Executed SQL</p>
                  <pre class="mt-3 max-h-48 overflow-auto whitespace-pre-wrap rounded-2xl bg-white p-4 text-xs text-slate-700">{{ currentTab.sqlCache }}</pre>
                </div>
              </div>
            </CardContent>
          </Card>
        </template>
      </CardContent>
    </Card>

    <TransitionGroup
      tag="div"
      name="toast"
      class="pointer-events-none fixed inset-x-4 bottom-4 z-50 flex flex-col items-stretch gap-3 sm:left-auto sm:right-6 sm:max-w-sm"
      aria-live="polite"
      aria-atomic="true"
    >
      <div
        v-for="toast in toasts"
        :key="toast.id"
        class="pointer-events-auto overflow-hidden rounded-2xl border px-4 py-3 shadow-lg backdrop-blur"
        :class="
          toast.tone === 'error'
            ? 'border-red-200 bg-red-50/95 text-red-800 shadow-red-100'
            : toast.tone === 'success'
              ? 'border-emerald-200 bg-emerald-50/95 text-emerald-800 shadow-emerald-100'
              : 'border-slate-200 bg-white/95 text-slate-800 shadow-slate-200'
        "
      >
        <div class="flex items-start gap-3">
          <p class="flex-1 text-sm font-medium">
            {{ toast.message }}
          </p>
          <button
            type="button"
            class="rounded-full p-1 transition hover:bg-black/5"
            @click="dismissToast(toast.id)"
          >
            <X class="h-4 w-4" />
            <span class="sr-only">Dismiss notification</span>
          </button>
        </div>
      </div>
    </TransitionGroup>
  </section>
</template>

<style scoped>
.toast-enter-active,
.toast-leave-active,
.toast-move {
  transition: all 0.24s ease;
}

.toast-enter-from,
.toast-leave-to {
  opacity: 0;
  transform: translateY(18px);
}
</style>
