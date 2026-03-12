<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref, watch } from 'vue'
import { format as formatSqlText } from 'sql-formatter'
import {
  Database,
  Minus,
  Pencil,
  Play,
  RefreshCcw,
  Search,
  Sparkles,
  Star,
  Wand2,
  Plus,
  X,
} from 'lucide-vue-next'

import QueryMetadataExplorer from '@/components/queries/QueryMetadataExplorer.vue'
import SqlCodeEditor from '@/components/queries/SqlCodeEditor.vue'
import type {
  QueryMetadataNode,
  QueryMetadataTableColumn,
  QueryMetadataTableIndex,
  QueryMetadataTableDetailsMap,
} from '@/components/queries/query-metadata-explorer'
import { Button } from '@/components/ui/button'
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
  schemaName: string
  dbType: string
  sqlCache: string
  tableName: string
}

type QueryEditorHandle = {
  getSelectedText: () => string
  setValue: (value: string) => void
  focus: () => void
  insertText: (value: string) => void
}

type QueryToast = {
  id: number
  message: string
  tone: 'success' | 'error' | 'info'
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
const explorerLoading = ref(false)
const queryRunning = ref(false)

const pageError = ref('')
const explorerError = ref('')
const sqlText = ref('')
const commonQueryId = ref('')
const activeTab = ref('history')
const selectedExplorerNodeId = ref('')
const tableStructureByNode = ref<Record<string, QueryDescribePayload>>({})
const toasts = ref<QueryToast[]>([])
const toastCounter = ref(0)
const editorPaneHeight = ref(220)

const instances = ref<QueryableInstance[]>([])
const explorerNodes = ref<QueryMetadataNode[]>([])
const favorites = ref<FavoriteQuery[]>([])
const queryTabs = ref<QueryTab[]>([])
const resultTabCounter = ref(0)
const explorerGeneration = ref(0)
const historyPage = ref<PaginatedResponse<QueryLogRecord>>({
  count: 0,
  next: null,
  previous: null,
  results: [],
})

const toastTimers = new Map<number, ReturnType<typeof window.setTimeout>>()
let removeResizeListeners: (() => void) | null = null

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

function clampEditorPaneHeight(height: number) {
  const viewportLimit = typeof window === 'undefined' ? 560 : Math.max(window.innerHeight - 320, 320)
  return Math.min(Math.max(height, 160), viewportLimit)
}

function stopEditorResize() {
  if (removeResizeListeners) {
    removeResizeListeners()
    removeResizeListeners = null
  }
  document.body.style.cursor = ''
  document.body.style.userSelect = ''
}

function startEditorResize(event: MouseEvent) {
  const startY = event.clientY
  const startHeight = editorPaneHeight.value

  document.body.style.cursor = 'row-resize'
  document.body.style.userSelect = 'none'

  const handlePointerMove = (moveEvent: MouseEvent) => {
    const delta = moveEvent.clientY - startY
    editorPaneHeight.value = clampEditorPaneHeight(startHeight + delta)
  }

  const handlePointerUp = () => {
    stopEditorResize()
  }

  window.addEventListener('mousemove', handlePointerMove)
  window.addEventListener('mouseup', handlePointerUp, { once: true })

  removeResizeListeners = () => {
    window.removeEventListener('mousemove', handlePointerMove)
    window.removeEventListener('mouseup', handlePointerUp)
  }
}

function selectClass(disabled: boolean) {
  return disabled
    ? 'rounded-xl border border-slate-200 bg-slate-100 px-3 py-2 text-sm text-slate-400 shadow-none transition-colors cursor-not-allowed'
    : 'rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm transition-colors focus:outline-none focus:ring-2 focus:ring-sky-100'
}

function clearQueryContext() {
  form.dbName = ''
  form.schemaName = ''
  form.tableName = ''
  selectedExplorerNodeId.value = ''
  tableStructureByNode.value = {}
}

function createResultTab(title?: string, sqlCache = '') {
  resultTabCounter.value += 1
  const tab: QueryTab = {
    id: `result-${resultTabCounter.value}`,
    title: title || `Execution Result ${resultTabCounter.value}`,
    kind: 'result',
    payload: null,
    error: '',
    instanceName: '',
    dbName: '',
    schemaName: '',
    dbType: '',
    sqlCache,
    tableName: '',
  }
  queryTabs.value.push(tab)
  activeTab.value = tab.id
  applyEditorValue(sqlCache)
  return tab
}

async function activateWorkspaceTab(tabId: string) {
  activeTab.value = tabId

  const tab = queryTabs.value.find((item) => item.id === tabId)
  if (tab) {
    const instance = instances.value.find((item) => item.instance_name === tab.instanceName)
    if (instance && form.instanceId !== instance.id) {
      await selectInstance(instance.id)
    } else if (instance) {
      form.instanceId = instance.id
    }

    form.dbName = tab.dbName
    form.schemaName = tab.schemaName
    form.tableName = tab.tableName
    applyEditorValue(tab.sqlCache)
  }
}

function renameWorkspaceTab(tabId: string) {
  const tab = queryTabs.value.find((item) => item.id === tabId && item.kind === 'result')
  if (!tab) {
    return
  }

  const nextTitle = window.prompt('Rename runtime tab', tab.title)
  if (nextTitle === null) {
    return
  }

  const trimmedTitle = nextTitle.trim()
  if (!trimmedTitle) {
    return
  }

  tab.title = trimmedTitle
}

function ensureEditorTab() {
  if (activeTab.value === 'history' || activeTab.value === 'redis-help' || !currentTab.value) {
    createResultTab()
  }
}

async function openQueryInNewTab(instanceName: string, dbName: string, statement: string, title?: string) {
  const instance = instances.value.find((item) => item.instance_name === instanceName)
  if (!instance) {
    throw new Error(`Instance "${instanceName}" is not available to this user.`)
  }

  await selectInstance(instance.id)
  form.dbName = dbName
  const dbNode = findNodeById(explorerNodes.value, createNodeId('database', dbName, dbName))
  selectedExplorerNodeId.value = dbNode?.id || ''

  const tab = createResultTab(title, statement)
  tab.instanceName = instance.instance_name
  tab.dbName = dbName
  tab.schemaName = ''
  tab.dbType = instance.db_type
}

function upsertResultTab(payload: QueryResultPayload, sqlCache: string, error = '') {
  const activeResultTab = queryTabs.value.find((tab) => tab.id === activeTab.value && tab.kind === 'result')
  const tab = activeResultTab || createResultTab(undefined, sqlCache)
  tab.payload = payload
  tab.error = error
  tab.instanceName = selectedInstance.value?.instance_name || ''
  tab.dbName = form.dbName
  tab.schemaName = form.schemaName
  tab.dbType = selectedDbType.value
  tab.sqlCache = sqlCache
  tab.tableName = form.tableName
  activeTab.value = tab.id
}

function setResultError(message: string, sqlCache: string) {
  const activeResultTab = queryTabs.value.find((tab) => tab.id === activeTab.value && tab.kind === 'result')
  const tab = activeResultTab || createResultTab(undefined, sqlCache)
  tab.error = message
  tab.payload = null
  tab.instanceName = selectedInstance.value?.instance_name || ''
  tab.dbName = form.dbName
  tab.schemaName = form.schemaName
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
  void activateWorkspaceTab(queryTabs.value[queryTabs.value.length - 1]?.id || 'history')
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

function structureDdl(payload: QueryDescribePayload | null) {
  if (!payload || payload.display_mode !== 'ddl') {
    return ''
  }

  const firstRow = payload.rows[0]
  if (!firstRow) {
    return ''
  }

  const ddlColumn = payload.column_list[1] || Object.keys(firstRow)[1]
  if (!ddlColumn) {
    return ''
  }

  const ddl = firstRow[ddlColumn] || Object.values(firstRow)[1]
  return typeof ddl === 'string' ? ddl : ''
}

function extractColumnType(definition: string) {
  const trimmed = definition.trim()
  const stopKeywords = new Set([
    'NOT',
    'NULL',
    'DEFAULT',
    'COMMENT',
    'AUTO_INCREMENT',
    'PRIMARY',
    'KEY',
    'UNIQUE',
    'REFERENCES',
    'CHECK',
    'COLLATE',
    'CHARACTER',
    'GENERATED',
    'VIRTUAL',
    'STORED',
    'ON',
    'UPDATE',
  ])

  const tokens = trimmed.split(/\s+/)
  const collected: string[] = []

  for (const token of tokens) {
    if (collected.length > 0 && stopKeywords.has(token.toUpperCase())) {
      break
    }
    collected.push(token)
  }

  return collected.join(' ')
}

function parseStructureFromDdl(payload: QueryDescribePayload | null) {
  const ddl = structureDdl(payload)
  const columns: QueryMetadataTableColumn[] = []
  const indexes: QueryMetadataTableIndex[] = []

  if (!ddl) {
    return { columns, indexes }
  }

  for (const rawLine of ddl.split('\n')) {
    const line = rawLine.trim().replace(/,$/, '')
    if (!line) {
      continue
    }

    if (line.startsWith('`')) {
      const columnMatch = line.match(/^`([^`]+)`\s+(.+)$/)
      if (!columnMatch) {
        continue
      }

      const name = columnMatch[1] || ''
      const definition = columnMatch[2] || ''
      columns.push({
        name,
        type: extractColumnType(definition),
        details: definition,
      })
      continue
    }

    const upperLine = line.toUpperCase()
    if (
      upperLine.startsWith('PRIMARY KEY')
      || upperLine.startsWith('UNIQUE KEY')
      || upperLine.startsWith('KEY ')
      || upperLine.startsWith('FULLTEXT KEY')
      || upperLine.startsWith('SPATIAL KEY')
    ) {
      const keyMatch = line.match(/^(PRIMARY KEY|UNIQUE KEY|FULLTEXT KEY|SPATIAL KEY|KEY)\s*(?:`([^`]+)`)?\s*\((.+)\)/i)
      if (!keyMatch) {
        continue
      }

      const keyType = keyMatch[1] || 'KEY'
      const keyName = keyMatch[2] || ''
      const keyColumns = keyMatch[3] || ''
      indexes.push({
        name: keyName || 'PRIMARY',
        type: keyType.toUpperCase(),
        columns: keyColumns.replace(/`/g, ''),
      })
    }
  }

  return { columns, indexes }
}

function parseStructureFromRows(payload: QueryDescribePayload | null) {
  const columns: QueryMetadataTableColumn[] = []
  const indexes: QueryMetadataTableIndex[] = []

  if (!payload || payload.display_mode !== 'table') {
    return { columns, indexes }
  }

  const rows = payload.rows || []
  const hasFieldLikeShape = rows.some((row) =>
    'Field' in row
    || 'field' in row
    || 'COLUMN_NAME' in row
    || 'column_name' in row,
  )

  if (hasFieldLikeShape) {
    for (const row of rows) {
      const name = String(
        row.Field
        ?? row.field
        ?? row.COLUMN_NAME
        ?? row.column_name
        ?? row.column
        ?? '',
      )
      if (!name) {
        continue
      }

      const type = String(
        row.Type
        ?? row.type
        ?? row.DATA_TYPE
        ?? row.data_type
        ?? row.column_type
        ?? '',
      )

      const detailParts = Object.entries(row)
        .filter(([key, value]) => value !== null && value !== '' && !['Field', 'field', 'COLUMN_NAME', 'column_name', 'Type', 'type', 'DATA_TYPE', 'data_type', 'column_type'].includes(key))
        .map(([key, value]) => `${key}: ${String(value)}`)

      columns.push({
        name,
        type,
        details: detailParts.join(' • '),
      })
    }
    return { columns, indexes }
  }

  for (const row of rows) {
    const keyName = String(row.Key_name ?? row.key_name ?? row.index_name ?? row.INDEX_NAME ?? '')
    const seq = row.Seq_in_index ?? row.seq_in_index ?? row.COLUMN_POSITION ?? row.ordinal_position
    if (!keyName && seq === undefined) {
      continue
    }

    indexes.push({
      name: keyName || 'INDEX',
      type: String(row.Non_unique === 0 || row.non_unique === 0 ? 'UNIQUE' : row.Index_type ?? row.index_type ?? 'INDEX'),
      columns: String(row.Column_name ?? row.column_name ?? row.COLUMN_NAME ?? ''),
    })
  }

  return { columns, indexes }
}

function applyEditorValue(value: string) {
  sqlText.value = value
  editorRef.value?.setValue(value)
  editorRef.value?.focus()
}

function createNodeId(kind: QueryMetadataNode['kind'], dbName: string, name: string, schemaName = '') {
  return [kind, dbName, schemaName, name].join('::')
}

function buildDatabaseNodes(items: string[]) {
  return items.map<QueryMetadataNode>((dbName) => ({
    id: createNodeId('database', dbName, dbName),
    kind: 'database',
    name: dbName,
    dbName,
    schemaName: '',
    children: [],
    isExpanded: false,
    isLoading: false,
    isLoaded: false,
  }))
}

function buildSchemaNodes(dbName: string, items: string[]) {
  return items.map<QueryMetadataNode>((schemaName) => ({
    id: createNodeId('schema', dbName, schemaName, schemaName),
    kind: 'schema',
    name: schemaName,
    dbName,
    schemaName,
    children: [],
    isExpanded: false,
    isLoading: false,
    isLoaded: false,
  }))
}

function buildTableNodes(dbName: string, items: string[], schemaName = '') {
  return items.map<QueryMetadataNode>((tableName) => ({
    id: createNodeId('table', dbName, tableName, schemaName),
    kind: 'table',
    name: tableName,
    dbName,
    schemaName,
    children: [],
    isExpanded: false,
    isLoading: false,
    isLoaded: true,
  }))
}

function findNodeById(nodes: QueryMetadataNode[], nodeId: string): QueryMetadataNode | null {
  for (const node of nodes) {
    if (node.id === nodeId) {
      return node
    }

    const child = findNodeById(node.children, nodeId)
    if (child) {
      return child
    }
  }

  return null
}

function quoteIdentifier(identifier: string) {
  switch (selectedDbType.value) {
    case 'mysql':
    case 'clickhouse':
    case 'doris':
      return `\`${identifier}\``
    case 'pgsql':
    case 'oracle':
    case 'mssql':
      return `"${identifier}"`
    default:
      return identifier
  }
}

function buildTableReference(node: QueryMetadataNode) {
  if (needsSchemaSelection.value && node.schemaName) {
    return [node.schemaName, node.name].map(quoteIdentifier).join('.')
  }

  return [node.dbName, node.name].map(quoteIdentifier).join('.')
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
        query_log_id: historyFilters.aliasId === 'all' ? undefined : Number(historyFilters.aliasId),
      },
      requireToken(),
    )
  } finally {
    historyLoading.value = false
  }
}

async function loadExplorerRoots() {
  if (!form.instanceId) {
    explorerNodes.value = []
    return
  }

  const generation = explorerGeneration.value
  explorerLoading.value = true
  explorerError.value = ''

  try {
    const payload = await fetchInstanceResources(form.instanceId, 'database', requireToken())
    if (generation !== explorerGeneration.value) {
      return
    }
    explorerNodes.value = buildDatabaseNodes(payload.result)
  } catch (error) {
    if (generation !== explorerGeneration.value) {
      return
    }
    explorerError.value = toUserFacingMessage(error, 'Failed to load instance metadata.')
    explorerNodes.value = []
  } finally {
    if (generation === explorerGeneration.value) {
      explorerLoading.value = false
    }
  }
}

async function selectInstance(instanceId: number) {
  form.instanceId = instanceId
  clearQueryContext()
  explorerNodes.value = []
  explorerError.value = ''
  explorerGeneration.value += 1

  if (!instanceId) {
    return
  }

  await loadExplorerRoots()
}

async function loadNodeChildren(node: QueryMetadataNode) {
  if (!form.instanceId || node.kind === 'table') {
    return
  }

  const generation = explorerGeneration.value
  node.isLoading = true
  explorerError.value = ''

  try {
    if (node.kind === 'database') {
      if (needsSchemaSelection.value) {
        const payload = await fetchInstanceResources(form.instanceId, 'schema', requireToken(), {
          db_name: node.dbName,
        })
        if (generation !== explorerGeneration.value) {
          return
        }
        node.children = buildSchemaNodes(node.dbName, payload.result)
      } else {
        const payload = await fetchInstanceResources(form.instanceId, 'table', requireToken(), {
          db_name: node.dbName,
        })
        if (generation !== explorerGeneration.value) {
          return
        }
        node.children = buildTableNodes(node.dbName, payload.result)
      }
    } else {
      const payload = await fetchInstanceResources(form.instanceId, 'table', requireToken(), {
        db_name: node.dbName,
        schema_name: node.schemaName,
      })
      if (generation !== explorerGeneration.value) {
        return
      }
      node.children = buildTableNodes(node.dbName, payload.result, node.schemaName)
    }
    node.isLoaded = true
  } catch (error) {
    if (generation !== explorerGeneration.value) {
      return
    }
    explorerError.value = toUserFacingMessage(error, 'Failed to load metadata children.')
  } finally {
    if (generation === explorerGeneration.value) {
      node.isLoading = false
    }
  }
}

async function toggleExplorerNode(nodeId: string) {
  const node = findNodeById(explorerNodes.value, nodeId)
  if (!node) {
    return
  }

  if (node.kind === 'table') {
    node.isExpanded = !node.isExpanded

    if (node.isExpanded) {
      selectedExplorerNodeId.value = node.id
      form.dbName = node.dbName
      form.schemaName = node.schemaName
      form.tableName = node.name
      await showTableStructure(node)
    }

    return
  }

  node.isExpanded = !node.isExpanded
  if (node.isExpanded && !node.isLoaded) {
    await loadNodeChildren(node)
  }
}

async function selectExplorerNode(nodeId: string) {
  const node = findNodeById(explorerNodes.value, nodeId)
  if (!node) {
    return
  }

  if (node.kind === 'database') {
    ensureEditorTab()
  }

  selectedExplorerNodeId.value = node.id
  form.dbName = node.dbName
  form.schemaName = node.kind === 'schema' || node.kind === 'table' ? node.schemaName : ''
  form.tableName = node.kind === 'table' ? node.name : ''

  if (node.kind === 'table') {
    node.isExpanded = true
    await showTableStructure(node)
  }
}

async function insertExplorerNode(nodeId: string) {
  const node = findNodeById(explorerNodes.value, nodeId)
  if (!node || node.kind !== 'table') {
    return
  }

  ensureEditorTab()
  await selectExplorerNode(nodeId)
  editorRef.value?.insertText(buildTableReference(node))
  pushToast(`Inserted ${node.name} into the editor.`, 'success')
}

async function restoreQueryContext(instanceName: string, dbName: string, statement: string) {
  const instance = instances.value.find((item) => item.instance_name === instanceName)
  if (!instance) {
    throw new Error(`Instance "${instanceName}" is not available to this user.`)
  }

  await selectInstance(instance.id)
  form.dbName = dbName
  const dbNode = findNodeById(explorerNodes.value, createNodeId('database', dbName, dbName))
  selectedExplorerNodeId.value = dbNode?.id || ''
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
    await openQueryInNewTab(
      item.instance_name,
      item.db_name,
      item.sqllog,
      item.alias?.trim() || `Query Log #${item.id}`,
    )
    pushToast(`Opened query log #${item.id} in a new runtime tab.`, 'success')
  } catch (error) {
    pushToast(toUserFacingMessage(error, 'Failed to restore the selected query log.'), 'error')
  }
}

async function toggleFavorite(item: QueryLogRecord) {
  const nextStar = !item.favorite
  let alias = item.alias

  if (nextStar) {
    const promptValue = window.prompt('Favorite alias', item.alias || item.sqllog.slice(0, 48))
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

async function showTableStructure(node: QueryMetadataNode) {
  if (!selectedInstance.value || node.kind !== 'table') {
    return
  }

  try {
    const payload = await describeQueryTable(
      {
        instance_id: selectedInstance.value.id,
        db_name: node.dbName,
        schema_name: needsSchemaSelection.value ? node.schemaName : undefined,
        tb_name: node.name,
      },
      requireToken(),
    )
    tableStructureByNode.value = {
      ...tableStructureByNode.value,
      [node.id]: payload,
    }
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

  if (!canRunQueries.value) {
    pushToast('Your account cannot run queries.', 'error')
    return
  }
  if (queryRunning.value) {
    return
  }
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

    if (canSeeWorkspace.value) {
      await loadInstances()
      await Promise.all([loadFavorites(), loadHistory()])
    }
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
const historyStart = computed(() => {
  if (historyPage.value.count === 0) {
    return 0
  }
  return (historyFilters.page - 1) * historyFilters.size + 1
})
const historyEnd = computed(() => {
  return Math.min(historyFilters.page * historyFilters.size, historyPage.value.count)
})
const explorerTableDetails = computed<QueryMetadataTableDetailsMap>(() => {
  return Object.fromEntries(
    Object.entries(tableStructureByNode.value).map(([nodeId, payload]) => {
      const fromRows = parseStructureFromRows(payload)
      const fromDdl = payload?.display_mode === 'ddl'
        ? parseStructureFromDdl(payload)
        : { columns: [], indexes: [] }

      return [
        nodeId,
        {
          nodeId,
          columns: fromRows.columns.length > 0 ? fromRows.columns : fromDdl.columns,
          indexes: fromRows.indexes.length > 0 ? fromRows.indexes : fromDdl.indexes,
        },
      ]
    }),
  )
})

watch(showRedisHelp, (enabled) => {
  if (!enabled && activeTab.value === 'redis-help') {
    activeTab.value = 'history'
  }
})

watch(sqlText, (value) => {
  const tab = queryTabs.value.find((item) => item.id === activeTab.value)
  if (tab) {
    tab.sqlCache = value
  }
})

onMounted(() => {
  void loadWorkspace()
})

onBeforeUnmount(() => {
  stopEditorResize()
  for (const timer of toastTimers.values()) {
    window.clearTimeout(timer)
  }
  toastTimers.clear()
})
</script>

<template>
  <section class="grid gap-6">
    <div class="flex flex-col gap-3 lg:flex-row lg:items-end lg:justify-between">
      <div class="space-y-1">
        <p class="text-xs font-semibold uppercase tracking-[0.24em] text-slate-500">Queries</p>
        <h1 class="flex items-center gap-2 text-2xl font-semibold text-slate-900">
          <Database class="h-5 w-5 text-slate-500" />
          SQL Query Center
        </h1>
        <p class="max-w-3xl text-sm text-slate-500">
          Browse instance objects on the left, write SQL in the editor, and inspect table structure without leaving the workspace.
        </p>
      </div>
    </div>

    <div v-if="pageError" class="rounded-3xl border border-red-200 bg-red-50 px-5 py-4 text-sm text-red-700">
      {{ pageError }}
    </div>

    <div class="grid gap-6 xl:grid-cols-[320px_minmax(0,1fr)]">
      <QueryMetadataExplorer
        :instances="instances"
        :selected-instance-id="form.instanceId"
        :selected-node-id="selectedExplorerNodeId"
        :nodes="explorerNodes"
        :loading="instancesLoading || workspaceLoading"
        :tree-loading="explorerLoading"
        :table-details="explorerTableDetails"
        :error="explorerError"
        @select-instance="void selectInstance($event)"
        @toggle-node="void toggleExplorerNode($event)"
        @select-node="void selectExplorerNode($event)"
        @insert-node="void insertExplorerNode($event)"
      />

      <div class="grid gap-6">
        <div
          v-if="!canSeeWorkspace"
          class="rounded-3xl border border-amber-200 bg-amber-50 px-5 py-4 text-sm text-amber-800"
        >
          Your account does not have access to the SQL query workspace. Use the Permission Management page if you need temporary access.
        </div>

        <template v-else>
          <div class="overflow-hidden rounded-[1.5rem] border border-slate-200 bg-white shadow-sm">
            <div class="flex flex-col gap-3 border-b border-slate-200 px-5 py-4 lg:flex-row lg:items-end lg:justify-between">
              <div class="flex flex-wrap items-end gap-2">
                <button
                  v-if="showRedisHelp"
                  type="button"
                  class="rounded-t-xl border border-b-0 px-4 py-2 text-sm font-medium transition"
                  :class="
                    activeTab === 'redis-help'
                      ? 'bg-white text-slate-900 shadow-sm'
                      : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
                  "
                  @click="void activateWorkspaceTab('redis-help')"
                >
                  Redis Help
                </button>
                <button
                  type="button"
                  class="rounded-t-xl border border-b-0 px-4 py-2 text-sm font-medium transition"
                  :class="
                    activeTab === 'history'
                      ? 'bg-white text-slate-900 shadow-sm'
                      : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
                  "
                  @click="void activateWorkspaceTab('history')"
                >
                  Query History
                </button>
                <button
                  v-for="tab in queryTabs"
                  :key="tab.id"
                  type="button"
                  class="group flex items-center gap-2 rounded-t-xl border border-b-0 px-4 py-2 text-sm font-medium transition"
                  :class="
                    activeTab === tab.id
                      ? 'bg-white text-slate-900 shadow-sm'
                      : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
                  "
                  @click="void activateWorkspaceTab(tab.id)"
                  @dblclick.stop="renameWorkspaceTab(tab.id)"
                >
                  <span class="max-w-[180px] truncate">{{ tab.title }}</span>
                  <span
                    class="rounded-full p-1 text-slate-400 opacity-0 transition group-hover:opacity-100 hover:bg-slate-200 hover:text-slate-700"
                    @click.stop="renameWorkspaceTab(tab.id)"
                  >
                    <Pencil class="h-3.5 w-3.5" />
                    <span class="sr-only">Rename tab</span>
                  </span>
                </button>
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

            <div v-if="activeTab !== 'history'" class="grid gap-4 p-5">
              <div class="flex flex-col gap-3 xl:flex-row xl:items-center xl:justify-between">
                <div class="flex flex-wrap items-center gap-3">
                  <label class="text-xs font-semibold uppercase tracking-[0.24em] text-slate-500">Common Queries</label>
                  <select
                    v-model="commonQueryId"
                    class="min-w-[240px]"
                    :class="selectClass(commonQueryDisabled)"
                    :disabled="commonQueryDisabled"
                    @change="void selectCommonQuery()"
                  >
                    <option value="">{{ commonQueryDisabled ? 'No saved queries' : 'Select a saved query' }}</option>
                    <option v-for="favorite in favorites" :key="favorite.id" :value="`${favorite.id}`">
                      {{ favorite.alias || `${favorite.instance_name} / ${favorite.db_name}` }}
                    </option>
                  </select>
                  <span v-if="favoritesLoading" class="text-xs text-slate-500">Loading favorites...</span>
                </div>

                <div class="flex flex-wrap items-center gap-2">
                  <select v-model.number="form.limitNum" :class="selectClass(false)">
                    <option :value="100">100 rows</option>
                    <option :value="500">500 rows</option>
                    <option :value="1000">1000 rows</option>
                    <option :value="0">Maximum allowed rows</option>
                  </select>
                  <Button variant="outline" :disabled="!canFormat || workspaceLoading" @click="void formatCurrentSql()">
                    <Wand2 class="h-4 w-4" />
                    Format
                  </Button>
                  <Button
                    variant="secondary"
                    :disabled="!canRunQueries || !canExplain || queryRunning"
                    @click="void runQuery('plan')"
                  >
                    <Sparkles class="h-4 w-4" />
                    Execution Plan
                  </Button>
                  <Button :disabled="!canRunQueries || queryRunning" @click="void runQuery('query')">
                    <Play class="h-4 w-4" />
                    {{ queryRunning ? 'Running...' : 'Run Query' }}
                  </Button>
                </div>
              </div>

              <SqlCodeEditor
                ref="editorRef"
                v-model="sqlText"
                :db-type="selectedDbType"
                :disabled="workspaceLoading"
                :height="editorPaneHeight"
                @submit="void runQuery('query')"
              />

              <div class="flex flex-wrap gap-x-5 gap-y-2 px-1 text-xs text-slate-500">
                <p>Single-click a table to inspect its structure.</p>
                <p>Double-click a table to insert its qualified name into the editor.</p>
                <p>If you select part of the editor, only that selection is executed.</p>
              </div>
            </div>

            <button
              v-if="activeTab !== 'history'"
              type="button"
              class="group flex w-full cursor-row-resize items-center gap-3 border-t border-slate-200 bg-slate-50/80 px-5 py-3 transition hover:bg-slate-100/80"
              @mousedown.prevent="startEditorResize"
            >
              <span class="h-px flex-1 bg-slate-200 transition group-hover:bg-slate-300" />
              <span class="flex items-center gap-1 rounded-full border border-slate-200 bg-white px-3 py-1 text-[11px] font-medium uppercase tracking-[0.18em] text-slate-500 shadow-sm">
                <span class="h-1.5 w-1.5 rounded-full bg-slate-300" />
                <span class="h-1.5 w-1.5 rounded-full bg-slate-300" />
                Resize
                <span class="h-1.5 w-1.5 rounded-full bg-slate-300" />
                <span class="h-1.5 w-1.5 rounded-full bg-slate-300" />
              </span>
              <span class="h-px flex-1 bg-slate-200 transition group-hover:bg-slate-300" />
            </button>

            <div class="bg-slate-50/40">
              <div class="p-0">
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
                        <tr
                          v-for="item in historyPage.results"
                          :key="item.id"
                          class="align-top transition hover:bg-slate-50/80 cursor-pointer"
                          @click="void rerunHistoryItem(item)"
                        >
                          <td class="px-4 py-3">
                            <div class="flex gap-2">
                              <Button variant="outline" size="sm" @click.stop="void rerunHistoryItem(item)">
                                <RefreshCcw class="h-4 w-4" />
                              </Button>
                              <Button
                                :variant="item.favorite ? 'default' : 'outline'"
                                size="sm"
                                @click.stop="void toggleFavorite(item)"
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
              <div v-if="currentTab.error" class="rounded-3xl border border-red-200 bg-red-50 px-5 py-4 text-sm text-red-700">
                {{ currentTab.error }}
              </div>

              <div v-else class="overflow-hidden rounded-[1.5rem] border border-slate-200 bg-white">
                <div class="flex flex-col gap-2 border-b border-slate-200 px-5 py-4 lg:flex-row lg:items-center lg:justify-between">
                  <div>
                    <p class="text-sm font-medium text-slate-900">
                      {{ tableRows(currentTab.payload).length }} rows returned
                    </p>
                    <p class="mt-1 text-xs text-slate-500">
                      {{ currentTab.instanceName || 'No instance' }} / {{ currentTab.dbName || 'No database' }}
                      <span v-if="form.schemaName"> / {{ form.schemaName }}</span>
                    </p>
                  </div>
                  <div class="flex flex-wrap gap-4 text-xs text-slate-500">
                    <span v-if="currentTab.payload && 'query_time' in currentTab.payload">
                      Execution {{ currentTab.payload.query_time }} sec
                    </span>
                    <span v-if="currentTab.payload && 'mask_time' in currentTab.payload && currentTab.payload.mask_time">
                      Mask {{ currentTab.payload.mask_time }} sec
                    </span>
                  </div>
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
                </div>
              </div>
            </div>
          </div>
        </template>
      </div>
    </div>

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
