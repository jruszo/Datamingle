<script setup lang="ts">
import 'gridstack/dist/gridstack.min.css'

import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { onBeforeRouteLeave, useRoute, useRouter } from 'vue-router'
import { GridStack, type GridStackNode } from 'gridstack'
import {
  ArrowLeft,
  Copy,
  Ellipsis,
  Maximize2,
  Pencil,
  Plus,
  RefreshCw,
  Save,
  Settings,
  Trash2,
  X,
} from 'lucide-vue-next'

import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import {
  DashboardConflictError,
  createMetricsDashboard,
  fetchMetricsDashboard,
  updateMetricsDashboard,
  type DashboardPanel,
  type DashboardVariable,
  type DashboardWritePayload,
  type MetricsDashboard,
} from '@/features/dashboards/api'
import DashboardLinePanel from '@/features/dashboards/components/DashboardLinePanel.vue'
import { nextDashboardPanelY } from '@/features/dashboards/layout'
import GraphEditor from '@/features/graph-editor/GraphEditor.vue'
import {
  cloneDashboardData,
  clonePanel,
  createGraphPanel,
  createUuid,
} from '@/features/graph-editor/model'
import { fetchMetricLabelValues } from '@/features/metrics/api'
import { useAuthStore } from '@/stores/auth'

const route = useRoute()
const router = useRouter()
const authStore = useAuthStore()
const dashboard = ref<MetricsDashboard | null>(null)
const draft = ref<DashboardWritePayload | null>(null)
const baseline = ref('')
const loading = ref(false)
const saving = ref(false)
const error = ref('')
const editing = ref(false)
const settingsOpen = ref(false)
const refreshTick = ref(0)
const gridRoot = ref<HTMLElement | null>(null)
const panelDraft = ref<DashboardPanel | null>(null)
const panelMenuId = ref('')
const fullscreenPanel = ref<DashboardPanel | null>(null)
const conflictLatest = ref<MetricsDashboard | null>(null)
const variableOptions = ref<Record<string, string[]>>({})
const variableValues = ref<Record<string, string[]>>({})
let grid: GridStack | null = null
let refreshTimer: ReturnType<typeof window.setInterval> | undefined

const timeRanges = [
  { value: 3600, label: 'Last 1 hour' },
  { value: 21600, label: 'Last 6 hours' },
  { value: 86400, label: 'Last 24 hours' },
  { value: 604800, label: 'Last 7 days' },
]
const refreshIntervals = [
  { value: 0, label: 'Refresh off' },
  { value: 30, label: 'Every 30 seconds' },
  { value: 60, label: 'Every minute' },
  { value: 300, label: 'Every 5 minutes' },
]

const isDirty = computed(() =>
  Boolean(draft.value && JSON.stringify(draft.value) !== baseline.value),
)
const panelRoute = computed(() => String(route.query.panel ?? ''))

function requireToken() {
  if (!authStore.accessToken) {
    throw new Error('Missing access token. Please login again.')
  }
  return authStore.accessToken
}

function toDraft(source: MetricsDashboard): DashboardWritePayload {
  return {
    name: source.name,
    description: source.description,
    time_range_seconds: source.time_range_seconds,
    refresh_interval_seconds: source.refresh_interval_seconds,
    variables: cloneDashboardData(source.variables ?? []),
    panels: cloneDashboardData(source.panels),
  }
}

function setLoadedDashboard(source: MetricsDashboard) {
  dashboard.value = source
  draft.value = toDraft(source)
  baseline.value = JSON.stringify(draft.value)
  conflictLatest.value = null
  void loadVariableOptions()
}

async function loadDashboard() {
  const dashboardId = Number(route.params.dashboardId)
  if (!Number.isInteger(dashboardId)) {
    error.value = 'Invalid dashboard ID.'
    return
  }
  loading.value = true
  error.value = ''
  let loaded = false
  try {
    setLoadedDashboard(await fetchMetricsDashboard(dashboardId, requireToken()))
    loaded = true
  } catch (loadError) {
    error.value = loadError instanceof Error ? loadError.message : 'Failed to load dashboard.'
  } finally {
    loading.value = false
  }
  if (loaded) {
    await nextTick()
    initializeGrid()
    configureRefreshTimer()
    syncPanelRoute()
  }
}

function updatePanelLayout(node: GridStackNode) {
  if (!draft.value) return
  const panelId = node.el?.dataset.panelId ?? node.id
  const panel = draft.value.panels.find((item) => item.id === panelId)
  if (!panel) return
  panel.layout = {
    x: node.x ?? panel.layout.x,
    y: node.y ?? panel.layout.y,
    w: node.w ?? panel.layout.w,
    h: node.h ?? panel.layout.h,
  }
}

function snapshotGridLayout() {
  for (const node of grid?.engine.nodes ?? []) {
    updatePanelLayout(node)
  }
}

function destroyGrid() {
  grid?.destroy(false)
  grid = null
}

function initializeGrid() {
  destroyGrid()
  if (!gridRoot.value) {
    return
  }
  grid = GridStack.init(
    {
      column: 12,
      cellHeight: 82,
      margin: 8,
      float: true,
      disableDrag: !editing.value,
      disableResize: !editing.value,
      handle: '.dashboard-panel-handle',
      resizable: { handles: 'e,se,s,sw,w' },
    },
    gridRoot.value,
  )
  grid.on('change', (_event: Event, nodes: GridStackNode[]) => {
    if (!editing.value || !draft.value) return
    for (const node of nodes ?? []) {
      updatePanelLayout(node)
    }
  })
  grid.on('dragstop resizestop', (_event: Event, element: HTMLElement) => {
    const node = (element as HTMLElement & { gridstackNode?: GridStackNode }).gridstackNode
    if (node) updatePanelLayout(node)
  })
}

function configureRefreshTimer() {
  if (refreshTimer) window.clearInterval(refreshTimer)
  refreshTimer = undefined
  const interval = draft.value?.refresh_interval_seconds ?? 0
  if (interval > 0) {
    refreshTimer = window.setInterval(() => refreshTick.value += 1, interval * 1000)
  }
}

function toggleEditing() {
  if (editing.value) {
    snapshotGridLayout()
  }
  editing.value = !editing.value
  grid?.enableMove(editing.value)
  grid?.enableResize(editing.value)
}

async function setPanelRoute(value?: string) {
  const query = { ...route.query }
  if (value) query.panel = value
  else delete query.panel
  try {
    await router.replace({ query })
  } catch {
    // The editor state is local-first; URL synchronization is best effort.
  }
}

function syncPanelRoute() {
  if (!draft.value || !panelRoute.value) {
    panelDraft.value = null
    return
  }
  if (panelRoute.value === 'new') {
    if (panelDraft.value && !draft.value.panels.some((panel) => panel.id === panelDraft.value?.id)) {
      return
    }
    const panel = createGraphPanel('', 'New panel')
    panel.layout.y = nextDashboardPanelY(draft.value.panels)
    panelDraft.value = panel
    return
  }
  const existing = draft.value.panels.find((panel) => panel.id === panelRoute.value)
  panelDraft.value = existing ? clonePanel(existing) : null
}

function openNewPanel() {
  if (!draft.value) return
  panelMenuId.value = ''
  const panel = createGraphPanel('', 'New panel')
  panel.layout.y = nextDashboardPanelY(draft.value.panels)
  panelDraft.value = panel
  void setPanelRoute('new')
}

function openEditPanel(panel: DashboardPanel) {
  panelMenuId.value = ''
  panelDraft.value = clonePanel(panel)
  void setPanelRoute(panel.id)
}

async function closePanelEditor() {
  panelDraft.value = null
  await setPanelRoute()
}

async function applyPanel(panel: DashboardPanel) {
  if (!draft.value || !panel.title.trim() || !panel.queries.some((query) => query.query.trim())) {
    error.value = 'A panel title and at least one query are required.'
    return
  }
  snapshotGridLayout()
  destroyGrid()
  const index = draft.value.panels.findIndex((item) => item.id === panel.id)
  if (index >= 0) draft.value.panels[index] = clonePanel(panel)
  else draft.value.panels.push(clonePanel(panel))
  await closePanelEditor()
  editing.value = true
  await nextTick()
  initializeGrid()
}

function duplicatePanel(panel: DashboardPanel) {
  if (!draft.value) return
  snapshotGridLayout()
  destroyGrid()
  const copy = clonePanel(panel)
  copy.id = createUuid()
  copy.title = `${copy.title} copy`
  copy.layout = { ...copy.layout, x: 0, y: nextDashboardPanelY(draft.value.panels) }
  draft.value.panels.push(copy)
  panelMenuId.value = ''
  void nextTick(initializeGrid)
}

function removePanel(panel: DashboardPanel) {
  if (!draft.value || !window.confirm(`Remove panel "${panel.title}"?`)) return
  snapshotGridLayout()
  destroyGrid()
  draft.value.panels = draft.value.panels.filter((item) => item.id !== panel.id)
  panelMenuId.value = ''
  void nextTick(initializeGrid)
}

function addVariable() {
  if (!draft.value) return
  const index = draft.value.variables.length + 1
  draft.value.variables.push({
    name: `variable_${index}`,
    label: `Variable ${index}`,
    metric: '',
    label_name: 'instance',
    multi: false,
    include_all: false,
  })
}

function removeVariable(index: number) {
  draft.value?.variables.splice(index, 1)
  void loadVariableOptions()
}

async function loadVariableOptions() {
  if (!draft.value || !authStore.accessToken) return
  const entries = await Promise.all(
    draft.value.variables.map(async (variable) => {
      try {
        return [
          variable.name,
          await fetchMetricLabelValues(
            variable.label_name,
            requireToken(),
            variable.metric,
          ),
        ] as const
      } catch {
        return [variable.name, []] as const
      }
    }),
  )
  variableOptions.value = Object.fromEntries(entries)
  const nextValues: Record<string, string[]> = {}
  for (const variable of draft.value.variables) {
    const routeValue = route.query[`var-${variable.name}`]
    const selected = Array.isArray(routeValue) ? routeValue : routeValue ? [routeValue] : []
    nextValues[variable.name] = selected.map(String)
  }
  variableValues.value = nextValues
}

async function updateVariableValue(variable: DashboardVariable, event: Event) {
  const select = event.target as HTMLSelectElement
  const selected = [...select.selectedOptions].map((option) => option.value)
  variableValues.value = { ...variableValues.value, [variable.name]: selected }
  const query = { ...route.query }
  if (selected.length) query[`var-${variable.name}`] = variable.multi ? selected : selected[0]!
  else delete query[`var-${variable.name}`]
  await router.replace({ query })
  refreshTick.value += 1
}

async function saveDashboard() {
  if (!dashboard.value || !draft.value) return
  snapshotGridLayout()
  saving.value = true
  error.value = ''
  try {
    const saved = await updateMetricsDashboard(
      dashboard.value.id,
      dashboard.value.revision,
      draft.value,
      requireToken(),
    )
    destroyGrid()
    setLoadedDashboard(saved)
    await nextTick()
    initializeGrid()
  } catch (saveError) {
    if (saveError instanceof DashboardConflictError) conflictLatest.value = saveError.latest
    else error.value = saveError instanceof Error ? saveError.message : 'Failed to save dashboard.'
  } finally {
    saving.value = false
  }
}

async function reloadLatest() {
  if (!conflictLatest.value) return
  setLoadedDashboard(conflictLatest.value)
  await nextTick()
  initializeGrid()
}

async function saveDraftAsNew() {
  if (!draft.value) return
  saving.value = true
  try {
    const created = await createMetricsDashboard(
      { ...cloneDashboardData(draft.value), name: `${draft.value.name} (copy)` },
      requireToken(),
    )
    await router.push(`/dashboards/${created.id}`)
  } catch (saveError) {
    error.value = saveError instanceof Error ? saveError.message : 'Failed to save dashboard copy.'
  } finally {
    saving.value = false
  }
}

function handleKeyboard(event: KeyboardEvent) {
  if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 's') {
    event.preventDefault()
    if (isDirty.value) void saveDashboard()
  }
  if (event.key === 'Escape') {
    if (panelDraft.value) void closePanelEditor()
    else if (fullscreenPanel.value) fullscreenPanel.value = null
    else settingsOpen.value = false
  }
}

function beforeUnload(event: BeforeUnloadEvent) {
  if (isDirty.value) event.preventDefault()
}

onBeforeRouteLeave(() =>
  !isDirty.value || window.confirm('Discard unsaved dashboard changes?'),
)

onMounted(() => {
  window.addEventListener('beforeunload', beforeUnload)
  window.addEventListener('keydown', handleKeyboard)
  void loadDashboard()
})

onBeforeUnmount(() => {
  window.removeEventListener('beforeunload', beforeUnload)
  window.removeEventListener('keydown', handleKeyboard)
  if (refreshTimer) window.clearInterval(refreshTimer)
  grid?.destroy(false)
})

watch(() => draft.value?.refresh_interval_seconds, configureRefreshTimer)
watch(panelRoute, syncPanelRoute)
</script>

<template>
  <section class="grid gap-4">
    <div v-if="loading" class="p-8 text-center text-sm text-slate-500">Loading dashboard...</div>
    <template v-else-if="draft && dashboard">
      <div class="sticky top-0 z-20 -mx-2 flex flex-col gap-3 border-b border-slate-200 bg-slate-50/95 px-2 pb-3 backdrop-blur xl:flex-row xl:items-center xl:justify-between">
        <div class="flex min-w-0 items-center gap-3">
          <Button variant="outline" size="icon" type="button" @click="void router.push('/dashboards')">
            <ArrowLeft class="h-4 w-4" />
          </Button>
          <div class="min-w-0">
            <Input v-if="editing" v-model="draft.name" maxlength="120" class="max-w-xl font-semibold" />
            <h2 v-else class="truncate text-lg font-semibold text-slate-950">{{ draft.name }}</h2>
            <p class="text-xs text-slate-500">
              {{ isDirty ? 'Unsaved changes' : `Revision ${dashboard.revision}` }}
            </p>
          </div>
        </div>
        <div class="flex flex-wrap items-center gap-2">
          <select v-model.number="draft.time_range_seconds" class="h-9 rounded-md border border-slate-200 bg-white px-3 text-sm">
            <option v-for="option in timeRanges" :key="option.value" :value="option.value">{{ option.label }}</option>
          </select>
          <select v-model.number="draft.refresh_interval_seconds" class="h-9 rounded-md border border-slate-200 bg-white px-3 text-sm">
            <option v-for="option in refreshIntervals" :key="option.value" :value="option.value">{{ option.label }}</option>
          </select>
          <Button variant="outline" type="button" @click="refreshTick += 1">
            <RefreshCw class="h-4 w-4" /> Refresh
          </Button>
          <Button variant="outline" type="button" @click="settingsOpen = true">
            <Settings class="h-4 w-4" /> Settings
          </Button>
          <Button variant="outline" type="button" @click="toggleEditing">
            <Pencil class="h-4 w-4" /> {{ editing ? 'View mode' : 'Edit layout' }}
          </Button>
          <Button variant="outline" type="button" @click="openNewPanel">
            <Plus class="h-4 w-4" /> Add panel
          </Button>
          <Button type="button" :disabled="saving || !isDirty" @click="void saveDashboard()">
            <Save class="h-4 w-4" /> {{ saving ? 'Saving...' : 'Save dashboard' }}
          </Button>
        </div>
      </div>

      <p v-if="error" class="rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">{{ error }}</p>

      <div v-if="draft.variables.length" class="flex flex-wrap items-end gap-3 rounded-lg border border-slate-200 bg-white p-3">
        <label v-for="variable in draft.variables" :key="variable.name" class="grid gap-1 text-xs text-slate-600">
          <span>{{ variable.label }}</span>
          <select
            :multiple="variable.multi"
            class="min-w-40 rounded-md border border-slate-200 bg-white px-2 py-1.5 text-sm"
            @change="void updateVariableValue(variable, $event)"
          >
            <option v-if="variable.include_all" value="">All</option>
            <option
              v-for="option in variableOptions[variable.name] ?? []"
              :key="option"
              :value="option"
              :selected="variableValues[variable.name]?.includes(option)"
            >{{ option }}</option>
          </select>
        </label>
      </div>

      <div v-if="draft.panels.length === 0" class="flex min-h-72 items-center justify-center rounded-lg border border-dashed border-slate-300 bg-white p-8 text-center">
        <div>
          <p class="font-medium text-slate-900">Build your first dashboard panel</p>
          <p class="mt-1 text-sm text-slate-500">Add a query, preview it, and choose the right visualization.</p>
          <Button class="mt-4" type="button" @click="openNewPanel"><Plus class="h-4 w-4" /> Add panel</Button>
        </div>
      </div>

      <div v-show="draft.panels.length" ref="gridRoot" class="grid-stack min-h-72">
        <div
          v-for="panel in draft.panels"
          :key="panel.id"
          class="grid-stack-item"
          :gs-x="panel.layout.x" :gs-y="panel.layout.y" :gs-w="panel.layout.w" :gs-h="panel.layout.h"
          :gs-id="panel.id"
          gs-min-w="2" gs-max-w="12" gs-min-h="2" gs-max-h="12"
          :data-panel-id="panel.id"
        >
          <div class="grid-stack-item-content overflow-visible rounded-lg border border-slate-200 bg-white shadow-sm">
            <div :class="['dashboard-panel-handle flex h-11 items-center justify-between border-b border-slate-200 px-3', editing ? 'cursor-move bg-slate-50' : 'bg-white']">
              <div class="min-w-0">
                <p class="truncate text-sm font-semibold text-slate-900">{{ panel.title }}</p>
                <p v-if="panel.description" class="truncate text-[11px] text-slate-500">{{ panel.description }}</p>
              </div>
              <div class="flex shrink-0 items-center gap-1">
                <Button
                  variant="ghost"
                  size="icon"
                  type="button"
                  title="View fullscreen"
                  aria-label="View panel fullscreen"
                  @click.stop="fullscreenPanel = clonePanel(panel); panelMenuId = ''"
                >
                  <Maximize2 class="h-4 w-4" />
                </Button>
                <div class="relative">
                  <Button variant="ghost" size="icon" type="button" aria-label="Panel actions" @click.stop="panelMenuId = panelMenuId === panel.id ? '' : panel.id">
                    <Ellipsis class="h-4 w-4" />
                  </Button>
                  <div v-if="panelMenuId === panel.id" class="absolute right-0 top-9 z-30 w-40 rounded-md border border-slate-200 bg-white p-1 shadow-lg">
                    <button class="panel-menu-item" type="button" @click="openEditPanel(panel)"><Pencil class="h-4 w-4" /> Edit</button>
                    <button class="panel-menu-item" type="button" @click="duplicatePanel(panel)"><Copy class="h-4 w-4" /> Duplicate</button>
                    <button class="panel-menu-item text-red-600" type="button" @click="removePanel(panel)"><Trash2 class="h-4 w-4" /> Remove</button>
                  </div>
                </div>
              </div>
            </div>
            <div class="h-[calc(100%-2.75rem)] overflow-hidden">
              <DashboardLinePanel :panel="panel" :range-seconds="draft.time_range_seconds" :refresh-tick="refreshTick" :variable-values="variableValues" />
            </div>
          </div>
        </div>
      </div>
    </template>

    <Teleport to="body">
      <div
        v-if="panelDraft && draft"
        key="dashboard-panel-editor"
        class="fixed inset-0 z-50 flex flex-col bg-slate-100"
      >
        <div class="flex items-center justify-between border-b border-slate-200 bg-white px-5 py-3">
          <div>
            <h3 class="font-semibold text-slate-950">{{ panelRoute === 'new' ? 'Add panel' : 'Edit panel' }}</h3>
            <p class="text-xs text-slate-500">Changes are applied to the dashboard draft. Save the dashboard when finished.</p>
          </div>
          <Button variant="ghost" size="icon" type="button" @click="void closePanelEditor()"><X class="h-5 w-5" /></Button>
        </div>
        <div class="min-h-0 flex-1 overflow-auto p-4">
          <GraphEditor
            :model-value="panelDraft"
            :token="requireToken()"
            :range-seconds="draft.time_range_seconds"
            :variable-values="variableValues"
            show-footer
            @update:model-value="panelDraft = $event"
            @apply="void applyPanel($event)"
            @cancel="void closePanelEditor()"
          />
        </div>
      </div>
    </Teleport>

    <Teleport to="body">
      <div v-if="settingsOpen && draft" key="dashboard-settings" class="fixed inset-0 z-50 flex justify-end bg-slate-950/35" @click.self="settingsOpen = false">
        <aside class="h-full w-full max-w-xl overflow-auto bg-white shadow-xl">
          <div class="sticky top-0 flex items-center justify-between border-b border-slate-200 bg-white px-5 py-4">
            <h3 class="font-semibold text-slate-950">Dashboard settings</h3>
            <Button variant="ghost" size="icon" type="button" @click="settingsOpen = false"><X class="h-4 w-4" /></Button>
          </div>
          <div class="grid gap-5 p-5">
            <label class="grid gap-1 text-sm"><span class="font-medium">Name</span><Input v-model="draft.name" maxlength="120" /></label>
            <label class="grid gap-1 text-sm"><span class="font-medium">Description</span><textarea v-model="draft.description" rows="3" class="rounded-md border border-slate-200 p-2" /></label>
            <div class="border-t border-slate-200 pt-5">
              <div class="mb-3 flex items-center justify-between">
                <div><p class="font-medium text-slate-900">Variables</p><p class="text-xs text-slate-500">Create reusable label filters for panel queries.</p></div>
                <Button variant="outline" size="sm" type="button" @click="addVariable"><Plus class="h-4 w-4" /> Add variable</Button>
              </div>
              <div v-for="(variable, index) in draft.variables" :key="index" class="mb-3 grid gap-2 rounded-md border border-slate-200 p-3">
                <div class="grid grid-cols-2 gap-2">
                  <Input v-model="variable.label" placeholder="Display label" />
                  <Input v-model="variable.name" placeholder="Variable name" />
                  <Input v-model="variable.metric" placeholder="Metric selector (optional)" />
                  <Input v-model="variable.label_name" placeholder="Prometheus label" />
                </div>
                <div class="flex items-center justify-between">
                  <div class="flex gap-4 text-sm">
                    <label><input v-model="variable.multi" type="checkbox" /> Multi-value</label>
                    <label><input v-model="variable.include_all" type="checkbox" /> Include All</label>
                  </div>
                  <Button variant="ghost" size="icon" type="button" @click="removeVariable(index)"><Trash2 class="h-4 w-4" /></Button>
                </div>
              </div>
              <Button v-if="draft.variables.length" variant="outline" size="sm" type="button" @click="void loadVariableOptions()">Refresh variable options</Button>
            </div>
          </div>
        </aside>
      </div>
    </Teleport>

    <Teleport to="body">
      <div v-if="fullscreenPanel && draft" key="dashboard-panel-fullscreen" class="fixed inset-0 z-50 bg-white p-5">
        <div class="flex h-full flex-col">
          <div class="flex items-center justify-between border-b border-slate-200 pb-3">
            <div><h3 class="font-semibold">{{ fullscreenPanel.title }}</h3><p class="text-sm text-slate-500">{{ fullscreenPanel.description }}</p></div>
            <Button variant="ghost" size="icon" type="button" @click="fullscreenPanel = null"><X class="h-5 w-5" /></Button>
          </div>
          <div class="min-h-0 flex-1 pt-4">
            <DashboardLinePanel :panel="fullscreenPanel" :range-seconds="draft.time_range_seconds" :refresh-tick="refreshTick" :variable-values="variableValues" />
          </div>
        </div>
      </div>
    </Teleport>

    <Teleport to="body">
      <div v-if="conflictLatest" key="dashboard-conflict" class="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/45 p-4">
        <div class="w-full max-w-lg rounded-lg bg-white shadow-xl">
          <div class="border-b border-slate-200 px-5 py-4"><h3 class="font-semibold">Dashboard changed by another user</h3></div>
          <p class="p-5 text-sm text-slate-600">Reload revision {{ conflictLatest.revision }}, or preserve your draft as a new dashboard.</p>
          <div class="flex justify-end gap-2 border-t border-slate-200 px-5 py-4">
            <Button variant="outline" type="button" @click="void reloadLatest()">Reload latest</Button>
            <Button type="button" :disabled="saving" @click="void saveDraftAsNew()">Save as new dashboard</Button>
          </div>
        </div>
      </div>
    </Teleport>
  </section>
</template>

<style scoped>
.grid-stack {
  background-image:
    linear-gradient(to right, rgb(226 232 240 / 0.45) 1px, transparent 1px),
    linear-gradient(to bottom, rgb(226 232 240 / 0.45) 1px, transparent 1px);
  background-size: calc(100% / 12) 82px;
}

.panel-menu-item {
  display: flex;
  width: 100%;
  align-items: center;
  gap: 0.5rem;
  border-radius: 0.25rem;
  padding: 0.5rem;
  text-align: left;
  font-size: 0.875rem;
}

.panel-menu-item:hover {
  background: rgb(241 245 249);
}
</style>
