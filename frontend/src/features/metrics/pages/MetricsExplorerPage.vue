<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import {
  Activity,
  ArrowLeft,
  BarChart3,
  Database,
  Info,
  LayoutDashboard,
  RefreshCw,
  Search,
  Tag,
  X,
} from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import {
  DashboardConflictError,
  createMetricsDashboard,
  emptyDashboardPayload,
  listMetricsDashboards,
  updateMetricsDashboard,
  type DashboardPanel,
  type MetricsDashboard,
} from '@/features/dashboards/api'
import { nextDashboardPanelY } from '@/features/dashboards/layout'
import GraphEditor from '@/features/graph-editor/GraphEditor.vue'
import { clonePanel, createGraphPanel, createUuid } from '@/features/graph-editor/model'
import {
  fetchMetricLabelNames,
  fetchMetricMetadata,
  fetchMetricNames,
  fetchMetricSeries,
  queryMetricInstant,
  type PrometheusMetadata,
  type PrometheusSeries,
  type PrometheusSeriesSelector,
} from '@/features/metrics/api'
import TimeRangePicker from '@/features/time-range/TimeRangePicker.vue'
import { defaultTimeRange } from '@/features/time-range/model'
import { useAuthStore } from '@/stores/auth'
import MetricsFilterBar from '@/features/metrics/MetricsFilterBar.vue'
import {
  metricsFiltersSelector,
  parseMetricsFilters,
  writeMetricsFilters,
} from '@/features/metrics/filters'

type LabelSummary = {
  label: string
  values: Array<{ value: string; count: number }>
  seriesCount: number
}

const authStore = useAuthStore()
const route = useRoute()
const router = useRouter()
const loading = ref(false)
const metricSearchLoading = ref(false)
const detailLoading = ref(false)
const statsLoading = ref(false)
const error = ref('')
const viewMode = ref<'explore' | 'graph'>('explore')
const metricSearch = ref('')
const selectedMetric = ref('')
const labelNames = ref<string[]>([])
const metricNames = ref<string[]>([])
const metricSeries = ref<PrometheusSeriesSelector[]>([])
const metricMetadata = ref<PrometheusMetadata>({})
const instantSamples = ref<PrometheusSeries[]>([])
const instantLoadedAt = ref<Date | null>(null)
const queryMode = ref<'raw' | 'rate' | 'sum' | 'avg' | 'max'>('rate')
const groupBy = ref('')
const timeRange = ref(defaultTimeRange())
const stepSeconds = ref(60)
const promql = ref('')
const legendLabels = ref<string[]>([])
const addToDashboardOpen = ref(false)
const dashboardOptions = ref<MetricsDashboard[]>([])
const dashboardOptionsLoading = ref(false)
const dashboardSaving = ref(false)
const selectedDashboardId = ref<string>('new')
const newDashboardName = ref('')
const dashboardPanelTitle = ref('')
const dashboardMessage = ref('')
const explorerPanel = ref<DashboardPanel>(createGraphPanel('', 'Metrics graph'))
let detailRequestId = 0
let metricSearchRequestId = 0
let metricSearchTimer: ReturnType<typeof window.setTimeout> | undefined
const metricsFilters = computed({
  get: () => parseMetricsFilters(route.query),
  set: (filters) => {
    void router.replace({ query: writeMetricsFilters(route.query, filters) })
  },
})

const noisyLegendLabels = new Set(['agent_id', 'node_id', '__name__'])
const preferredLegendLabels = [
  'instance_name',
  'node_name',
  'db_type',
  'service_name',
  'mode',
  'cpu',
  'device',
  'mountpoint',
  'fstype',
  'interface',
  'job',
]

const filteredMetricNames = computed(() => {
  return metricNames.value
})

const selector = computed(() => {
  if (!selectedMetric.value) {
    return ''
  }
  return `${selectedMetric.value}${metricsFiltersSelector(metricsFilters.value)}`
})

const metadataEntries = computed(() => {
  if (!selectedMetric.value) {
    return []
  }
  return metricMetadata.value[selectedMetric.value] ?? []
})

const primaryMetadata = computed(() => metadataEntries.value[0] ?? {})

const filteredSeries = computed(() => {
  return metricSeries.value
})

const labelSummaries = computed<LabelSummary[]>(() => {
  const summaries = new Map<string, Map<string, number>>()
  for (const series of filteredSeries.value) {
    for (const [label, value] of Object.entries(series)) {
      if (label === '__name__' || !value) {
        continue
      }
      const values = summaries.get(label) ?? new Map<string, number>()
      values.set(value, (values.get(value) ?? 0) + 1)
      summaries.set(label, values)
    }
  }

  return [...summaries.entries()]
    .map(([label, values]) => ({
      label,
      values: [...values.entries()]
        .map(([value, count]) => ({ value, count }))
        .sort((left, right) => right.count - left.count || left.value.localeCompare(right.value)),
      seriesCount: [...values.values()].reduce((total, count) => total + count, 0),
    }))
    .sort((left, right) => left.label.localeCompare(right.label))
})

const numericInstantValues = computed(() =>
  instantSamples.value
    .map((sample) => Number.parseFloat(sample.value?.[1] ?? ''))
    .filter((value) => Number.isFinite(value)),
)

const metricStats = computed(() => {
  const values = numericInstantValues.value
  const labelValueCount = labelSummaries.value.reduce(
    (total, summary) => total + summary.values.length,
    0,
  )
  const sum = values.reduce((total, value) => total + value, 0)
  return {
    configuredSeries: filteredSeries.value.length,
    activeSeries: values.length,
    labelCount: labelSummaries.value.length,
    labelValueCount,
    min: values.length ? Math.min(...values) : null,
    max: values.length ? Math.max(...values) : null,
    avg: values.length ? sum / values.length : null,
  }
})

function requireToken() {
  if (!authStore.accessToken) {
    throw new Error('Missing access token. Please login again.')
  }
  return authStore.accessToken
}

function seriesName(labels: Record<string, string>, index: number) {
  const entries = Object.entries(labels).filter(([key]) => key !== '__name__')
  if (entries.length === 0) {
    return labels.__name__ || `series ${index + 1}`
  }
  return entries.map(([key, value]) => `${key}=${value}`).join(', ')
}

function chooseDefaultLegendLabels(series: PrometheusSeriesSelector[]) {
  const available = new Set<string>()
  for (const item of series) {
    for (const label of Object.keys(item)) {
      if (!noisyLegendLabels.has(label)) {
        available.add(label)
      }
    }
  }
  const preferred = preferredLegendLabels.filter((label) => available.has(label)).slice(0, 3)
  if (preferred.length > 0) {
    return preferred
  }
  return [...available].sort().slice(0, 3)
}

function formatNumber(value: number | null) {
  if (value === null) {
    return 'n/a'
  }
  return new Intl.NumberFormat(undefined, {
    maximumFractionDigits: Math.abs(value) >= 100 ? 1 : 4,
  }).format(value)
}

function formatTimestamp(value: Date | null) {
  if (!value) {
    return 'Not loaded'
  }
  return new Intl.DateTimeFormat(undefined, {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  }).format(value)
}

function buildQuery() {
  const base = selector.value
  if (!base) {
    promql.value = ''
    return
  }
  const grouping = groupBy.value.trim()
  const rateExpression = `rate(${base}[5m])`
  switch (queryMode.value) {
    case 'raw':
      promql.value = base
      break
    case 'sum':
      promql.value = grouping
        ? `sum by (${grouping}) (${rateExpression})`
        : `sum(${rateExpression})`
      break
    case 'avg':
      promql.value = grouping ? `avg by (${grouping}) (${base})` : `avg(${base})`
      break
    case 'max':
      promql.value = grouping ? `max by (${grouping}) (${base})` : `max(${base})`
      break
    default:
      promql.value = rateExpression
  }
}

async function loadCatalog() {
  loading.value = true
  error.value = ''
  const searchRequestId = ++metricSearchRequestId
  metricSearchLoading.value = true
  try {
    const token = requireToken()
    const [names, labels] = await Promise.all([
      fetchMetricNames(
        token,
        metricSearch.value.trim(),
        300,
        metricsFiltersSelector(metricsFilters.value),
      ),
      fetchMetricLabelNames(token),
    ])
    if (searchRequestId === metricSearchRequestId) {
      metricNames.value = names
      metricSearchLoading.value = false
    }
    labelNames.value = labels.filter((label) => label !== '__name__').sort()
  } catch (loadError) {
    if (searchRequestId === metricSearchRequestId) {
      error.value = loadError instanceof Error ? loadError.message : 'Failed to load metrics.'
      metricSearchLoading.value = false
    }
  } finally {
    loading.value = false
  }
}

async function searchMetricNames(search: string, requestId: number) {
  try {
    const names = await fetchMetricNames(
      requireToken(),
      search,
      300,
      metricsFiltersSelector(metricsFilters.value),
    )
    if (requestId !== metricSearchRequestId) {
      return
    }
    metricNames.value = names
    error.value = ''
  } catch (loadError) {
    if (requestId === metricSearchRequestId) {
      error.value = loadError instanceof Error ? loadError.message : 'Failed to search metrics.'
    }
  } finally {
    if (requestId === metricSearchRequestId) {
      metricSearchLoading.value = false
    }
  }
}

function scheduleMetricSearch() {
  if (metricSearchTimer) {
    window.clearTimeout(metricSearchTimer)
  }
  const requestId = ++metricSearchRequestId
  const search = metricSearch.value.trim()
  metricSearchLoading.value = true
  metricSearchTimer = window.setTimeout(() => {
    void searchMetricNames(search, requestId)
  }, 200)
}

async function selectMetric(metricName: string) {
  const requestId = ++detailRequestId
  selectedMetric.value = metricName
  metricSeries.value = []
  metricMetadata.value = {}
  instantSamples.value = []
  instantLoadedAt.value = null
  error.value = ''
  viewMode.value = 'explore'
  buildQuery()
  detailLoading.value = true

  try {
    const token = requireToken()
    const [series, metadata] = await Promise.all([
      fetchMetricSeries(metricName, token, metricsFiltersSelector(metricsFilters.value)),
      fetchMetricMetadata(metricName, token),
    ])
    if (requestId !== detailRequestId) {
      return
    }
    metricSeries.value = series
    metricMetadata.value = metadata
    legendLabels.value = chooseDefaultLegendLabels(series)
    buildQuery()
    await refreshInstantStats(requestId)
  } catch (loadError) {
    if (requestId === detailRequestId) {
      error.value =
        loadError instanceof Error ? loadError.message : 'Failed to load metric details.'
    }
  } finally {
    if (requestId === detailRequestId) {
      detailLoading.value = false
    }
  }
}

async function refreshInstantStats(requestId = detailRequestId) {
  if (!selector.value) {
    instantSamples.value = []
    instantLoadedAt.value = null
    return
  }
  statsLoading.value = true
  try {
    const response = await queryMetricInstant(selector.value, requireToken())
    if (requestId !== detailRequestId) {
      return
    }
    instantSamples.value = response.result ?? []
    instantLoadedAt.value = new Date()
  } catch {
    if (requestId === detailRequestId) {
      instantSamples.value = []
      instantLoadedAt.value = null
    }
  } finally {
    if (requestId === detailRequestId) {
      statsLoading.value = false
    }
  }
}

function graphSelectedMetric() {
  if (!selectedMetric.value) {
    return
  }
  buildQuery()
  explorerPanel.value = createGraphPanel(promql.value, selectedMetric.value)
  explorerPanel.value.step_seconds = stepSeconds.value
  explorerPanel.value.queries[0]!.legend = legendLabels.value
    .map((label) => `{{${label}}}`)
    .join(' · ')
  viewMode.value = 'graph'
}

async function openAddToDashboard() {
  dashboardPanelTitle.value = explorerPanel.value.title || selectedMetric.value || 'Metrics graph'
  newDashboardName.value = ''
  selectedDashboardId.value = 'new'
  dashboardMessage.value = ''
  addToDashboardOpen.value = true
  dashboardOptionsLoading.value = true
  try {
    dashboardOptions.value = await listMetricsDashboards(requireToken())
    if (dashboardOptions.value.length > 0) {
      selectedDashboardId.value = `${dashboardOptions.value[0]!.id}`
    }
  } catch (loadError) {
    dashboardMessage.value =
      loadError instanceof Error ? loadError.message : 'Failed to load dashboards.'
  } finally {
    dashboardOptionsLoading.value = false
  }
}

function buildDashboardPanel(dashboard: MetricsDashboard | null): DashboardPanel {
  const nextY = nextDashboardPanelY(dashboard?.panels ?? [])
  const panel = clonePanel(explorerPanel.value)
  panel.id = createUuid()
  panel.title = dashboardPanelTitle.value.trim()
  panel.layout = { x: 0, y: nextY, w: 6, h: 4 }
  return panel
}

async function addGraphToDashboard() {
  if (
    !dashboardPanelTitle.value.trim() ||
    !explorerPanel.value.queries.some((query) => query.query.trim())
  ) {
    dashboardMessage.value = 'Panel title and PromQL query are required.'
    return
  }
  dashboardSaving.value = true
  dashboardMessage.value = ''
  try {
    if (selectedDashboardId.value === 'new') {
      if (!newDashboardName.value.trim()) {
        dashboardMessage.value = 'Dashboard name is required.'
        return
      }
      const payload = emptyDashboardPayload(newDashboardName.value.trim())
      payload.time_range_mode = timeRange.value.mode
      payload.time_range_seconds = timeRange.value.seconds
      payload.time_range_start = timeRange.value.start
      payload.time_range_end = timeRange.value.end
      payload.panels = [buildDashboardPanel(null)]
      await createMetricsDashboard(payload, requireToken())
    } else {
      const dashboard = dashboardOptions.value.find(
        (item) => item.id === Number(selectedDashboardId.value),
      )
      if (!dashboard) {
        dashboardMessage.value = 'Select a dashboard.'
        return
      }
      const panel = buildDashboardPanel(dashboard)
      const payload = {
        name: dashboard.name,
        description: dashboard.description,
        time_range_mode: dashboard.time_range_mode,
        time_range_seconds: dashboard.time_range_seconds,
        time_range_start: dashboard.time_range_start,
        time_range_end: dashboard.time_range_end,
        refresh_interval_seconds: dashboard.refresh_interval_seconds,
        variables: dashboard.variables,
        panels: [...dashboard.panels, panel],
      }
      try {
        await updateMetricsDashboard(dashboard.id, dashboard.revision, payload, requireToken())
      } catch (saveError) {
        if (!(saveError instanceof DashboardConflictError)) {
          throw saveError
        }
        const latest = saveError.latest
        await updateMetricsDashboard(
          latest.id,
          latest.revision,
          {
            name: latest.name,
            description: latest.description,
            time_range_mode: latest.time_range_mode,
            time_range_seconds: latest.time_range_seconds,
            time_range_start: latest.time_range_start,
            time_range_end: latest.time_range_end,
            refresh_interval_seconds: latest.refresh_interval_seconds,
            variables: latest.variables,
            panels: [...latest.panels, { ...panel, layout: buildDashboardPanel(latest).layout }],
          },
          requireToken(),
        )
      }
    }
    addToDashboardOpen.value = false
    dashboardMessage.value = 'Graph added to dashboard.'
  } catch (saveError) {
    dashboardMessage.value =
      saveError instanceof Error ? saveError.message : 'Failed to add graph to dashboard.'
  } finally {
    dashboardSaving.value = false
  }
}

onMounted(() => {
  void loadCatalog()
})

onBeforeUnmount(() => {
  if (metricSearchTimer) {
    window.clearTimeout(metricSearchTimer)
  }
})

watch(metricSearch, () => {
  scheduleMetricSearch()
})
watch(
  metricsFilters,
  () => {
    void loadCatalog()
    if (selectedMetric.value) void selectMetric(selectedMetric.value)
  },
  { deep: true },
)

</script>

<template>
  <section class="grid gap-4">
    <div class="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
      <div>
        <h2 class="text-lg font-semibold text-slate-950">Metrics Explorer</h2>
        <p class="text-sm text-slate-500">
          Inspect tenant metrics, labels, series, and PromQL queries.
        </p>
      </div>
      <div class="flex flex-wrap gap-2">
        <p
          v-if="dashboardMessage && !addToDashboardOpen"
          class="self-center text-sm text-emerald-700"
        >
          {{ dashboardMessage }}
        </p>
        <Button
          v-if="viewMode === 'graph'"
          variant="outline"
          type="button"
          @click="viewMode = 'explore'"
        >
          <ArrowLeft class="h-4 w-4" />
          Explorer
        </Button>
        <Button variant="outline" type="button" :disabled="loading" @click="void loadCatalog()">
          <RefreshCw :class="['h-4 w-4', { 'animate-spin': loading }]" />
          Refresh
        </Button>
      </div>
    </div>

    <p
      v-if="error"
      class="rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700"
    >
      {{ error }}
    </p>

    <MetricsFilterBar v-model="metricsFilters" :token="requireToken()" />

    <div
      v-if="viewMode === 'explore'"
      class="grid min-h-[calc(100vh-12rem)] gap-4 xl:grid-cols-[24rem_1fr]"
    >
      <aside class="min-h-0 rounded-lg border border-slate-200 bg-white">
        <div class="border-b border-slate-200 p-3">
          <div class="relative">
            <Search class="pointer-events-none absolute left-3 top-2.5 h-4 w-4 text-slate-400" />
            <Input v-model="metricSearch" class="pl-9" placeholder="Search metrics" />
            <RefreshCw
              v-if="metricSearchLoading"
              class="pointer-events-none absolute right-3 top-2.5 h-4 w-4 animate-spin text-slate-400"
            />
          </div>
          <div class="mt-2 flex items-center justify-between text-xs text-slate-500">
            <span>{{ filteredMetricNames.length }} results</span>
            <span v-if="metricSearch.trim()">server filtered</span>
            <span v-else>latest metrics</span>
          </div>
        </div>
        <div class="max-h-[calc(100vh-18rem)] overflow-y-auto p-2">
          <button
            v-for="metricName in filteredMetricNames"
            :key="metricName"
            type="button"
            :class="
              metricName === selectedMetric
                ? 'bg-slate-900 text-white'
                : 'text-slate-700 hover:bg-slate-100'
            "
            class="block w-full rounded-md px-2 py-1.5 text-left font-mono text-xs"
            @click="void selectMetric(metricName)"
          >
            {{ metricName }}
          </button>
          <p
            v-if="!loading && filteredMetricNames.length === 0"
            class="p-4 text-center text-sm text-slate-500"
          >
            No metrics found.
          </p>
        </div>
      </aside>

      <div v-if="selectedMetric" class="grid min-h-0 gap-4">
        <div class="rounded-lg border border-slate-200 bg-white p-4">
          <div class="grid gap-3 xl:grid-cols-[1fr_auto] xl:items-start">
            <div class="min-w-0">
              <div class="mb-2 flex flex-wrap items-center gap-2">
                <Badge variant="outline" class="gap-1">
                  <Database class="h-3 w-3" />
                  {{ primaryMetadata.type || 'unknown type' }}
                </Badge>
                <Badge v-if="primaryMetadata.unit" variant="outline">
                  {{ primaryMetadata.unit }}
                </Badge>
                <Badge variant="outline">
                  {{ selector }}
                </Badge>
              </div>
              <h3 class="break-all font-mono text-base font-semibold text-slate-950">
                {{ selectedMetric }}
              </h3>
              <p v-if="primaryMetadata.help" class="mt-2 max-w-5xl text-sm text-slate-600">
                {{ primaryMetadata.help }}
              </p>
              <p v-else class="mt-2 text-sm text-slate-500">
                No metric metadata is available from the metrics backend.
              </p>
            </div>
            <div class="flex flex-wrap gap-2">
              <Button
                variant="outline"
                type="button"
                :disabled="statsLoading"
                @click="void refreshInstantStats()"
              >
                <RefreshCw :class="['h-4 w-4', { 'animate-spin': statsLoading }]" />
                Refresh stats
              </Button>
              <Button
                type="button"
                :disabled="detailLoading || !selectedMetric"
                @click="graphSelectedMetric"
              >
                <BarChart3 class="h-4 w-4" />
                Graph metric
              </Button>
            </div>
          </div>
        </div>

        <div class="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
          <div class="rounded-lg border border-slate-200 bg-white p-4">
            <div class="mb-2 flex items-center gap-2 text-sm font-medium text-slate-600">
              <Activity class="h-4 w-4" />
              Series
            </div>
            <p class="text-2xl font-semibold text-slate-950">{{ metricStats.configuredSeries }}</p>
            <p class="mt-1 text-xs text-slate-500">{{ metricStats.activeSeries }} active now</p>
          </div>
          <div class="rounded-lg border border-slate-200 bg-white p-4">
            <div class="mb-2 flex items-center gap-2 text-sm font-medium text-slate-600">
              <Tag class="h-4 w-4" />
              Labels
            </div>
            <p class="text-2xl font-semibold text-slate-950">{{ metricStats.labelCount }}</p>
            <p class="mt-1 text-xs text-slate-500">
              {{ metricStats.labelValueCount }} distinct values
            </p>
          </div>
          <div class="rounded-lg border border-slate-200 bg-white p-4">
            <div class="mb-2 flex items-center gap-2 text-sm font-medium text-slate-600">
              <Info class="h-4 w-4" />
              Latest avg
            </div>
            <p class="text-2xl font-semibold text-slate-950">{{ formatNumber(metricStats.avg) }}</p>
            <p class="mt-1 text-xs text-slate-500">{{ formatTimestamp(instantLoadedAt) }}</p>
          </div>
          <div class="rounded-lg border border-slate-200 bg-white p-4">
            <div class="mb-2 flex items-center gap-2 text-sm font-medium text-slate-600">
              <BarChart3 class="h-4 w-4" />
              Range
            </div>
            <p class="text-sm font-semibold text-slate-950">
              {{ formatNumber(metricStats.min) }} to {{ formatNumber(metricStats.max) }}
            </p>
            <p class="mt-1 text-xs text-slate-500">Current instant samples</p>
          </div>
        </div>

        <div class="grid gap-4 xl:grid-cols-[minmax(0,1.2fr)_minmax(22rem,0.8fr)]">
          <div class="rounded-lg border border-slate-200 bg-white">
            <div class="border-b border-slate-200 px-4 py-3">
              <p class="text-sm font-semibold text-slate-900">Labels on matching series</p>
            </div>
            <div class="max-h-[24rem] overflow-auto">
              <table class="w-full text-left text-sm">
                <thead class="sticky top-0 bg-slate-50 text-xs uppercase text-slate-500">
                  <tr>
                    <th class="px-4 py-2 font-medium">Label</th>
                    <th class="px-4 py-2 font-medium">Values</th>
                    <th class="px-4 py-2 font-medium">Examples</th>
                  </tr>
                </thead>
                <tbody class="divide-y divide-slate-100">
                  <tr v-for="summary in labelSummaries" :key="summary.label">
                    <td class="px-4 py-3 font-mono text-xs text-slate-900">
                      {{ summary.label }}
                    </td>
                    <td class="px-4 py-3 text-slate-700">
                      {{ summary.values.length }}
                    </td>
                    <td class="px-4 py-3">
                      <div class="flex flex-wrap gap-1">
                        <Badge
                          v-for="item in summary.values.slice(0, 5)"
                          :key="item.value"
                          variant="outline"
                          class="max-w-56 truncate"
                        >
                          {{ item.value }} ({{ item.count }})
                        </Badge>
                      </div>
                    </td>
                  </tr>
                </tbody>
              </table>
              <p
                v-if="!detailLoading && labelSummaries.length === 0"
                class="p-4 text-center text-sm text-slate-500"
              >
                No labels found for the current selector.
              </p>
            </div>
          </div>

          <div class="rounded-lg border border-slate-200 bg-white">
            <div class="border-b border-slate-200 px-4 py-3">
              <p class="text-sm font-semibold text-slate-900">Series preview</p>
            </div>
            <div class="grid max-h-[24rem] gap-2 overflow-auto p-3">
              <code
                v-for="(series, index) in filteredSeries.slice(0, 30)"
                :key="index"
                class="rounded-md bg-slate-100 px-2 py-1 text-xs text-slate-700"
              >
                {{ seriesName(series, index) }}
              </code>
              <p
                v-if="!detailLoading && filteredSeries.length === 0"
                class="p-4 text-center text-sm text-slate-500"
              >
                No series match the current filters.
              </p>
            </div>
          </div>
        </div>
      </div>

      <div
        v-else
        class="flex min-h-[28rem] items-center justify-center rounded-lg border border-slate-200 bg-white p-6 text-center"
      >
        <div>
          <Search class="mx-auto mb-3 h-8 w-8 text-slate-400" />
          <p class="font-medium text-slate-900">Select a metric to inspect it.</p>
          <p class="mt-1 text-sm text-slate-500">
            The detail view shows metadata, labels, series, and current sample statistics before
            graphing.
          </p>
        </div>
      </div>
    </div>

    <div v-else class="grid min-h-[calc(100vh-12rem)] gap-3">
      <div class="flex flex-col gap-2 rounded-lg border border-slate-200 bg-white p-3 lg:flex-row lg:items-center lg:justify-between">
        <div class="min-w-0">
          <p class="text-sm font-semibold text-slate-900">Graph</p>
          <p class="truncate font-mono text-xs text-slate-500">{{ selectedMetric || 'Custom PromQL' }}</p>
        </div>
        <div class="flex flex-wrap gap-2">
          <TimeRangePicker v-model="timeRange" />
          <Button
            variant="outline"
            type="button"
            :disabled="!explorerPanel.queries.some((query) => query.query.trim())"
            @click="void openAddToDashboard()"
          >
            <LayoutDashboard class="h-4 w-4" />
            Add to dashboard
          </Button>
          <Button variant="outline" type="button" :disabled="!selectedMetric" @click="viewMode = 'explore'">
            <Info class="h-4 w-4" />
            Metric details
          </Button>
        </div>
      </div>
      <GraphEditor
        v-model="explorerPanel"
        :token="requireToken()"
        :time-range="timeRange"
        :context-metric="selectedMetric"
        :metrics-filters="metricsFilters"
      />
    </div>

    <div
      v-if="addToDashboardOpen"
      class="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/45 p-4"
      @click.self="addToDashboardOpen = false"
    >
      <form
        class="w-full max-w-lg rounded-lg border border-slate-200 bg-white shadow-xl"
        @submit.prevent="void addGraphToDashboard()"
      >
        <div class="flex items-center justify-between border-b border-slate-200 px-5 py-4">
          <h3 class="font-semibold text-slate-950">Add graph to dashboard</h3>
          <Button variant="ghost" size="icon" type="button" @click="addToDashboardOpen = false">
            <X class="h-4 w-4" />
          </Button>
        </div>
        <div class="grid gap-4 p-5">
          <p
            v-if="dashboardMessage"
            class="rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700"
          >
            {{ dashboardMessage }}
          </p>
          <label class="grid gap-1 text-sm">
            <span class="font-medium text-slate-700">Dashboard</span>
            <select
              v-model="selectedDashboardId"
              :disabled="dashboardOptionsLoading"
              class="h-10 rounded-md border border-slate-200 px-3 text-sm"
            >
              <option value="new">Create a new dashboard</option>
              <option
                v-for="dashboardOption in dashboardOptions"
                :key="dashboardOption.id"
                :value="`${dashboardOption.id}`"
              >
                {{ dashboardOption.name }}
              </option>
            </select>
          </label>
          <label v-if="selectedDashboardId === 'new'" class="grid gap-1 text-sm">
            <span class="font-medium text-slate-700">New dashboard name</span>
            <Input v-model="newDashboardName" maxlength="120" />
          </label>
          <label class="grid gap-1 text-sm">
            <span class="font-medium text-slate-700">Panel title</span>
            <Input v-model="dashboardPanelTitle" maxlength="120" />
          </label>
          <div class="rounded-md bg-slate-50 p-3">
            <p class="text-xs font-medium uppercase text-slate-500">PromQL</p>
            <code
              v-for="query in explorerPanel.queries"
              :key="query.ref_id"
              class="mt-1 block break-all text-xs text-slate-700"
            >{{ query.ref_id }}: {{ query.query }}</code>
          </div>
        </div>
        <div class="flex justify-end gap-2 border-t border-slate-200 px-5 py-4">
          <Button variant="outline" type="button" @click="addToDashboardOpen = false">
            Cancel
          </Button>
          <Button type="submit" :disabled="dashboardSaving || dashboardOptionsLoading">
            {{ dashboardSaving ? 'Adding...' : 'Add graph' }}
          </Button>
        </div>
      </form>
    </div>
  </section>
</template>
