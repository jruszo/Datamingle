<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { LineChart } from 'echarts/charts'
import { GridComponent, LegendComponent, TooltipComponent } from 'echarts/components'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import {
  Activity,
  ArrowLeft,
  BarChart3,
  Database,
  Filter,
  Info,
  Play,
  RefreshCw,
  Search,
  Tag,
  X,
} from 'lucide-vue-next'
import VChart from 'vue-echarts'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import {
  fetchMetricLabelNames,
  fetchMetricLabelValues,
  fetchMetricMetadata,
  fetchMetricNames,
  fetchMetricSeries,
  queryMetricInstant,
  queryMetricRange,
  type PrometheusMetadata,
  type PrometheusRangeResult,
  type PrometheusSeries,
  type PrometheusSeriesSelector,
} from '@/features/metrics/api'
import { useAuthStore } from '@/stores/auth'

use([CanvasRenderer, LineChart, GridComponent, TooltipComponent, LegendComponent])

type LabelFilter = {
  label: string
  value: string
}

type LabelSummary = {
  label: string
  values: Array<{ value: string; count: number }>
  seriesCount: number
}

const authStore = useAuthStore()
const loading = ref(false)
const detailLoading = ref(false)
const statsLoading = ref(false)
const running = ref(false)
const error = ref('')
const viewMode = ref<'explore' | 'graph'>('explore')
const metricSearch = ref('')
const selectedMetric = ref('')
const labelNames = ref<string[]>([])
const labelValues = ref<string[]>([])
const metricNames = ref<string[]>([])
const metricSeries = ref<PrometheusSeriesSelector[]>([])
const metricMetadata = ref<PrometheusMetadata>({})
const instantSamples = ref<PrometheusSeries[]>([])
const instantLoadedAt = ref<Date | null>(null)
const selectedLabel = ref('')
const selectedValue = ref('')
const labelFilters = ref<LabelFilter[]>([])
const queryMode = ref<'raw' | 'rate' | 'sum' | 'avg' | 'max'>('rate')
const groupBy = ref('')
const rangePreset = ref('1h')
const stepSeconds = ref(60)
const promql = ref('')
const result = ref<PrometheusRangeResult | null>(null)
let detailRequestId = 0

const rangeOptions = [
  { value: '1h', label: '1 hour', seconds: 60 * 60 },
  { value: '6h', label: '6 hours', seconds: 6 * 60 * 60 },
  { value: '24h', label: '24 hours', seconds: 24 * 60 * 60 },
  { value: '7d', label: '7 days', seconds: 7 * 24 * 60 * 60 },
]

const filteredMetricNames = computed(() => {
  const search = metricSearch.value.trim().toLowerCase()
  const names = search
    ? metricNames.value.filter((name) => name.toLowerCase().includes(search))
    : metricNames.value
  return names.slice(0, 300)
})

const selectedRange = computed(
  () => rangeOptions.find((item) => item.value === rangePreset.value) ?? rangeOptions[0]!,
)

const selector = computed(() => {
  if (!selectedMetric.value) {
    return ''
  }
  const filters = labelFilters.value
    .filter((item) => item.label && item.value)
    .map((item) => `${item.label}="${escapeLabelValue(item.value)}"`)
  return filters.length > 0 ? `${selectedMetric.value}{${filters.join(',')}}` : selectedMetric.value
})

const metadataEntries = computed(() => {
  if (!selectedMetric.value) {
    return []
  }
  return metricMetadata.value[selectedMetric.value] ?? []
})

const primaryMetadata = computed(() => metadataEntries.value[0] ?? {})

const filteredSeries = computed(() => {
  if (labelFilters.value.length === 0) {
    return metricSeries.value
  }
  return metricSeries.value.filter((series) =>
    labelFilters.value.every((filter) => series[filter.label] === filter.value),
  )
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

const selectedMetricLabels = computed(() => labelSummaries.value.map((summary) => summary.label))

const activeLabelOptions = computed(() => {
  if (selectedMetric.value) {
    return selectedMetricLabels.value
  }
  return labelNames.value
})

const activeLabelValues = computed(() => {
  if (!selectedLabel.value) {
    return []
  }
  if (!selectedMetric.value) {
    return labelValues.value
  }
  const summary = labelSummaries.value.find((item) => item.label === selectedLabel.value)
  return summary?.values.map((item) => item.value) ?? []
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

const chartOption = computed(() => {
  const series = result.value?.result ?? []
  return {
    tooltip: { trigger: 'axis' },
    legend: { type: 'scroll', bottom: 0, textStyle: { color: '#475569' } },
    grid: { top: 24, left: 56, right: 24, bottom: 72 },
    xAxis: {
      type: 'time',
      axisLabel: { color: '#64748b' },
    },
    yAxis: {
      type: 'value',
      axisLabel: { color: '#64748b' },
      splitLine: { lineStyle: { color: '#e2e8f0' } },
    },
    series: series.map((item, index) => ({
      name: seriesName(item.metric, index),
      type: 'line',
      showSymbol: false,
      smooth: true,
      data: (item.values ?? []).map(([timestamp, value]) => [
        timestamp * 1000,
        Number.parseFloat(value),
      ]),
    })),
  }
})

function requireToken() {
  if (!authStore.accessToken) {
    throw new Error('Missing access token. Please login again.')
  }
  return authStore.accessToken
}

function escapeLabelValue(value: string) {
  return value.replace(/\\/g, '\\\\').replace(/"/g, '\\"')
}

function seriesName(labels: Record<string, string>, index: number) {
  const entries = Object.entries(labels).filter(([key]) => key !== '__name__')
  if (entries.length === 0) {
    return labels.__name__ || `series ${index + 1}`
  }
  return entries.map(([key, value]) => `${key}=${value}`).join(', ')
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
  try {
    const token = requireToken()
    const [names, labels] = await Promise.all([
      fetchMetricNames(token),
      fetchMetricLabelNames(token),
    ])
    metricNames.value = names.sort()
    labelNames.value = labels.filter((label) => label !== '__name__').sort()
  } catch (loadError) {
    error.value = loadError instanceof Error ? loadError.message : 'Failed to load metrics.'
  } finally {
    loading.value = false
  }
}

async function selectMetric(metricName: string) {
  const requestId = ++detailRequestId
  selectedMetric.value = metricName
  labelFilters.value = []
  selectedLabel.value = ''
  selectedValue.value = ''
  labelValues.value = []
  metricSeries.value = []
  metricMetadata.value = {}
  instantSamples.value = []
  instantLoadedAt.value = null
  result.value = null
  error.value = ''
  viewMode.value = 'explore'
  buildQuery()
  detailLoading.value = true

  try {
    const token = requireToken()
    const [series, metadata] = await Promise.all([
      fetchMetricSeries(metricName, token),
      fetchMetricMetadata(metricName, token),
    ])
    if (requestId !== detailRequestId) {
      return
    }
    metricSeries.value = series
    metricMetadata.value = metadata
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

async function loadLabelValues() {
  selectedValue.value = ''
  if (!selectedLabel.value) {
    labelValues.value = []
    return
  }
  if (selectedMetric.value) {
    labelValues.value = activeLabelValues.value
    return
  }
  try {
    labelValues.value = (await fetchMetricLabelValues(selectedLabel.value, requireToken())).sort()
  } catch (loadError) {
    error.value = loadError instanceof Error ? loadError.message : 'Failed to load label values.'
  }
}

function addFilter() {
  if (!selectedLabel.value || !selectedValue.value) {
    return
  }
  labelFilters.value = [
    ...labelFilters.value.filter((item) => item.label !== selectedLabel.value),
    { label: selectedLabel.value, value: selectedValue.value },
  ]
  buildQuery()
  void refreshInstantStats()
}

function removeFilter(label: string) {
  labelFilters.value = labelFilters.value.filter((item) => item.label !== label)
  buildQuery()
  void refreshInstantStats()
}

function graphSelectedMetric() {
  if (!selectedMetric.value) {
    return
  }
  buildQuery()
  viewMode.value = 'graph'
  void runQuery()
}

async function runQuery() {
  if (!promql.value.trim()) {
    error.value = 'Select a metric or enter a PromQL query.'
    return
  }
  running.value = true
  error.value = ''
  try {
    const end = new Date()
    const start = new Date(end.getTime() - selectedRange.value.seconds * 1000)
    result.value = await queryMetricRange(
      promql.value.trim(),
      start,
      end,
      Math.max(15, stepSeconds.value),
      requireToken(),
    )
  } catch (queryError) {
    error.value = queryError instanceof Error ? queryError.message : 'Metrics query failed.'
  } finally {
    running.value = false
  }
}

onMounted(() => {
  void loadCatalog()
})
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

    <div
      v-if="viewMode === 'explore'"
      class="grid min-h-[calc(100vh-12rem)] gap-4 xl:grid-cols-[24rem_1fr]"
    >
      <aside class="min-h-0 rounded-lg border border-slate-200 bg-white">
        <div class="border-b border-slate-200 p-3">
          <div class="relative">
            <Search class="pointer-events-none absolute left-3 top-2.5 h-4 w-4 text-slate-400" />
            <Input v-model="metricSearch" class="pl-9" placeholder="Search metrics" />
          </div>
          <div class="mt-2 flex items-center justify-between text-xs text-slate-500">
            <span>{{ metricNames.length }} metrics</span>
            <span>{{ filteredMetricNames.length }} shown</span>
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

        <div class="rounded-lg border border-slate-200 bg-white p-4">
          <div class="mb-3 flex flex-wrap items-center justify-between gap-2">
            <div class="flex items-center gap-2">
              <Filter class="h-4 w-4 text-slate-500" />
              <p class="text-sm font-semibold text-slate-900">Filters</p>
            </div>
            <Button
              variant="outline"
              size="sm"
              type="button"
              :disabled="statsLoading"
              @click="void refreshInstantStats()"
            >
              <RefreshCw :class="['h-4 w-4', { 'animate-spin': statsLoading }]" />
              Refresh stats
            </Button>
          </div>
          <div class="grid gap-3 xl:grid-cols-[1fr_1fr_auto] xl:items-end">
            <label class="grid gap-1 text-sm">
              <span class="font-medium text-slate-700">Label</span>
              <select
                v-model="selectedLabel"
                class="h-10 rounded-md border border-slate-200 px-3 text-sm"
                @change="void loadLabelValues()"
              >
                <option value="">Select label</option>
                <option v-for="label in activeLabelOptions" :key="label" :value="label">
                  {{ label }}
                </option>
              </select>
            </label>
            <label class="grid gap-1 text-sm">
              <span class="font-medium text-slate-700">Value</span>
              <select
                v-model="selectedValue"
                class="h-10 rounded-md border border-slate-200 px-3 text-sm"
                :disabled="!selectedLabel"
              >
                <option value="">Select value</option>
                <option v-for="value in activeLabelValues" :key="value" :value="value">
                  {{ value }}
                </option>
              </select>
            </label>
            <Button
              variant="outline"
              type="button"
              :disabled="!selectedLabel || !selectedValue"
              @click="addFilter"
            >
              <Filter class="h-4 w-4" />
              Add filter
            </Button>
          </div>

          <div class="mt-3 flex flex-wrap gap-2">
            <Badge
              v-for="filter in labelFilters"
              :key="filter.label"
              variant="outline"
              class="gap-1"
            >
              {{ filter.label }}="{{ filter.value }}"
              <button
                type="button"
                class="text-slate-500 hover:text-slate-900"
                @click="removeFilter(filter.label)"
              >
                <X class="h-3 w-3" />
              </button>
            </Badge>
            <span v-if="labelFilters.length === 0" class="text-sm text-slate-500">
              No filters applied.
            </span>
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

    <div v-else class="grid min-h-[calc(100vh-12rem)] gap-4">
      <div class="rounded-lg border border-slate-200 bg-white p-4">
        <div class="mb-3 flex flex-col gap-2 lg:flex-row lg:items-start lg:justify-between">
          <div class="min-w-0">
            <p class="text-sm font-semibold text-slate-900">Graphing workspace</p>
            <p class="mt-1 break-all font-mono text-xs text-slate-500">
              {{ selector || 'Custom PromQL query' }}
            </p>
          </div>
          <Button
            variant="outline"
            type="button"
            :disabled="!selectedMetric"
            @click="viewMode = 'explore'"
          >
            <Info class="h-4 w-4" />
            Metric details
          </Button>
        </div>

        <div class="grid gap-3 xl:grid-cols-[10rem_1fr_9rem_8rem_auto] xl:items-end">
          <label class="grid gap-1 text-sm">
            <span class="font-medium text-slate-700">Template</span>
            <select
              v-model="queryMode"
              class="h-10 rounded-md border border-slate-200 px-3 text-sm"
              @change="buildQuery"
            >
              <option value="rate">Rate</option>
              <option value="raw">Raw</option>
              <option value="sum">Sum rate</option>
              <option value="avg">Average</option>
              <option value="max">Maximum</option>
            </select>
          </label>
          <label class="grid gap-1 text-sm">
            <span class="font-medium text-slate-700">Group by</span>
            <Input v-model="groupBy" placeholder="instance_name, job" @input="buildQuery" />
          </label>
          <label class="grid gap-1 text-sm">
            <span class="font-medium text-slate-700">Range</span>
            <select
              v-model="rangePreset"
              class="h-10 rounded-md border border-slate-200 px-3 text-sm"
            >
              <option v-for="option in rangeOptions" :key="option.value" :value="option.value">
                {{ option.label }}
              </option>
            </select>
          </label>
          <label class="grid gap-1 text-sm">
            <span class="font-medium text-slate-700">Step</span>
            <Input v-model.number="stepSeconds" type="number" min="15" step="15" />
          </label>
          <Button type="button" :disabled="running" @click="void runQuery()">
            <Play class="h-4 w-4" />
            {{ running ? 'Running...' : 'Run' }}
          </Button>
        </div>

        <textarea
          v-model="promql"
          class="mt-3 min-h-24 w-full rounded-md border border-slate-200 bg-slate-50 p-3 font-mono text-sm outline-none focus:border-slate-400"
          spellcheck="false"
          placeholder="Select a metric or enter PromQL"
        />
      </div>

      <div class="min-h-[30rem] rounded-lg border border-slate-200 bg-white p-4">
        <VChart
          v-if="result?.result?.length"
          :option="chartOption"
          autoresize
          class="h-[34rem] w-full"
        />
        <div v-else class="flex h-[34rem] items-center justify-center text-sm text-slate-500">
          Run a query to render metric series.
        </div>
      </div>
    </div>
  </section>
</template>
