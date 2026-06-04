<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { LineChart } from 'echarts/charts'
import {
  GridComponent,
  LegendComponent,
  TooltipComponent,
} from 'echarts/components'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { Filter, Play, RefreshCw, Search, X } from 'lucide-vue-next'
import VChart from 'vue-echarts'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { useAuthStore } from '@/stores/auth'
import {
  fetchMetricLabelNames,
  fetchMetricLabelValues,
  fetchMetricNames,
  fetchMetricSeries,
  queryMetricRange,
  type PrometheusRangeResult,
  type PrometheusSeriesSelector,
} from '@/features/metrics/api'

use([CanvasRenderer, LineChart, GridComponent, TooltipComponent, LegendComponent])

type LabelFilter = {
  label: string
  value: string
}

const authStore = useAuthStore()
const loading = ref(false)
const running = ref(false)
const error = ref('')
const metricSearch = ref('')
const selectedMetric = ref('')
const labelNames = ref<string[]>([])
const labelValues = ref<string[]>([])
const metricNames = ref<string[]>([])
const metricSeries = ref<PrometheusSeriesSelector[]>([])
const selectedLabel = ref('')
const selectedValue = ref('')
const labelFilters = ref<LabelFilter[]>([])
const queryMode = ref<'raw' | 'rate' | 'sum' | 'avg' | 'max'>('rate')
const groupBy = ref('')
const rangePreset = ref('1h')
const stepSeconds = ref(60)
const promql = ref('')
const result = ref<PrometheusRangeResult | null>(null)

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
  return names.slice(0, 200)
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

const chartOption = computed(() => {
  const series = result.value?.result ?? []
  return {
    tooltip: { trigger: 'axis' },
    legend: { type: 'scroll', bottom: 0 },
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
      promql.value = grouping ? `sum by (${grouping}) (${rateExpression})` : `sum(${rateExpression})`
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
  selectedMetric.value = metricName
  labelFilters.value = []
  selectedLabel.value = ''
  selectedValue.value = ''
  labelValues.value = []
  result.value = null
  error.value = ''
  try {
    metricSeries.value = await fetchMetricSeries(metricName, requireToken())
    buildQuery()
  } catch (loadError) {
    error.value = loadError instanceof Error ? loadError.message : 'Failed to load metric labels.'
  }
}

async function loadLabelValues() {
  selectedValue.value = ''
  if (!selectedLabel.value) {
    labelValues.value = []
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
}

function removeFilter(label: string) {
  labelFilters.value = labelFilters.value.filter((item) => item.label !== label)
  buildQuery()
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
        <p class="text-sm text-slate-500">Browse tenant metrics, filter labels, and graph PromQL.</p>
      </div>
      <Button variant="outline" type="button" :disabled="loading" @click="void loadCatalog()">
        <RefreshCw :class="['h-4 w-4', { 'animate-spin': loading }]" />
        Refresh
      </Button>
    </div>

    <p v-if="error" class="rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">
      {{ error }}
    </p>

    <div class="grid min-h-[calc(100vh-12rem)] gap-4 xl:grid-cols-[22rem_1fr]">
      <aside class="min-h-0 rounded-lg border border-slate-200 bg-white">
        <div class="border-b border-slate-200 p-3">
          <div class="relative">
            <Search class="pointer-events-none absolute left-3 top-2.5 h-4 w-4 text-slate-400" />
            <Input v-model="metricSearch" class="pl-9" placeholder="Search metrics" />
          </div>
        </div>
        <div class="max-h-[calc(100vh-17rem)] overflow-y-auto p-2">
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
          <p v-if="!loading && filteredMetricNames.length === 0" class="p-4 text-center text-sm text-slate-500">
            No metrics found.
          </p>
        </div>
      </aside>

      <div class="grid min-h-0 gap-4">
        <div class="rounded-lg border border-slate-200 bg-white p-4">
          <div class="grid gap-3 xl:grid-cols-[1fr_auto] xl:items-end">
            <div class="grid gap-3 lg:grid-cols-2">
              <label class="grid gap-1 text-sm">
                <span class="font-medium text-slate-700">Label</span>
                <select v-model="selectedLabel" class="h-10 rounded-md border border-slate-200 px-3 text-sm" @change="void loadLabelValues()">
                  <option value="">Select label</option>
                  <option v-for="label in labelNames" :key="label" :value="label">{{ label }}</option>
                </select>
              </label>
              <label class="grid gap-1 text-sm">
                <span class="font-medium text-slate-700">Value</span>
                <select v-model="selectedValue" class="h-10 rounded-md border border-slate-200 px-3 text-sm" :disabled="!selectedLabel">
                  <option value="">Select value</option>
                  <option v-for="value in labelValues" :key="value" :value="value">{{ value }}</option>
                </select>
              </label>
            </div>
            <Button variant="outline" type="button" :disabled="!selectedLabel || !selectedValue" @click="addFilter">
              <Filter class="h-4 w-4" />
              Add filter
            </Button>
          </div>

          <div class="mt-3 flex flex-wrap gap-2">
            <Badge v-for="filter in labelFilters" :key="filter.label" variant="outline" class="gap-1">
              {{ filter.label }}="{{ filter.value }}"
              <button type="button" @click="removeFilter(filter.label)">
                <X class="h-3 w-3" />
              </button>
            </Badge>
          </div>
        </div>

        <div class="rounded-lg border border-slate-200 bg-white p-4">
          <div class="grid gap-3 xl:grid-cols-[10rem_1fr_9rem_8rem_auto] xl:items-end">
            <label class="grid gap-1 text-sm">
              <span class="font-medium text-slate-700">Template</span>
              <select v-model="queryMode" class="h-10 rounded-md border border-slate-200 px-3 text-sm" @change="buildQuery">
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
              <select v-model="rangePreset" class="h-10 rounded-md border border-slate-200 px-3 text-sm">
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

        <div class="min-h-[24rem] rounded-lg border border-slate-200 bg-white p-4">
          <VChart v-if="result?.result?.length" :option="chartOption" autoresize class="h-[28rem] w-full" />
          <div v-else class="flex h-[28rem] items-center justify-center text-sm text-slate-500">
            Run a query to render metric series.
          </div>
        </div>

        <div v-if="metricSeries.length > 0" class="rounded-lg border border-slate-200 bg-white p-4">
          <p class="mb-3 text-sm font-semibold text-slate-900">Discovered series</p>
          <div class="grid gap-2">
            <code
              v-for="(series, index) in metricSeries.slice(0, 20)"
              :key="index"
              class="rounded-md bg-slate-100 px-2 py-1 text-xs text-slate-700"
            >
              {{ seriesName(series, index) }}
            </code>
          </div>
        </div>
      </div>
    </div>
  </section>
</template>
