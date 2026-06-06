<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import {
  Bot,
  Check,
  Copy,
  Lightbulb,
  Play,
  Plus,
  RefreshCw,
  Sparkles,
  Trash2,
  WandSparkles,
} from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import type { DashboardPanel, DashboardQuery } from '@/features/dashboards/api'
import {
  fetchPromQLAssistantAvailability,
  requestPromQLAssistance,
  type PromQLAssistantSuggestion,
} from '@/features/graph-editor/api'
import GraphPreview from '@/features/graph-editor/GraphPreview.vue'
import {
  buildPromQL,
  cloneDashboardData,
  clonePanel,
  createDashboardQuery,
  defaultBuilderState,
  inferBuilderState,
  nextQueryRef,
  substituteDashboardVariables,
  visualizationTypes,
  type QueryBuilderState,
} from '@/features/graph-editor/model'
import PromQLEditor from '@/features/graph-editor/PromQLEditor.vue'
import {
  fetchMetricLabelNames,
  fetchMetricMetadata,
  fetchMetricNames,
  fetchMetricSeries,
  formatPromQL,
  parsePromQL,
  queryMetricRange,
  type PrometheusMetadata,
  type PrometheusSeries,
  type PrometheusSeriesSelector,
} from '@/features/metrics/api'

const props = withDefaults(
  defineProps<{
    modelValue: DashboardPanel
    token: string
    rangeSeconds: number
    variableValues?: Record<string, string[]>
    contextMetric?: string
    showFooter?: boolean
  }>(),
  {
    variableValues: () => ({}),
    contextMetric: '',
    showFooter: false,
  },
)

const emit = defineEmits<{
  'update:modelValue': [panel: DashboardPanel]
  apply: [panel: DashboardPanel]
  cancel: []
}>()

const draft = ref(clonePanel(props.modelValue))
const activeRefId = ref(draft.value.queries[0]?.ref_id ?? 'A')
const results = ref<Record<string, PrometheusSeries[]>>({})
const queryErrors = ref<Record<string, string>>({})
const queryDurations = ref<Record<string, number>>({})
const running = ref(false)
const lastRunAt = ref<Date | null>(null)
const metricNames = ref<string[]>([])
const metricSearchLoading = ref(false)
const metricSearchOpen = ref(false)
const highlightedMetricIndex = ref(0)
const metricMetadata = ref<PrometheusMetadata>({})
const metricSeries = ref<PrometheusSeriesSelector[]>([])
const labelNames = ref<string[]>([])
const catalogLoading = ref(false)
const builderStates = ref<Record<string, QueryBuilderState>>({})
const validationMessage = ref('')
const validationSuccess = ref(false)
const previewMode = ref<'visual' | 'table'>('visual')
const aiAvailable = ref(false)
const aiOpen = ref(false)
const aiPrompt = ref('')
const aiLoading = ref(false)
const aiSuggestion = ref<PromQLAssistantSuggestion | null>(null)
let builderTimer: ReturnType<typeof window.setTimeout> | undefined
let metricSearchTimer: ReturnType<typeof window.setTimeout> | undefined
let metricSearchRequestId = 0

const activeQuery = computed(
  () => draft.value.queries.find((query) => query.ref_id === activeRefId.value) ?? null,
)
const activeBuilder = computed(() => builderStates.value[activeRefId.value] ?? defaultBuilderState())
const enabledQueries = computed(() => draft.value.queries.filter((query) => !query.disabled))
const totalSeries = computed(() =>
  Object.values(results.value).reduce((total, series) => total + series.length, 0),
)
const previewPanel = computed(() => {
  const panel = clonePanel(draft.value)
  if (previewMode.value === 'table') {
    panel.visualization.type = 'table'
  }
  return panel
})

function emitDraft() {
  emit('update:modelValue', clonePanel(draft.value))
}

function initializeBuilders() {
  const states: Record<string, QueryBuilderState> = {}
  for (const query of draft.value.queries) {
    const inferred = inferBuilderState(query.query)
    states[query.ref_id] = inferred ?? {
      ...defaultBuilderState(),
      metric: props.contextMetric,
    }
  }
  builderStates.value = states
}

async function loadCatalog() {
  catalogLoading.value = true
  try {
    const [names, labels, metadata] = await Promise.all([
      fetchMetricNames(props.token, '', 300),
      fetchMetricLabelNames(props.token),
      fetchMetricMetadata('', props.token),
    ])
    metricNames.value = names
    labelNames.value = labels.filter((label) => label !== '__name__')
    metricMetadata.value = metadata
  } finally {
    catalogLoading.value = false
  }
}

async function loadBuilderMetric(metric: string) {
  if (!metric) {
    metricSeries.value = []
    return
  }
  try {
    metricSeries.value = await fetchMetricSeries(metric, props.token)
  } catch {
    metricSeries.value = []
  }
}

function availableLabelValues(label: string) {
  return [
    ...new Set(
      metricSeries.value
        .map((series) => series[label])
        .filter((value): value is string => Boolean(value)),
    ),
  ].sort()
}

function updateActiveQuery(patch: Partial<DashboardQuery>) {
  const query = activeQuery.value
  if (!query) {
    return
  }
  Object.assign(query, patch)
  validationMessage.value = ''
  validationSuccess.value = false
  emitDraft()
}

function setEditorMode(mode: 'builder' | 'code') {
  const query = activeQuery.value
  if (!query) {
    return
  }
  if (mode === 'builder') {
    const inferred = inferBuilderState(query.query)
    if (!inferred && query.query.trim()) {
      validationMessage.value =
        'This advanced query cannot be represented in Builder mode without losing operations.'
      return
    }
    builderStates.value[query.ref_id] = inferred ?? defaultBuilderState()
  }
  updateActiveQuery({ editor_mode: mode })
}

function updateBuilder(patch: Partial<QueryBuilderState>, runPreview = true) {
  const query = activeQuery.value
  if (!query) {
    return
  }
  const state = { ...activeBuilder.value, ...patch }
  builderStates.value = { ...builderStates.value, [query.ref_id]: state }
  query.query = buildPromQL(state)
  emitDraft()
  if (patch.metric !== undefined && runPreview) {
    void loadBuilderMetric(state.metric)
  }
  if (runPreview) {
    if (builderTimer) {
      window.clearTimeout(builderTimer)
    }
    builderTimer = window.setTimeout(() => void runQueries([query.ref_id]), 650)
  }
}

function scheduleMetricSearch(value: string) {
  if (metricSearchTimer) {
    window.clearTimeout(metricSearchTimer)
  }
  const requestId = ++metricSearchRequestId
  const search = value.trim()
  metricSearchOpen.value = true
  highlightedMetricIndex.value = 0
  metricSearchLoading.value = true
  metricNames.value = []
  metricSearchTimer = window.setTimeout(async () => {
    try {
      const names = await fetchMetricNames(props.token, search, 100)
      if (requestId === metricSearchRequestId) {
        metricNames.value = names
      }
    } catch {
      if (requestId === metricSearchRequestId) {
        metricNames.value = []
      }
    } finally {
      if (requestId === metricSearchRequestId) {
        metricSearchLoading.value = false
      }
    }
  }, 200)
}

function handleMetricInput(event: Event) {
  const value = (event.target as HTMLInputElement).value
  updateBuilder({ metric: value }, false)
  scheduleMetricSearch(value)
}

function selectMetric(metric: string) {
  metricSearchOpen.value = false
  updateBuilder({ metric })
}

function closeMetricSearch() {
  window.setTimeout(() => {
    metricSearchOpen.value = false
  }, 150)
}

function handleMetricKeydown(event: KeyboardEvent) {
  if (!metricSearchOpen.value || metricNames.value.length === 0) {
    return
  }
  if (event.key === 'ArrowDown') {
    event.preventDefault()
    highlightedMetricIndex.value = Math.min(
      highlightedMetricIndex.value + 1,
      metricNames.value.length - 1,
    )
  } else if (event.key === 'ArrowUp') {
    event.preventDefault()
    highlightedMetricIndex.value = Math.max(highlightedMetricIndex.value - 1, 0)
  } else if (event.key === 'Enter') {
    event.preventDefault()
    selectMetric(metricNames.value[highlightedMetricIndex.value]!)
  } else if (event.key === 'Escape') {
    metricSearchOpen.value = false
  }
}

function updateMatcher(
  index: number,
  patch: Partial<QueryBuilderState['matchers'][number]>,
) {
  const matchers = activeBuilder.value.matchers.map((matcher, matcherIndex) =>
    matcherIndex === index ? { ...matcher, ...patch } : matcher,
  )
  updateBuilder({ matchers })
}

function addMatcher() {
  updateBuilder({
    matchers: [
      ...activeBuilder.value.matchers,
      { label: '', operator: '=', value: '' },
    ],
  })
}

function removeMatcher(index: number) {
  updateBuilder({
    matchers: activeBuilder.value.matchers.filter((_item, itemIndex) => itemIndex !== index),
  })
}

function addQuery() {
  const query = createDashboardQuery(nextQueryRef(draft.value.queries))
  draft.value.queries.push(query)
  builderStates.value[query.ref_id] = defaultBuilderState()
  activeRefId.value = query.ref_id
  emitDraft()
}

function duplicateQuery(query: DashboardQuery) {
  const copy = cloneDashboardData(query)
  copy.ref_id = nextQueryRef(draft.value.queries)
  draft.value.queries.push(copy)
  const builder = builderStates.value[query.ref_id]
  builderStates.value[copy.ref_id] = builder
    ? cloneDashboardData(builder)
    : defaultBuilderState()
  activeRefId.value = copy.ref_id
  emitDraft()
}

function moveQuery(query: DashboardQuery, direction: -1 | 1) {
  const index = draft.value.queries.findIndex((item) => item.ref_id === query.ref_id)
  const nextIndex = index + direction
  if (index < 0 || nextIndex < 0 || nextIndex >= draft.value.queries.length) {
    return
  }
  const nextQueries = [...draft.value.queries]
  ;[nextQueries[index], nextQueries[nextIndex]] = [nextQueries[nextIndex]!, nextQueries[index]!]
  draft.value.queries = nextQueries
  emitDraft()
}
function removeQuery(query: DashboardQuery) {
  if (draft.value.queries.length === 1) {
    return
  }
  draft.value.queries = draft.value.queries.filter((item) => item.ref_id !== query.ref_id)
  delete results.value[query.ref_id]
  delete queryErrors.value[query.ref_id]
  activeRefId.value = draft.value.queries[0]!.ref_id
  emitDraft()
}

async function runQueries(refIds = enabledQueries.value.map((query) => query.ref_id)) {
  const queries = draft.value.queries.filter(
    (query) => refIds.includes(query.ref_id) && !query.disabled && query.query.trim(),
  )
  if (!queries.length) {
    return
  }
  running.value = true
  await Promise.all(
    queries.map(async (query) => {
      const started = performance.now()
      try {
        const end = new Date()
        const start = new Date(end.getTime() - props.rangeSeconds * 1000)
        const resolvedQuery = substituteDashboardVariables(query.query, props.variableValues)
        const response = await queryMetricRange(
          resolvedQuery,
          start,
          end,
          Math.max(15, draft.value.step_seconds),
          props.token,
        )
        results.value = { ...results.value, [query.ref_id]: response.result ?? [] }
        const nextErrors = { ...queryErrors.value }
        delete nextErrors[query.ref_id]
        queryErrors.value = nextErrors
      } catch (error) {
        queryErrors.value = {
          ...queryErrors.value,
          [query.ref_id]: error instanceof Error ? error.message : 'Query failed.',
        }
      } finally {
        queryDurations.value = {
          ...queryDurations.value,
          [query.ref_id]: Math.round(performance.now() - started),
        }
      }
    }),
  )
  lastRunAt.value = new Date()
  running.value = false
}

async function validateActiveQuery() {
  if (!activeQuery.value?.query.trim()) {
    return
  }
  validationMessage.value = ''
  validationSuccess.value = false
  try {
    await parsePromQL(activeQuery.value.query, props.token)
    validationSuccess.value = true
    validationMessage.value = 'PromQL is valid.'
  } catch (error) {
    validationMessage.value = error instanceof Error ? error.message : 'PromQL is invalid.'
  }
}

async function formatActiveQuery() {
  if (!activeQuery.value?.query.trim()) {
    return
  }
  try {
    const formatted = await formatPromQL(activeQuery.value.query, props.token)
    updateActiveQuery({ query: formatted })
  } catch (error) {
    validationMessage.value = error instanceof Error ? error.message : 'Formatting failed.'
  }
}

function updateVisualization<K extends keyof DashboardPanel['visualization']>(
  key: K,
  value: DashboardPanel['visualization'][K],
) {
  draft.value.visualization[key] = value
  emitDraft()
}

function addThreshold() {
  draft.value.visualization.thresholds.push({ value: 80, color: '#dc2626' })
  emitDraft()
}

function removeThreshold(index: number) {
  draft.value.visualization.thresholds.splice(index, 1)
  emitDraft()
}

async function askAI() {
  if (!aiPrompt.value.trim() || !activeQuery.value) {
    return
  }
  aiLoading.value = true
  aiSuggestion.value = null
  validationMessage.value = ''
  try {
    aiSuggestion.value = await requestPromQLAssistance(
      {
        prompt: aiPrompt.value.trim(),
      },
      props.token,
    )
  } catch (error) {
    validationMessage.value =
      error instanceof Error ? error.message : 'The PromQL assistant is unavailable.'
  } finally {
    aiLoading.value = false
  }
}

function applyAISuggestion() {
  if (!aiSuggestion.value) {
    return
  }
  updateActiveQuery({ query: aiSuggestion.value.query, editor_mode: 'code' })
  aiOpen.value = false
}

function applyPanel() {
  emit('apply', clonePanel(draft.value))
}

function formatRunTime() {
  return lastRunAt.value
    ? new Intl.DateTimeFormat(undefined, {
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
      }).format(lastRunAt.value)
    : 'Not run'
}

onMounted(() => {
  initializeBuilders()
  void loadCatalog()
  void fetchPromQLAssistantAvailability(props.token)
    .then((available) => {
      aiAvailable.value = available
    })
    .catch(() => {
      aiAvailable.value = false
    })
  if (draft.value.queries.some((query) => query.query.trim())) {
    void runQueries()
  }
})

onBeforeUnmount(() => {
  if (builderTimer) {
    window.clearTimeout(builderTimer)
  }
  if (metricSearchTimer) {
    window.clearTimeout(metricSearchTimer)
  }
})

watch(
  () => props.modelValue,
  (panel) => {
    if (JSON.stringify(panel) !== JSON.stringify(draft.value)) {
      draft.value = clonePanel(panel)
      initializeBuilders()
    }
  },
  { deep: true },
)
</script>

<template>
  <div class="grid min-h-0 gap-3 xl:grid-cols-[minmax(0,1fr)_19rem]">
    <div class="grid min-h-0 gap-3">
      <section class="overflow-hidden rounded-lg border border-slate-200 bg-white">
        <div class="flex flex-wrap items-center justify-between gap-2 border-b border-slate-200 px-4 py-2.5">
          <div class="flex items-center gap-2">
            <Button
              :variant="previewMode === 'visual' ? 'default' : 'outline'"
              size="sm"
              type="button"
              @click="previewMode = 'visual'"
            >
              Preview
            </Button>
            <Button
              :variant="previewMode === 'table' ? 'default' : 'outline'"
              size="sm"
              type="button"
              @click="previewMode = 'table'"
            >
              Table
            </Button>
          </div>
          <div class="flex flex-wrap items-center gap-3 text-xs text-slate-500">
            <span>{{ totalSeries }} series</span>
            <span>{{ formatRunTime() }}</span>
            <Button size="sm" type="button" :disabled="running" @click="void runQueries()">
              <RefreshCw :class="['h-4 w-4', { 'animate-spin': running }]" />
              Run queries
            </Button>
          </div>
        </div>
        <div class="h-[22rem]">
          <GraphPreview
            :panel="previewPanel"
            :results="results"
            :errors="queryErrors"
            :loading="running"
          />
        </div>
      </section>

      <section class="rounded-lg border border-slate-200 bg-white">
        <div class="flex flex-wrap items-center justify-between gap-2 border-b border-slate-200 px-4 py-3">
          <div>
            <p class="text-sm font-semibold text-slate-900">Queries</p>
            <p class="text-xs text-slate-500">Build visually or write PromQL directly.</p>
          </div>
          <div class="flex gap-2">
            <Button
              v-if="aiAvailable"
              variant="outline"
              size="sm"
              type="button"
              @click="aiOpen = !aiOpen"
            >
              <Sparkles class="h-4 w-4" />
              AI assist
            </Button>
            <Button variant="outline" size="sm" type="button" @click="addQuery">
              <Plus class="h-4 w-4" />
              Add query
            </Button>
          </div>
        </div>

        <div v-if="aiOpen" class="border-b border-slate-200 bg-indigo-50/60 p-4">
          <div class="mb-2 flex items-center gap-2 text-sm font-semibold text-indigo-950">
            <Bot class="h-4 w-4" />
            PromQL assistant
          </div>
          <div class="flex gap-2">
            <Input
              v-model="aiPrompt"
              placeholder="Example: Show CPU usage percent by node"
              @keydown.enter.prevent="void askAI()"
            />
            <Button type="button" :disabled="aiLoading" @click="void askAI()">
              <WandSparkles class="h-4 w-4" />
              {{ aiLoading ? 'Thinking...' : 'Suggest' }}
            </Button>
          </div>
          <div v-if="aiSuggestion" class="mt-3 rounded-md border border-indigo-200 bg-white p-3">
            <code class="block whitespace-pre-wrap text-xs text-slate-800">{{ aiSuggestion.query }}</code>
            <p v-if="aiSuggestion.explanation" class="mt-2 text-sm text-slate-600">
              {{ aiSuggestion.explanation }}
            </p>
            <div class="mt-3 flex justify-end">
              <Button size="sm" type="button" @click="applyAISuggestion">
                Apply suggestion
              </Button>
            </div>
          </div>
        </div>

        <div class="flex gap-1 overflow-x-auto border-b border-slate-200 bg-slate-50 px-3 pt-2">
          <button
            v-for="query in draft.queries"
            :key="query.ref_id"
            type="button"
            :class="[
              'flex items-center gap-2 rounded-t-md border border-b-0 px-3 py-2 text-sm',
              activeRefId === query.ref_id
                ? 'border-slate-200 bg-white font-medium text-slate-950'
                : 'border-transparent text-slate-500 hover:text-slate-900',
            ]"
            @click="activeRefId = query.ref_id"
          >
            <span class="font-mono">{{ query.ref_id }}</span>
            <span v-if="query.disabled" class="text-xs text-slate-400">disabled</span>
            <span
              v-if="queryErrors[query.ref_id]"
              class="h-2 w-2 rounded-full bg-red-500"
              title="Query failed"
            />
          </button>
        </div>

        <div v-if="activeQuery" class="grid gap-4 p-4">
          <div class="flex flex-wrap items-center justify-between gap-2">
            <div class="flex gap-1 rounded-md bg-slate-100 p-1">
              <Button
                :variant="activeQuery.editor_mode === 'builder' ? 'default' : 'ghost'"
                size="sm"
                type="button"
                @click="setEditorMode('builder')"
              >
                Builder
              </Button>
              <Button
                :variant="activeQuery.editor_mode === 'code' ? 'default' : 'ghost'"
                size="sm"
                type="button"
                @click="setEditorMode('code')"
              >
                Code
              </Button>
            </div>
            <div class="flex items-center gap-2">
              <label class="flex items-center gap-2 text-xs text-slate-600">
                <input
                  :checked="!activeQuery.disabled"
                  type="checkbox"
                  @change="updateActiveQuery({ disabled: !activeQuery.disabled })"
                />
                Enabled
              </label>
              <Button variant="ghost" size="icon" type="button" title="Duplicate query" @click="duplicateQuery(activeQuery)">
                <Copy class="h-4 w-4" />
              </Button>
              <Button
                variant="ghost"
                size="xs"
                type="button"
                title="Move query left"
                :disabled="draft.queries[0]?.ref_id === activeQuery.ref_id"
                @click="moveQuery(activeQuery, -1)"
              >
                Move left
              </Button>
              <Button
                variant="ghost"
                size="xs"
                type="button"
                title="Move query right"
                :disabled="draft.queries.at(-1)?.ref_id === activeQuery.ref_id"
                @click="moveQuery(activeQuery, 1)"
              >
                Move right
              </Button>
              <Button
                variant="ghost"
                size="icon"
                type="button"
                title="Remove query"
                :disabled="draft.queries.length === 1"
                @click="removeQuery(activeQuery)"
              >
                <Trash2 class="h-4 w-4" />
              </Button>
            </div>
          </div>

          <div v-if="activeQuery.editor_mode === 'builder'" class="grid gap-3">
            <div class="grid gap-3 lg:grid-cols-[minmax(18rem,1fr)_12rem_9rem]">
              <label class="relative grid gap-1 text-sm">
                <span class="font-medium text-slate-700">Metric</span>
                <input
                  :value="activeBuilder.metric"
                  class="h-10 rounded-md border border-slate-200 px-3 font-mono text-sm"
                  placeholder="Search or enter a metric"
                  autocomplete="off"
                  @focus="scheduleMetricSearch(activeBuilder.metric)"
                  @input="handleMetricInput"
                  @keydown="handleMetricKeydown"
                  @blur="closeMetricSearch"
                />
                <div
                  v-if="metricSearchOpen"
                  class="absolute left-0 right-0 top-[4.25rem] z-40 max-h-72 overflow-auto rounded-md border border-slate-200 bg-white p-1 shadow-xl"
                >
                  <p v-if="metricSearchLoading" class="px-3 py-2 text-xs text-slate-500">
                    Searching metrics...
                  </p>
                  <button
                    v-for="(metric, index) in metricNames"
                    :key="metric"
                    type="button"
                    :class="[
                      'block w-full rounded px-3 py-2 text-left font-mono text-xs',
                      index === highlightedMetricIndex
                        ? 'bg-slate-900 text-white'
                        : 'text-slate-700 hover:bg-slate-100',
                    ]"
                    @mousedown.prevent="selectMetric(metric)"
                  >
                    {{ metric }}
                  </button>
                  <p
                    v-if="!metricSearchLoading && metricNames.length === 0"
                    class="px-3 py-2 text-xs text-slate-500"
                  >
                    No matching metrics.
                  </p>
                </div>
              </label>
              <label class="grid gap-1 text-sm">
                <span class="font-medium text-slate-700">Operation</span>
                <select
                  :value="activeBuilder.operation"
                  class="h-10 rounded-md border border-slate-200 px-3 text-sm"
                  @change="updateBuilder({ operation: ($event.target as HTMLSelectElement).value as QueryBuilderState['operation'] })"
                >
                  <option value="raw">Raw metric</option>
                  <option value="rate">Rate</option>
                  <option value="increase">Increase</option>
                  <option value="sum">Sum</option>
                  <option value="avg">Average</option>
                  <option value="max">Maximum</option>
                  <option value="min">Minimum</option>
                </select>
              </label>
              <label
                v-if="activeBuilder.operation === 'rate' || activeBuilder.operation === 'increase'"
                class="grid gap-1 text-sm"
              >
                <span class="font-medium text-slate-700">Range</span>
                <select
                  :value="activeBuilder.range"
                  class="h-10 rounded-md border border-slate-200 px-3 text-sm"
                  @change="updateBuilder({ range: ($event.target as HTMLSelectElement).value })"
                >
                  <option value="1m">1 minute</option>
                  <option value="5m">5 minutes</option>
                  <option value="15m">15 minutes</option>
                  <option value="1h">1 hour</option>
                </select>
              </label>
            </div>

            <div
              v-if="metricMetadata[activeBuilder.metric]?.[0]"
              class="flex flex-wrap items-center gap-2 rounded-md border border-blue-200 bg-blue-50 px-3 py-2 text-xs text-blue-900"
            >
              <Lightbulb class="h-4 w-4" />
              <Badge variant="outline">{{ metricMetadata[activeBuilder.metric]?.[0]?.type || 'metric' }}</Badge>
              <span>{{ metricMetadata[activeBuilder.metric]?.[0]?.help }}</span>
            </div>

            <div class="rounded-md border border-slate-200">
              <div class="flex items-center justify-between border-b border-slate-200 bg-slate-50 px-3 py-2">
                <span class="text-sm font-medium text-slate-700">Label filters</span>
                <Button variant="outline" size="xs" type="button" @click="addMatcher">
                  <Plus class="h-3.5 w-3.5" />
                  Add filter
                </Button>
              </div>
              <div v-if="activeBuilder.matchers.length" class="grid gap-2 p-3">
                <div
                  v-for="(matcher, index) in activeBuilder.matchers"
                  :key="index"
                  class="grid gap-2 sm:grid-cols-[1fr_5rem_1fr_auto]"
                >
                  <input
                    :value="matcher.label"
                    :list="`labels-${activeRefId}`"
                    class="h-9 rounded-md border border-slate-200 px-2 font-mono text-xs"
                    placeholder="label"
                    @change="updateMatcher(index, { label: ($event.target as HTMLInputElement).value })"
                  />
                  <datalist :id="`labels-${activeRefId}`">
                    <option v-for="label in labelNames" :key="label" :value="label" />
                  </datalist>
                  <select
                    :value="matcher.operator"
                    class="h-9 rounded-md border border-slate-200 px-2 font-mono text-xs"
                    @change="updateMatcher(index, { operator: ($event.target as HTMLSelectElement).value as '=' | '!=' | '=~' | '!~' })"
                  >
                    <option value="=">=</option>
                    <option value="!=">!=</option>
                    <option value="=~">=~</option>
                    <option value="!~">!~</option>
                  </select>
                  <input
                    :value="matcher.value"
                    :list="`values-${activeRefId}-${index}`"
                    class="h-9 rounded-md border border-slate-200 px-2 text-xs"
                    placeholder="value or $variable"
                    @change="updateMatcher(index, { value: ($event.target as HTMLInputElement).value })"
                  />
                  <datalist :id="`values-${activeRefId}-${index}`">
                    <option v-for="value in availableLabelValues(matcher.label)" :key="value" :value="value" />
                  </datalist>
                  <Button variant="ghost" size="icon" type="button" @click="removeMatcher(index)">
                    <Trash2 class="h-4 w-4" />
                  </Button>
                </div>
              </div>
              <p v-else class="p-3 text-sm text-slate-500">No label filters.</p>
            </div>

            <label
              v-if="['sum', 'avg', 'max', 'min'].includes(activeBuilder.operation)"
              class="grid gap-1 text-sm"
            >
              <span class="font-medium text-slate-700">Group by labels</span>
              <Input
                :model-value="activeBuilder.groupBy"
                placeholder="instance_name, job"
                @update:model-value="updateBuilder({ groupBy: String($event) })"
              />
            </label>
            <div class="rounded-md bg-slate-950 p-3">
              <code class="break-all text-xs text-slate-100">{{ activeQuery.query || 'Select a metric to build PromQL.' }}</code>
            </div>
          </div>

          <div v-else class="grid gap-2">
            <PromQLEditor
              :model-value="activeQuery.query"
              :token="token"
              @update:model-value="updateActiveQuery({ query: $event })"
              @run="void runQueries([activeQuery.ref_id])"
            />
            <div class="flex flex-wrap items-center justify-between gap-2">
              <p
                v-if="validationMessage"
                :class="validationSuccess ? 'text-emerald-700' : 'text-red-600'"
                class="text-xs"
              >
                {{ validationMessage }}
              </p>
              <div class="ml-auto flex gap-2">
                <Button variant="outline" size="sm" type="button" @click="void formatActiveQuery()">
                  Format
                </Button>
                <Button variant="outline" size="sm" type="button" @click="void validateActiveQuery()">
                  <Check class="h-4 w-4" />
                  Validate
                </Button>
                <Button size="sm" type="button" @click="void runQueries([activeQuery.ref_id])">
                  <Play class="h-4 w-4" />
                  Run
                </Button>
              </div>
            </div>
          </div>

          <div class="grid gap-3 sm:grid-cols-[1fr_10rem]">
            <label class="grid gap-1 text-sm">
              <span class="font-medium text-slate-700">Legend alias</span>
              <Input
                :model-value="activeQuery.legend"
                placeholder="{{instance_name}} · {{job}}"
                @update:model-value="updateActiveQuery({ legend: String($event) })"
              />
            </label>
            <label class="grid gap-1 text-sm">
              <span class="font-medium text-slate-700">Step</span>
              <Input
                v-model.number="draft.step_seconds"
                type="number"
                min="15"
                max="86400"
                @change="emitDraft"
              />
            </label>
          </div>
          <div class="flex flex-wrap gap-3 text-xs text-slate-500">
            <span>{{ queryDurations[activeQuery.ref_id] ?? 0 }} ms</span>
            <span>{{ results[activeQuery.ref_id]?.length ?? 0 }} series</span>
            <span v-if="queryErrors[activeQuery.ref_id]" class="text-red-600">
              {{ queryErrors[activeQuery.ref_id] }}
            </span>
          </div>
        </div>
      </section>
    </div>

    <aside class="min-h-0 rounded-lg border border-slate-200 bg-white">
      <div class="border-b border-slate-200 px-4 py-3">
        <p class="text-sm font-semibold text-slate-900">Panel settings</p>
      </div>
      <div class="grid max-h-[calc(100vh-12rem)] gap-5 overflow-y-auto p-4">
        <label class="grid gap-1 text-sm">
          <span class="font-medium text-slate-700">Title</span>
          <Input v-model="draft.title" maxlength="120" @input="emitDraft" />
        </label>
        <label class="grid gap-1 text-sm">
          <span class="font-medium text-slate-700">Description</span>
          <textarea
            v-model="draft.description"
            rows="3"
            class="rounded-md border border-slate-200 p-2 text-sm"
            @input="emitDraft"
          />
        </label>

        <div class="grid gap-2">
          <span class="text-sm font-medium text-slate-700">Visualization</span>
          <button
            v-for="visualization in visualizationTypes"
            :key="visualization.value"
            type="button"
            :class="[
              'rounded-md border p-3 text-left',
              draft.visualization.type === visualization.value
                ? 'border-slate-900 bg-slate-900 text-white'
                : 'border-slate-200 hover:border-slate-400',
            ]"
            @click="updateVisualization('type', visualization.value)"
          >
            <span class="block text-sm font-medium">{{ visualization.label }}</span>
            <span
              :class="draft.visualization.type === visualization.value ? 'text-slate-300' : 'text-slate-500'"
              class="text-xs"
            >
              {{ visualization.description }}
            </span>
          </button>
        </div>

        <div class="grid gap-3 border-t border-slate-200 pt-4">
          <p class="text-sm font-medium text-slate-700">Value options</p>
          <label class="grid gap-1 text-sm">
            <span class="text-xs text-slate-500">Unit</span>
            <select
              :value="draft.visualization.unit"
              class="h-9 rounded-md border border-slate-200 px-2 text-sm"
              @change="updateVisualization('unit', ($event.target as HTMLSelectElement).value)"
            >
              <option value="">None</option>
              <option value="%">Percent</option>
              <option value="bytes">Bytes</option>
              <option value="s">Seconds</option>
              <option value="ms">Milliseconds</option>
              <option value="ops/s">Ops/sec</option>
            </select>
          </label>
          <div class="grid grid-cols-2 gap-2">
            <label class="grid gap-1 text-sm">
              <span class="text-xs text-slate-500">Min</span>
              <Input
                :model-value="draft.visualization.min ?? ''"
                type="number"
                @update:model-value="updateVisualization('min', $event === '' ? null : Number($event))"
              />
            </label>
            <label class="grid gap-1 text-sm">
              <span class="text-xs text-slate-500">Max</span>
              <Input
                :model-value="draft.visualization.max ?? ''"
                type="number"
                @update:model-value="updateVisualization('max', $event === '' ? null : Number($event))"
              />
            </label>
          </div>
          <label class="grid gap-1 text-sm">
            <span class="text-xs text-slate-500">Decimals</span>
            <Input
              :model-value="draft.visualization.decimals ?? ''"
              type="number"
              min="0"
              max="10"
              @update:model-value="updateVisualization('decimals', $event === '' ? null : Number($event))"
            />
          </label>
          <label class="grid gap-1 text-sm">
            <span class="text-xs text-slate-500">Color scheme</span>
            <select
              :value="draft.visualization.color_scheme"
              class="h-9 rounded-md border border-slate-200 px-2 text-sm"
              @change="updateVisualization('color_scheme', ($event.target as HTMLSelectElement).value as DashboardPanel['visualization']['color_scheme'])"
            >
              <option value="classic">Classic</option>
              <option value="cool">Cool</option>
              <option value="warm">Warm</option>
              <option value="status">Status</option>
            </select>
          </label>
        </div>

        <div
          v-if="draft.visualization.type === 'time_series'"
          class="grid gap-3 border-t border-slate-200 pt-4"
        >
          <p class="text-sm font-medium text-slate-700">Time series</p>
          <label class="grid gap-1 text-sm">
            <span class="text-xs text-slate-500">Line width</span>
            <Input
              v-model.number="draft.visualization.line_width"
              type="range"
              min="0"
              max="8"
              @input="emitDraft"
            />
          </label>
          <label class="grid gap-1 text-sm">
            <span class="text-xs text-slate-500">Fill opacity</span>
            <Input
              v-model.number="draft.visualization.fill_opacity"
              type="range"
              min="0"
              max="100"
              @input="emitDraft"
            />
          </label>
          <label class="flex items-center gap-2 text-sm text-slate-700">
            <input v-model="draft.visualization.stack" type="checkbox" @change="emitDraft" />
            Stack series
          </label>
        </div>

        <div class="grid gap-3 border-t border-slate-200 pt-4">
          <div class="flex items-center justify-between">
            <p class="text-sm font-medium text-slate-700">Thresholds</p>
            <Button variant="outline" size="xs" type="button" @click="addThreshold">
              <Plus class="h-3.5 w-3.5" />
              Add
            </Button>
          </div>
          <div
            v-for="(threshold, index) in draft.visualization.thresholds"
            :key="index"
            class="grid grid-cols-[1fr_5rem_auto] gap-2"
          >
            <Input v-model.number="threshold.value" type="number" @change="emitDraft" />
            <input
              v-model="threshold.color"
              type="color"
              class="h-10 w-full rounded border border-slate-200"
              @change="emitDraft"
            />
            <Button variant="ghost" size="icon" type="button" @click="removeThreshold(index)">
              <Trash2 class="h-4 w-4" />
            </Button>
          </div>
        </div>
      </div>
    </aside>

    <div v-if="showFooter" class="xl:col-span-2 flex justify-end gap-2 border-t border-slate-200 pt-3">
      <Button variant="outline" type="button" @click="emit('cancel')">Cancel</Button>
      <Button type="button" @click="applyPanel">Apply panel</Button>
    </div>
  </div>
</template>
