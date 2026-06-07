<script setup lang="ts">
import { ref, watch } from 'vue'

import type { DashboardPanel } from '@/features/dashboards/api'
import GraphPreview from '@/features/graph-editor/GraphPreview.vue'
import { substituteDashboardVariables } from '@/features/graph-editor/model'
import { queryMetricRange, type PrometheusSeries } from '@/features/metrics/api'
import { useAuthStore } from '@/stores/auth'
import {
  effectiveQueryStep,
  resolveTimeRange,
  type TimeRangeValue,
} from '@/features/time-range/model'

const props = withDefaults(
  defineProps<{
    panel: DashboardPanel
    timeRange: TimeRangeValue
    refreshTick: number
    variableValues?: Record<string, string[]>
  }>(),
  {
    variableValues: () => ({}),
  },
)

const authStore = useAuthStore()
const loading = ref(false)
const results = ref<Record<string, PrometheusSeries[]>>({})
const errors = ref<Record<string, string>>({})
let requestId = 0

async function loadPanel() {
  if (!authStore.accessToken) {
    return
  }
  const currentRequest = ++requestId
  loading.value = true
  const nextResults: Record<string, PrometheusSeries[]> = {}
  const nextErrors: Record<string, string> = {}
  const { start, end } = resolveTimeRange(props.timeRange)

  await Promise.all(
    props.panel.queries
      .filter((query) => !query.disabled && query.query.trim())
      .map(async (query) => {
        try {
          const response = await queryMetricRange(
            substituteDashboardVariables(query.query, props.variableValues),
            start,
            end,
            effectiveQueryStep(start, end, props.panel.step_seconds),
            authStore.accessToken!,
          )
          nextResults[query.ref_id] = response.result ?? []
        } catch (error) {
          nextErrors[query.ref_id] =
            error instanceof Error ? error.message : 'Panel query failed.'
        }
      }),
  )

  if (currentRequest === requestId) {
    results.value = nextResults
    errors.value = nextErrors
    loading.value = false
  }
}

watch(
  () => [
    props.panel,
    props.timeRange,
    props.refreshTick,
    props.variableValues,
  ],
  () => void loadPanel(),
  { immediate: true, deep: true },
)
</script>

<template>
  <GraphPreview
    :panel="panel"
    :results="results"
    :errors="errors"
    :loading="loading"
  />
</template>
