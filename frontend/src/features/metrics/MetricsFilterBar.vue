<script setup lang="ts">
import { onMounted, ref, watch } from 'vue'

import LabelFilterBar from '@/components/LabelFilterBar.vue'
import { fetchMetricLabelNames, fetchMetricLabelValues } from '@/features/metrics/api'
import {
  displayMetricsLabel,
  metricsFiltersSelector,
  type MetricsFilter,
} from '@/features/metrics/filters'

const props = defineProps<{
  modelValue: MetricsFilter[]
  token: string
}>()

const emit = defineEmits<{
  'update:modelValue': [filters: MetricsFilter[]]
}>()

const labelNames = ref<string[]>([])

async function loadLabels() {
  labelNames.value = (await fetchMetricLabelNames(props.token))
    .filter((name) => name !== '__name__')
    .sort()
}

function loadValues(name: string, filters: MetricsFilter[]) {
  return fetchMetricLabelValues(name, props.token, metricsFiltersSelector(filters))
}

watch(() => props.token, loadLabels)
onMounted(loadLabels)
</script>

<template>
  <LabelFilterBar
    :model-value="modelValue"
    :label-names="labelNames"
    :load-values="loadValues"
    :display-label="displayMetricsLabel"
    @update:model-value="emit('update:modelValue', $event)"
  />
</template>
