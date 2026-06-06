<script setup lang="ts">
import { computed } from 'vue'
import { BarChart, GaugeChart, LineChart } from 'echarts/charts'
import {
  GridComponent,
  LegendComponent,
  TooltipComponent,
} from 'echarts/components'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import VChart from 'vue-echarts'

import type {
  DashboardPanel,
  DashboardQuery,
  DashboardThreshold,
} from '@/features/dashboards/api'
import type { PrometheusSeries } from '@/features/metrics/api'

use([
  CanvasRenderer,
  LineChart,
  BarChart,
  GaugeChart,
  GridComponent,
  TooltipComponent,
  LegendComponent,
])

const props = defineProps<{
  panel: DashboardPanel
  results: Record<string, PrometheusSeries[]>
  loading?: boolean
  errors?: Record<string, string>
}>()

const palettes = {
  classic: ['#7EB26D', '#EAB839', '#6ED0E0', '#EF843C', '#E24D42', '#1F78C1', '#BA43A9'],
  cool: ['#2563eb', '#0891b2', '#0d9488', '#4f46e5', '#7c3aed', '#0284c7'],
  warm: ['#dc2626', '#ea580c', '#d97706', '#ca8a04', '#db2777', '#e11d48'],
  status: ['#16a34a', '#eab308', '#dc2626', '#2563eb', '#9333ea'],
}

type FlatSeries = {
  refId: string
  query: DashboardQuery
  metric: Record<string, string>
  values: Array<[number, string]>
  value?: [number, string]
  index: number
}

const flatSeries = computed<FlatSeries[]>(() => {
  const rows: FlatSeries[] = []
  for (const query of props.panel.queries.filter((item) => !item.disabled)) {
    for (const [index, series] of (props.results[query.ref_id] ?? []).entries()) {
      rows.push({
        refId: query.ref_id,
        query,
        metric: series.metric,
        values: series.values ?? [],
        value: series.value,
        index,
      })
    }
  }
  return rows
})

function interpolateLegend(template: string, labels: Record<string, string>) {
  return template.replace(/\{\{([^}]+)\}\}/g, (_match, label) => labels[label] ?? '')
}

function seriesName(item: FlatSeries) {
  if (item.query.legend.trim()) {
    return interpolateLegend(item.query.legend, item.metric) || item.refId
  }
  const entries = Object.entries(item.metric).filter(([key]) => key !== '__name__')
  return entries.length
    ? entries.map(([key, value]) => `${key}=${value}`).join(', ')
    : item.metric.__name__ || `${item.refId} series ${item.index + 1}`
}

function latestValue(item: FlatSeries) {
  const raw = item.value?.[1] ?? item.values.at(-1)?.[1] ?? ''
  const parsed = Number.parseFloat(raw)
  return Number.isFinite(parsed) ? parsed : null
}

function formatValue(value: number | null) {
  if (value === null) {
    return 'n/a'
  }
  const decimals = props.panel.visualization.decimals
  const formatted = new Intl.NumberFormat(undefined, {
    maximumFractionDigits: decimals ?? (Math.abs(value) >= 100 ? 1 : 4),
    minimumFractionDigits: decimals ?? 0,
  }).format(value)
  return `${formatted}${props.panel.visualization.unit ? ` ${props.panel.visualization.unit}` : ''}`
}

function thresholdColor(value: number | null, thresholds: DashboardThreshold[]) {
  if (value === null) {
    return '#64748b'
  }
  let color = '#16a34a'
  for (const threshold of thresholds) {
    if (value >= threshold.value) {
      color = threshold.color
    }
  }
  return color
}

const chartOption = computed(() => {
  const visualization = props.panel.visualization
  const type = visualization.type
  const colors = palettes[visualization.color_scheme]
  const legend = {
    show: visualization.legend_placement !== 'hidden',
    type: 'scroll',
    orient: visualization.legend_placement === 'right' ? 'vertical' : 'horizontal',
    right: visualization.legend_placement === 'right' ? 8 : undefined,
    top: visualization.legend_placement === 'right' ? 18 : undefined,
    bottom: visualization.legend_placement === 'bottom' ? 0 : undefined,
    textStyle: { color: '#334155', overflow: 'truncate', width: 180 },
  }
  if (type === 'gauge') {
    const first = flatSeries.value[0]
    const value = first ? latestValue(first) : null
    return {
      color: colors,
      series: [
        {
          type: 'gauge',
          min: visualization.min ?? 0,
          max: visualization.max ?? Math.max(100, (value ?? 0) * 1.2),
          progress: { show: true, width: 14 },
          axisLine: { lineStyle: { width: 14 } },
          detail: {
            formatter: () => formatValue(value),
            fontSize: 24,
            color: thresholdColor(value, visualization.thresholds),
          },
          data: [{ value: value ?? 0, name: first ? seriesName(first) : 'No data' }],
        },
      ],
    }
  }
  if (type === 'bar') {
    return {
      color: colors,
      tooltip: { trigger: 'axis' },
      grid: { top: 20, left: 56, right: 24, bottom: 70 },
      legend,
      xAxis: {
        type: 'category',
        data: flatSeries.value.map(seriesName),
        axisLabel: { color: '#64748b', interval: 0, rotate: 20 },
      },
      yAxis: {
        type: 'value',
        min: visualization.min ?? undefined,
        max: visualization.max ?? undefined,
        axisLabel: { color: '#64748b' },
      },
      series: [
        {
          type: 'bar',
          data: flatSeries.value.map(latestValue),
          barMaxWidth: 64,
        },
      ],
    }
  }
  return {
    color: colors,
    animation: false,
    tooltip: {
      trigger: visualization.tooltip_mode === 'all' ? 'axis' : 'item',
      confine: true,
    },
    legend,
    grid: {
      top: 18,
      left: 56,
      right: visualization.legend_placement === 'right' ? 210 : 24,
      bottom: visualization.legend_placement === 'bottom' ? 72 : 34,
    },
    xAxis: {
      type: 'time',
      axisLabel: { color: '#64748b' },
      axisLine: { lineStyle: { color: '#cbd5e1' } },
    },
    yAxis: {
      type: 'value',
      min: visualization.min ?? undefined,
      max: visualization.max ?? undefined,
      axisLabel: { color: '#64748b' },
      splitLine: { lineStyle: { color: '#e2e8f0' } },
    },
    series: flatSeries.value.map((item) => ({
      name: seriesName(item),
      type: 'line',
      showSymbol: false,
      lineStyle: { width: visualization.line_width },
      areaStyle:
        visualization.fill_opacity > 0
          ? { opacity: visualization.fill_opacity / 100 }
          : undefined,
      stack: visualization.stack ? 'total' : undefined,
      data: item.values.map(([timestamp, value]) => [
        timestamp * 1000,
        Number.parseFloat(value),
      ]),
    })),
  }
})
</script>

<template>
  <div class="relative h-full min-h-0">
    <div
      v-if="loading && flatSeries.length === 0"
      class="flex h-full items-center justify-center text-sm text-slate-500"
    >
      Running queries...
    </div>
    <div
      v-else-if="panel.visualization.type === 'stat'"
      class="grid h-full place-items-center overflow-auto p-6"
    >
      <div class="flex flex-wrap justify-center gap-8">
        <div v-for="item in flatSeries" :key="`${item.refId}-${item.index}`" class="text-center">
          <p class="max-w-72 truncate text-sm text-slate-500">{{ seriesName(item) }}</p>
          <p
            class="mt-2 text-4xl font-semibold"
            :style="{ color: thresholdColor(latestValue(item), panel.visualization.thresholds) }"
          >
            {{ formatValue(latestValue(item)) }}
          </p>
        </div>
      </div>
    </div>
    <div
      v-else-if="panel.visualization.type === 'table'"
      class="h-full overflow-auto p-3"
    >
      <table class="w-full text-left text-sm">
        <thead class="sticky top-0 bg-white text-xs uppercase text-slate-500">
          <tr>
            <th class="px-3 py-2">Query</th>
            <th class="px-3 py-2">Series</th>
            <th class="px-3 py-2 text-right">Latest</th>
          </tr>
        </thead>
        <tbody class="divide-y divide-slate-100">
          <tr v-for="item in flatSeries" :key="`${item.refId}-${item.index}`">
            <td class="px-3 py-2 font-mono text-xs">{{ item.refId }}</td>
            <td class="max-w-xl truncate px-3 py-2">{{ seriesName(item) }}</td>
            <td class="px-3 py-2 text-right font-mono">{{ formatValue(latestValue(item)) }}</td>
          </tr>
        </tbody>
      </table>
    </div>
    <VChart
      v-else-if="flatSeries.length"
      :option="chartOption"
      autoresize
      class="h-full w-full"
    />
    <div v-else class="flex h-full items-center justify-center p-6 text-center text-sm text-slate-500">
      <div>
        <p>No data to preview.</p>
        <p v-if="Object.keys(errors ?? {}).length" class="mt-2 text-red-600">
          {{ Object.values(errors ?? {})[0] }}
        </p>
      </div>
    </div>
  </div>
</template>
