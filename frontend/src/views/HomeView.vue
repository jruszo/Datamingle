<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import {
  BarChart,
  LineChart,
  PieChart,
} from 'echarts/charts'
import {
  GridComponent,
  LegendComponent,
  TooltipComponent,
} from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'
import { use } from 'echarts/core'
import { Database, FileCheck2, Search, Users } from 'lucide-vue-next'
import VChart from 'vue-echarts'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { fetchCurrentUserContext, fetchDashboard, type CurrentUserContext, type DashboardPayload } from '@/lib/api'
import { useAuthStore } from '@/stores/auth'

use([CanvasRenderer, BarChart, LineChart, PieChart, GridComponent, TooltipComponent, LegendComponent])

const authStore = useAuthStore()

const dashboard = ref<DashboardPayload | null>(null)
const currentUser = ref<CurrentUserContext | null>(null)

const loading = ref(false)
const error = ref('')

const endDate = ref(formatDate(new Date()))
const startDate = ref(formatDate(subtractDays(new Date(), 6)))

function subtractDays(baseDate: Date, days: number): Date {
  const nextDate = new Date(baseDate)
  nextDate.setDate(nextDate.getDate() - days)
  return nextDate
}

function formatDate(value: Date): string {
  const year = value.getFullYear()
  const month = `${value.getMonth() + 1}`.padStart(2, '0')
  const day = `${value.getDate()}`.padStart(2, '0')
  return `${year}-${month}-${day}`
}

async function loadDashboard() {
  if (!authStore.accessToken) {
    error.value = 'Missing access token. Please login again.'
    return
  }

  loading.value = true
  error.value = ''

  try {
    const [profile, payload] = await Promise.all([
      fetchCurrentUserContext(authStore.accessToken),
      fetchDashboard(startDate.value, endDate.value, authStore.accessToken),
    ])
    currentUser.value = profile
    dashboard.value = payload
  } catch (requestError) {
    error.value = requestError instanceof Error ? requestError.message : 'Failed to load dashboard data'
  } finally {
    loading.value = false
  }
}

function refreshDashboard() {
  if (startDate.value > endDate.value) {
    error.value = 'Start date cannot be greater than end date.'
    return
  }
  loadDashboard()
}

const summaryCards = computed(() => {
  const summary = dashboard.value?.summary
  return [
    {
      label: 'Platform Users',
      value: summary?.active_user_count ?? 0,
      icon: Users,
      gradient: 'linear-gradient(120deg, #f59e7a 0%, #f06292 100%)',
    },
    {
      label: 'Data Sources',
      value: summary?.instance_count ?? 0,
      icon: Database,
      gradient: 'linear-gradient(120deg, #9f7aea 0%, #8b5cf6 100%)',
    },
    {
      label: 'SQL Workflows',
      value: summary?.sql_workflow_count ?? 0,
      icon: FileCheck2,
      gradient: 'linear-gradient(120deg, #63a9ff 0%, #1d7bd9 100%)',
    },
    {
      label: 'Query Workflows',
      value: summary?.query_workflow_count ?? 0,
      icon: Search,
      gradient: 'linear-gradient(120deg, #5ed8cc 0%, #20b39f 100%)',
    },
  ]
})

const profileRows = computed(() => {
  const profile = currentUser.value
  if (!profile) {
    return []
  }

  return [
    { label: 'User', value: profile.username },
    { label: 'Display', value: profile.display || '-' },
    { label: 'Email', value: profile.email || '-' },
    {
      label: 'Groups',
      value: profile.groups.length > 0 ? profile.groups.map((group) => group.name).join(', ') : '-',
    },
    {
      label: 'Resource Groups',
      value:
        profile.resource_groups.length > 0
          ? profile.resource_groups.map((group) => group.group_name).join(', ')
          : '-',
    },
  ]
})

function buildBarOption(labels: string[], values: number[], color: string) {
  return {
    tooltip: { trigger: 'axis' },
    grid: { top: 20, left: 42, right: 20, bottom: 40 },
    xAxis: {
      type: 'category',
      data: labels,
      axisLabel: { color: '#64748b', rotate: labels.length > 8 ? 18 : 0 },
    },
    yAxis: {
      type: 'value',
      axisLabel: { color: '#64748b' },
      splitLine: { lineStyle: { color: '#e2e8f0' } },
    },
    series: [
      {
        type: 'bar',
        data: values,
        itemStyle: {
          borderRadius: [6, 6, 0, 0],
          color,
        },
      },
    ],
  }
}

function buildPieOption(labels: string[], values: number[]) {
  return {
    tooltip: { trigger: 'item' },
    legend: { bottom: 0 },
    series: [
      {
        type: 'pie',
        radius: ['40%', '72%'],
        center: ['50%', '45%'],
        data: labels.map((label, index) => ({ name: label, value: values[index] ?? 0 })),
        label: { formatter: '{b}: {d}%' },
      },
    ],
  }
}

const workflowTrendOption = computed(() => {
  const series = dashboard.value?.charts.workflow_by_date
  return buildBarOption(series?.labels ?? [], series?.values ?? [], '#f472b6')
})

const workflowStatusOption = computed(() => {
  const series = dashboard.value?.charts.workflow_status
  return buildBarOption(series?.labels ?? [], series?.values ?? [], '#60a5fa')
})

const queryRowsByUserOption = computed(() => {
  const series = dashboard.value?.charts.query_rows_by_user
  return buildPieOption(series?.labels ?? [], series?.values ?? [])
})

const syntaxTypeOption = computed(() => {
  const series = dashboard.value?.charts.syntax_type
  return buildPieOption(series?.labels ?? [], series?.values ?? [])
})

const instanceTypeOption = computed(() => {
  const series = dashboard.value?.charts.instance_type_distribution
  return buildPieOption(series?.labels ?? [], series?.values ?? [])
})

const slowQueryByDbOption = computed(() => {
  const series = dashboard.value?.charts.slow_query_by_db
  return buildBarOption(series?.labels ?? [], series?.values ?? [], '#f59e0b')
})

const queryActivityOption = computed(() => {
  const series = dashboard.value?.charts.query_activity
  return {
    tooltip: { trigger: 'axis' },
    legend: { bottom: 0 },
    grid: { top: 20, left: 52, right: 24, bottom: 52 },
    xAxis: {
      type: 'category',
      data: series?.labels ?? [],
      axisLabel: { color: '#64748b' },
    },
    yAxis: [
      {
        type: 'value',
        name: 'Rows',
        axisLabel: { color: '#64748b' },
        splitLine: { lineStyle: { color: '#e2e8f0' } },
      },
      {
        type: 'value',
        name: 'Queries',
        axisLabel: { color: '#64748b' },
        splitLine: { show: false },
      },
    ],
    series: [
      {
        name: 'Rows scanned',
        type: 'line',
        smooth: true,
        data: series?.scanned_rows ?? [],
        lineStyle: { width: 3, color: '#ec4899' },
        itemStyle: { color: '#ec4899' },
        areaStyle: {
          color: 'rgba(236,72,153,0.15)',
        },
      },
      {
        name: 'Query count',
        type: 'line',
        smooth: true,
        yAxisIndex: 1,
        data: series?.query_count ?? [],
        lineStyle: { width: 3, color: '#2563eb' },
        itemStyle: { color: '#2563eb' },
      },
    ],
  }
})

const instanceEnvOption = computed(() => {
  const series = dashboard.value?.charts.instance_env_distribution
  return {
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
    legend: { bottom: 0 },
    grid: { top: 20, left: 42, right: 20, bottom: 52 },
    xAxis: {
      type: 'category',
      data: series?.categories ?? [],
      axisLabel: { color: '#64748b' },
    },
    yAxis: {
      type: 'value',
      axisLabel: { color: '#64748b' },
      splitLine: { lineStyle: { color: '#e2e8f0' } },
    },
    series:
      series?.series.map((entry) => ({
        name: entry.name,
        type: 'bar',
        stack: 'total',
        data: entry.values,
      })) ?? [],
  }
})

const topDatabases = computed(() => {
  const source = dashboard.value?.charts.query_rows_by_db
  if (!source) {
    return []
  }
  return source.labels.slice(0, 5).map((name, index) => ({
    name,
    rows: source.values[index] ?? 0,
  }))
})

onMounted(() => {
  loadDashboard()
})
</script>

<template>
  <section class="space-y-4">
    <div class="flex flex-col gap-3 rounded-lg border border-slate-200 bg-white p-4 md:flex-row md:items-end md:justify-between">
      <div>
        <h2 class="text-lg font-semibold text-slate-900">Dashboard</h2>
        <p class="text-sm text-slate-500">
          Real-time overview based on backend dashboard metrics and workload statistics.
        </p>
      </div>
      <div class="flex flex-col gap-2 sm:flex-row sm:items-center">
        <Input v-model="startDate" type="date" class="w-full sm:w-44" />
        <Input v-model="endDate" type="date" class="w-full sm:w-44" />
        <Button :disabled="loading" @click="refreshDashboard">
          {{ loading ? 'Loading...' : 'Refresh' }}
        </Button>
      </div>
    </div>

    <p v-if="error" class="rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">
      {{ error }}
    </p>

    <div class="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
      <Card v-for="card in summaryCards" :key="card.label" class="relative overflow-hidden border-0 text-white shadow-sm">
        <CardContent class="relative p-5">
          <div class="absolute inset-0" :style="{ background: card.gradient }" />
          <div class="absolute -right-8 -top-8 h-28 w-28 rounded-full bg-white/20" />
          <div class="absolute -bottom-8 right-12 h-24 w-24 rounded-full bg-white/15" />
          <div class="relative flex items-start justify-between">
            <div>
              <p class="text-xs uppercase tracking-wide text-white/80">{{ card.label }}</p>
              <p class="mt-3 text-4xl font-semibold">{{ card.value }}</p>
            </div>
            <component :is="card.icon" class="h-5 w-5 text-white/90" />
          </div>
        </CardContent>
      </Card>
    </div>

    <div class="grid gap-4 xl:grid-cols-12">
      <Card class="xl:col-span-4">
        <CardHeader class="pb-3">
          <CardTitle>Profile</CardTitle>
          <CardDescription>Current user context from <code>/api/v1/me/</code>.</CardDescription>
        </CardHeader>
        <CardContent class="space-y-3">
          <div
            v-for="row in profileRows"
            :key="row.label"
            class="flex items-center justify-between border-b border-slate-100 pb-2"
          >
            <span class="text-sm text-slate-500">{{ row.label }}</span>
            <span class="text-sm font-medium text-slate-900">{{ row.value }}</span>
          </div>

          <div class="flex flex-wrap gap-2 pt-1">
            <Badge
              v-for="item in currentUser?.two_factor_auth_types ?? []"
              :key="item"
              variant="secondary"
            >
              2FA: {{ item }}
            </Badge>
          </div>
        </CardContent>
      </Card>

      <Card class="xl:col-span-4">
        <CardHeader class="pb-3">
          <CardTitle>Instance Type Distribution</CardTitle>
          <CardDescription>Equivalent to legacy dashboard pie chart.</CardDescription>
        </CardHeader>
        <CardContent>
          <VChart class="h-72 w-full" :option="instanceTypeOption" autoresize />
        </CardContent>
      </Card>

      <Card class="xl:col-span-4">
        <CardHeader class="pb-3">
          <CardTitle>Instance Environment Matrix</CardTitle>
          <CardDescription>Stacked view of DB type by deployment type.</CardDescription>
        </CardHeader>
        <CardContent>
          <VChart class="h-72 w-full" :option="instanceEnvOption" autoresize />
        </CardContent>
      </Card>
    </div>

    <div class="grid gap-4 xl:grid-cols-12">
      <Card class="xl:col-span-8">
        <CardHeader class="pb-3">
          <CardTitle>SQL Query Activity</CardTitle>
          <CardDescription>Rows scanned and query count over selected dates.</CardDescription>
        </CardHeader>
        <CardContent>
          <VChart class="h-80 w-full" :option="queryActivityOption" autoresize />
        </CardContent>
      </Card>

      <Card class="xl:col-span-4">
        <CardHeader class="pb-3">
          <CardTitle>SQL Syntax Types</CardTitle>
          <CardDescription>DDL/DML/Other breakdown.</CardDescription>
        </CardHeader>
        <CardContent>
          <VChart class="h-80 w-full" :option="syntaxTypeOption" autoresize />
        </CardContent>
      </Card>
    </div>

    <div class="grid gap-4 xl:grid-cols-12">
      <Card class="xl:col-span-6">
        <CardHeader class="pb-3">
          <CardTitle>SQL Release Trend</CardTitle>
          <CardDescription>Daily workflow submission count.</CardDescription>
        </CardHeader>
        <CardContent>
          <VChart class="h-72 w-full" :option="workflowTrendOption" autoresize />
        </CardContent>
      </Card>

      <Card class="xl:col-span-6">
        <CardHeader class="pb-3">
          <CardTitle>Workflow Status Distribution</CardTitle>
          <CardDescription>Current state spread across SQL workflows.</CardDescription>
        </CardHeader>
        <CardContent>
          <VChart class="h-72 w-full" :option="workflowStatusOption" autoresize />
        </CardContent>
      </Card>
    </div>

    <div class="grid gap-4 xl:grid-cols-12">
      <Card class="xl:col-span-6">
        <CardHeader class="pb-3">
          <CardTitle>24h Slow Queries by Database</CardTitle>
          <CardDescription>Slow query volume per database.</CardDescription>
        </CardHeader>
        <CardContent>
          <VChart class="h-72 w-full" :option="slowQueryByDbOption" autoresize />
        </CardContent>
      </Card>

      <Card class="xl:col-span-6">
        <CardHeader class="pb-3">
          <CardTitle>Rows Retrieved by User</CardTitle>
          <CardDescription>Top users by scanned rows.</CardDescription>
        </CardHeader>
        <CardContent>
          <VChart class="h-72 w-full" :option="queryRowsByUserOption" autoresize />
        </CardContent>
      </Card>
    </div>

    <Card>
      <CardHeader class="pb-3">
        <CardTitle>Top Databases by Rows Retrieved</CardTitle>
        <CardDescription>Equivalent to high-frequency database block on legacy dashboard.</CardDescription>
      </CardHeader>
      <CardContent>
        <div class="overflow-hidden rounded-md border border-slate-200">
          <table class="w-full text-left text-sm">
            <thead class="bg-slate-50 text-slate-600">
              <tr>
                <th class="px-3 py-2 font-medium">Database</th>
                <th class="px-3 py-2 font-medium">Rows</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="database in topDatabases" :key="database.name" class="border-t border-slate-200">
                <td class="px-3 py-2 text-slate-700">{{ database.name }}</td>
                <td class="px-3 py-2 font-semibold text-slate-900">{{ database.rows }}</td>
              </tr>
              <tr v-if="topDatabases.length === 0" class="border-t border-slate-200">
                <td colspan="2" class="px-3 py-3 text-center text-slate-500">No data in selected date range.</td>
              </tr>
            </tbody>
          </table>
        </div>
      </CardContent>
    </Card>
  </section>
</template>
