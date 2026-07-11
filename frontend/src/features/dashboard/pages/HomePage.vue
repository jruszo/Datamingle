<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { BarChart, LineChart, PieChart } from 'echarts/charts'
import { GridComponent, LegendComponent, TooltipComponent } from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'
import { use } from 'echarts/core'
import {
  ArrowRight,
  CalendarDays,
  Database,
  FileCheck2,
  RefreshCw,
  Search,
  Users,
} from 'lucide-vue-next'
import VChart from 'vue-echarts'

import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import {
  fetchCurrentUserContext,
  fetchDashboard,
  type CurrentUserContext,
  type DashboardPayload,
} from '../api'
import { useAuthStore } from '@/stores/auth'

use([
  CanvasRenderer,
  BarChart,
  LineChart,
  PieChart,
  GridComponent,
  TooltipComponent,
  LegendComponent,
])

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
    error.value =
      requestError instanceof Error ? requestError.message : 'Failed to load dashboard data'
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

const dateRangeLabel = computed(() => {
  const formatter = new Intl.DateTimeFormat(undefined, {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  })
  const start = new Date(`${startDate.value}T00:00:00`)
  const end = new Date(`${endDate.value}T00:00:00`)
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) return 'Selected period'
  return `${formatter.format(start)} – ${formatter.format(end)}`
})

const teamNames = computed(() => currentUser.value?.teams?.map((team) => team.team_name) ?? [])

function hasValues(values: number[] | undefined): boolean {
  return Boolean(values?.some((value) => value > 0))
}

const chartHasData = computed(() => ({
  instanceType: hasValues(dashboard.value?.charts.instance_type_distribution?.values),
  instanceEnv: Boolean(
    dashboard.value?.charts.instance_env_distribution?.series?.some((entry) =>
      hasValues(entry.values),
    ),
  ),
  queryActivity:
    hasValues(dashboard.value?.charts.query_activity?.scanned_rows) ||
    hasValues(dashboard.value?.charts.query_activity?.query_count),
  syntaxType: hasValues(dashboard.value?.charts.syntax_type?.values),
  workflowTrend: hasValues(dashboard.value?.charts.workflow_by_date?.values),
  workflowStatus: hasValues(dashboard.value?.charts.workflow_status?.values),
  queryRowsByUser: hasValues(dashboard.value?.charts.query_rows_by_user?.values),
}))

onMounted(() => {
  loadDashboard()
})
</script>

<template>
  <section class="space-y-5 pb-6">
    <div
      class="flex flex-col gap-4 rounded-xl border border-slate-200 bg-white p-5 shadow-sm md:flex-row md:items-center md:justify-between"
    >
      <div>
        <p class="text-xs font-semibold uppercase tracking-wider text-violet-600">
          Workspace overview
        </p>
        <h2 class="mt-1 text-xl font-semibold text-slate-950">
          Welcome back<span v-if="currentUser?.display">, {{ currentUser.display }}</span>
        </h2>
        <p class="mt-1 text-sm text-slate-500">
          Monitor data sources, SQL activity, and workflow health in one place.
        </p>
      </div>
      <div class="flex flex-col gap-2 sm:flex-row sm:items-end">
        <label class="grid gap-1 text-xs font-medium text-slate-600">
          From
          <Input v-model="startDate" type="date" class="w-full sm:w-40" />
        </label>
        <label class="grid gap-1 text-xs font-medium text-slate-600">
          To
          <Input v-model="endDate" type="date" class="w-full sm:w-40" />
        </label>
        <Button class="gap-2" :disabled="loading" @click="refreshDashboard">
          <RefreshCw class="h-4 w-4" :class="{ 'animate-spin': loading }" />
          {{ loading ? 'Updating' : 'Apply' }}
        </Button>
      </div>
    </div>

    <p
      v-if="error"
      class="rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700"
    >
      {{ error }}
    </p>

    <div class="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
      <Card
        v-for="card in summaryCards"
        :key="card.label"
        class="overflow-hidden border-slate-200 shadow-sm"
      >
        <CardContent class="p-5">
          <div class="flex items-start justify-between">
            <div>
              <p class="text-sm font-medium text-slate-500">{{ card.label }}</p>
              <p class="mt-2 text-3xl font-semibold tracking-tight text-slate-950">
                {{ card.value }}
              </p>
            </div>
            <span
              class="grid h-10 w-10 place-items-center rounded-lg text-white"
              :style="{ background: card.gradient }"
            >
              <component :is="card.icon" class="h-5 w-5" />
            </span>
          </div>
        </CardContent>
      </Card>
    </div>

    <div class="grid gap-4 xl:grid-cols-12">
      <Card class="xl:col-span-8">
        <CardHeader class="flex flex-row items-start justify-between space-y-0 pb-2">
          <div>
            <CardTitle>Query activity</CardTitle>
            <CardDescription class="mt-1"
              >Rows scanned and queries executed during this period.</CardDescription
            >
          </div>
          <span class="hidden items-center gap-1.5 text-xs text-slate-500 sm:flex">
            <CalendarDays class="h-3.5 w-3.5" />{{ dateRangeLabel }}
          </span>
        </CardHeader>
        <CardContent class="relative min-h-80">
          <VChart
            v-if="chartHasData.queryActivity"
            class="h-80 w-full"
            :option="queryActivityOption"
            autoresize
          />
          <div
            v-else
            class="grid h-80 place-items-center rounded-lg border border-dashed border-slate-200 bg-slate-50/60 text-center"
          >
            <div>
              <Search class="mx-auto h-7 w-7 text-slate-300" />
              <p class="mt-2 text-sm font-medium text-slate-700">No query activity</p>
              <p class="mt-1 text-xs text-slate-500">Try a wider date range or run a query.</p>
            </div>
          </div>
        </CardContent>
      </Card>

      <Card class="xl:col-span-4">
        <CardHeader class="pb-2">
          <CardTitle>Database types</CardTitle>
          <CardDescription>Connected data sources by engine.</CardDescription>
        </CardHeader>
        <CardContent class="relative min-h-80">
          <VChart
            v-if="chartHasData.instanceType"
            class="h-80 w-full"
            :option="instanceTypeOption"
            autoresize
          />
          <div
            v-else
            class="grid h-80 place-items-center rounded-lg border border-dashed border-slate-200 bg-slate-50/60 text-center"
          >
            <div>
              <Database class="mx-auto h-7 w-7 text-slate-300" />
              <p class="mt-2 text-sm font-medium text-slate-700">No data sources yet</p>
              <p class="mt-1 text-xs text-slate-500">Connected databases will appear here.</p>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>

    <div class="grid gap-4 xl:grid-cols-12">
      <Card class="xl:col-span-7">
        <CardHeader class="pb-2">
          <CardTitle>Workflow activity</CardTitle>
          <CardDescription>SQL workflow submissions by day.</CardDescription>
        </CardHeader>
        <CardContent>
          <VChart
            v-if="chartHasData.workflowTrend"
            class="h-72 w-full"
            :option="workflowTrendOption"
            autoresize
          />
          <div
            v-else
            class="grid h-72 place-items-center rounded-lg border border-dashed border-slate-200 bg-slate-50/60 text-center"
          >
            <div>
              <FileCheck2 class="mx-auto h-7 w-7 text-slate-300" />
              <p class="mt-2 text-sm font-medium text-slate-700">No workflow submissions</p>
              <p class="mt-1 text-xs text-slate-500">Activity for this period will appear here.</p>
            </div>
          </div>
        </CardContent>
      </Card>

      <Card class="xl:col-span-5">
        <CardHeader class="pb-2">
          <CardTitle>Workflow status</CardTitle>
          <CardDescription>Current workflow workload by status.</CardDescription>
        </CardHeader>
        <CardContent>
          <VChart
            v-if="chartHasData.workflowStatus"
            class="h-72 w-full"
            :option="workflowStatusOption"
            autoresize
          />
          <div
            v-else
            class="grid h-72 place-items-center rounded-lg border border-dashed border-slate-200 bg-slate-50/60 text-center"
          >
            <div>
              <FileCheck2 class="mx-auto h-7 w-7 text-slate-300" />
              <p class="mt-2 text-sm font-medium text-slate-700">No workflow status data</p>
              <p class="mt-1 text-xs text-slate-500">Workflow states will appear here.</p>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>

    <div class="grid gap-4 xl:grid-cols-12">
      <Card class="xl:col-span-5">
        <CardHeader class="pb-2">
          <CardTitle>Deployment environments</CardTitle>
          <CardDescription>Database engines across each environment.</CardDescription>
        </CardHeader>
        <CardContent>
          <VChart
            v-if="chartHasData.instanceEnv"
            class="h-72 w-full"
            :option="instanceEnvOption"
            autoresize
          />
          <div
            v-else
            class="grid h-72 place-items-center rounded-lg border border-dashed border-slate-200 bg-slate-50/60 text-center"
          >
            <div>
              <Database class="mx-auto h-7 w-7 text-slate-300" />
              <p class="mt-2 text-sm font-medium text-slate-700">No environment data</p>
              <p class="mt-1 text-xs text-slate-500">Classify data sources to see the breakdown.</p>
            </div>
          </div>
        </CardContent>
      </Card>

      <Card class="xl:col-span-3">
        <CardHeader class="pb-2">
          <CardTitle>SQL operations</CardTitle>
          <CardDescription>Queries grouped by statement type.</CardDescription>
        </CardHeader>
        <CardContent>
          <VChart
            v-if="chartHasData.syntaxType"
            class="h-72 w-full"
            :option="syntaxTypeOption"
            autoresize
          />
          <div
            v-else
            class="grid h-72 place-items-center rounded-lg border border-dashed border-slate-200 bg-slate-50/60 text-center"
          >
            <div>
              <Search class="mx-auto h-7 w-7 text-slate-300" />
              <p class="mt-2 text-sm font-medium text-slate-700">No SQL operations</p>
              <p class="mt-1 text-xs text-slate-500">Statement types will appear here.</p>
            </div>
          </div>
        </CardContent>
      </Card>

      <Card class="xl:col-span-4">
        <CardHeader class="pb-2">
          <CardTitle>Your workspace</CardTitle>
          <CardDescription>Account and team access at a glance.</CardDescription>
        </CardHeader>
        <CardContent class="space-y-4">
          <div class="rounded-lg bg-slate-50 p-3">
            <p class="text-xs font-medium uppercase tracking-wide text-slate-500">Signed in as</p>
            <p class="mt-1 truncate text-sm font-medium text-slate-900">
              {{ currentUser?.email || currentUser?.username || '—' }}
            </p>
          </div>
          <div>
            <p class="text-xs font-medium uppercase tracking-wide text-slate-500">Teams</p>
            <div v-if="teamNames.length" class="mt-2 flex flex-wrap gap-2">
              <span
                v-for="team in teamNames"
                :key="team"
                class="rounded-full bg-violet-50 px-2.5 py-1 text-xs font-medium text-violet-700"
                >{{ team }}</span
              >
            </div>
            <p v-else class="mt-2 text-sm text-slate-500">No teams assigned.</p>
          </div>
          <RouterLink
            to="/profile"
            class="inline-flex items-center gap-1 text-sm font-medium text-violet-700 hover:text-violet-900"
            >View profile <ArrowRight class="h-4 w-4"
          /></RouterLink>
        </CardContent>
      </Card>
    </div>

    <div class="grid gap-4 xl:grid-cols-12">
      <Card class="xl:col-span-6">
        <CardHeader class="pb-2"
          ><CardTitle>Most active users</CardTitle
          ><CardDescription
            >Users scanning the most rows during this period.</CardDescription
          ></CardHeader
        >
        <CardContent>
          <VChart
            v-if="chartHasData.queryRowsByUser"
            class="h-72 w-full"
            :option="queryRowsByUserOption"
            autoresize
          />
          <div
            v-else
            class="grid h-72 place-items-center rounded-lg border border-dashed border-slate-200 bg-slate-50/60 text-center"
          >
            <div>
              <Users class="mx-auto h-7 w-7 text-slate-300" />
              <p class="mt-2 text-sm font-medium text-slate-700">No user activity</p>
              <p class="mt-1 text-xs text-slate-500">Query usage will appear here.</p>
            </div>
          </div>
        </CardContent>
      </Card>

      <Card class="xl:col-span-6">
        <CardHeader class="pb-2"
          ><CardTitle>Most active databases</CardTitle
          ><CardDescription
            >Databases returning the most rows during this period.</CardDescription
          ></CardHeader
        >
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
                <tr
                  v-for="database in topDatabases"
                  :key="database.name"
                  class="border-t border-slate-200"
                >
                  <td class="px-3 py-2 text-slate-700">{{ database.name }}</td>
                  <td class="px-3 py-2 font-semibold text-slate-900">{{ database.rows }}</td>
                </tr>
                <tr v-if="topDatabases.length === 0" class="border-t border-slate-200">
                  <td colspan="2" class="px-3 py-10 text-center text-slate-500">
                    No database activity in this period.
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>
    </div>
  </section>
</template>
