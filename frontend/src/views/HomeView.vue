<script setup lang="ts">
import { computed, ref } from 'vue'
import { Database, FileCheck2, Search, Users } from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { fetchSchemaInfo } from '@/lib/api'

const kpis = [
  {
    label: 'Platform Users',
    value: '6',
    icon: Users,
    gradient: 'linear-gradient(120deg, #f59e7a 0%, #f06292 100%)',
  },
  {
    label: 'Data Sources',
    value: '22',
    icon: Database,
    gradient: 'linear-gradient(120deg, #9f7aea 0%, #8b5cf6 100%)',
  },
  {
    label: 'Ticket Count',
    value: '98',
    icon: FileCheck2,
    gradient: 'linear-gradient(120deg, #63a9ff 0%, #1d7bd9 100%)',
  },
  {
    label: 'Query Count',
    value: '18',
    icon: Search,
    gradient: 'linear-gradient(120deg, #5ed8cc 0%, #20b39f 100%)',
  },
]

const profileFields = [
  { label: 'User', value: 'admin' },
  { label: 'Name', value: 'cookie3213211' },
  { label: 'Department', value: 'cccso' },
  { label: 'Role', value: 'admin' },
  { label: 'Email', value: 'admin@example.com' },
]

const topDatabases = [
  { name: 'Yearning', tickets: 80 },
  { name: 'test01', tickets: 18 },
  { name: 'analytics', tickets: 12 },
  { name: 'orders', tickets: 9 },
  { name: 'billing', tickets: 6 },
]

const submissionBreakdown = [
  { label: 'DML Tickets', value: 53, color: '#ec6f91' },
  { label: 'DDL Tickets', value: 31, color: '#66a9d9' },
  { label: 'Query Tickets', value: 16, color: '#68c1b3' },
]

const trend = [
  { day: 'Mon', value: 2 },
  { day: 'Tue', value: 3 },
  { day: 'Wed', value: 2 },
  { day: 'Thu', value: 24 },
  { day: 'Fri', value: 17 },
  { day: 'Sat', value: 3 },
  { day: 'Sun', value: 2 },
]

const loading = ref(false)
const status = ref<'idle' | 'ok' | 'error'>('idle')

const maxTrendValue = computed(() => Math.max(...trend.map((point) => point.value)))

const donutBackground = computed(() => {
  let current = 0
  const segments = submissionBreakdown.map((entry) => {
    const start = current
    current += entry.value
    return `${entry.color} ${start}% ${current}%`
  })
  return `conic-gradient(${segments.join(', ')})`
})

async function checkBackend() {
  loading.value = true
  status.value = 'idle'

  try {
    await fetchSchemaInfo()
    status.value = 'ok'
  } catch {
    status.value = 'error'
  } finally {
    loading.value = false
  }
}
</script>

<template>
  <section class="space-y-4">
    <div class="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
      <Card v-for="kpi in kpis" :key="kpi.label" class="relative overflow-hidden border-0 text-white shadow-sm">
        <CardContent class="relative p-5">
          <div class="absolute inset-0" :style="{ background: kpi.gradient }" />
          <div class="absolute -right-8 -top-8 h-28 w-28 rounded-full bg-white/20" />
          <div class="absolute -bottom-8 right-12 h-24 w-24 rounded-full bg-white/15" />
          <div class="relative flex items-start justify-between">
            <div>
              <p class="text-xs uppercase tracking-wide text-white/80">{{ kpi.label }}</p>
              <p class="mt-3 text-4xl font-semibold">{{ kpi.value }}</p>
            </div>
            <component :is="kpi.icon" class="h-5 w-5 text-white/90" />
          </div>
        </CardContent>
      </Card>
    </div>

    <div class="grid gap-4 xl:grid-cols-12">
      <Card class="xl:col-span-4">
        <CardHeader class="pb-3">
          <CardTitle>Profile</CardTitle>
          <CardDescription>Signed-in account details and connectivity health.</CardDescription>
        </CardHeader>
        <CardContent class="space-y-4">
          <div class="space-y-3">
            <div
              v-for="field in profileFields"
              :key="field.label"
              class="flex items-center justify-between border-b border-slate-100 pb-2"
            >
              <span class="text-sm text-slate-500">{{ field.label }}</span>
              <span class="text-sm font-medium text-slate-900">{{ field.value }}</span>
            </div>
          </div>

          <div class="flex items-center gap-2">
            <Badge v-if="status === 'ok'" class="bg-emerald-500 text-white hover:bg-emerald-500">API reachable</Badge>
            <Badge v-else-if="status === 'error'" variant="destructive">API unreachable</Badge>
            <Badge v-else variant="secondary">Status unknown</Badge>
            <Button size="sm" variant="outline" :disabled="loading" @click="checkBackend">
              {{ loading ? 'Checking...' : 'Check API' }}
            </Button>
          </div>
        </CardContent>
      </Card>

      <Card class="xl:col-span-4">
        <CardHeader class="pb-3">
          <CardTitle>Ticket Submission Share</CardTitle>
          <CardDescription>DML, DDL, and Query ticket distribution.</CardDescription>
        </CardHeader>
        <CardContent>
          <div class="flex flex-col items-center gap-5">
            <div class="relative h-44 w-44 rounded-full" :style="{ background: donutBackground }">
              <div class="absolute inset-[22%] rounded-full bg-white" />
            </div>
            <div class="grid w-full gap-2">
              <div
                v-for="entry in submissionBreakdown"
                :key="entry.label"
                class="flex items-center justify-between rounded-md border border-slate-200 px-3 py-2"
              >
                <div class="flex items-center gap-2 text-sm">
                  <span class="h-2.5 w-2.5 rounded-full" :style="{ backgroundColor: entry.color }" />
                  <span class="text-slate-600">{{ entry.label }}</span>
                </div>
                <span class="text-sm font-semibold text-slate-900">{{ entry.value }}%</span>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      <Card class="xl:col-span-4">
        <CardHeader class="pb-3">
          <CardTitle>High-Frequency Databases</CardTitle>
          <CardDescription>Top reviewed databases by ticket volume.</CardDescription>
        </CardHeader>
        <CardContent>
          <div class="overflow-hidden rounded-md border border-slate-200">
            <table class="w-full text-left text-sm">
              <thead class="bg-slate-50 text-slate-600">
                <tr>
                  <th class="px-3 py-2 font-medium">Database</th>
                  <th class="px-3 py-2 font-medium">Tickets</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="database in topDatabases" :key="database.name" class="border-t border-slate-200">
                  <td class="px-3 py-2 text-slate-700">{{ database.name }}</td>
                  <td class="px-3 py-2 font-semibold text-slate-900">{{ database.tickets }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>
    </div>

    <Card>
      <CardHeader class="pb-3">
        <CardTitle>Ticket Trend</CardTitle>
        <CardDescription>Daily submission volume over the current week.</CardDescription>
      </CardHeader>
      <CardContent>
        <div class="grid h-64 grid-cols-7 items-end gap-3 rounded-md border border-slate-200 bg-gradient-to-b from-rose-50/70 to-transparent p-4">
          <div v-for="point in trend" :key="point.day" class="flex h-full flex-col items-center justify-end gap-2">
            <div
              class="w-full rounded-t-md bg-gradient-to-t from-rose-400 to-pink-300"
              :style="{ height: `${Math.max((point.value / maxTrendValue) * 100, 8)}%` }"
              :title="`${point.day}: ${point.value}`"
            />
            <span class="text-xs text-slate-500">{{ point.day }}</span>
          </div>
        </div>
      </CardContent>
    </Card>
  </section>
</template>
