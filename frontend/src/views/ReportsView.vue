<script setup lang="ts">
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'

const metrics = [
  { label: 'Approval Rate', value: '94%' },
  { label: 'Avg. Review Time', value: '12m' },
  { label: 'Query Success', value: '98.7%' },
]
</script>

<template>
  <section class="grid gap-6">
    <Card>
      <CardHeader>
        <CardTitle>Operational Reports</CardTitle>
        <CardDescription>Platform-level overview for audit, review, and execution performance.</CardDescription>
      </CardHeader>
      <CardContent>
        <div class="grid gap-3 sm:grid-cols-3">
          <div
            v-for="metric in metrics"
            :key="metric.label"
            class="rounded-md border border-slate-200 bg-slate-50 p-4"
          >
            <p class="text-xs uppercase tracking-wide text-slate-500">{{ metric.label }}</p>
            <p class="mt-2 text-2xl font-semibold text-slate-900">{{ metric.value }}</p>
          </div>
        </div>
      </CardContent>
    </Card>
  </section>
</template>
