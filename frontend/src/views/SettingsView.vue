<script setup lang="ts">
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'

const settings = [
  {
    key: 'VITE_API_BASE_URL',
    value: import.meta.env.VITE_API_BASE_URL || '/api',
  },
  {
    key: 'VITE_BACKEND_PROXY_TARGET',
    value: import.meta.env.VITE_BACKEND_PROXY_TARGET || 'http://localhost:9123',
  },
]
</script>

<template>
  <section class="grid gap-6">
    <Card>
      <CardHeader>
        <CardTitle>Environment</CardTitle>
        <CardDescription>
          Local frontend settings used to connect to Archery in Docker.
        </CardDescription>
      </CardHeader>
      <CardContent class="space-y-3">
        <div
          v-for="item in settings"
          :key="item.key"
          class="rounded-md border border-border/70 bg-muted/40 p-3"
        >
          <div class="text-xs font-medium uppercase tracking-wide text-muted-foreground">{{ item.key }}</div>
          <code class="mt-1 block text-sm">{{ item.value }}</code>
        </div>
      </CardContent>
    </Card>
  </section>
</template>
