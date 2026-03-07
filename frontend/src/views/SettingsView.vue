<script setup lang="ts">
import { computed } from 'vue'
import { RouterLink } from 'vue-router'
import { KeyRound, Settings2 } from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { useAuthStore } from '@/stores/auth'

const authStore = useAuthStore()

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

const canManageGroups = computed(() => {
  if (authStore.currentUser?.is_superuser) {
    return true
  }
  const permissions = authStore.currentUser?.permissions ?? []
  return permissions.includes('sql.menu_system') && permissions.includes('auth.view_group')
})
</script>

<template>
  <section class="grid gap-6">
    <Card class="overflow-hidden border-slate-200">
      <CardContent class="p-0">
        <div class="grid gap-0 lg:grid-cols-[minmax(0,1.2fr)_minmax(0,0.8fr)]">
          <div class="bg-[radial-gradient(circle_at_top_left,_rgba(14,165,233,0.2),_transparent_48%),linear-gradient(135deg,#f8fafc_0%,#eefbf7_48%,#fff7ed_100%)] p-6 lg:p-8">
            <Badge variant="outline" class="border-slate-300 bg-white/80 text-slate-700">Datamingle Settings</Badge>
            <h2 class="mt-5 text-2xl font-semibold text-slate-900">Workspace controls for Datamingle administrators</h2>
            <p class="mt-2 max-w-2xl text-sm leading-6 text-slate-600">
              Use Settings to inspect local frontend connectivity and, when authorized, manage Django auth groups
              and their permission bundles for Datamingle.
            </p>
          </div>
          <div class="flex flex-col justify-between gap-4 border-t border-slate-200 bg-white p-6 lg:border-l lg:border-t-0 lg:p-8">
            <div>
              <div class="flex items-center gap-3">
                <div class="rounded-2xl bg-slate-900 p-3 text-white">
                  <KeyRound class="h-5 w-5" />
                </div>
                <div>
                  <p class="text-sm font-semibold text-slate-900">Permission Groups</p>
                  <p class="text-sm text-slate-500">Admin-only access to Datamingle permission bundles.</p>
                </div>
              </div>
              <p class="mt-4 text-sm leading-6 text-slate-600">
                Review seeded roles such as DBA, RD, PM, and QA, or create new groups with custom permission sets.
              </p>
            </div>
            <div class="flex items-center justify-between gap-3">
              <Badge
                :variant="canManageGroups ? 'secondary' : 'outline'"
                :class="canManageGroups ? 'bg-emerald-100 text-emerald-800' : 'text-slate-500'"
              >
                <Settings2 class="h-3.5 w-3.5" />
                {{ canManageGroups ? 'Access available' : 'No access' }}
              </Badge>
              <Button
                v-if="canManageGroups"
                as-child
              >
                <RouterLink to="/settings/groups">Open permission groups</RouterLink>
              </Button>
            </div>
          </div>
        </div>
      </CardContent>
    </Card>

    <Card>
      <CardHeader>
        <CardTitle>Environment</CardTitle>
        <CardDescription>
          Local frontend settings used to connect to Datamingle in Docker.
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
