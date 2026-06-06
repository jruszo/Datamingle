<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'
import { LayoutDashboard, Plus, Trash2, X } from 'lucide-vue-next'

import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import {
  createMetricsDashboard,
  deleteMetricsDashboard,
  emptyDashboardPayload,
  listMetricsDashboards,
  type MetricsDashboard,
} from '@/features/dashboards/api'
import { useAuthStore } from '@/stores/auth'

const authStore = useAuthStore()
const router = useRouter()
const dashboards = ref<MetricsDashboard[]>([])
const loading = ref(false)
const saving = ref(false)
const error = ref('')
const createOpen = ref(false)
const dashboardName = ref('')

function requireToken() {
  if (!authStore.accessToken) {
    throw new Error('Missing access token. Please login again.')
  }
  return authStore.accessToken
}

async function loadDashboards() {
  loading.value = true
  error.value = ''
  try {
    dashboards.value = await listMetricsDashboards(requireToken())
  } catch (loadError) {
    error.value = loadError instanceof Error ? loadError.message : 'Failed to load dashboards.'
  } finally {
    loading.value = false
  }
}

function openCreate() {
  dashboardName.value = ''
  createOpen.value = true
}

async function createDashboard() {
  if (!dashboardName.value.trim()) {
    error.value = 'Dashboard name is required.'
    return
  }
  saving.value = true
  error.value = ''
  try {
    const dashboard = await createMetricsDashboard(
      emptyDashboardPayload(dashboardName.value.trim()),
      requireToken(),
    )
    createOpen.value = false
    await router.push(`/dashboards/${dashboard.id}`)
  } catch (createError) {
    error.value = createError instanceof Error ? createError.message : 'Failed to create dashboard.'
  } finally {
    saving.value = false
  }
}

async function removeDashboard(dashboard: MetricsDashboard) {
  if (!window.confirm(`Delete dashboard "${dashboard.name}"?`)) {
    return
  }
  error.value = ''
  try {
    await deleteMetricsDashboard(dashboard.id, requireToken())
    dashboards.value = dashboards.value.filter((item) => item.id !== dashboard.id)
  } catch (deleteError) {
    error.value = deleteError instanceof Error ? deleteError.message : 'Failed to delete dashboard.'
  }
}

onMounted(() => {
  void loadDashboards()
})
</script>

<template>
  <section class="grid gap-4">
    <div class="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
      <div>
        <h2 class="text-lg font-semibold text-slate-950">Dashboards</h2>
        <p class="text-sm text-slate-500">
          Shared organization dashboards built from PromQL graph panels.
        </p>
      </div>
      <Button type="button" @click="openCreate">
        <Plus class="h-4 w-4" />
        New dashboard
      </Button>
    </div>

    <p
      v-if="error"
      class="rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700"
    >
      {{ error }}
    </p>

    <div class="overflow-hidden rounded-lg border border-slate-200 bg-white">
      <div v-if="loading" class="p-8 text-center text-sm text-slate-500">Loading dashboards...</div>
      <div
        v-else-if="dashboards.length === 0"
        class="flex min-h-64 items-center justify-center p-8 text-center"
      >
        <div>
          <LayoutDashboard class="mx-auto mb-3 h-8 w-8 text-slate-400" />
          <p class="font-medium text-slate-900">No dashboards yet.</p>
          <p class="mt-1 text-sm text-slate-500">
            Create one here or save a graph from Metrics Explorer.
          </p>
        </div>
      </div>
      <table v-else class="w-full text-left text-sm">
        <thead class="border-b border-slate-200 bg-slate-50 text-xs uppercase text-slate-500">
          <tr>
            <th class="px-4 py-3 font-medium">Dashboard</th>
            <th class="px-4 py-3 font-medium">Panels</th>
            <th class="px-4 py-3 font-medium">Created by</th>
            <th class="px-4 py-3 font-medium">Updated</th>
            <th class="w-16 px-4 py-3" />
          </tr>
        </thead>
        <tbody class="divide-y divide-slate-100">
          <tr
            v-for="dashboard in dashboards"
            :key="dashboard.id"
            class="cursor-pointer hover:bg-slate-50"
            @click="void router.push(`/dashboards/${dashboard.id}`)"
          >
            <td class="px-4 py-3">
              <p class="font-medium text-slate-950">{{ dashboard.name }}</p>
              <p v-if="dashboard.description" class="mt-0.5 truncate text-xs text-slate-500">
                {{ dashboard.description }}
              </p>
            </td>
            <td class="px-4 py-3 text-slate-600">{{ dashboard.panels.length }}</td>
            <td class="px-4 py-3 text-slate-600">
              {{ dashboard.created_by?.display || 'Deleted user' }}
            </td>
            <td class="px-4 py-3 text-slate-600">
              {{ new Date(dashboard.update_time).toLocaleString() }}
            </td>
            <td class="px-4 py-3 text-right">
              <Button
                variant="ghost"
                size="icon"
                type="button"
                title="Delete dashboard"
                @click.stop="void removeDashboard(dashboard)"
              >
                <Trash2 class="h-4 w-4 text-slate-500" />
              </Button>
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <div
      v-if="createOpen"
      class="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/45 p-4"
      @click.self="createOpen = false"
    >
      <form
        class="w-full max-w-md rounded-lg border border-slate-200 bg-white shadow-xl"
        @submit.prevent="void createDashboard()"
      >
        <div class="flex items-center justify-between border-b border-slate-200 px-5 py-4">
          <h3 class="font-semibold text-slate-950">Create dashboard</h3>
          <Button variant="ghost" size="icon" type="button" @click="createOpen = false">
            <X class="h-4 w-4" />
          </Button>
        </div>
        <div class="p-5">
          <label class="grid gap-1 text-sm">
            <span class="font-medium text-slate-700">Name</span>
            <Input v-model="dashboardName" autofocus maxlength="120" />
          </label>
        </div>
        <div class="flex justify-end gap-2 border-t border-slate-200 px-5 py-4">
          <Button variant="outline" type="button" @click="createOpen = false">Cancel</Button>
          <Button type="submit" :disabled="saving">
            {{ saving ? 'Creating...' : 'Create' }}
          </Button>
        </div>
      </form>
    </div>
  </section>
</template>
