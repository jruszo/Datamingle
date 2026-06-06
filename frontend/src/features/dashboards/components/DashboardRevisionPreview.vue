<script setup lang="ts">
import { computed, ref, watch } from 'vue'

import type { DashboardRevision, DashboardVariable } from '@/features/dashboards/api'
import DashboardLinePanel from '@/features/dashboards/components/DashboardLinePanel.vue'
import { fetchMetricLabelValues } from '@/features/metrics/api'
import { useAuthStore } from '@/stores/auth'

const props = defineProps<{
  revision: DashboardRevision
}>()

const authStore = useAuthStore()
const refreshTick = ref(0)
const variableOptions = ref<Record<string, string[]>>({})
const variableValues = ref<Record<string, string[]>>({})
const previewHeight = computed(() => {
  const bottom = props.revision.panels.reduce(
    (maximum, panel) => Math.max(maximum, panel.layout.y + panel.layout.h),
    0,
  )
  return Math.max(320, bottom * 82)
})

async function loadVariables() {
  const token = authStore.accessToken
  variableValues.value = Object.fromEntries(
    props.revision.variables.map((variable) => [variable.name, []]),
  )
  if (!token) return
  const entries = await Promise.all(
    props.revision.variables.map(async (variable) => {
      try {
        return [
          variable.name,
          await fetchMetricLabelValues(variable.label_name, token, variable.metric),
        ] as const
      } catch {
        return [variable.name, []] as const
      }
    }),
  )
  variableOptions.value = Object.fromEntries(entries)
}

function updateVariable(variable: DashboardVariable, event: Event) {
  const select = event.target as HTMLSelectElement
  variableValues.value = {
    ...variableValues.value,
    [variable.name]: [...select.selectedOptions].map((option) => option.value),
  }
  refreshTick.value += 1
}

watch(() => props.revision, () => void loadVariables(), { immediate: true })
</script>

<template>
  <div class="grid gap-3">
    <div class="rounded-md border border-blue-200 bg-blue-50 px-3 py-2 text-xs text-blue-800">
      This preview uses the saved dashboard configuration with current metric data.
    </div>
    <div class="flex flex-wrap gap-x-5 gap-y-1 text-xs text-slate-500">
      <span>Time range: {{ revision.time_range_seconds / 3600 }} hour(s)</span>
      <span>{{ revision.panels.length }} panel(s)</span>
      <span>{{ revision.variables.length }} variable(s)</span>
    </div>
    <div v-if="revision.variables.length" class="flex flex-wrap gap-3 rounded-md border border-slate-200 bg-white p-3">
      <label
        v-for="variable in revision.variables"
        :key="variable.name"
        class="grid gap-1 text-xs text-slate-600"
      >
        <span>{{ variable.label }}</span>
        <select
          :multiple="variable.multi"
          class="min-w-36 rounded-md border border-slate-200 bg-white px-2 py-1.5 text-sm"
          @change="updateVariable(variable, $event)"
        >
          <option v-if="variable.include_all" value="">All</option>
          <option
            v-for="option in variableOptions[variable.name] ?? []"
            :key="option"
            :value="option"
          >
            {{ option }}
          </option>
        </select>
      </label>
    </div>
    <div
      v-if="revision.panels.length"
      class="revision-grid relative overflow-hidden rounded-lg border border-slate-200 bg-slate-50"
      :style="{ height: `${previewHeight}px` }"
    >
      <div
        v-for="panel in revision.panels"
        :key="panel.id"
        class="absolute overflow-hidden rounded-lg border border-slate-200 bg-white shadow-sm"
        :style="{
          left: `calc(${(panel.layout.x / 12) * 100}% + 8px)`,
          top: `${panel.layout.y * 82 + 8}px`,
          width: `calc(${(panel.layout.w / 12) * 100}% - 16px)`,
          height: `${panel.layout.h * 82 - 16}px`,
        }"
      >
        <div class="flex h-11 items-center border-b border-slate-200 px-3">
          <div class="min-w-0">
            <p class="truncate text-sm font-semibold text-slate-900">{{ panel.title }}</p>
            <p v-if="panel.description" class="truncate text-[11px] text-slate-500">
              {{ panel.description }}
            </p>
          </div>
        </div>
        <div class="h-[calc(100%-2.75rem)] overflow-hidden">
          <DashboardLinePanel
            :panel="panel"
            :range-seconds="revision.time_range_seconds"
            :refresh-tick="refreshTick"
            :variable-values="variableValues"
          />
        </div>
      </div>
    </div>
    <div
      v-else
      class="flex min-h-64 items-center justify-center rounded-lg border border-dashed border-slate-300 text-sm text-slate-500"
    >
      This revision has no panels.
    </div>
  </div>
</template>

<style scoped>
.revision-grid {
  background-image:
    linear-gradient(to right, rgb(226 232 240 / 0.45) 1px, transparent 1px),
    linear-gradient(to bottom, rgb(226 232 240 / 0.45) 1px, transparent 1px);
  background-size: calc(100% / 12) 82px;
}
</style>
