<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from 'vue'
import { LayoutDashboard } from 'lucide-vue-next'

import { fetchDashboardIcon } from '@/features/dashboards/api'
import { useAuthStore } from '@/stores/auth'

const props = withDefaults(
  defineProps<{
    dashboardId: number
    hasIcon: boolean
    name: string
    version?: string
    size?: 'sm' | 'md' | 'lg'
  }>(),
  {
    version: '',
    size: 'md',
  },
)

const authStore = useAuthStore()
const source = ref('')
let requestId = 0

const classes = computed(() => ({
  sm: 'h-8 w-8 rounded-md',
  md: 'h-10 w-10 rounded-lg',
  lg: 'h-12 w-12 rounded-xl',
})[props.size])

const initials = computed(() =>
  props.name
    .trim()
    .split(/\s+/)
    .slice(0, 2)
    .map((part) => part[0]?.toUpperCase())
    .join(''),
)

function clearSource() {
  if (source.value) URL.revokeObjectURL(source.value)
  source.value = ''
}

async function loadIcon() {
  const currentRequest = ++requestId
  clearSource()
  if (!props.hasIcon || !authStore.accessToken) return
  try {
    const blob = await fetchDashboardIcon(props.dashboardId, authStore.accessToken)
    if (currentRequest !== requestId) return
    source.value = URL.createObjectURL(blob)
  } catch {
    source.value = ''
  }
}

watch(
  () => [props.dashboardId, props.hasIcon, props.version, authStore.accessToken],
  () => void loadIcon(),
  { immediate: true },
)

onBeforeUnmount(() => {
  requestId += 1
  clearSource()
})
</script>

<template>
  <div
    :class="[
      classes,
      'flex shrink-0 items-center justify-center overflow-hidden border border-slate-200 bg-slate-100 text-xs font-semibold text-slate-600',
    ]"
  >
    <img v-if="source" :src="source" :alt="`${name} dashboard icon`" class="h-full w-full object-cover" />
    <span v-else-if="initials">{{ initials }}</span>
    <LayoutDashboard v-else class="h-4 w-4" />
  </div>
</template>
