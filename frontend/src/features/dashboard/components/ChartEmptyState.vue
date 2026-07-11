<script setup lang="ts">
import type { Component } from 'vue'

withDefaults(
  defineProps<{
    icon: Component
    title: string
    subtitle: string
    height?: string
  }>(),
  { height: '18rem' },
)
</script>

<template>
  <div
    class="grid place-items-center rounded-lg border border-dashed border-slate-200 bg-slate-50/60 text-center"
    :style="{ height }"
  >
    <div>
      <component :is="icon" class="mx-auto h-7 w-7 text-slate-300" />
      <p class="mt-2 text-sm font-medium text-slate-700">{{ title }}</p>
      <p class="mt-1 text-xs text-slate-500">{{ subtitle }}</p>
    </div>
  </div>
</template>
