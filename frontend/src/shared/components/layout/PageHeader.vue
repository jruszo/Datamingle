<script setup lang="ts">
defineProps<{
  title: string
  description?: string
}>()
</script>

<template>
  <div class="space-y-1">
    <h2 class="text-2xl font-semibold text-slate-900">{{ title }}</h2>
    <p v-if="description" class="text-sm text-slate-600">
      {{ description }}
    </p>
  </div>
</template>
