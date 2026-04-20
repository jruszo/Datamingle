<script setup lang="ts">
import { computed } from 'vue'

const props = defineProps<{
  error?: string
  success?: string
}>()

const hasError = computed(() => Boolean(props.error))
const hasSuccess = computed(() => !hasError.value && Boolean(props.success))
</script>

<template>
  <p v-if="hasError" class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
    {{ error }}
  </p>
  <p
    v-else-if="hasSuccess"
    class="rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700"
  >
    {{ success }}
  </p>
</template>
