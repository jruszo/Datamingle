<script setup lang="ts">
import { useSlots } from 'vue'

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'

defineProps<{
  title: string
  description: string
}>()

const slots = useSlots()
</script>

<template>
  <Card class="border-slate-200">
    <CardHeader>
      <div class="flex items-start gap-3">
        <slot name="icon" />
        <div class="min-w-0 flex-1">
          <slot name="header">
            <CardTitle>{{ title }}</CardTitle>
            <CardDescription>{{ description }}</CardDescription>
          </slot>
        </div>
      </div>
    </CardHeader>
    <CardContent v-if="slots.default">
      <slot />
    </CardContent>
  </Card>
</template>
