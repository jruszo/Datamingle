<script setup lang="ts">
import { ref, watch } from 'vue'
import { Plus, Trash2 } from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'

type LabelRow = { id: number; name: string; value: string }

const props = withDefaults(
  defineProps<{
    modelValue: Record<string, string>
    inherited?: Record<string, string>
  }>(),
  { inherited: () => ({}) },
)

const emit = defineEmits<{
  'update:modelValue': [labels: Record<string, string>]
}>()

const rows = ref<LabelRow[]>([])
let nextId = 1
let syncing = false

function resetRows(labels: Record<string, string>) {
  const current = Object.fromEntries(
    rows.value
      .filter((row) => row.name.trim() && row.value.trim())
      .map((row) => [row.name.trim(), row.value.trim()]),
  )
  if (JSON.stringify(current) === JSON.stringify(labels)) return
  syncing = true
  rows.value = Object.entries(labels).map(([name, value]) => ({
    id: nextId++,
    name,
    value,
  }))
  syncing = false
}

function addRow() {
  rows.value.push({ id: nextId++, name: '', value: '' })
}

function removeRow(id: number) {
  rows.value = rows.value.filter((row) => row.id !== id)
}

watch(
  () => props.modelValue,
  (labels) => resetRows(labels),
  { immediate: true, deep: true },
)
watch(
  rows,
  (value) => {
    if (syncing) return
    const labels: Record<string, string> = {}
    for (const row of value) {
      if (row.name.trim() && row.value.trim()) {
        labels[row.name.trim()] = row.value.trim()
      }
    }
    emit('update:modelValue', labels)
  },
  { deep: true },
)
</script>

<template>
  <div data-testid="monitoring-labels-editor" class="grid gap-3 rounded-lg border border-slate-200 bg-slate-50 p-3">
    <div class="flex items-center justify-between">
      <div>
        <p class="text-sm font-medium text-slate-800">Metrics labels</p>
        <p class="text-xs text-slate-500">Add labels used to find and filter metrics.</p>
      </div>
      <Button data-testid="monitoring-label-add" variant="outline" size="sm" type="button" @click="addRow">
        <Plus class="h-3.5 w-3.5" /> Add label
      </Button>
    </div>
    <div v-if="Object.keys(inherited).length" class="flex flex-wrap gap-1">
      <span class="mr-1 text-xs text-slate-500">Inherited:</span>
      <Badge v-for="(item, name) in inherited" :key="name" variant="outline">
        {{ name }}={{ item }}
      </Badge>
    </div>
    <div v-if="rows.length" class="grid gap-2">
      <div v-for="row in rows" :key="row.id" class="grid grid-cols-[1fr_1fr_auto] gap-2">
        <Input v-model="row.name" data-testid="monitoring-label-name" placeholder="environment" />
        <Input v-model="row.value" data-testid="monitoring-label-value" placeholder="prod" />
        <Button
          data-testid="monitoring-label-remove"
          variant="ghost"
          size="icon"
          type="button"
          @click="removeRow(row.id)"
        >
          <Trash2 class="h-4 w-4" />
        </Button>
      </div>
    </div>
    <p v-else class="text-xs text-slate-500">No custom metrics labels.</p>
  </div>
</template>
