<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { Search, X } from 'lucide-vue-next'

import type { LabelFilter, LabelFilterMode } from '@/shared/filters/labelFilters'

type Suggestion = {
  key: string
  primary: string
  secondary: string
  value: string
}

const props = withDefaults(
  defineProps<{
    modelValue: LabelFilter[]
    labelNames: string[]
    loadValues: (name: string, filters: LabelFilter[]) => Promise<string[]>
    displayLabel?: (name: string) => string
    placeholder?: string
  }>(),
  {
    displayLabel: (name: string) => name,
    placeholder: 'Filter with environment:prod or -team:legacy',
  },
)

const emit = defineEmits<{
  'update:modelValue': [filters: LabelFilter[]]
}>()

const root = ref<HTMLElement | null>(null)
const input = ref<HTMLInputElement | null>(null)
const query = ref('')
const menuOpen = ref(false)
const highlightedIndex = ref(0)
const valueOptions = ref<Record<string, string[]>>({})
const valuesLoading = ref(false)
let valueRequestId = 0

const tokenState = computed(() => {
  const raw = query.value.trimStart()
  const mode: LabelFilterMode = raw.startsWith('-') ? 'exclude' : 'include'
  const expression = mode === 'exclude' ? raw.slice(1) : raw
  const separatorIndex = expression.indexOf(':')
  const labelText =
    separatorIndex === -1 ? expression.trim() : expression.slice(0, separatorIndex).trim()
  const valueText = separatorIndex === -1 ? '' : expression.slice(separatorIndex + 1).trim()
  const label = resolveLabel(labelText)
  return {
    mode,
    hasSeparator: separatorIndex !== -1,
    labelText,
    label,
    valueText,
  }
})

const suggestions = computed<Suggestion[]>(() => {
  const state = tokenState.value
  if (!state.hasSeparator) {
    const search = state.labelText.toLowerCase()
    return props.labelNames
      .map((name) => ({
        key: `label:${name}`,
        primary: props.displayLabel(name),
        secondary: props.displayLabel(name) === name ? '' : name,
        value: name,
      }))
      .filter(
        (item) =>
          !search ||
          item.primary.toLowerCase().includes(search) ||
          item.value.toLowerCase().includes(search),
      )
  }

  if (!state.label) return []
  const selectedLabel = state.label
  const search = state.valueText.toLowerCase()
  return (valueOptions.value[selectedLabel] ?? [])
    .filter((item) => !search || item.toLowerCase().includes(search))
    .map((item) => ({
      key: `value:${selectedLabel}:${item}`,
      primary: item,
      secondary: props.displayLabel(selectedLabel),
      value: item,
    }))
})

function resolveLabel(value: string) {
  const normalized = value.toLowerCase()
  return props.labelNames.find(
    (name) =>
      name.toLowerCase() === normalized || props.displayLabel(name).toLowerCase() === normalized,
  )
}

async function requestValues(name: string) {
  if (!name) return
  const requestId = ++valueRequestId
  valuesLoading.value = true
  const otherFilters = props.modelValue.filter((filter) => filter.label !== name)
  try {
    const values = await props.loadValues(name, otherFilters)
    if (requestId === valueRequestId) {
      valueOptions.value = { ...valueOptions.value, [name]: values.sort() }
    }
  } finally {
    if (requestId === valueRequestId) valuesLoading.value = false
  }
}

function mergeFilter(label: string, mode: LabelFilterMode, value: string) {
  const next = props.modelValue.map((filter) => ({
    ...filter,
    values: [...filter.values],
  }))
  const existing = next.find((filter) => filter.label === label && filter.mode === mode)
  if (existing) {
    existing.values = [...new Set([...existing.values, value])].sort()
  } else {
    next.push({ label, mode, values: [value] })
  }
  emit('update:modelValue', next)
}

function commitCurrentToken() {
  const state = tokenState.value
  if (!state.hasSeparator || !state.label || !state.valueText) return false
  mergeFilter(state.label, state.mode, state.valueText)
  query.value = ''
  menuOpen.value = false
  return true
}

function selectSuggestion(suggestion: Suggestion) {
  const state = tokenState.value
  if (!state.hasSeparator) {
    query.value = `${state.mode === 'exclude' ? '-' : ''}${props.displayLabel(suggestion.value)}:`
    void requestValues(suggestion.value)
    void nextTick(() => input.value?.focus())
    return
  }
  if (!state.label) return
  mergeFilter(state.label, state.mode, suggestion.value)
  query.value = ''
  menuOpen.value = false
  void nextTick(() => input.value?.focus())
}

function removeValue(target: LabelFilter, targetValue: string) {
  const next = props.modelValue
    .map((filter) => ({
      ...filter,
      values:
        filter.label === target.label && filter.mode === target.mode
          ? filter.values.filter((item) => item !== targetValue)
          : [...filter.values],
    }))
    .filter((filter) => filter.values.length > 0)
  emit('update:modelValue', next)
}

function removeLastValue() {
  const filter = props.modelValue.at(-1)
  const value = filter?.values.at(-1)
  if (filter && value) removeValue(filter, value)
}

function handleInput(event: Event) {
  const value = (event.target as HTMLInputElement).value
  const pieces = value.split(/\s+/)
  if (pieces.length > 1) {
    for (const piece of pieces.slice(0, -1)) {
      query.value = piece
      commitCurrentToken()
    }
    query.value = pieces.at(-1) ?? ''
  }
  menuOpen.value = true
}

function handleKeydown(event: KeyboardEvent) {
  if (event.key === 'ArrowDown') {
    event.preventDefault()
    menuOpen.value = true
    if (suggestions.value.length) {
      highlightedIndex.value = Math.min(highlightedIndex.value + 1, suggestions.value.length - 1)
    }
  } else if (event.key === 'ArrowUp') {
    event.preventDefault()
    highlightedIndex.value = Math.max(highlightedIndex.value - 1, 0)
  } else if (event.key === 'Enter' || event.key === 'Tab') {
    if (menuOpen.value && suggestions.value[highlightedIndex.value]) {
      event.preventDefault()
      selectSuggestion(suggestions.value[highlightedIndex.value]!)
    } else if (commitCurrentToken()) {
      event.preventDefault()
    }
  } else if (event.key === ' ' && tokenState.value.hasSeparator) {
    if (commitCurrentToken()) event.preventDefault()
  } else if (event.key === 'Backspace' && !query.value) {
    removeLastValue()
  } else if (event.key === 'Escape') {
    menuOpen.value = false
  }
}

function handleDocumentClick(event: MouseEvent) {
  if (!root.value?.contains(event.target as Node)) menuOpen.value = false
}

watch(
  () => tokenState.value.label,
  (label) => {
    if (tokenState.value.hasSeparator && label) void requestValues(label)
  },
)
watch(suggestions, () => {
  highlightedIndex.value = 0
})
onMounted(() => document.addEventListener('mousedown', handleDocumentClick))
onBeforeUnmount(() => document.removeEventListener('mousedown', handleDocumentClick))
</script>

<template>
  <div ref="root" class="relative">
    <div
      class="flex min-h-11 w-full flex-wrap items-center gap-1.5 rounded-md border border-slate-200 bg-white px-3 py-1.5 shadow-sm focus-within:border-slate-400 focus-within:ring-1 focus-within:ring-slate-400"
      @click="input?.focus()"
    >
      <Search class="h-4 w-4 shrink-0 text-slate-400" />
      <template v-for="filter in modelValue" :key="`${filter.mode}:${filter.label}`">
        <span
          v-for="item in filter.values"
          :key="item"
          class="flex max-w-full items-center gap-1 rounded bg-slate-100 px-2 py-1 text-xs text-slate-700"
        >
          <span class="truncate font-mono">
            {{ filter.mode === 'exclude' ? '-' : '' }}{{ displayLabel(filter.label) }}:{{ item }}
          </span>
          <button
            type="button"
            class="shrink-0 rounded text-slate-400 hover:text-slate-700"
            :aria-label="`Remove ${displayLabel(filter.label)}:${item}`"
            @click.stop="removeValue(filter, item)"
          >
            <X class="h-3 w-3" />
          </button>
        </span>
      </template>
      <input
        ref="input"
        v-model="query"
        class="h-7 min-w-44 flex-1 border-0 bg-transparent px-1 text-sm text-slate-900 outline-none placeholder:text-slate-400"
        :placeholder="placeholder"
        autocomplete="off"
        @focus="menuOpen = true"
        @input="handleInput"
        @keydown="handleKeydown"
      />
    </div>

    <div
      v-if="menuOpen"
      class="absolute left-0 right-0 top-[calc(100%+0.25rem)] z-40 max-h-72 overflow-auto rounded-md border border-slate-200 bg-white p-1 shadow-xl"
    >
      <p v-if="valuesLoading" class="px-3 py-2 text-xs text-slate-500">Loading values...</p>
      <button
        v-for="(suggestion, index) in suggestions"
        :key="suggestion.key"
        type="button"
        :class="[
          'flex w-full items-center justify-between gap-4 rounded px-3 py-2 text-left',
          index === highlightedIndex
            ? 'bg-slate-900 text-white'
            : 'text-slate-700 hover:bg-slate-100',
        ]"
        @mousedown.prevent="selectSuggestion(suggestion)"
      >
        <span class="font-mono text-sm">{{ suggestion.primary }}</span>
        <span v-if="suggestion.secondary" class="truncate text-[11px] opacity-60">
          {{ suggestion.secondary }}
        </span>
      </button>
      <p v-if="!valuesLoading && suggestions.length === 0" class="px-3 py-2 text-xs text-slate-500">
        {{
          tokenState.hasSeparator && !tokenState.label
            ? 'Select a known label before entering a value.'
            : 'No matching filters.'
        }}
      </p>
      <p class="border-t border-slate-100 px-3 py-2 text-[11px] text-slate-400">
        Use <span class="font-mono">label:value</span> to include and
        <span class="font-mono">-label:value</span> to exclude.
      </p>
    </div>
  </div>
</template>
