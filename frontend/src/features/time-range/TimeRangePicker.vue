<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { CalendarDays, Check, ChevronLeft, ChevronRight, Clock3 } from 'lucide-vue-next'

import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import {
  compactTimeRange,
  fromLocalDateTime,
  parseRelativeDuration,
  quickTimeRanges,
  toLocalDateTime,
  type TimeRangeValue,
} from '@/features/time-range/model'

const props = defineProps<{
  modelValue: TimeRangeValue
}>()

const emit = defineEmits<{
  'update:modelValue': [value: TimeRangeValue]
  change: [value: TimeRangeValue]
}>()

const open = ref(false)
const root = ref<HTMLElement | null>(null)
const customRelative = ref('4h')
const absoluteStart = ref('')
const absoluteEnd = ref('')
const month = ref(startOfMonth(new Date()))
const error = ref('')

const displayLabel = computed(() => compactTimeRange(props.modelValue))

watch(
  () => props.modelValue,
  (value) => {
    absoluteStart.value = toLocalDateTime(value.start)
    absoluteEnd.value = toLocalDateTime(value.end)
    const baseDate = value.start ? new Date(value.start) : new Date()
    month.value = startOfMonth(Number.isNaN(baseDate.getTime()) ? new Date() : baseDate)
  },
  { immediate: true, deep: true },
)

function apply(value: TimeRangeValue) {
  error.value = ''
  emit('update:modelValue', value)
  emit('change', value)
  open.value = false
}

function applyQuick(seconds: number) {
  apply({ mode: 'relative', seconds, start: '', end: '' })
}

function applyRelative() {
  const seconds = parseRelativeDuration(customRelative.value)
  if (seconds === null) {
    error.value = 'Use a duration such as 30m, 4h, 2d, or 1w (maximum 30d).'
    return
  }
  apply({ mode: 'relative', seconds, start: '', end: '' })
}

function applyAbsolute() {
  if (!absoluteStart.value || !absoluteEnd.value) {
    error.value = 'Select both a start and end time.'
    return
  }
  const start = new Date(absoluteStart.value)
  const end = new Date(absoluteEnd.value)
  const duration = end.getTime() - start.getTime()
  if (duration <= 0) {
    error.value = 'End must be after start.'
    return
  }
  if (duration > 30 * 86400 * 1000) {
    error.value = 'Absolute ranges cannot exceed 30 days.'
    return
  }
  apply({
    mode: 'absolute',
    seconds: Math.round(duration / 1000),
    start: fromLocalDateTime(absoluteStart.value),
    end: fromLocalDateTime(absoluteEnd.value),
  })
}

function selectDay(day: Date) {
  const selectedStart = absoluteStart.value ? new Date(absoluteStart.value) : null
  const selectedEnd = absoluteEnd.value ? new Date(absoluteEnd.value) : null
  if (!selectedStart || selectedEnd) {
    absoluteStart.value = toDateTimeInput(day, 0, 0)
    absoluteEnd.value = ''
    return
  }
  if (day < startOfDay(selectedStart)) {
    absoluteStart.value = toDateTimeInput(day, 0, 0)
    return
  }
  absoluteEnd.value = toDateTimeInput(day, 23, 59)
}

function dayState(day: Date) {
  const start = absoluteStart.value ? new Date(absoluteStart.value) : null
  const end = absoluteEnd.value ? new Date(absoluteEnd.value) : null
  const timestamp = startOfDay(day).getTime()
  return {
    selected:
      timestamp === (start ? startOfDay(start).getTime() : -1)
      || timestamp === (end ? startOfDay(end).getTime() : -1),
    inRange: Boolean(start && end && timestamp > startOfDay(start).getTime() && timestamp < startOfDay(end).getTime()),
  }
}

function calendarDays(value: Date) {
  const first = startOfMonth(value)
  const offset = first.getDay()
  const gridStart = new Date(first)
  gridStart.setDate(first.getDate() - offset)
  return Array.from({ length: 42 }, (_item, index) => {
    const day = new Date(gridStart)
    day.setDate(gridStart.getDate() + index)
    return day
  })
}

function startOfMonth(value: Date) {
  return new Date(value.getFullYear(), value.getMonth(), 1)
}

function startOfDay(value: Date) {
  return new Date(value.getFullYear(), value.getMonth(), value.getDate())
}

function addMonths(value: Date, amount: number) {
  return new Date(value.getFullYear(), value.getMonth() + amount, 1)
}

function toDateTimeInput(value: Date, hours: number, minutes: number) {
  const date = new Date(value.getFullYear(), value.getMonth(), value.getDate(), hours, minutes)
  const offset = date.getTimezoneOffset() * 60_000
  return new Date(date.getTime() - offset).toISOString().slice(0, 16)
}

function monthLabel(value: Date) {
  return new Intl.DateTimeFormat(undefined, { month: 'long', year: 'numeric' }).format(value)
}

function handleDocumentPointer(event: PointerEvent) {
  if (open.value && !root.value?.contains(event.target as Node)) {
    open.value = false
  }
}

function handleKeydown(event: KeyboardEvent) {
  if (event.key === 'Escape') open.value = false
}

onMounted(() => {
  document.addEventListener('pointerdown', handleDocumentPointer)
  window.addEventListener('keydown', handleKeydown)
})

onBeforeUnmount(() => {
  document.removeEventListener('pointerdown', handleDocumentPointer)
  window.removeEventListener('keydown', handleKeydown)
})
</script>

<template>
  <div ref="root" class="relative">
    <Button variant="outline" size="sm" type="button" class="min-w-24 justify-between px-2.5" @click="open = !open">
      <span class="flex min-w-0 items-center gap-1.5">
        <Clock3 class="h-3.5 w-3.5 shrink-0" />
        <span class="truncate text-xs">{{ displayLabel }}</span>
      </span>
      <ChevronRight :class="['h-3.5 w-3.5 transition-transform', open ? 'rotate-90' : '']" />
    </Button>

    <div
      v-if="open"
      class="absolute right-0 top-10 z-50 w-[min(46rem,calc(100vw-1rem))] overflow-hidden rounded-md border border-slate-200 bg-white shadow-2xl"
    >
      <div class="grid md:grid-cols-[12rem_minmax(0,1fr)]">
        <aside class="border-b border-slate-200 bg-slate-50 p-3 md:border-b-0 md:border-r">
          <p class="mb-1.5 text-[10px] font-semibold uppercase tracking-wide text-slate-500">
            Frequently used
          </p>
          <div class="grid grid-cols-2 gap-x-1 md:grid-cols-1">
            <button
              v-for="item in quickTimeRanges"
              :key="item.seconds"
              type="button"
              :class="[
                'flex h-7 items-center justify-between rounded px-2 text-left text-xs hover:bg-slate-200/70',
                modelValue.mode === 'relative' && modelValue.seconds === item.seconds
                  ? 'bg-slate-200 font-medium text-slate-950'
                  : 'text-slate-600',
              ]"
              @click="applyQuick(item.seconds)"
            >
              <span>{{ item.shortLabel }}</span>
              <span class="truncate text-[11px] text-slate-400">{{ item.label.replace('Past ', '') }}</span>
              <Check v-if="modelValue.mode === 'relative' && modelValue.seconds === item.seconds" class="h-3 w-3" />
            </button>
          </div>
          <p class="mb-1 mt-3 text-[10px] font-semibold uppercase tracking-wide text-slate-500">
            Custom
          </p>
          <form class="flex gap-1" @submit.prevent="applyRelative">
            <Input
              v-model="customRelative"
              class="h-7 font-mono text-xs"
              placeholder="4h"
              aria-label="Custom relative duration"
            />
            <Button size="xs" type="submit">Apply</Button>
          </form>
          <p class="mt-1 text-[10px] text-slate-400">Examples: 30m, 4h, 2d, 1w</p>
        </aside>

        <section class="p-3">
          <div class="mb-2 flex items-center justify-between">
            <span class="flex items-center gap-1.5 text-xs font-semibold text-slate-700">
              <CalendarDays class="h-3.5 w-3.5" /> Absolute range
            </span>
            <span class="text-[10px] text-slate-400">Local timezone</span>
          </div>
          <div class="mb-2 grid gap-2 sm:grid-cols-2">
            <label class="grid gap-0.5 text-xs">
              <span class="text-slate-500">From</span>
              <Input v-model="absoluteStart" type="datetime-local" class="h-8 text-xs" />
            </label>
            <label class="grid gap-0.5 text-xs">
              <span class="text-slate-500">To</span>
              <Input v-model="absoluteEnd" type="datetime-local" class="h-8 text-xs" />
            </label>
          </div>
          <div class="rounded border border-slate-200">
            <div class="flex h-8 items-center justify-between border-b border-slate-200 px-1">
              <Button variant="ghost" size="icon" type="button" @click="month = addMonths(month, -1)">
                <ChevronLeft class="h-3.5 w-3.5" />
              </Button>
              <span class="text-xs font-medium text-slate-700">{{ monthLabel(month) }}</span>
              <Button variant="ghost" size="icon" type="button" @click="month = addMonths(month, 1)">
                <ChevronRight class="h-3.5 w-3.5" />
              </Button>
            </div>
            <div class="p-2">
              <div class="grid grid-cols-7 text-center text-[11px] font-medium text-slate-400">
                <span v-for="dayName in ['Su', 'Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa']" :key="dayName">{{ dayName }}</span>
              </div>
              <div class="mt-1 grid grid-cols-7">
                <button
                  v-for="day in calendarDays(month)"
                  :key="day.toISOString()"
                  type="button"
                  :class="[
                    'h-7 rounded text-[11px]',
                    day.getMonth() === month.getMonth() ? 'text-slate-700' : 'text-slate-300',
                    dayState(day).inRange ? 'rounded-none bg-blue-50' : '',
                    dayState(day).selected ? 'bg-slate-900 font-semibold text-white' : 'hover:bg-slate-100',
                  ]"
                  @click="selectDay(day)"
                >
                  {{ day.getDate() }}
                </button>
              </div>
            </div>
          </div>
          <div class="mt-2 flex justify-end">
            <Button size="sm" type="button" @click="applyAbsolute">Apply dates</Button>
          </div>
        </section>
      </div>
      <p v-if="error" class="border-t border-red-200 bg-red-50 px-3 py-1.5 text-xs text-red-700">{{ error }}</p>
    </div>
  </div>
</template>
