<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { ChevronDown, ChevronLeft, ChevronRight, ChevronsUpDown, Columns3, Search } from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { cn } from '@/lib/utils'

import type { DataTableColumn } from './types'

type RowRecord = Record<string, unknown>
type SortDirection = 'asc' | 'desc'

const props = withDefaults(defineProps<{
  rows: RowRecord[]
  columns: DataTableColumn[]
  rowKey: string
  loading?: boolean
  emptyText?: string
  searchPlaceholder?: string
  searchKeys?: string[]
  initialSortKey?: string
  initialSortDirection?: SortDirection
  pageSizeOptions?: number[]
  initialPageSize?: number
  manualPagination?: boolean
  manualSearch?: boolean
  manualSort?: boolean
  totalRows?: number
  page?: number
  pageSize?: number
  searchQuery?: string
  sortKey?: string
  sortDirection?: SortDirection
}>(), {
  loading: false,
  emptyText: 'No records found.',
  searchPlaceholder: 'Filter rows',
  searchKeys: () => [],
  initialSortKey: '',
  initialSortDirection: 'asc',
  pageSizeOptions: () => [10, 20, 50],
  initialPageSize: 10,
  manualPagination: false,
  manualSearch: false,
  manualSort: false,
  totalRows: undefined,
  page: undefined,
  pageSize: undefined,
  searchQuery: undefined,
  sortKey: undefined,
  sortDirection: undefined,
})

const emit = defineEmits<{
  (e: 'update:page', value: number): void
  (e: 'update:pageSize', value: number): void
  (e: 'update:searchQuery', value: string): void
  (e: 'update:sortKey', value: string): void
  (e: 'update:sortDirection', value: SortDirection): void
}>()

const internalSearchQuery = ref('')
const internalCurrentPage = ref(1)
const internalSortKey = ref(props.initialSortKey)
const internalSortDirection = ref<SortDirection>(props.initialSortDirection)
const internalPageSize = ref(props.initialPageSize)
const visibleColumnKeys = ref(
  props.columns
    .filter((column) => column.defaultVisible !== false)
    .map((column) => column.key),
)

const currentSearchQuery = computed({
  get: () => props.searchQuery ?? internalSearchQuery.value,
  set: (value: string) => {
    if (props.searchQuery !== undefined) {
      emit('update:searchQuery', value)
      return
    }
    internalSearchQuery.value = value
  },
})

const currentPage = computed({
  get: () => props.page ?? internalCurrentPage.value,
  set: (value: number) => {
    if (props.page !== undefined) {
      emit('update:page', value)
      return
    }
    internalCurrentPage.value = value
  },
})

const currentSortKey = computed({
  get: () => props.sortKey ?? internalSortKey.value,
  set: (value: string) => {
    if (props.sortKey !== undefined) {
      emit('update:sortKey', value)
      return
    }
    internalSortKey.value = value
  },
})

const currentSortDirection = computed<SortDirection>({
  get: () => props.sortDirection ?? internalSortDirection.value,
  set: (value: SortDirection) => {
    if (props.sortDirection !== undefined) {
      emit('update:sortDirection', value)
      return
    }
    internalSortDirection.value = value
  },
})

const currentPageSize = computed({
  get: () => props.pageSize ?? internalPageSize.value,
  set: (value: number) => {
    if (props.pageSize !== undefined) {
      emit('update:pageSize', value)
      return
    }
    internalPageSize.value = value
  },
})

const hideableColumns = computed(() => props.columns.filter((column) => column.hideable !== false))
const visibleColumns = computed(() => props.columns.filter((column) => visibleColumnKeys.value.includes(column.key)))

const normalizedSearch = computed(() => currentSearchQuery.value.trim().toLowerCase())
const effectiveSearchKeys = computed(() => {
  if (props.searchKeys.length > 0) {
    return props.searchKeys
  }
  return props.columns
    .filter((column) => column.key !== 'actions')
    .map((column) => column.key)
})

function formatCellValue(value: unknown) {
  if (value === null || value === undefined) {
    return ''
  }
  if (Array.isArray(value)) {
    return value.join(', ')
  }
  if (typeof value === 'object') {
    return JSON.stringify(value)
  }
  return `${value}`
}

function getCellValue(row: RowRecord, key: string) {
  return row[key]
}

function getRowIdentifier(row: RowRecord) {
  const value = row[props.rowKey]
  if (typeof value === 'string' || typeof value === 'number') {
    return value
  }
  return JSON.stringify(value)
}

function toggleSort(column: DataTableColumn) {
  if (!column.sortable) {
    return
  }

  if (currentSortKey.value !== column.key) {
    currentSortKey.value = column.key
    currentSortDirection.value = 'asc'
    return
  }

  currentSortDirection.value = currentSortDirection.value === 'asc' ? 'desc' : 'asc'
}

function toggleColumn(columnKey: string) {
  if (visibleColumnKeys.value.includes(columnKey)) {
    if (visibleColumnKeys.value.length === 1) {
      return
    }
    visibleColumnKeys.value = visibleColumnKeys.value.filter((key) => key !== columnKey)
    return
  }

  visibleColumnKeys.value = [...visibleColumnKeys.value, columnKey]
}

const filteredRows = computed(() => {
  if (props.manualSearch) {
    return props.rows
  }

  if (!normalizedSearch.value) {
    return props.rows
  }

  return props.rows.filter((row) =>
    effectiveSearchKeys.value.some((key) =>
      formatCellValue(getCellValue(row, key)).toLowerCase().includes(normalizedSearch.value),
    ),
  )
})

const sortedRows = computed(() => {
  if (props.manualSort || !currentSortKey.value) {
    return filteredRows.value
  }

  const rows = [...filteredRows.value]
  rows.sort((leftRow, rightRow) => {
    const leftValue = formatCellValue(getCellValue(leftRow, currentSortKey.value)).toLowerCase()
    const rightValue = formatCellValue(getCellValue(rightRow, currentSortKey.value)).toLowerCase()

    if (leftValue === rightValue) {
      return 0
    }

    const order = leftValue.localeCompare(rightValue, undefined, {
      numeric: true,
      sensitivity: 'base',
    })
    return currentSortDirection.value === 'asc' ? order : -order
  })
  return rows
})

const totalRowCount = computed(() => {
  if (props.totalRows !== undefined) {
    return props.totalRows
  }
  return sortedRows.value.length
})

const totalPages = computed(() => Math.max(1, Math.ceil(totalRowCount.value / currentPageSize.value)))
const paginatedRows = computed(() => {
  if (props.manualPagination) {
    return sortedRows.value
  }
  const startIndex = (currentPage.value - 1) * currentPageSize.value
  return sortedRows.value.slice(startIndex, startIndex + currentPageSize.value)
})

watch(
  () => props.rows,
  () => {
    currentPage.value = 1
  },
  { deep: true },
)

watch([currentPageSize, sortedRows], () => {
  if (currentPage.value > totalPages.value) {
    currentPage.value = totalPages.value
  }
})

watch(currentSearchQuery, () => {
  currentPage.value = 1
})
</script>

<template>
  <div class="space-y-4">
    <div class="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
      <div class="relative w-full max-w-md">
        <Search class="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-400" />
        <Input
          v-model="currentSearchQuery"
          class="pl-10"
          :placeholder="searchPlaceholder"
        />
      </div>
      <div class="flex flex-wrap items-center gap-2">
        <Badge variant="secondary" class="bg-slate-100 text-slate-700">
          {{ totalRowCount }} rows
        </Badge>
        <details class="relative">
          <summary class="flex cursor-pointer list-none items-center gap-2 rounded-md border border-slate-200 bg-white px-3 py-2 text-sm font-medium text-slate-700 shadow-sm">
            <Columns3 class="h-4 w-4" />
            Columns
            <ChevronDown class="h-4 w-4" />
          </summary>
          <div class="absolute right-0 z-10 mt-2 w-56 rounded-xl border border-slate-200 bg-white p-3 shadow-lg">
            <div class="space-y-2">
              <label
                v-for="column in hideableColumns"
                :key="column.key"
                class="flex items-center gap-2 text-sm text-slate-700"
              >
                <input
                  :checked="visibleColumnKeys.includes(column.key)"
                  class="rounded border-slate-300"
                  type="checkbox"
                  @change="toggleColumn(column.key)"
                >
                <span>{{ column.label }}</span>
              </label>
            </div>
          </div>
        </details>
        <slot name="toolbar-actions" />
      </div>
    </div>

    <div class="overflow-hidden rounded-2xl border border-slate-200">
      <div class="overflow-x-auto">
        <table class="min-w-full divide-y divide-slate-200 bg-white">
          <thead class="bg-slate-50">
            <tr class="text-left text-xs uppercase tracking-wide text-slate-500">
              <th
                v-for="column in visibleColumns"
                :key="column.key"
                :class="cn('px-4 py-3 font-medium', column.headerClass)"
              >
                <button
                  :class="cn('inline-flex items-center gap-2', column.sortable ? 'cursor-pointer text-slate-700' : 'cursor-default')"
                  type="button"
                  @click="toggleSort(column)"
                >
                  <span>{{ column.label }}</span>
                  <ChevronsUpDown v-if="column.sortable" class="h-3.5 w-3.5" />
                </button>
              </th>
            </tr>
          </thead>
          <tbody class="divide-y divide-slate-200">
            <tr v-if="loading">
              <td :colspan="visibleColumns.length" class="px-4 py-12 text-center text-sm text-slate-500">
                Loading rows…
              </td>
            </tr>
            <tr v-else-if="paginatedRows.length === 0">
              <td :colspan="visibleColumns.length" class="px-4 py-12 text-center text-sm text-slate-500">
                {{ emptyText }}
              </td>
            </tr>
            <tr v-for="row in paginatedRows" :key="getRowIdentifier(row)" class="align-top">
              <td
                v-for="column in visibleColumns"
                :key="column.key"
                :class="cn('px-4 py-4 text-sm text-slate-700', column.class)"
              >
                <slot
                  :name="`cell-${column.key}`"
                  :column="column"
                  :row="row"
                  :value="getCellValue(row, column.key)"
                >
                  {{ formatCellValue(getCellValue(row, column.key)) || '—' }}
                </slot>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <div class="flex flex-col gap-3 border-t border-slate-200 pt-4 sm:flex-row sm:items-center sm:justify-between">
      <div class="flex items-center gap-3 text-sm text-slate-500">
        <span>Page {{ currentPage }} of {{ totalPages }}</span>
        <label class="flex items-center gap-2">
          <span>Rows</span>
          <select
            v-model.number="currentPageSize"
            class="rounded-md border border-slate-200 bg-white px-2 py-1 text-sm text-slate-700"
          >
            <option v-for="option in pageSizeOptions" :key="option" :value="option">{{ option }}</option>
          </select>
        </label>
      </div>
      <div class="flex gap-2">
        <Button variant="outline" :disabled="currentPage <= 1" @click="currentPage -= 1">
          <ChevronLeft class="h-4 w-4" />
          Previous
        </Button>
        <Button variant="outline" :disabled="currentPage >= totalPages" @click="currentPage += 1">
          Next
          <ChevronRight class="h-4 w-4" />
        </Button>
      </div>
    </div>
  </div>
</template>
