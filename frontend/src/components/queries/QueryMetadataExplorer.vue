<script setup lang="ts">
import { computed } from 'vue'
import { Database, FolderTree, LoaderCircle, Minus, Plus, Table2 } from 'lucide-vue-next'

import type { QueryableInstance } from '@/lib/api'

import type {
  QueryMetadataNode,
  QueryMetadataTableDetailsMap,
} from './query-metadata-explorer'

const props = defineProps<{
  instances: QueryableInstance[]
  selectedInstanceId: number
  selectedNodeId: string
  nodes: QueryMetadataNode[]
  loading: boolean
  treeLoading: boolean
  tableDetails: QueryMetadataTableDetailsMap
  error?: string
}>()

const emit = defineEmits<{
  'select-instance': [instanceId: number]
  'toggle-node': [nodeId: string]
  'select-node': [nodeId: string]
  'insert-node': [nodeId: string]
}>()

const ENGINE_LABELS: Record<string, string> = {
  mysql: 'MySQL',
  pgsql: 'PgSQL',
  oracle: 'Oracle',
  mssql: 'MsSQL',
  mongo: 'Mongo',
  redis: 'Redis',
  clickhouse: 'ClickHouse',
  doris: 'Doris',
  cassandra: 'Cassandra',
  phoenix: 'Phoenix',
}

type VisibleTreeNode = {
  depth: number
  node: QueryMetadataNode
}

function engineLabel(dbType: string) {
  return ENGINE_LABELS[dbType] || dbType
}

const groupedInstances = computed(() => {
  const groups = new Map<string, QueryableInstance[]>()

  for (const instance of props.instances) {
    const label = engineLabel(instance.db_type)
    const items = groups.get(label) || []
    items.push(instance)
    groups.set(label, items)
  }

  return Array.from(groups.entries()).map(([label, items]) => ({ label, items }))
})

const visibleNodes = computed(() => {
  const rows: VisibleTreeNode[] = []

  function walk(nodes: QueryMetadataNode[], depth: number) {
    for (const node of nodes) {
      rows.push({ depth, node })
      if (node.isExpanded && node.children.length > 0) {
        walk(node.children, depth + 1)
      }
    }
  }

  walk(props.nodes, 0)
  return rows
})

function nodeIcon(node: QueryMetadataNode) {
  switch (node.kind) {
    case 'database':
      return Database
    case 'schema':
      return FolderTree
    default:
      return Table2
  }
}

function nodeBadge(node: QueryMetadataNode) {
  switch (node.kind) {
    case 'database':
      return 'DB'
    case 'schema':
      return 'Schema'
    default:
      return 'Table'
  }
}

function isSelected(node: QueryMetadataNode) {
  return props.selectedNodeId === node.id
}

function detailsForNode(node: QueryMetadataNode) {
  if (node.kind !== 'table' || !node.isExpanded) {
    return null
  }
  return props.tableDetails[node.id] || null
}
</script>

<template>
  <div class="flex h-fit flex-col gap-4 self-start rounded-[1.5rem] border border-slate-200 bg-white p-4 shadow-sm">
    <div class="space-y-3">
      <div class="flex items-center justify-between gap-3">
        <div>
          <p class="text-xs font-semibold uppercase tracking-[0.24em] text-slate-500">Explorer</p>
          <h2 class="mt-1 text-lg font-semibold text-slate-900">Instances and objects</h2>
        </div>
      </div>
      <select
        :value="selectedInstanceId"
        class="h-11 w-full rounded-xl border border-slate-200 bg-white px-3 text-sm text-slate-900 shadow-sm transition-colors focus:outline-none focus:ring-2 focus:ring-sky-100 disabled:cursor-not-allowed disabled:bg-slate-100 disabled:text-slate-400"
        :disabled="loading"
        @change="emit('select-instance', Number(($event.target as HTMLSelectElement).value))"
      >
        <option :value="0">{{ loading ? 'Loading instances...' : 'Select instance' }}</option>
        <optgroup v-for="group in groupedInstances" :key="group.label" :label="group.label">
          <option v-for="instance in group.items" :key="instance.id" :value="instance.id">
            {{ instance.instance_name }}
          </option>
        </optgroup>
      </select>

      <p class="text-sm text-slate-500">
        Pick an instance and browse databases, schemas, and tables. Double-click a table to insert it into the editor.
      </p>
    </div>

    <div class="grid gap-3">

      <p
        v-if="error"
        class="rounded-2xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700"
      >
        {{ error }}
      </p>

      <div class="min-h-[18rem] overflow-hidden rounded-[1.5rem] border border-slate-200 bg-slate-50/80">
        <div
          v-if="!selectedInstanceId"
          class="px-4 py-10 text-center text-sm text-slate-500"
        >
          Select an instance to load its metadata tree.
        </div>

        <div
          v-else-if="treeLoading && nodes.length === 0"
          class="flex items-center justify-center gap-2 px-4 py-10 text-sm text-slate-500"
        >
          <LoaderCircle class="h-4 w-4 animate-spin" />
          Loading databases...
        </div>

        <div
          v-else-if="visibleNodes.length === 0"
          class="px-4 py-10 text-center text-sm text-slate-500"
        >
          No visible objects were returned for this instance.
        </div>

        <div v-else class="max-h-[36rem] overflow-y-auto py-2">
          <div
            v-for="row in visibleNodes"
            :key="row.node.id"
            class="px-2"
          >
            <div
              class="flex items-center gap-2 rounded-xl px-2 py-2 text-left transition"
              :class="
                isSelected(row.node)
                  ? 'bg-sky-50 text-sky-900'
                  : 'text-slate-700 hover:bg-white hover:text-slate-900'
              "
              :style="{ paddingLeft: `${row.depth * 18 + 8}px` }"
            >
              <button
                type="button"
                class="flex h-6 w-6 items-center justify-center rounded-full text-slate-500 transition hover:bg-slate-200 hover:text-slate-900"
                @click.stop="emit('toggle-node', row.node.id)"
              >
                <LoaderCircle
                  v-if="row.node.isLoading"
                  class="h-4 w-4 animate-spin"
                />
                <Minus
                  v-else-if="row.node.isExpanded"
                  class="h-4 w-4"
                />
                <Plus
                  v-else
                  class="h-4 w-4"
                />
                <span class="sr-only">Toggle node</span>
              </button>

              <button
                type="button"
                class="flex min-w-0 flex-1 items-center gap-3 text-left"
                @click="emit('select-node', row.node.id)"
                @dblclick="row.node.kind === 'table' ? emit('insert-node', row.node.id) : undefined"
              >
                <component :is="nodeIcon(row.node)" class="h-4 w-4 shrink-0" />
                <span class="min-w-0 flex-1 truncate text-sm font-medium">
                  {{ row.node.name }}
                </span>
                <span class="rounded-full bg-white px-2 py-0.5 text-[11px] font-medium uppercase tracking-wide text-slate-500">
                  {{ nodeBadge(row.node) }}
                </span>
              </button>
            </div>

            <div
              v-if="detailsForNode(row.node)"
              class="mt-2 space-y-2"
              :style="{ marginLeft: `${row.depth * 18 + 42}px` }"
            >
              <div
                v-if="(detailsForNode(row.node)?.columns.length ?? 0) === 0"
                class="px-2 py-1 text-[11px] text-slate-500"
              >
                No structured column data was returned.
              </div>

              <div
                v-for="column in detailsForNode(row.node)?.columns ?? []"
                :key="`${column.name}-${column.type}`"
                class="flex items-center gap-3 rounded-lg px-2 py-1 text-left text-slate-600"
              >
                <span class="min-w-0 flex-1 truncate text-[11px] font-medium text-slate-700">
                  {{ column.name }}
                </span>
                <span class="shrink-0 text-[11px] text-slate-500">
                  {{ column.type || 'Unknown type' }}
                </span>
              </div>

              <div
                v-if="(detailsForNode(row.node)?.indexes.length ?? 0) > 0"
                class="pt-1"
              >
                <p class="px-2 text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-400">
                  Indexes
                </p>
                <div
                  v-for="index in detailsForNode(row.node)?.indexes ?? []"
                  :key="`${index.type}-${index.name}-${index.columns}`"
                  class="flex items-center gap-3 rounded-lg px-2 py-1 text-left text-slate-500"
                >
                  <span class="min-w-0 flex-1 truncate text-[11px]">
                    {{ index.name }}
                  </span>
                  <span class="truncate text-[11px]">
                    {{ index.columns || index.type }}
                  </span>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>
