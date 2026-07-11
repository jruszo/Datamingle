<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { onClickOutside } from '@vueuse/core'
import {
  AlertTriangle,
  ArrowDown,
  CheckCircle2,
  Database,
  Network,
  RefreshCw,
  Search,
  Server,
} from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { useAuthStore } from '@/stores/auth'
import { fetchMysqlTopology, type MysqlClusterRecord, type MysqlTopologyMember } from '../api'

const authStore = useAuthStore()
const clusters = ref<MysqlClusterRecord[]>([])
const standaloneServices = ref<MysqlTopologyMember[]>([])
const loading = ref(false)
const error = ref('')
const searchQuery = ref('')
const selectedEntry = ref('')
const showSearchResults = ref(false)
const searchContainer = ref<HTMLElement | null>(null)
const activeOptionIndex = ref(-1)
const summary = ref({ cluster_count: 0, healthy_cluster_count: 0, service_count: 0 })
let searchTimer: ReturnType<typeof setTimeout> | null = null
let latestRequest = 0

const issueCount = computed(() => summary.value.cluster_count - summary.value.healthy_cluster_count)
const hasTopologyEntries = computed(() => summary.value.service_count > 0)
const topologyOptions = computed(() => {
  return [
    ...clusters.value.map((cluster) => ({
      key: `cluster:${cluster.id}`,
      label: cluster.name,
      detail: `${cluster.member_count} members · ${statusLabel(cluster.topology_status)}`,
    })),
    ...standaloneServices.value.map((service) => ({
      key: `service:${service.id}`,
      label: service.name,
      detail: `${service.host}:${service.port} · Standalone`,
    })),
  ]
})
const activeOptionId = computed(() =>
  activeOptionIndex.value >= 0 ? `topology-option-${activeOptionIndex.value}` : undefined,
)
const selectedCluster = computed(() => {
  if (!selectedEntry.value.startsWith('cluster:')) return null
  const id = Number(selectedEntry.value.slice('cluster:'.length))
  return clusters.value.find((cluster) => cluster.id === id) ?? null
})
const selectedService = computed(() => {
  if (!selectedEntry.value.startsWith('service:')) return null
  const id = Number(selectedEntry.value.slice('service:'.length))
  return standaloneServices.value.find((service) => service.id === id) ?? null
})

async function loadTopology(search = searchQuery.value) {
  if (!authStore.accessToken) {
    error.value = 'Missing access token. Please sign in again.'
    return
  }
  loading.value = true
  error.value = ''
  const requestId = ++latestRequest
  try {
    const response = await fetchMysqlTopology(authStore.accessToken, search)
    if (requestId !== latestRequest) return
    summary.value = response.summary
    clusters.value = response.clusters
    standaloneServices.value = response.standalone_services
    if (!topologyOptions.value.some((option) => option.key === selectedEntry.value)) {
      selectedEntry.value = ''
    }
  } catch (requestError) {
    if (requestId !== latestRequest) return
    error.value =
      requestError instanceof Error ? requestError.message : 'Failed to load cluster topology.'
  } finally {
    if (requestId === latestRequest) loading.value = false
  }
}

function statusLabel(status: string) {
  const labels: Record<string, string> = {
    ok: 'Healthy',
    missing_master: 'Missing primary',
    ambiguous_master: 'Multiple primaries',
    drift: 'Topology drift',
    unknown: 'Unknown',
  }
  return labels[status] ?? status.replaceAll('_', ' ')
}

function statusClass(status: string) {
  if (status === 'ok') return 'border-emerald-200 bg-emerald-50 text-emerald-700'
  if (status === 'unknown') return 'border-slate-200 bg-slate-50 text-slate-600'
  return 'border-amber-200 bg-amber-50 text-amber-700'
}

function formatSeen(value: string | null) {
  if (!value) return 'Not reported'
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString()
}

function primaryMembers(cluster: MysqlClusterRecord) {
  return cluster.members.filter((member) => member.role === 'primary')
}

function replicaMembers(cluster: MysqlClusterRecord) {
  return cluster.members.filter((member) => member.role !== 'primary')
}

onMounted(() => loadTopology(''))

function selectTopologyEntry(key: string) {
  selectedEntry.value = key
  const option = topologyOptions.value.find((entry) => entry.key === key)
  searchQuery.value = option?.label ?? ''
  showSearchResults.value = false
  activeOptionIndex.value = -1
}

function handleSearchInput() {
  latestRequest += 1
  loading.value = false
  selectedEntry.value = ''
  clusters.value = []
  standaloneServices.value = []
  activeOptionIndex.value = -1
  showSearchResults.value = true
  if (searchTimer) clearTimeout(searchTimer)
  searchTimer = setTimeout(() => void loadTopology(searchQuery.value), 300)
}

function handleSearchKeydown(event: KeyboardEvent) {
  if (event.key === 'Escape') {
    showSearchResults.value = false
    activeOptionIndex.value = -1
    return
  }
  if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
    event.preventDefault()
    showSearchResults.value = true
    if (topologyOptions.value.length === 0) return
    const direction = event.key === 'ArrowDown' ? 1 : -1
    activeOptionIndex.value =
      (activeOptionIndex.value + direction + topologyOptions.value.length) %
      topologyOptions.value.length
    return
  }
  if (event.key === 'Enter' && activeOptionIndex.value >= 0) {
    event.preventDefault()
    const option = topologyOptions.value[activeOptionIndex.value]
    if (option) selectTopologyEntry(option.key)
  }
}

onBeforeUnmount(() => {
  if (searchTimer) clearTimeout(searchTimer)
})

onClickOutside(searchContainer, () => {
  showSearchResults.value = false
  activeOptionIndex.value = -1
})
</script>

<template>
  <section class="space-y-5 pb-6">
    <div
      class="flex flex-col gap-4 rounded-xl border border-slate-200 bg-white p-5 shadow-sm sm:flex-row sm:items-center sm:justify-between"
    >
      <div>
        <p class="text-xs font-semibold uppercase tracking-wider text-violet-600">Infrastructure</p>
        <h2 class="mt-1 text-xl font-semibold text-slate-950">Cluster topology</h2>
        <p class="mt-1 text-sm text-slate-500">
          See how MySQL services are grouped and identify replication issues.
        </p>
      </div>
      <Button
        variant="outline"
        class="gap-2"
        :disabled="loading"
        @click="loadTopology(searchQuery)"
      >
        <RefreshCw class="h-4 w-4" :class="{ 'animate-spin': loading }" />
        Refresh
      </Button>
    </div>

    <p
      v-if="error"
      class="rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700"
    >
      {{ error }}
    </p>

    <div class="grid gap-3 sm:grid-cols-3">
      <Card
        ><CardContent class="flex items-center gap-3 p-4"
          ><span class="grid h-10 w-10 place-items-center rounded-lg bg-violet-50 text-violet-700"
            ><Network class="h-5 w-5"
          /></span>
          <div>
            <p class="text-2xl font-semibold">{{ summary.cluster_count }}</p>
            <p class="text-sm text-slate-500">Clusters</p>
          </div></CardContent
        ></Card
      >
      <Card
        ><CardContent class="flex items-center gap-3 p-4"
          ><span class="grid h-10 w-10 place-items-center rounded-lg bg-emerald-50 text-emerald-700"
            ><CheckCircle2 class="h-5 w-5"
          /></span>
          <div>
            <p class="text-2xl font-semibold">{{ summary.healthy_cluster_count }}</p>
            <p class="text-sm text-slate-500">Healthy</p>
          </div></CardContent
        ></Card
      >
      <Card
        ><CardContent class="flex items-center gap-3 p-4"
          ><span class="grid h-10 w-10 place-items-center rounded-lg bg-amber-50 text-amber-700"
            ><Database class="h-5 w-5"
          /></span>
          <div>
            <p class="text-2xl font-semibold">{{ summary.service_count }}</p>
            <p class="text-sm text-slate-500">MySQL services</p>
          </div></CardContent
        ></Card
      >
    </div>

    <Card v-if="hasTopologyEntries">
      <CardContent class="p-4">
        <div ref="searchContainer" class="relative max-w-2xl">
          <label class="grid gap-1.5 text-sm font-medium text-slate-700">
            Search clusters and services
            <span class="relative">
              <Search
                class="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-400"
              />
              <input
                v-model="searchQuery"
                type="search"
                placeholder="Search by name, host, or status…"
                class="h-10 w-full rounded-md border border-slate-200 bg-white pl-9 pr-3 text-sm outline-none transition focus:border-violet-400 focus:ring-2 focus:ring-violet-100"
                autocomplete="off"
                role="combobox"
                :aria-expanded="showSearchResults"
                :aria-activedescendant="activeOptionId"
                aria-controls="topology-search-results"
                @focus="showSearchResults = true"
                @input="handleSearchInput"
                @keydown="handleSearchKeydown"
              />
            </span>
          </label>
          <div
            v-if="showSearchResults"
            id="topology-search-results"
            class="absolute z-20 mt-2 max-h-72 w-full overflow-y-auto rounded-lg border border-slate-200 bg-white p-1.5 shadow-lg"
            role="listbox"
          >
            <div
              v-for="(option, optionIndex) in topologyOptions"
              :key="option.key"
              :id="`topology-option-${optionIndex}`"
              role="option"
              :aria-selected="optionIndex === activeOptionIndex"
              class="flex w-full cursor-pointer items-center justify-between gap-4 rounded-md px-3 py-2.5 text-left transition hover:bg-slate-100"
              :class="{ 'bg-slate-100': optionIndex === activeOptionIndex }"
              @mousemove="activeOptionIndex = optionIndex"
              @mousedown.prevent="selectTopologyEntry(option.key)"
            >
              <span class="truncate text-sm font-medium text-slate-900">{{ option.label }}</span>
              <span class="shrink-0 text-xs text-slate-500">{{ option.detail }}</span>
            </div>
            <p
              v-if="topologyOptions.length === 0"
              class="px-3 py-6 text-center text-sm text-slate-500"
            >
              No matching clusters or services.
            </p>
          </div>
        </div>
      </CardContent>
    </Card>

    <div
      v-if="loading && !hasTopologyEntries"
      class="grid min-h-64 place-items-center rounded-xl border border-slate-200 bg-white text-sm text-slate-500"
    >
      Loading cluster topology…
    </div>
    <div
      v-else-if="!hasTopologyEntries"
      class="grid min-h-64 place-items-center rounded-xl border border-dashed border-slate-300 bg-white text-center"
    >
      <div>
        <Network class="mx-auto h-8 w-8 text-slate-300" />
        <p class="mt-3 font-medium text-slate-800">No MySQL services discovered</p>
        <p class="mt-1 max-w-sm text-sm text-slate-500">
          Services appear after they are added to a node and their inventory is collected.
        </p>
        <RouterLink
          to="/infrastructure"
          class="mt-4 inline-flex text-sm font-medium text-violet-700 hover:text-violet-900"
          >View nodes and services</RouterLink
        >
      </div>
    </div>

    <div
      v-else-if="!selectedCluster && !selectedService"
      class="grid min-h-56 place-items-center rounded-xl border border-dashed border-slate-300 bg-white text-center"
    >
      <div>
        <Search class="mx-auto h-8 w-8 text-slate-300" />
        <p class="mt-3 font-medium text-slate-800">Select a cluster or service</p>
        <p class="mt-1 text-sm text-slate-500">
          Search above and choose a result to display its topology.
        </p>
      </div>
    </div>

    <div v-if="selectedCluster">
      <Card class="overflow-hidden">
        <CardHeader class="border-b border-slate-100 bg-slate-50/60 pb-4">
          <div class="flex items-start justify-between gap-3">
            <div>
              <CardTitle>{{ selectedCluster.name }}</CardTitle
              ><CardDescription class="mt-1"
                >{{ selectedCluster.member_count }} members ·
                {{
                  selectedCluster.membership_source === 'manual'
                    ? 'Manually managed'
                    : 'Automatically discovered'
                }}</CardDescription
              >
            </div>
            <Badge variant="outline" :class="statusClass(selectedCluster.topology_status)">{{
              statusLabel(selectedCluster.topology_status)
            }}</Badge>
          </div>
        </CardHeader>
        <CardContent class="space-y-4 p-5">
          <div v-if="primaryMembers(selectedCluster).length" class="grid gap-2">
            <p class="text-xs font-semibold uppercase tracking-wide text-slate-500">Primary</p>
            <div
              v-for="member in primaryMembers(selectedCluster)"
              :key="member.id"
              class="flex items-center justify-between gap-3 rounded-lg border border-emerald-200 bg-emerald-50/60 p-3"
            >
              <div class="flex min-w-0 items-center gap-3">
                <span
                  class="grid h-9 w-9 shrink-0 place-items-center rounded-lg bg-emerald-100 text-emerald-700"
                  ><Database class="h-4 w-4"
                /></span>
                <div class="min-w-0">
                  <p class="truncate text-sm font-semibold text-slate-900">{{ member.name }}</p>
                  <p class="truncate text-xs text-slate-500">
                    {{ member.host }}:{{ member.port }} ·
                    {{ member.node_name || 'Unassigned node' }}
                  </p>
                </div>
              </div>
              <div class="grid justify-items-end gap-1">
                <Badge v-if="member.write_eligible" class="bg-emerald-600">Writable</Badge>
                <Badge v-else variant="outline" class="border-slate-200 bg-white text-slate-600"
                  >Not writable</Badge
                >
                <p v-if="member.block_reason" class="max-w-56 text-right text-xs text-slate-500">
                  {{ member.block_reason }}
                </p>
              </div>
            </div>
          </div>
          <div
            v-else
            class="flex items-center gap-2 rounded-lg border border-amber-200 bg-amber-50 p-3 text-sm text-amber-800"
          >
            <AlertTriangle class="h-4 w-4 shrink-0" />No managed primary was detected.
          </div>

          <div
            v-if="replicaMembers(selectedCluster).length"
            class="flex justify-center text-slate-300"
          >
            <ArrowDown class="h-5 w-5" />
          </div>
          <div v-if="replicaMembers(selectedCluster).length" class="grid gap-2 sm:grid-cols-2">
            <div
              v-for="member in replicaMembers(selectedCluster)"
              :key="member.id"
              class="rounded-lg border border-slate-200 p-3"
            >
              <div class="flex items-center gap-2">
                <Server class="h-4 w-4 text-slate-400" />
                <p class="truncate text-sm font-medium text-slate-900">{{ member.name }}</p>
              </div>
              <p class="mt-1 truncate text-xs text-slate-500">
                {{ member.host }}:{{ member.port }}
              </p>
              <p class="mt-1 truncate text-xs text-slate-500">
                {{ member.node_name || 'Unassigned node' }} ·
                {{ member.role === 'replica' ? 'Replica' : statusLabel(member.topology_status) }}
              </p>
              <div class="mt-2 flex items-start justify-between gap-2">
                <Badge v-if="member.write_eligible" class="bg-emerald-600">Writable</Badge>
                <Badge v-else variant="outline" class="border-slate-200 bg-slate-50 text-slate-600"
                  >Not writable</Badge
                >
                <p v-if="member.block_reason" class="text-right text-xs text-slate-500">
                  {{ member.block_reason }}
                </p>
              </div>
            </div>
          </div>

          <div
            v-if="selectedCluster.unmanaged_peers.length"
            class="rounded-lg border border-dashed border-amber-300 bg-amber-50/50 p-3"
          >
            <p class="text-xs font-semibold uppercase tracking-wide text-amber-800">
              Unmanaged peers
            </p>
            <div class="mt-2 flex flex-wrap gap-2">
              <span
                v-for="peer in selectedCluster.unmanaged_peers"
                :key="`${peer.host}:${peer.port}`"
                class="rounded-md bg-white px-2 py-1 text-xs text-slate-700"
                >{{ peer.host }}:{{ peer.port }} · {{ peer.role }}</span
              >
            </div>
          </div>
          <div
            class="flex items-center justify-between border-t border-slate-100 pt-3 text-xs text-slate-500"
          >
            <span>Last detected {{ formatSeen(selectedCluster.last_seen_at) }}</span
            ><span v-if="selectedCluster.active_alert_count" class="font-medium text-amber-700"
              >{{ selectedCluster.active_alert_count }} active alert{{
                selectedCluster.active_alert_count === 1 ? '' : 's'
              }}</span
            >
          </div>
        </CardContent>
      </Card>
    </div>

    <Card v-if="selectedService" class="overflow-hidden">
      <CardHeader class="border-b border-slate-100 bg-slate-50/60 pb-4">
        <div class="flex items-start justify-between gap-3">
          <div>
            <CardTitle>{{ selectedService.name }}</CardTitle>
            <CardDescription class="mt-1"
              >Standalone MySQL service · not part of a replication cluster.</CardDescription
            >
          </div>
          <Badge variant="outline" class="border-slate-200 bg-white text-slate-600">
            Standalone
          </Badge>
        </div>
      </CardHeader>
      <CardContent class="p-5">
        <div class="rounded-lg border border-slate-200 p-4">
          <div class="flex items-start justify-between gap-2">
            <div class="flex min-w-0 items-center gap-3">
              <span
                class="grid h-9 w-9 shrink-0 place-items-center rounded-lg bg-slate-100 text-slate-600"
                ><Database class="h-4 w-4"
              /></span>
              <div class="min-w-0">
                <p class="truncate text-sm font-semibold text-slate-900">
                  {{ selectedService.name }}
                </p>
                <p class="truncate text-xs text-slate-500">
                  {{ selectedService.host }}:{{ selectedService.port }}
                </p>
              </div>
            </div>
            <div class="grid justify-items-end gap-1">
              <Badge v-if="selectedService.write_eligible" class="bg-emerald-600">Writable</Badge>
              <Badge v-else variant="outline" class="border-slate-200 bg-slate-50 text-slate-600"
                >Not writable</Badge
              >
              <p
                v-if="selectedService.block_reason"
                class="max-w-56 text-right text-xs text-slate-500"
              >
                {{ selectedService.block_reason }}
              </p>
            </div>
          </div>
          <div
            class="mt-3 flex items-center justify-between border-t border-slate-100 pt-3 text-xs text-slate-500"
          >
            <span>{{ selectedService.node_name || 'Unassigned node' }}</span
            ><span>{{ formatSeen(selectedService.last_seen_at) }}</span>
          </div>
        </div>
      </CardContent>
    </Card>

    <p v-if="issueCount" class="text-xs text-slate-500">
      {{ issueCount }} cluster{{ issueCount === 1 ? '' : 's' }} need attention.
    </p>
  </section>
</template>
