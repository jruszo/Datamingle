<script setup lang="ts">
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { RouterLink } from 'vue-router'
import { useDebounceFn } from '@vueuse/core'
import {
  ChevronDown,
  ChevronRight,
  Copy,
  Database,
  Network,
  Pencil,
  Plus,
  RefreshCw,
  ServerCog,
  X,
} from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { useAuthStore } from '@/stores/auth'
import {
  assignInfrastructureNodeRemoteManager,
  clearInfrastructureNodeRemoteManager,
  createAgent,
  createInfrastructureNode,
  fetchAgents,
  fetchInfrastructureNodes,
  updateInfrastructureNode,
  type AgentCreateResponse,
  type AgentRecord,
  type AgentStatus,
  type InfrastructureAgentSummary,
  type InfrastructureNodeRecord,
  type InfrastructureServiceRecord,
} from '../api'

const authStore = useAuthStore()

const nodes = ref<InfrastructureNodeRecord[]>([])
const totalCount = ref(0)
const currentPage = ref(1)
const pageSize = ref(20)
const searchQuery = ref('')
const isLoading = ref(false)
const error = ref('')
const feedback = ref('')
const latestRequestId = ref(0)
const expandedNodeIds = ref<Set<number>>(new Set())

const isNodeDialogOpen = ref(false)
const editingNode = ref<InfrastructureNodeRecord | null>(null)
const nodeSubmitting = ref(false)
const nodeForm = reactive({
  node_name: '',
  hostname: '',
  environment: '',
  provider: '',
  enabled: true,
})

const isProvisionDialogOpen = ref(false)
const provisionNode = ref<InfrastructureNodeRecord | null>(null)
const provisionSubmitting = ref(false)
const provisionError = ref('')
const provisionForm = reactive({ name: '', display_name: '' })
const createdAgent = ref<AgentCreateResponse | null>(null)

const isRemoteDialogOpen = ref(false)
const remoteNode = ref<InfrastructureNodeRecord | null>(null)
const availableAgents = ref<AgentRecord[]>([])
const remoteAgentsLoading = ref(false)
const remoteSubmitting = ref(false)
const remoteError = ref('')
const remoteForm = reactive({
  agentId: 0,
  command_enabled: true,
  metrics_enabled: true,
  online_schema_enabled: false,
  logs_enabled: false,
})

const totalPages = computed(() => Math.max(1, Math.ceil(totalCount.value / pageSize.value)))

const canAccessInfrastructure = computed(() =>
  hasAnyPermission([
    'sql.menu_instance',
    'sql.menu_instance_list',
    'sql.menu_database',
    'api_agents.menu_agent',
  ]),
)
const canManageNodes = computed(() => hasPermission('sql.menu_instance'))
const canManageAgents = computed(() => hasPermission('api_agents.menu_agent'))

function hasPermission(permission: string) {
  if (authStore.currentUser?.is_superuser) {
    return true
  }
  return authStore.currentUser?.permissions?.includes(permission) ?? false
}

function hasAnyPermission(permissions: string[]) {
  return permissions.some((permission) => hasPermission(permission))
}

function requireToken() {
  if (!authStore.accessToken) {
    throw new Error('Missing access token. Please login again.')
  }
  return authStore.accessToken
}

function toUserFacingMessage(errorValue: unknown, fallback: string) {
  if (!(errorValue instanceof Error)) {
    return fallback
  }

  const separator = '): '
  const separatorIndex = errorValue.message.indexOf(separator)
  if (separatorIndex === -1) {
    return errorValue.message
  }

  return errorValue.message.slice(separatorIndex + separator.length)
}

async function loadNodes() {
  const requestId = latestRequestId.value + 1
  latestRequestId.value = requestId
  isLoading.value = true
  error.value = ''

  try {
    await authStore.loadCurrentUser()

    if (!canAccessInfrastructure.value) {
      nodes.value = []
      totalCount.value = 0
      error.value = 'You do not have permission to access infrastructure.'
      return
    }

    const response = await fetchInfrastructureNodes(requireToken(), {
      page: currentPage.value,
      size: pageSize.value,
      search: searchQuery.value,
    })

    if (requestId !== latestRequestId.value) {
      return
    }

    nodes.value = response.results
    totalCount.value = response.count
  } catch (errorValue) {
    if (requestId !== latestRequestId.value) {
      return
    }
    error.value = toUserFacingMessage(errorValue, 'Failed to load infrastructure.')
  } finally {
    if (requestId === latestRequestId.value) {
      isLoading.value = false
    }
  }
}

function handleSearchQueryChange(value: string) {
  searchQuery.value = value
  currentPage.value = 1
}

function handlePageSizeChange(event: Event) {
  const target = event.target as HTMLSelectElement
  pageSize.value = Number(target.value)
  currentPage.value = 1
}

function toggleNode(nodeId: number) {
  const next = new Set(expandedNodeIds.value)
  if (next.has(nodeId)) {
    next.delete(nodeId)
  } else {
    next.add(nodeId)
  }
  expandedNodeIds.value = next
}

function isNodeExpanded(nodeId: number) {
  return expandedNodeIds.value.has(nodeId)
}

function openCreateNodeDialog() {
  editingNode.value = null
  nodeForm.node_name = ''
  nodeForm.hostname = ''
  nodeForm.environment = ''
  nodeForm.provider = ''
  nodeForm.enabled = true
  isNodeDialogOpen.value = true
}

function openEditNodeDialog(node: InfrastructureNodeRecord) {
  editingNode.value = node
  nodeForm.node_name = node.node_name
  nodeForm.hostname = node.hostname
  nodeForm.environment = node.environment
  nodeForm.provider = node.provider
  nodeForm.enabled = node.enabled
  isNodeDialogOpen.value = true
}

function closeNodeDialog() {
  isNodeDialogOpen.value = false
  editingNode.value = null
}

async function submitNodeForm() {
  nodeSubmitting.value = true
  error.value = ''
  feedback.value = ''
  const payload = {
    node_name: nodeForm.node_name.trim(),
    hostname: nodeForm.hostname.trim(),
    environment: nodeForm.environment.trim(),
    provider: nodeForm.provider.trim(),
    enabled: nodeForm.enabled,
  }

  try {
    if (editingNode.value) {
      await updateInfrastructureNode(requireToken(), editingNode.value.id, payload)
      feedback.value = `Node "${payload.node_name}" updated.`
    } else {
      await createInfrastructureNode(requireToken(), payload)
      feedback.value = `Node "${payload.node_name}" created.`
    }
    closeNodeDialog()
    await loadNodes()
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to save node.')
  } finally {
    nodeSubmitting.value = false
  }
}

function openProvisionDialog(node: InfrastructureNodeRecord) {
  provisionNode.value = node
  provisionForm.name = `${node.node_name}-agent`
  provisionForm.display_name = `${node.node_name} Agent`
  provisionError.value = ''
  createdAgent.value = null
  isProvisionDialogOpen.value = true
}

function closeProvisionDialog() {
  isProvisionDialogOpen.value = false
  provisionNode.value = null
  createdAgent.value = null
}

async function submitProvisionAgent() {
  if (!provisionNode.value) {
    return
  }
  provisionSubmitting.value = true
  provisionError.value = ''

  try {
    createdAgent.value = await createAgent(
      {
        name: provisionForm.name.trim(),
        display_name: provisionForm.display_name.trim(),
        local_node: provisionNode.value.id,
      },
      requireToken(),
    )
    await loadNodes()
  } catch (errorValue) {
    provisionError.value = toUserFacingMessage(errorValue, 'Failed to provision agent.')
  } finally {
    provisionSubmitting.value = false
  }
}

async function copyText(value: string) {
  try {
    await navigator.clipboard.writeText(value)
  } catch (errorValue) {
    console.error('Failed to copy text to clipboard.', errorValue)
  }
}

async function openRemoteDialog(node: InfrastructureNodeRecord) {
  remoteNode.value = node
  remoteError.value = ''
  remoteForm.agentId = node.remote_manager?.id ?? 0
  remoteForm.command_enabled = node.remote_manager?.command_enabled ?? true
  remoteForm.metrics_enabled = node.remote_manager?.metrics_enabled ?? true
  remoteForm.online_schema_enabled = node.remote_manager?.online_schema_enabled ?? false
  remoteForm.logs_enabled = node.remote_manager?.logs_enabled ?? false
  isRemoteDialogOpen.value = true
  await loadAvailableAgents()
  if (!remoteForm.agentId && availableAgents.value[0]) {
    remoteForm.agentId = availableAgents.value[0].id
  }
}

function closeRemoteDialog() {
  isRemoteDialogOpen.value = false
  remoteNode.value = null
  remoteError.value = ''
}

async function loadAvailableAgents() {
  remoteAgentsLoading.value = true
  try {
    const response = await fetchAgents(requireToken(), { page: 1, size: 100 })
    availableAgents.value = response.results
  } catch (errorValue) {
    remoteError.value = toUserFacingMessage(errorValue, 'Failed to load agents.')
  } finally {
    remoteAgentsLoading.value = false
  }
}

async function submitRemoteManager() {
  if (!remoteNode.value || !remoteForm.agentId) {
    remoteError.value = 'Select an agent.'
    return
  }

  remoteSubmitting.value = true
  remoteError.value = ''
  feedback.value = ''

  try {
    await assignInfrastructureNodeRemoteManager(requireToken(), remoteNode.value.id, {
      agent: remoteForm.agentId,
      modules: [],
      capabilities: [],
      command_enabled: remoteForm.command_enabled,
      metrics_enabled: remoteForm.metrics_enabled,
      online_schema_enabled: remoteForm.online_schema_enabled,
      logs_enabled: remoteForm.logs_enabled,
    })
    feedback.value = `Remote manager assigned to "${remoteNode.value.node_name}".`
    closeRemoteDialog()
    await loadNodes()
  } catch (errorValue) {
    remoteError.value = toUserFacingMessage(errorValue, 'Failed to assign remote manager.')
  } finally {
    remoteSubmitting.value = false
  }
}

async function clearRemoteManager(node: InfrastructureNodeRecord) {
  remoteSubmitting.value = true
  error.value = ''
  feedback.value = ''

  try {
    await clearInfrastructureNodeRemoteManager(requireToken(), node.id)
    feedback.value = `Remote manager cleared from "${node.node_name}".`
    closeRemoteDialog()
    await loadNodes()
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to clear remote manager.')
  } finally {
    remoteSubmitting.value = false
  }
}

function agentDisplayName(agent: InfrastructureAgentSummary) {
  return agent.display_name || agent.name
}

function statusLabel(value: AgentStatus) {
  switch (value) {
    case 'online':
      return 'Online'
    case 'offline':
      return 'Offline'
    case 'disabled':
      return 'Disabled'
    case 'revoked':
      return 'Revoked'
    default:
      return 'Pending'
  }
}

function statusBadgeClass(value: AgentStatus) {
  switch (value) {
    case 'online':
      return 'bg-emerald-100 text-emerald-800'
    case 'offline':
      return 'bg-amber-100 text-amber-800'
    case 'disabled':
    case 'revoked':
      return 'bg-red-100 text-red-800'
    default:
      return 'bg-slate-100 text-slate-700'
  }
}

function inventoryStatusLabel(value: InfrastructureServiceRecord['inventory_status']) {
  switch (value) {
    case 'ok':
      return 'OK'
    case 'stale':
      return 'Stale'
    case 'failed':
      return 'Failed'
    default:
      return 'Never'
  }
}

function inventoryStatusBadgeClass(value: InfrastructureServiceRecord['inventory_status']) {
  switch (value) {
    case 'ok':
      return 'bg-emerald-100 text-emerald-800'
    case 'stale':
      return 'bg-amber-100 text-amber-800'
    case 'failed':
      return 'bg-red-100 text-red-800'
    default:
      return 'bg-slate-100 text-slate-700'
  }
}

function formatDateTime(value: string | null) {
  if (!value) {
    return 'Never'
  }
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return value
  }
  return date.toLocaleString()
}

onMounted(() => {
  void loadNodes()
})

const debouncedLoadNodes = useDebounceFn(() => {
  feedback.value = ''
  void loadNodes()
}, 250)

watch([currentPage, pageSize], () => {
  feedback.value = ''
  void loadNodes()
})

watch(searchQuery, () => {
  feedback.value = ''
  debouncedLoadNodes()
})
</script>

<template>
  <section class="grid gap-5">
    <div class="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
      <div class="space-y-1">
        <h2 class="text-2xl font-semibold text-slate-900">Infrastructure</h2>
        <p class="text-sm text-slate-600">Nodes, services, and Datamingle agent management.</p>
      </div>
      <div class="flex flex-wrap gap-2">
        <Button variant="outline" type="button" :disabled="isLoading" @click="void loadNodes()">
          <RefreshCw class="h-4 w-4" />
          Refresh
        </Button>
        <Button v-if="canManageNodes" type="button" @click="openCreateNodeDialog">
          <Plus class="h-4 w-4" />
          New node
        </Button>
      </div>
    </div>

    <p v-if="error" class="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
      {{ error }}
    </p>
    <p
      v-else-if="feedback"
      class="rounded-lg border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700"
    >
      {{ feedback }}
    </p>

    <Card class="border-slate-200">
      <CardHeader class="gap-4 lg:flex-row lg:items-center lg:justify-between">
        <CardTitle class="flex items-center gap-2 text-base">
          <Network class="h-5 w-5 text-slate-500" />
          Nodes
        </CardTitle>
        <div class="flex flex-col gap-2 sm:flex-row sm:items-center">
          <Input
            :model-value="searchQuery"
            class="w-full sm:w-80"
            placeholder="Search nodes, hosts, services, or agents"
            @update:model-value="handleSearchQueryChange(String($event))"
          />
          <select
            :value="pageSize"
            class="h-10 rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-700"
            @change="handlePageSizeChange"
          >
            <option :value="10">10 rows</option>
            <option :value="20">20 rows</option>
            <option :value="50">50 rows</option>
          </select>
        </div>
      </CardHeader>
      <CardContent class="space-y-4">
        <div class="overflow-hidden rounded-lg border border-slate-200">
          <div class="overflow-x-auto">
            <table class="min-w-full divide-y divide-slate-200 bg-white">
              <thead class="bg-slate-50 text-left text-xs uppercase text-slate-500">
                <tr>
                  <th class="w-[30rem] px-4 py-3 font-medium">Node</th>
                  <th class="px-4 py-3 font-medium">Agents</th>
                  <th class="px-4 py-3 font-medium">Services</th>
                  <th class="px-4 py-3 font-medium">Environment</th>
                  <th class="px-4 py-3 text-right font-medium">Actions</th>
                </tr>
              </thead>
              <tbody class="divide-y divide-slate-200">
                <tr v-if="isLoading">
                  <td colspan="5" class="px-4 py-10 text-center text-sm text-slate-500">
                    Loading infrastructure...
                  </td>
                </tr>
                <tr v-else-if="nodes.length === 0">
                  <td colspan="5" class="px-4 py-10 text-center text-sm text-slate-500">
                    No infrastructure nodes are available.
                  </td>
                </tr>
                <template v-for="node in nodes" v-else :key="node.id">
                  <tr class="align-top">
                    <td class="px-4 py-4">
                      <div class="flex min-w-0 items-start gap-3">
                        <Button
                          variant="ghost"
                          size="icon"
                          type="button"
                          class="mt-1 h-8 w-8 shrink-0"
                          :aria-label="isNodeExpanded(node.id) ? `Collapse ${node.node_name}` : `Expand ${node.node_name}`"
                          @click="toggleNode(node.id)"
                        >
                          <ChevronDown v-if="isNodeExpanded(node.id)" class="h-4 w-4" />
                          <ChevronRight v-else class="h-4 w-4" />
                        </Button>
                        <div class="min-w-0">
                          <div class="flex min-w-0 flex-wrap items-center gap-2">
                            <p class="truncate text-sm font-semibold text-slate-900">{{ node.node_name }}</p>
                            <Badge v-if="!node.enabled" variant="secondary" class="bg-slate-100 text-slate-600">
                              Disabled
                            </Badge>
                          </div>
                          <p class="mt-1 truncate text-xs text-slate-500">
                            {{ node.hostname || 'No hostname' }}
                          </p>
                        </div>
                      </div>
                    </td>
                    <td class="px-4 py-4">
                      <div class="grid gap-2">
                        <div v-if="node.local_agent" class="rounded-md border border-slate-200 px-3 py-2">
                          <div class="flex flex-wrap items-center gap-2">
                            <Badge variant="outline" class="border-slate-300 text-slate-700">Local</Badge>
                            <Badge variant="secondary" :class="statusBadgeClass(node.local_agent.status)">
                              {{ statusLabel(node.local_agent.status) }}
                            </Badge>
                          </div>
                          <p class="mt-2 text-sm font-medium text-slate-900">
                            {{ agentDisplayName(node.local_agent) }}
                          </p>
                          <p class="mt-1 text-xs text-slate-500">
                            {{ node.local_agent.agent_version || 'Unknown version' }} · {{ formatDateTime(node.local_agent.last_seen_at) }}
                          </p>
                        </div>
                        <div v-if="node.remote_manager" class="rounded-md border border-slate-200 px-3 py-2">
                          <div class="flex flex-wrap items-center gap-2">
                            <Badge variant="outline" class="border-slate-300 text-slate-700">Remote</Badge>
                            <Badge variant="secondary" :class="statusBadgeClass(node.remote_manager.status)">
                              {{ statusLabel(node.remote_manager.status) }}
                            </Badge>
                          </div>
                          <p class="mt-2 text-sm font-medium text-slate-900">
                            {{ agentDisplayName(node.remote_manager) }}
                          </p>
                          <p class="mt-1 text-xs text-slate-500">
                            {{ node.remote_manager.agent_version || 'Unknown version' }} · {{ formatDateTime(node.remote_manager.last_seen_at) }}
                          </p>
                        </div>
                        <Badge
                          v-if="!node.local_agent && !node.remote_manager"
                          variant="secondary"
                          class="w-fit bg-amber-100 text-amber-800"
                        >
                          No agent
                        </Badge>
                      </div>
                    </td>
                    <td class="px-4 py-4">
                      <Badge variant="outline" class="border-slate-300 text-slate-700">
                        {{ node.service_count }} service{{ node.service_count === 1 ? '' : 's' }}
                      </Badge>
                    </td>
                    <td class="px-4 py-4 text-sm text-slate-600">
                      <div>{{ node.environment || '-' }}</div>
                      <div class="mt-1 text-xs text-slate-500">{{ node.provider || '-' }}</div>
                    </td>
                    <td class="px-4 py-4">
                      <div class="flex flex-wrap justify-end gap-2">
                        <Button
                          v-if="canManageAgents && !node.local_agent"
                          variant="outline"
                          size="sm"
                          type="button"
                          @click="openProvisionDialog(node)"
                        >
                          <ServerCog class="h-4 w-4" />
                          Provision
                        </Button>
                        <Button
                          v-if="canManageAgents"
                          variant="outline"
                          size="sm"
                          type="button"
                          @click="void openRemoteDialog(node)"
                        >
                          <Network class="h-4 w-4" />
                          Assign
                        </Button>
                        <Button
                          v-if="canManageNodes"
                          variant="outline"
                          size="sm"
                          type="button"
                          @click="openEditNodeDialog(node)"
                        >
                          <Pencil class="h-4 w-4" />
                          Edit
                        </Button>
                        <Button v-if="canManageNodes" as-child variant="outline" size="sm">
                          <RouterLink :to="{ name: 'inventory-new', query: { node: node.id } }">
                            <Plus class="h-4 w-4" />
                            Service
                          </RouterLink>
                        </Button>
                      </div>
                    </td>
                  </tr>
                  <tr v-if="isNodeExpanded(node.id)" class="bg-slate-50/70">
                    <td colspan="5" class="px-4 py-4">
                      <div class="overflow-hidden rounded-md border border-slate-200 bg-white">
                        <div v-if="node.services.length === 0" class="px-4 py-6 text-sm text-slate-500">
                          No services on this node.
                        </div>
                        <table v-else class="min-w-full divide-y divide-slate-200">
                          <thead class="bg-white text-left text-xs uppercase text-slate-500">
                            <tr>
                              <th class="px-4 py-3 font-medium">Service</th>
                              <th class="px-4 py-3 font-medium">Engine</th>
                              <th class="px-4 py-3 font-medium">Inventory</th>
                              <th class="px-4 py-3 font-medium">Endpoint</th>
                              <th class="px-4 py-3 text-right font-medium">Actions</th>
                            </tr>
                          </thead>
                          <tbody class="divide-y divide-slate-100">
                            <tr v-for="service in node.services" :key="service.id">
                              <td class="px-4 py-3">
                                <div class="flex items-center gap-2">
                                  <Database class="h-4 w-4 text-slate-400" />
                                  <span class="text-sm font-medium text-slate-900">{{ service.instance_name }}</span>
                                </div>
                                <p class="mt-1 text-xs text-slate-500">{{ service.db_name || 'Default database' }}</p>
                              </td>
                              <td class="px-4 py-3">
                                <Badge variant="outline" class="border-slate-300 text-slate-700">
                                  {{ service.db_type }}
                                </Badge>
                              </td>
                              <td class="px-4 py-3">
                                <Badge variant="secondary" :class="inventoryStatusBadgeClass(service.inventory_status)">
                                  {{ inventoryStatusLabel(service.inventory_status) }}
                                </Badge>
                                <p class="mt-1 text-xs text-slate-500">
                                  {{ service.inventory_detected_version || 'Unknown version' }}
                                </p>
                              </td>
                              <td class="px-4 py-3 text-sm text-slate-600">
                                {{ service.host }}:{{ service.port }}
                              </td>
                              <td class="px-4 py-3">
                                <div class="flex justify-end gap-2">
                                  <Button v-if="canManageNodes" as-child variant="outline" size="sm">
                                    <RouterLink :to="`/inventory/${service.id}`">Edit</RouterLink>
                                  </Button>
                                  <Button as-child variant="outline" size="sm">
                                    <RouterLink to="/instance-operations/databases">Databases</RouterLink>
                                  </Button>
                                </div>
                              </td>
                            </tr>
                          </tbody>
                        </table>
                      </div>
                    </td>
                  </tr>
                </template>
              </tbody>
            </table>
          </div>
        </div>

        <div class="flex flex-col gap-3 text-sm text-slate-600 sm:flex-row sm:items-center sm:justify-between">
          <span>{{ totalCount }} node{{ totalCount === 1 ? '' : 's' }}</span>
          <div class="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              type="button"
              :disabled="currentPage <= 1 || isLoading"
              @click="currentPage -= 1"
            >
              Previous
            </Button>
            <span>Page {{ currentPage }} of {{ totalPages }}</span>
            <Button
              variant="outline"
              size="sm"
              type="button"
              :disabled="currentPage >= totalPages || isLoading"
              @click="currentPage += 1"
            >
              Next
            </Button>
          </div>
        </div>
      </CardContent>
    </Card>

    <div
      v-if="isNodeDialogOpen"
      class="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/45 px-4 py-6"
      @click.self="closeNodeDialog"
    >
      <div class="max-h-[90vh] w-full max-w-xl overflow-y-auto rounded-lg bg-white shadow-xl">
        <div class="flex items-start justify-between border-b border-slate-200 px-6 py-4">
          <h3 class="text-lg font-semibold text-slate-900">{{ editingNode ? 'Edit node' : 'New node' }}</h3>
          <Button variant="ghost" size="icon" type="button" @click="closeNodeDialog">
            <X class="h-4 w-4" />
          </Button>
        </div>
        <form class="grid gap-4 px-6 py-5" @submit.prevent="void submitNodeForm()">
          <label class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">Name</span>
            <Input v-model="nodeForm.node_name" required placeholder="prod-db-node-01" />
          </label>
          <label class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">Hostname</span>
            <Input v-model="nodeForm.hostname" placeholder="db-01.internal" />
          </label>
          <div class="grid gap-4 sm:grid-cols-2">
            <label class="grid gap-2">
              <span class="text-sm font-medium text-slate-700">Environment</span>
              <Input v-model="nodeForm.environment" placeholder="production" />
            </label>
            <label class="grid gap-2">
              <span class="text-sm font-medium text-slate-700">Provider</span>
              <Input v-model="nodeForm.provider" placeholder="aws" />
            </label>
          </div>
          <label class="flex items-center gap-2 text-sm text-slate-700">
            <input v-model="nodeForm.enabled" class="h-4 w-4 rounded border-slate-300" type="checkbox">
            Enabled
          </label>
          <div class="flex justify-end gap-2 border-t border-slate-200 pt-4">
            <Button variant="outline" type="button" @click="closeNodeDialog">Cancel</Button>
            <Button type="submit" :disabled="nodeSubmitting">
              <Plus v-if="!editingNode" class="h-4 w-4" />
              <Pencil v-else class="h-4 w-4" />
              Save
            </Button>
          </div>
        </form>
      </div>
    </div>

    <div
      v-if="isProvisionDialogOpen"
      class="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/45 px-4 py-6"
      @click.self="closeProvisionDialog"
    >
      <div class="max-h-[90vh] w-full max-w-2xl overflow-y-auto rounded-lg bg-white shadow-xl">
        <div class="flex items-start justify-between border-b border-slate-200 px-6 py-4">
          <h3 class="text-lg font-semibold text-slate-900">Provision local agent</h3>
          <Button variant="ghost" size="icon" type="button" @click="closeProvisionDialog">
            <X class="h-4 w-4" />
          </Button>
        </div>

        <form v-if="!createdAgent" class="grid gap-4 px-6 py-5" @submit.prevent="void submitProvisionAgent()">
          <p v-if="provisionError" class="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
            {{ provisionError }}
          </p>
          <label class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">Agent name</span>
            <Input v-model="provisionForm.name" required />
          </label>
          <label class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">Display name</span>
            <Input v-model="provisionForm.display_name" />
          </label>
          <div class="flex justify-end gap-2 border-t border-slate-200 pt-4">
            <Button variant="outline" type="button" @click="closeProvisionDialog">Cancel</Button>
            <Button type="submit" :disabled="provisionSubmitting">
              <ServerCog class="h-4 w-4" />
              Provision
            </Button>
          </div>
        </form>

        <div v-else class="grid gap-4 px-6 py-5">
          <div class="rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">
            The API key is shown once.
          </div>
          <div class="grid gap-2">
            <div class="flex items-center justify-between gap-2">
              <span class="text-sm font-medium text-slate-700">API Key</span>
              <Button variant="outline" size="sm" type="button" @click="void copyText(createdAgent.api_key)">
                <Copy class="h-4 w-4" />
                Copy
              </Button>
            </div>
            <textarea class="min-h-24 rounded-md border border-slate-200 p-3 text-sm" readonly :value="createdAgent.api_key" />
          </div>
          <div class="grid gap-2">
            <div class="flex items-center justify-between gap-2">
              <span class="text-sm font-medium text-slate-700">Install Command</span>
              <Button variant="outline" size="sm" type="button" @click="void copyText(createdAgent.install_command)">
                <Copy class="h-4 w-4" />
                Copy
              </Button>
            </div>
            <textarea class="min-h-32 rounded-md border border-slate-200 p-3 text-sm" readonly :value="createdAgent.install_command" />
          </div>
          <div class="flex justify-end border-t border-slate-200 pt-4">
            <Button type="button" @click="closeProvisionDialog">Done</Button>
          </div>
        </div>
      </div>
    </div>

    <div
      v-if="isRemoteDialogOpen"
      class="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/45 px-4 py-6"
      @click.self="closeRemoteDialog"
    >
      <div class="max-h-[90vh] w-full max-w-xl overflow-y-auto rounded-lg bg-white shadow-xl">
        <div class="flex items-start justify-between border-b border-slate-200 px-6 py-4">
          <h3 class="text-lg font-semibold text-slate-900">Assign remote manager</h3>
          <Button variant="ghost" size="icon" type="button" @click="closeRemoteDialog">
            <X class="h-4 w-4" />
          </Button>
        </div>
        <form class="grid gap-4 px-6 py-5" @submit.prevent="void submitRemoteManager()">
          <p v-if="remoteError" class="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
            {{ remoteError }}
          </p>
          <label class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">Agent</span>
            <select
              v-model.number="remoteForm.agentId"
              class="h-10 rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-700"
              :disabled="remoteAgentsLoading"
            >
              <option :value="0">Select agent</option>
              <option v-for="agent in availableAgents" :key="agent.id" :value="agent.id">
                {{ agent.display_name || agent.name }}
              </option>
            </select>
          </label>
          <div class="grid gap-3 rounded-md border border-slate-200 p-4">
            <label class="flex items-center gap-2 text-sm text-slate-700">
              <input v-model="remoteForm.command_enabled" class="h-4 w-4 rounded border-slate-300" type="checkbox">
              Command execution
            </label>
            <label class="flex items-center gap-2 text-sm text-slate-700">
              <input v-model="remoteForm.metrics_enabled" class="h-4 w-4 rounded border-slate-300" type="checkbox">
              Metrics
            </label>
            <label class="flex items-center gap-2 text-sm text-slate-700">
              <input v-model="remoteForm.online_schema_enabled" class="h-4 w-4 rounded border-slate-300" type="checkbox">
              Online schema
            </label>
            <label class="flex items-center gap-2 text-sm text-slate-700">
              <input v-model="remoteForm.logs_enabled" class="h-4 w-4 rounded border-slate-300" type="checkbox">
              Logs
            </label>
          </div>
          <div class="flex flex-wrap justify-between gap-2 border-t border-slate-200 pt-4">
            <Button
              v-if="remoteNode && remoteNode.remote_manager"
              variant="outline"
              type="button"
              :disabled="remoteSubmitting"
              @click="remoteNode && void clearRemoteManager(remoteNode)"
            >
              Clear
            </Button>
            <div class="ml-auto flex gap-2">
              <Button variant="outline" type="button" @click="closeRemoteDialog">Cancel</Button>
              <Button type="submit" :disabled="remoteSubmitting || remoteAgentsLoading">
                <Network class="h-4 w-4" />
                Assign
              </Button>
            </div>
          </div>
        </form>
      </div>
    </div>
  </section>
</template>
