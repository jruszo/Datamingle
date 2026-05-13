<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { useDebounceFn } from '@vueuse/core'
import { Ban, Copy, Eye, FileText, Plus, RefreshCw, Save, X } from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { DataTable, type DataTableColumn } from '@/components/ui/data-table'
import { Input } from '@/components/ui/input'
import { useAuthStore } from '@/stores/auth'
import {
  cancelAgentCommand,
  createAgent,
  fetchAgent,
  fetchAgentCommand,
  fetchAgentCommands,
  fetchAgents,
  fetchInstanceInventory,
  replaceAgentAssignments,
  type AgentAssignmentRecord,
  type AgentAssignmentReplaceItem,
  type AgentCommandDetailRecord,
  type AgentCommandSummaryRecord,
  type AgentCreateResponse,
  type AgentDetailRecord,
  type AgentRecord,
  type AgentStatus,
  type InstanceInventoryRecord,
} from '../api'

type AssignmentDraft = AgentAssignmentReplaceItem & {
  instance_name: string
  db_type: string
  host: string
  port: number
}

const authStore = useAuthStore()

const agents = ref<AgentRecord[]>([])
const isLoading = ref(false)
const error = ref('')
const totalCount = ref(0)
const searchQuery = ref('')
const currentPage = ref(1)
const pageSize = ref(20)
const latestRequestId = ref(0)

const isCreateDialogOpen = ref(false)
const createSubmitting = ref(false)
const createError = ref('')
const createForm = ref({ name: '', display_name: '' })
const createdAgent = ref<AgentCreateResponse | null>(null)

const isDetailDialogOpen = ref(false)
const detailLoading = ref(false)
const detailError = ref('')
const selectedAgent = ref<AgentDetailRecord | null>(null)
const availableInstances = ref<InstanceInventoryRecord[]>([])
const assignmentRows = ref<AssignmentDraft[]>([])
const assignmentsSaving = ref(false)
const commandRows = ref<AgentCommandSummaryRecord[]>([])
const commandTotalCount = ref(0)
const commandPage = ref(1)
const commandPageSize = ref(10)
const commandsLoading = ref(false)
const commandActionError = ref('')
const selectedCommand = ref<AgentCommandDetailRecord | null>(null)
const selectedCommandLoading = ref(false)
const cancellingCommandId = ref<number | null>(null)

const fieldClass =
  'w-full rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm focus:border-slate-400 focus:outline-none focus:ring-1 focus:ring-slate-400'

const columns: DataTableColumn[] = [
  { key: 'name', label: 'Agent', sortable: true, hideable: false },
  { key: 'status', label: 'Status', sortable: true },
  { key: 'agent_version', label: 'Version', sortable: true },
  { key: 'hostname', label: 'Host', sortable: true },
  { key: 'assignment_count', label: 'Assignments', sortable: true },
  { key: 'config_revision', label: 'Config Revision' },
  { key: 'last_seen_at', label: 'Last Seen', sortable: true },
  { key: 'platform', label: 'Platform', defaultVisible: false },
  { key: 'architecture', label: 'Architecture', defaultVisible: false },
  { key: 'actions', label: '', hideable: false, class: 'w-20 text-right' },
]

const canAccessAgents = computed(() => {
  if (authStore.currentUser?.is_superuser) {
    return true
  }
  return authStore.currentUser?.permissions?.includes('api_agents.menu_agent') ?? false
})

const commandTotalPages = computed(() =>
  Math.max(1, Math.ceil(commandTotalCount.value / commandPageSize.value)),
)

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

async function loadAgents() {
  const requestId = latestRequestId.value + 1
  latestRequestId.value = requestId
  isLoading.value = true
  error.value = ''

  try {
    await authStore.loadCurrentUser()

    if (!canAccessAgents.value) {
      agents.value = []
      totalCount.value = 0
      error.value = 'You do not have permission to access Datamingle agents.'
      return
    }

    const response = await fetchAgents(requireToken(), {
      page: currentPage.value,
      size: pageSize.value,
      search: searchQuery.value,
    })

    if (requestId !== latestRequestId.value) {
      return
    }

    agents.value = response.results
    totalCount.value = response.count
  } catch (errorValue) {
    if (requestId !== latestRequestId.value) {
      return
    }
    error.value = toUserFacingMessage(errorValue, 'Failed to load agents.')
  } finally {
    if (requestId === latestRequestId.value) {
      isLoading.value = false
    }
  }
}

function handlePageSizeChange(value: number) {
  pageSize.value = value
  currentPage.value = 1
}

function handleSearchQueryChange(value: string) {
  searchQuery.value = value
  currentPage.value = 1
}

function openCreateDialog() {
  createForm.value = { name: '', display_name: '' }
  createError.value = ''
  createdAgent.value = null
  isCreateDialogOpen.value = true
}

function closeCreateDialog() {
  isCreateDialogOpen.value = false
  createdAgent.value = null
}

async function submitCreateAgent() {
  createSubmitting.value = true
  createError.value = ''
  try {
    createdAgent.value = await createAgent(
      {
        name: createForm.value.name.trim(),
        display_name: createForm.value.display_name.trim(),
      },
      requireToken(),
    )
    await loadAgents()
  } catch (errorValue) {
    createError.value = toUserFacingMessage(errorValue, 'Failed to create agent.')
  } finally {
    createSubmitting.value = false
  }
}

async function copyText(value: string) {
  await navigator.clipboard.writeText(value)
}

async function openAgentDetail(agent: AgentRecord) {
  isDetailDialogOpen.value = true
  detailLoading.value = true
  detailError.value = ''
  commandActionError.value = ''
  selectedAgent.value = null
  selectedCommand.value = null
  assignmentRows.value = []
  commandRows.value = []
  commandTotalCount.value = 0
  commandPage.value = 1

  try {
    const [detail, inventory, commands] = await Promise.all([
      fetchAgent(agent.id, requireToken()),
      fetchInstanceInventory(requireToken(), { page: 1, size: 100 }),
      fetchAgentCommands(agent.id, requireToken(), {
        page: commandPage.value,
        size: commandPageSize.value,
      }),
    ])
    selectedAgent.value = detail
    availableInstances.value = inventory.results
    assignmentRows.value = buildAssignmentDrafts(detail.assignments, inventory.results)
    commandRows.value = commands.results
    commandTotalCount.value = commands.count
  } catch (errorValue) {
    detailError.value = toUserFacingMessage(errorValue, 'Failed to load agent.')
  } finally {
    detailLoading.value = false
  }
}

function closeDetailDialog() {
  isDetailDialogOpen.value = false
  selectedAgent.value = null
  selectedCommand.value = null
  assignmentRows.value = []
  commandRows.value = []
  commandTotalCount.value = 0
  commandActionError.value = ''
}

function buildAssignmentDrafts(
  assignments: AgentAssignmentRecord[],
  instances: InstanceInventoryRecord[],
): AssignmentDraft[] {
  const byInstanceId = new Map(assignments.map((assignment) => [assignment.instance, assignment]))
  const drafts = instances.map((instance) => assignmentDraftForInstance(instance, byInstanceId.get(instance.id)))
  const instanceIds = new Set(instances.map((instance) => instance.id))
  for (const assignment of assignments) {
    if (!instanceIds.has(assignment.instance)) {
      drafts.push(assignmentDraftForAssignment(assignment))
    }
  }
  return drafts
}

function assignmentDraftForInstance(
  instance: InstanceInventoryRecord,
  assignment?: AgentAssignmentRecord,
): AssignmentDraft {
  return {
    instance: instance.id,
    instance_name: instance.instance_name,
    db_type: instance.db_type,
    host: instance.host,
    port: instance.port,
    enabled: assignment?.enabled ?? false,
    modules: assignment?.modules ?? [],
    capabilities: assignment?.capabilities ?? [],
    command_enabled: assignment?.command_enabled ?? false,
    metrics_enabled: assignment?.metrics_enabled ?? true,
    online_schema_enabled: assignment?.online_schema_enabled ?? false,
    logs_enabled: assignment?.logs_enabled ?? false,
  }
}

function assignmentDraftForAssignment(assignment: AgentAssignmentRecord): AssignmentDraft {
  return {
    instance: assignment.instance,
    instance_name: assignment.instance_name,
    db_type: assignment.db_type,
    host: assignment.host,
    port: assignment.port,
    enabled: assignment.enabled,
    modules: assignment.modules,
    capabilities: assignment.capabilities,
    command_enabled: assignment.command_enabled,
    metrics_enabled: assignment.metrics_enabled,
    online_schema_enabled: assignment.online_schema_enabled,
    logs_enabled: assignment.logs_enabled,
  }
}

function toggleAssignment(row: AssignmentDraft, checked: boolean) {
  row.enabled = checked
  if (!checked) {
    row.command_enabled = false
    row.online_schema_enabled = false
    row.logs_enabled = false
  }
}

function assignmentModules(row: AssignmentDraft) {
  const modules = new Set(row.modules)
  if (row.db_type === 'mysql') {
    modules.add('mysql')
  }
  if (row.metrics_enabled) {
    modules.add('metrics')
  } else {
    modules.delete('metrics')
  }
  if (row.online_schema_enabled) {
    modules.add('online_schema')
  } else {
    modules.delete('online_schema')
  }
  if (row.logs_enabled) {
    modules.add('logs')
  } else {
    modules.delete('logs')
  }
  return [...modules].sort()
}

async function saveAssignments() {
  if (!selectedAgent.value) {
    return
  }
  assignmentsSaving.value = true
  detailError.value = ''
  try {
    const assignments = assignmentRows.value
      .filter((row) => row.enabled)
      .map((row) => ({
        instance: row.instance,
        enabled: row.enabled,
        modules: assignmentModules(row),
        capabilities: row.capabilities,
        command_enabled: row.command_enabled,
        metrics_enabled: row.metrics_enabled,
        online_schema_enabled: row.online_schema_enabled,
        logs_enabled: row.logs_enabled,
      }))
    await replaceAgentAssignments(selectedAgent.value.id, { assignments }, requireToken())
    selectedAgent.value = await fetchAgent(selectedAgent.value.id, requireToken())
    assignmentRows.value = buildAssignmentDrafts(selectedAgent.value.assignments, availableInstances.value)
    await loadAgents()
  } catch (errorValue) {
    detailError.value = toUserFacingMessage(errorValue, 'Failed to save assignments.')
  } finally {
    assignmentsSaving.value = false
  }
}

async function loadAgentCommands() {
  if (!selectedAgent.value) {
    return
  }

  commandsLoading.value = true
  commandActionError.value = ''
  try {
    const response = await fetchAgentCommands(selectedAgent.value.id, requireToken(), {
      page: commandPage.value,
      size: commandPageSize.value,
    })
    commandRows.value = response.results
    commandTotalCount.value = response.count
  } catch (errorValue) {
    commandActionError.value = toUserFacingMessage(errorValue, 'Failed to load command history.')
  } finally {
    commandsLoading.value = false
  }
}

async function openCommandDetail(command: AgentCommandSummaryRecord) {
  if (!selectedAgent.value) {
    return
  }

  selectedCommandLoading.value = true
  commandActionError.value = ''
  try {
    selectedCommand.value = await fetchAgentCommand(selectedAgent.value.id, command.id, requireToken())
  } catch (errorValue) {
    commandActionError.value = toUserFacingMessage(errorValue, 'Failed to load command detail.')
  } finally {
    selectedCommandLoading.value = false
  }
}

async function cancelCommand(command: AgentCommandSummaryRecord) {
  if (!selectedAgent.value || !canCancelCommand(command)) {
    return
  }

  cancellingCommandId.value = command.id
  commandActionError.value = ''
  try {
    const detail = await cancelAgentCommand(selectedAgent.value.id, command.id, requireToken())
    if (selectedCommand.value?.id === command.id) {
      selectedCommand.value = detail
    }
    await loadAgentCommands()
  } catch (errorValue) {
    commandActionError.value = toUserFacingMessage(errorValue, 'Failed to cancel command.')
  } finally {
    cancellingCommandId.value = null
  }
}

function canCancelCommand(command: Pick<AgentCommandSummaryRecord, 'status' | 'cancel_requested_at'>) {
  return (
    !command.cancel_requested_at &&
    !['succeeded', 'failed', 'cancelled', 'expired'].includes(command.status)
  )
}

function commandStatusBadgeClass(value: string) {
  switch (value) {
    case 'succeeded':
      return 'bg-emerald-100 text-emerald-800'
    case 'failed':
    case 'cancelled':
    case 'expired':
      return 'bg-red-100 text-red-800'
    case 'running':
      return 'bg-blue-100 text-blue-800'
    case 'accepted':
    case 'dispatched':
      return 'bg-amber-100 text-amber-800'
    default:
      return 'bg-slate-100 text-slate-700'
  }
}

function formatJson(value: Record<string, unknown>) {
  return JSON.stringify(value ?? {}, null, 2)
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
  void loadAgents()
})

const debouncedLoadAgents = useDebounceFn(() => {
  void loadAgents()
}, 250)

watch([currentPage, pageSize], () => {
  void loadAgents()
})

watch(searchQuery, () => {
  debouncedLoadAgents()
})

watch([commandPage, commandPageSize], () => {
  if (isDetailDialogOpen.value && selectedAgent.value) {
    void loadAgentCommands()
  }
})
</script>

<template>
  <section class="grid gap-6">
    <div class="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
      <div class="space-y-1">
        <h2 class="text-2xl font-semibold text-slate-900">Agents</h2>
        <p class="text-sm text-slate-600">
          Track registered host agents, assigned Datamingle instances, and config reconciliation state.
        </p>
      </div>
      <Button type="button" @click="openCreateDialog">
        <Plus class="h-4 w-4" />
        Create Agent
      </Button>
    </div>

    <Card class="border-slate-200">
      <CardHeader>
        <CardTitle>Registered Agents</CardTitle>
        <CardDescription>
          Agents stay connected over websocket and fetch full configuration through authenticated API calls.
        </CardDescription>
      </CardHeader>
      <CardContent class="space-y-5">
        <p v-if="error" class="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
          {{ error }}
        </p>

        <DataTable
          :columns="columns"
          :empty-text="'No Datamingle agents are registered.'"
          :manual-pagination="true"
          :manual-search="true"
          :rows="agents"
          :loading="isLoading"
          :page="currentPage"
          :page-size="pageSize"
          :search-query="searchQuery"
          :total-rows="totalCount"
          row-key="id"
          search-placeholder="Filter agents by name, host, version, or status"
          @update:page="currentPage = $event"
          @update:page-size="handlePageSizeChange"
          @update:search-query="handleSearchQueryChange"
        >
          <template #toolbar-actions>
            <Button variant="outline" @click="void loadAgents()">
              <RefreshCw class="h-4 w-4" />
              Refresh
            </Button>
          </template>

          <template #cell-name="{ row }">
            <div class="font-medium text-slate-900">
              {{ row.display_name || row.name }}
            </div>
            <div class="mt-1 text-xs text-slate-500">{{ row.name }}</div>
          </template>

          <template #cell-status="{ value }">
            <Badge variant="secondary" :class="statusBadgeClass(value as AgentStatus)">
              {{ statusLabel(value as AgentStatus) }}
            </Badge>
          </template>

          <template #cell-agent_version="{ value }">
            <span>{{ value || 'Unknown' }}</span>
          </template>

          <template #cell-hostname="{ row }">
            <div>{{ row.hostname || 'Unknown host' }}</div>
            <div class="mt-1 text-xs text-slate-500">
              {{ [row.platform, row.architecture].filter(Boolean).join('/') || 'Unknown platform' }}
            </div>
          </template>

          <template #cell-assignment_count="{ value }">
            <Badge variant="outline" class="border-slate-300 text-slate-700">
              {{ value }}
            </Badge>
          </template>

          <template #cell-config_revision="{ row }">
            <span>{{ row.last_config_revision }} / {{ row.desired_config_revision }}</span>
          </template>

          <template #cell-last_seen_at="{ value }">
            {{ formatDateTime(value as string | null) }}
          </template>

          <template #cell-actions="{ row }">
            <Button variant="outline" size="sm" type="button" @click="void openAgentDetail(row as AgentRecord)">
              <Eye class="h-4 w-4" />
              View
            </Button>
          </template>
        </DataTable>
      </CardContent>
    </Card>

    <div
      v-if="isCreateDialogOpen"
      class="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/45 px-4 py-6"
      @click.self="closeCreateDialog"
    >
      <div class="max-h-[90vh] w-full max-w-2xl overflow-y-auto rounded-lg bg-white shadow-xl">
        <div class="flex items-start justify-between border-b border-slate-200 px-6 py-4">
          <div>
            <h3 class="text-lg font-semibold text-slate-900">Create Agent</h3>
            <p class="mt-1 text-sm text-slate-500">Provision a dedicated WorkOS API key for one host agent.</p>
          </div>
          <Button variant="ghost" size="icon" type="button" @click="closeCreateDialog">
            <X class="h-4 w-4" />
          </Button>
        </div>

        <form v-if="!createdAgent" class="space-y-5 px-6 py-5" @submit.prevent="void submitCreateAgent()">
          <p v-if="createError" class="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
            {{ createError }}
          </p>
          <label class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">Name</span>
            <Input v-model="createForm.name" required placeholder="prod-db-agent-01" />
          </label>
          <label class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">Display Name</span>
            <Input v-model="createForm.display_name" placeholder="Production DB Agent" />
          </label>
          <div class="flex justify-end gap-2 border-t border-slate-200 pt-4">
            <Button variant="outline" type="button" @click="closeCreateDialog">Cancel</Button>
            <Button type="submit" :disabled="createSubmitting">
              <Plus class="h-4 w-4" />
              Create
            </Button>
          </div>
        </form>

        <div v-else class="space-y-5 px-6 py-5">
          <div class="rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">
            The API key is shown once. Store it before closing this window.
          </div>
          <div class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">API Key Backend</span>
            <Badge variant="secondary" class="w-fit bg-slate-100 text-slate-700">
              {{ createdAgent.api_key_backend }}
            </Badge>
          </div>
          <div class="grid gap-2">
            <div class="flex items-center justify-between gap-2">
              <span class="text-sm font-medium text-slate-700">API Key</span>
              <Button variant="outline" size="sm" type="button" @click="void copyText(createdAgent.api_key)">
                <Copy class="h-4 w-4" />
                Copy
              </Button>
            </div>
            <textarea :class="fieldClass" rows="3" readonly :value="createdAgent.api_key" />
          </div>
          <div class="grid gap-2">
            <div class="flex items-center justify-between gap-2">
              <span class="text-sm font-medium text-slate-700">Install Command</span>
              <Button variant="outline" size="sm" type="button" @click="void copyText(createdAgent.install_command)">
                <Copy class="h-4 w-4" />
                Copy
              </Button>
            </div>
            <textarea :class="fieldClass" rows="5" readonly :value="createdAgent.install_command" />
          </div>
          <div class="flex justify-end border-t border-slate-200 pt-4">
            <Button type="button" @click="closeCreateDialog">Done</Button>
          </div>
        </div>
      </div>
    </div>

    <div
      v-if="isDetailDialogOpen"
      class="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/45 px-4 py-6"
      @click.self="closeDetailDialog"
    >
      <div class="max-h-[92vh] w-full max-w-6xl overflow-y-auto rounded-lg bg-white shadow-xl">
        <div class="flex items-start justify-between border-b border-slate-200 px-6 py-4">
          <div>
            <h3 class="text-lg font-semibold text-slate-900">
              {{ selectedAgent?.display_name || selectedAgent?.name || 'Agent Detail' }}
            </h3>
            <p class="mt-1 text-sm text-slate-500">{{ selectedAgent?.name }}</p>
          </div>
          <Button variant="ghost" size="icon" type="button" @click="closeDetailDialog">
            <X class="h-4 w-4" />
          </Button>
        </div>

        <div class="space-y-6 px-6 py-5">
          <p v-if="detailError" class="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
            {{ detailError }}
          </p>
          <div v-if="detailLoading" class="py-12 text-center text-sm text-slate-500">
            Loading agent…
          </div>

          <template v-else-if="selectedAgent">
            <div class="grid gap-4 md:grid-cols-4">
              <div class="rounded-lg border border-slate-200 p-4">
                <p class="text-xs uppercase text-slate-500">Status</p>
                <Badge variant="secondary" :class="statusBadgeClass(selectedAgent.status)">
                  {{ statusLabel(selectedAgent.status) }}
                </Badge>
              </div>
              <div class="rounded-lg border border-slate-200 p-4">
                <p class="text-xs uppercase text-slate-500">Config</p>
                <p class="mt-2 text-sm font-medium text-slate-900">
                  {{ selectedAgent.last_config_revision }} / {{ selectedAgent.desired_config_revision }}
                </p>
              </div>
              <div class="rounded-lg border border-slate-200 p-4">
                <p class="text-xs uppercase text-slate-500">Host</p>
                <p class="mt-2 text-sm font-medium text-slate-900">{{ selectedAgent.hostname || 'Unknown' }}</p>
              </div>
              <div class="rounded-lg border border-slate-200 p-4">
                <p class="text-xs uppercase text-slate-500">Last Seen</p>
                <p class="mt-2 text-sm font-medium text-slate-900">{{ formatDateTime(selectedAgent.last_seen_at) }}</p>
              </div>
            </div>

            <section class="space-y-3">
              <div class="flex items-center justify-between gap-3">
                <div>
                  <h4 class="text-base font-semibold text-slate-900">Assignments</h4>
                  <p class="text-sm text-slate-500">Select instances and modules for this agent.</p>
                </div>
                <Button type="button" :disabled="assignmentsSaving" @click="void saveAssignments()">
                  <Save class="h-4 w-4" />
                  Save Assignments
                </Button>
              </div>
              <div class="overflow-hidden rounded-lg border border-slate-200">
                <div class="overflow-x-auto">
                  <table class="min-w-full divide-y divide-slate-200 bg-white">
                    <thead class="bg-slate-50 text-left text-xs uppercase text-slate-500">
                      <tr>
                        <th class="px-4 py-3 font-medium">Instance</th>
                        <th class="px-4 py-3 font-medium">Enabled</th>
                        <th class="px-4 py-3 font-medium">Command</th>
                        <th class="px-4 py-3 font-medium">Metrics</th>
                        <th class="px-4 py-3 font-medium">Online Schema</th>
                        <th class="px-4 py-3 font-medium">Logs</th>
                      </tr>
                    </thead>
                    <tbody class="divide-y divide-slate-200">
                      <tr v-if="assignmentRows.length === 0">
                        <td colspan="6" class="px-4 py-8 text-center text-sm text-slate-500">
                          No instances are available.
                        </td>
                      </tr>
                      <tr v-for="row in assignmentRows" :key="row.instance" class="align-top">
                        <td class="px-4 py-3 text-sm">
                          <div class="font-medium text-slate-900">{{ row.instance_name }}</div>
                          <div class="mt-1 text-xs text-slate-500">
                            {{ row.db_type }} · {{ row.host }}:{{ row.port }}
                          </div>
                        </td>
                        <td class="px-4 py-3">
                          <input
                            :checked="row.enabled"
                            class="h-4 w-4 rounded border-slate-300"
                            type="checkbox"
                            @change="toggleAssignment(row, ($event.target as HTMLInputElement).checked)"
                          >
                        </td>
                        <td class="px-4 py-3">
                          <input
                            v-model="row.command_enabled"
                            class="h-4 w-4 rounded border-slate-300"
                            type="checkbox"
                            :disabled="!row.enabled"
                          >
                        </td>
                        <td class="px-4 py-3">
                          <input
                            v-model="row.metrics_enabled"
                            class="h-4 w-4 rounded border-slate-300"
                            type="checkbox"
                            :disabled="!row.enabled"
                          >
                        </td>
                        <td class="px-4 py-3">
                          <input
                            v-model="row.online_schema_enabled"
                            class="h-4 w-4 rounded border-slate-300"
                            type="checkbox"
                            :disabled="!row.enabled || row.db_type !== 'mysql'"
                          >
                        </td>
                        <td class="px-4 py-3">
                          <input
                            v-model="row.logs_enabled"
                            class="h-4 w-4 rounded border-slate-300"
                            type="checkbox"
                            :disabled="!row.enabled"
                          >
                        </td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>
            </section>

            <section class="space-y-3">
              <div class="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
                <h4 class="text-base font-semibold text-slate-900">Command History</h4>
                <Button variant="outline" type="button" :disabled="commandsLoading" @click="void loadAgentCommands()">
                  <RefreshCw class="h-4 w-4" />
                  Refresh
                </Button>
              </div>
              <p
                v-if="commandActionError"
                class="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700"
              >
                {{ commandActionError }}
              </p>
              <div class="overflow-hidden rounded-lg border border-slate-200">
                <div v-if="commandsLoading" class="px-4 py-8 text-center text-sm text-slate-500">
                  Loading commands…
                </div>
                <div v-else-if="commandRows.length === 0" class="px-4 py-8 text-center text-sm text-slate-500">
                  No commands have been dispatched to this agent.
                </div>
                <div v-else class="overflow-x-auto">
                  <table class="min-w-full divide-y divide-slate-200 bg-white">
                    <thead class="bg-slate-50 text-left text-xs uppercase text-slate-500">
                      <tr>
                        <th class="px-4 py-3 font-medium">Command</th>
                        <th class="px-4 py-3 font-medium">Status</th>
                        <th class="px-4 py-3 font-medium">Workflow</th>
                        <th class="px-4 py-3 font-medium">Created</th>
                        <th class="px-4 py-3 text-right font-medium">Actions</th>
                      </tr>
                    </thead>
                    <tbody class="divide-y divide-slate-200">
                      <tr v-for="command in commandRows" :key="command.id" class="align-top">
                        <td class="px-4 py-3 text-sm">
                          <div class="font-medium text-slate-900">{{ command.command_type }}</div>
                          <div class="mt-1 text-xs text-slate-500">{{ command.instance_name }}</div>
                        </td>
                        <td class="px-4 py-3">
                          <Badge variant="secondary" :class="commandStatusBadgeClass(command.status)">
                            {{ command.status }}
                          </Badge>
                        </td>
                        <td class="px-4 py-3 text-sm text-slate-600">
                          <div>{{ command.workflow_type }}</div>
                          <div class="mt-1 text-xs text-slate-500">#{{ command.workflow_id }}</div>
                        </td>
                        <td class="px-4 py-3 text-sm text-slate-500">
                          {{ formatDateTime(command.create_time) }}
                        </td>
                        <td class="px-4 py-3">
                          <div class="flex justify-end gap-2">
                            <Button variant="outline" size="sm" type="button" @click="void openCommandDetail(command)">
                              <FileText class="h-4 w-4" />
                              Details
                            </Button>
                            <Button
                              v-if="canCancelCommand(command)"
                              variant="outline"
                              size="sm"
                              type="button"
                              :disabled="cancellingCommandId === command.id"
                              @click="void cancelCommand(command)"
                            >
                              <Ban class="h-4 w-4" />
                              Cancel
                            </Button>
                          </div>
                        </td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>
              <div
                v-if="commandTotalCount > commandPageSize"
                class="flex items-center justify-end gap-3 text-sm text-slate-600"
              >
                <Button
                  variant="outline"
                  size="sm"
                  type="button"
                  :disabled="commandPage <= 1 || commandsLoading"
                  @click="commandPage -= 1"
                >
                  Previous
                </Button>
                <span>Page {{ commandPage }} of {{ commandTotalPages }}</span>
                <Button
                  variant="outline"
                  size="sm"
                  type="button"
                  :disabled="commandPage >= commandTotalPages || commandsLoading"
                  @click="commandPage += 1"
                >
                  Next
                </Button>
              </div>

              <div v-if="selectedCommandLoading" class="rounded-lg border border-slate-200 px-4 py-8 text-center text-sm text-slate-500">
                Loading command detail…
              </div>
              <div v-else-if="selectedCommand" class="rounded-lg border border-slate-200">
                <div class="flex items-start justify-between border-b border-slate-200 px-4 py-3">
                  <div>
                    <h5 class="text-sm font-semibold text-slate-900">
                      {{ selectedCommand.command_type }} #{{ selectedCommand.id }}
                    </h5>
                    <p class="mt-1 text-xs text-slate-500">
                      {{ selectedCommand.instance_name }} · {{ selectedCommand.workflow_type }}
                    </p>
                  </div>
                  <Button variant="ghost" size="icon" type="button" @click="selectedCommand = null">
                    <X class="h-4 w-4" />
                  </Button>
                </div>
                <div class="grid gap-4 p-4 lg:grid-cols-3">
                  <div class="space-y-2">
                    <h6 class="text-xs font-semibold uppercase text-slate-500">Payload</h6>
                    <pre class="max-h-72 overflow-auto rounded-md bg-slate-950 p-3 text-xs text-slate-100">{{ formatJson(selectedCommand.payload) }}</pre>
                  </div>
                  <div class="space-y-2">
                    <h6 class="text-xs font-semibold uppercase text-slate-500">Result</h6>
                    <pre class="max-h-72 overflow-auto rounded-md bg-slate-950 p-3 text-xs text-slate-100">{{ formatJson(selectedCommand.result) }}</pre>
                  </div>
                  <div class="space-y-2">
                    <h6 class="text-xs font-semibold uppercase text-slate-500">Events</h6>
                    <div class="max-h-72 overflow-auto rounded-md border border-slate-200">
                      <div
                        v-for="event in selectedCommand.events"
                        :key="event.id"
                        class="border-b border-slate-100 px-3 py-2 text-xs last:border-b-0"
                      >
                        <div class="font-medium text-slate-900">{{ event.event_type }}</div>
                        <div class="mt-1 text-slate-500">{{ formatDateTime(event.create_time) }}</div>
                        <div v-if="event.message" class="mt-1 text-slate-700">{{ event.message }}</div>
                      </div>
                      <div
                        v-if="selectedCommand.events.length === 0"
                        class="px-3 py-8 text-center text-xs text-slate-500"
                      >
                        No events recorded.
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </section>
          </template>
        </div>
      </div>
    </div>
  </section>
</template>
