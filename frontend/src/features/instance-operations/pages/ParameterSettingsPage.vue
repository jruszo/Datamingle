<script setup lang="ts">
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { Edit3, History, RefreshCw, Save, SlidersHorizontal, X } from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import {
  editInstanceOperationParam,
  fetchInstanceOperationParamHistory,
  fetchInstanceOperationParamInstances,
  fetchInstanceOperationParams,
  type InstanceOperationParamHistoryRecord,
  type InstanceOperationParamInstance,
  type InstanceOperationParamRecord,
} from '../api'
import { useAuthStore } from '@/stores/auth'

const authStore = useAuthStore()

const instances = ref<InstanceOperationParamInstance[]>([])
const params = ref<InstanceOperationParamRecord[]>([])
const history = ref<InstanceOperationParamHistoryRecord[]>([])
const selectedInstanceId = ref<number | null>(null)
const editableFilter = ref('all')
const search = ref('')

const loadingInstances = ref(false)
const loadingParams = ref(false)
const loadingHistory = ref(false)
const submitting = ref(false)
const error = ref('')
const feedback = ref('')
const selectedParam = ref<InstanceOperationParamRecord | null>(null)

const editForm = reactive({
  variableName: '',
  runtimeValue: '',
})

const selectClass =
  'block h-10 w-full rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-700 shadow-sm outline-none transition focus:border-slate-400'

function requireToken() {
  if (!authStore.accessToken) {
    throw new Error('Missing access token. Please login again.')
  }
  return authStore.accessToken
}

function hasPermission(permission: string) {
  if (authStore.currentUser?.is_superuser) {
    return true
  }
  return authStore.currentUser?.permissions?.includes(permission) ?? false
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

const canOpenParamMenu = computed(() => hasPermission('sql.menu_param'))
const canViewParams = computed(() => hasPermission('sql.param_view'))
const canEditParams = computed(() => hasPermission('sql.param_edit'))

const selectedInstance = computed(() =>
  instances.value.find((instance) => instance.id === selectedInstanceId.value) ?? null,
)

const editableOption = computed(() => {
  if (editableFilter.value === 'editable') {
    return true
  }
  if (editableFilter.value === 'readonly') {
    return false
  }
  return undefined
})

function closeEdit() {
  selectedParam.value = null
  editForm.variableName = ''
  editForm.runtimeValue = ''
}

function openEdit(param: InstanceOperationParamRecord) {
  selectedParam.value = param
  editForm.variableName = param.variable_name
  editForm.runtimeValue = `${param.runtime_value ?? ''}`
  feedback.value = ''
}

async function loadInstances() {
  loadingInstances.value = true
  error.value = ''

  try {
    instances.value = await fetchInstanceOperationParamInstances(requireToken())
    const firstInstance = instances.value[0]
    if (!selectedInstanceId.value && firstInstance) {
      selectedInstanceId.value = firstInstance.id
    }
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to load parameter-management instances.')
  } finally {
    loadingInstances.value = false
  }
}

async function loadParams() {
  if (!selectedInstanceId.value) {
    params.value = []
    return
  }

  loadingParams.value = true
  error.value = ''

  try {
    const response = await fetchInstanceOperationParams(requireToken(), {
      instance_id: selectedInstanceId.value,
      editable: editableOption.value,
      search: search.value.trim(),
    })
    params.value = response.results
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to load parameters.')
  } finally {
    loadingParams.value = false
  }
}

async function loadHistory() {
  if (!selectedInstanceId.value) {
    history.value = []
    return
  }

  loadingHistory.value = true

  try {
    const response = await fetchInstanceOperationParamHistory(requireToken(), {
      instance_id: selectedInstanceId.value,
      search: search.value.trim(),
      size: 10,
    })
    history.value = response.results
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to load parameter history.')
  } finally {
    loadingHistory.value = false
  }
}

async function refreshAll() {
  feedback.value = ''
  const priorInstanceId = selectedInstanceId.value
  await loadInstances()
  if (selectedInstanceId.value && selectedInstanceId.value === priorInstanceId) {
    await Promise.all([loadParams(), loadHistory()])
  }
}

async function applyFilters() {
  await Promise.all([loadParams(), loadHistory()])
}

async function submitEdit() {
  if (!selectedInstanceId.value || !selectedParam.value) {
    return
  }

  submitting.value = true
  error.value = ''
  feedback.value = ''

  try {
    await editInstanceOperationParam(
      {
        instance_id: selectedInstanceId.value,
        variable_name: editForm.variableName,
        runtime_value: editForm.runtimeValue,
      },
      requireToken(),
    )
    feedback.value = `Parameter "${editForm.variableName}" updated.`
    closeEdit()
    await Promise.all([loadParams(), loadHistory()])
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to update parameter.')
  } finally {
    submitting.value = false
  }
}

onMounted(async () => {
  await authStore.loadCurrentUser()

  if (!canOpenParamMenu.value) {
    error.value = 'You do not have permission to open parameter settings.'
    return
  }
  if (!canViewParams.value) {
    error.value = 'You do not have permission to view instance parameters.'
    return
  }

  await refreshAll()
})

watch([selectedInstanceId, editableFilter], () => {
  if (!canViewParams.value) {
    return
  }
  closeEdit()
  void Promise.all([loadParams(), loadHistory()])
})
</script>

<template>
  <section class="grid gap-6">
    <div class="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
      <div class="space-y-1">
        <div class="flex flex-wrap items-center gap-2">
          <RouterLink to="/instance-operations/databases" class="text-sm font-medium text-slate-500 hover:text-slate-900">
            Databases
          </RouterLink>
          <span class="text-slate-300">/</span>
          <RouterLink to="/instance-operations/accounts" class="text-sm font-medium text-slate-500 hover:text-slate-900">
            Accounts
          </RouterLink>
          <span class="text-slate-300">/</span>
          <span class="text-sm font-semibold text-slate-900">Parameters</span>
        </div>
        <h2 class="text-2xl font-semibold text-slate-900">Parameter Settings</h2>
        <p class="text-sm text-slate-600">
          Review runtime values, edit approved dynamic parameters, and audit recent changes.
        </p>
      </div>
      <Button variant="outline" type="button" class="gap-2" :disabled="loadingParams" @click="void refreshAll()">
        <RefreshCw class="h-4 w-4" />
        Refresh
      </Button>
    </div>

    <p v-if="error" class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
      {{ error }}
    </p>
    <p
      v-else-if="feedback"
      class="rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700"
    >
      {{ feedback }}
    </p>

    <Card class="border-slate-200">
      <CardHeader>
        <CardTitle>Scope</CardTitle>
        <CardDescription>Select an instance and narrow the parameter list.</CardDescription>
      </CardHeader>
      <CardContent>
        <div class="grid gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(0,0.7fr)_minmax(0,0.8fr)_auto]">
          <label class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">Instance</span>
            <select v-model.number="selectedInstanceId" :class="selectClass" :disabled="loadingInstances">
              <option v-if="instances.length === 0" :value="null">No available instances</option>
              <option v-for="instance in instances" :key="instance.id" :value="instance.id">
                {{ instance.label }}
              </option>
            </select>
          </label>
          <label class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">Mode</span>
            <select v-model="editableFilter" :class="selectClass">
              <option value="all">All</option>
              <option value="editable">Editable</option>
              <option value="readonly">Read-only</option>
            </select>
          </label>
          <label class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">Search</span>
            <Input v-model="search" placeholder="Parameter or description" @keyup.enter="void applyFilters()" />
          </label>
          <div class="flex items-end">
            <Button variant="outline" type="button" class="gap-2" @click="void applyFilters()">
              <RefreshCw class="h-4 w-4" />
              Apply
            </Button>
          </div>
        </div>
      </CardContent>
    </Card>

    <div class="grid gap-6 xl:grid-cols-[minmax(0,1fr)_minmax(21rem,0.34fr)]">
      <Card class="border-slate-200">
        <CardHeader>
          <CardTitle class="flex items-center gap-2">
            <SlidersHorizontal class="h-5 w-5" />
            Parameters
          </CardTitle>
          <CardDescription>
            {{ params.length }} shown for {{ selectedInstance?.instance_name || 'no instance' }}
          </CardDescription>
        </CardHeader>
        <CardContent class="p-0">
          <div v-if="loadingParams" class="p-6 text-sm text-slate-500">
            Loading parameters...
          </div>
          <div v-else-if="params.length === 0" class="p-6 text-sm text-slate-500">
            No parameters match the current filters.
          </div>
          <div v-else class="overflow-x-auto">
            <table class="min-w-full divide-y divide-slate-200 text-sm">
              <thead class="bg-slate-50">
                <tr>
                  <th class="px-4 py-3 text-left font-medium text-slate-600">Parameter</th>
                  <th class="px-4 py-3 text-left font-medium text-slate-600">Runtime</th>
                  <th class="px-4 py-3 text-left font-medium text-slate-600">Default</th>
                  <th class="px-4 py-3 text-left font-medium text-slate-600">Allowed</th>
                  <th class="px-4 py-3 text-right font-medium text-slate-600">Actions</th>
                </tr>
              </thead>
              <tbody class="divide-y divide-slate-100 bg-white">
                <tr v-for="param in params" :key="param.variable_name">
                  <td class="px-4 py-3">
                    <div class="grid gap-1">
                      <div class="flex flex-wrap items-center gap-2">
                        <span class="font-medium text-slate-900">{{ param.variable_name }}</span>
                        <Badge v-if="param.editable" variant="outline">Editable</Badge>
                        <Badge v-if="param.configured" variant="secondary">Template</Badge>
                      </div>
                      <span v-if="param.description" class="max-w-[34rem] truncate text-xs text-slate-500">
                        {{ param.description }}
                      </span>
                    </div>
                  </td>
                  <td class="max-w-[12rem] truncate px-4 py-3 text-slate-600">{{ param.runtime_value || '-' }}</td>
                  <td class="max-w-[12rem] truncate px-4 py-3 text-slate-600">{{ param.default_value || '-' }}</td>
                  <td class="max-w-[14rem] truncate px-4 py-3 text-slate-600">{{ param.valid_values || '-' }}</td>
                  <td class="px-4 py-3 text-right">
                    <Button variant="outline" type="button" size="sm" class="gap-2" :disabled="!canEditParams || !param.editable" @click="openEdit(param)">
                      <Edit3 class="h-4 w-4" />
                      Edit
                    </Button>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>

      <div class="grid content-start gap-6">
        <Card v-if="selectedParam" class="border-slate-200">
          <CardHeader>
            <CardTitle>Edit parameter</CardTitle>
            <CardDescription>{{ selectedParam.variable_name }}</CardDescription>
          </CardHeader>
          <CardContent>
            <form class="grid gap-4" @submit.prevent="void submitEdit()">
              <label class="grid gap-2">
                <span class="text-sm font-medium text-slate-700">Runtime value</span>
                <Input v-model="editForm.runtimeValue" />
              </label>
              <div class="flex flex-wrap justify-end gap-2">
                <Button variant="outline" type="button" class="gap-2" @click="closeEdit">
                  <X class="h-4 w-4" />
                  Cancel
                </Button>
                <Button type="submit" class="gap-2" :disabled="submitting">
                  <Save class="h-4 w-4" />
                  Save
                </Button>
              </div>
            </form>
          </CardContent>
        </Card>

        <Card class="border-slate-200">
          <CardHeader>
            <CardTitle class="flex items-center gap-2">
              <History class="h-5 w-5" />
              Recent History
            </CardTitle>
            <CardDescription>Latest parameter edits for the selected instance.</CardDescription>
          </CardHeader>
          <CardContent class="grid gap-3">
            <div v-if="loadingHistory" class="text-sm text-slate-500">Loading history...</div>
            <div v-else-if="history.length === 0" class="text-sm text-slate-500">No parameter edits recorded.</div>
            <div v-for="entry in history" v-else :key="`${entry.variable_name}-${entry.create_time}`" class="rounded-md border border-slate-200 p-3">
              <div class="flex flex-wrap items-center justify-between gap-2">
                <span class="font-medium text-slate-900">{{ entry.variable_name }}</span>
                <span class="text-xs text-slate-500">{{ new Date(entry.create_time).toLocaleString() }}</span>
              </div>
              <div class="mt-2 grid gap-1 text-sm text-slate-600">
                <span>{{ entry.old_var }} -> {{ entry.new_var }}</span>
                <span>{{ entry.user_display || entry.user_name }}</span>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  </section>
</template>
