<script setup lang="ts">
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { Database, Edit3, Plus, RefreshCw, Save, X } from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import {
  createInstanceOperationDatabase,
  fetchInstanceOperationDatabaseInstances,
  fetchInstanceOperationDatabases,
  updateInstanceOperationDatabase,
  type InstanceOperationDatabaseInstance,
  type InstanceOperationDatabaseRecord,
} from '../api'
import { useAuthStore } from '@/stores/auth'

type FormMode = 'create' | 'edit'

const authStore = useAuthStore()

const instances = ref<InstanceOperationDatabaseInstance[]>([])
const databases = ref<InstanceOperationDatabaseRecord[]>([])
const selectedInstanceId = ref<number | null>(null)
const savedOnly = ref(false)
const search = ref('')

const loadingInstances = ref(false)
const loadingDatabases = ref(false)
const submitting = ref(false)
const error = ref('')
const feedback = ref('')
const activeFormMode = ref<FormMode | null>(null)

const form = reactive({
  dbName: '',
  owner: '',
  remark: '',
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

function cellValue(value: unknown) {
  if (value === null || value === undefined || value === '') {
    return '-'
  }
  return `${value}`
}

const canManageDatabases = computed(() => hasPermission('sql.menu_database'))

const selectedInstance = computed(() =>
  instances.value.find((instance) => instance.id === selectedInstanceId.value) ?? null,
)

const filteredDatabases = computed(() => {
  const query = search.value.trim().toLowerCase()
  if (!query) {
    return databases.value
  }

  return databases.value.filter((databaseRecord) =>
    [
      databaseRecord.db_name,
      databaseRecord.owner,
      databaseRecord.owner_display,
      databaseRecord.remark,
    ]
      .filter(Boolean)
      .join(' ')
      .toLowerCase()
      .includes(query),
  )
})

function resetForm() {
  form.dbName = ''
  form.owner = ''
  form.remark = ''
}

function openCreateForm() {
  resetForm()
  activeFormMode.value = 'create'
  feedback.value = ''
}

function openEditForm(databaseRecord: InstanceOperationDatabaseRecord) {
  form.dbName = databaseRecord.db_name
  form.owner = databaseRecord.owner ?? ''
  form.remark = databaseRecord.remark ?? ''
  activeFormMode.value = 'edit'
  feedback.value = ''
}

function closeForm() {
  resetForm()
  activeFormMode.value = null
}

async function loadInstances() {
  loadingInstances.value = true
  error.value = ''

  try {
    instances.value = await fetchInstanceOperationDatabaseInstances(requireToken())
    const firstInstance = instances.value[0]
    if (!selectedInstanceId.value && firstInstance) {
      selectedInstanceId.value = firstInstance.id
    }
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to load database-management instances.')
  } finally {
    loadingInstances.value = false
  }
}

async function loadDatabases() {
  if (!selectedInstanceId.value) {
    databases.value = []
    return
  }

  loadingDatabases.value = true
  error.value = ''

  try {
    const response = await fetchInstanceOperationDatabases(requireToken(), {
      instance_id: selectedInstanceId.value,
      saved: savedOnly.value,
    })
    databases.value = response.results
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to load databases.')
  } finally {
    loadingDatabases.value = false
  }
}

async function refreshDatabases() {
  feedback.value = ''
  await loadInstances()
  await loadDatabases()
}

async function submitDatabaseForm() {
  if (!selectedInstanceId.value || !activeFormMode.value) {
    return
  }
  if (!form.dbName.trim()) {
    error.value = 'Database name is required.'
    return
  }

  submitting.value = true
  error.value = ''
  feedback.value = ''

  const payload = {
    instance_id: selectedInstanceId.value,
    db_name: form.dbName.trim(),
    owner: form.owner.trim(),
    remark: form.remark.trim(),
  }

  try {
    if (activeFormMode.value === 'create') {
      await createInstanceOperationDatabase(payload, requireToken())
      feedback.value = `Database "${payload.db_name}" created.`
    } else {
      await updateInstanceOperationDatabase(payload, requireToken())
      feedback.value = `Database "${payload.db_name}" metadata updated.`
    }
    closeForm()
    await loadDatabases()
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to save database metadata.')
  } finally {
    submitting.value = false
  }
}

onMounted(async () => {
  try {
    await authStore.loadCurrentUser()
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to load user session.')
    return
  }

  if (!canManageDatabases.value) {
    error.value = 'You do not have permission to manage instance databases.'
    return
  }

  await refreshDatabases()
})

watch([selectedInstanceId, savedOnly], () => {
  if (!canManageDatabases.value) {
    return
  }
  void loadDatabases()
})
</script>

<template>
  <section class="grid gap-6">
    <div class="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
      <div class="space-y-1">
        <h2 class="text-2xl font-semibold text-slate-900">Database Management</h2>
        <p class="text-sm text-slate-600">
          Manage database owner metadata and create supported databases for operational instances.
        </p>
      </div>
      <div class="flex flex-wrap gap-2">
        <Button variant="outline" type="button" class="gap-2" :disabled="loadingDatabases" @click="void refreshDatabases()">
          <RefreshCw class="h-4 w-4" />
          Refresh
        </Button>
        <Button type="button" class="gap-2" :disabled="!selectedInstanceId" @click="openCreateForm">
          <Plus class="h-4 w-4" />
          New database
        </Button>
      </div>
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
        <CardDescription>Select an operational instance before editing database metadata.</CardDescription>
      </CardHeader>
      <CardContent>
        <div class="grid gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(0,0.8fr)_minmax(0,0.8fr)]">
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
            <span class="text-sm font-medium text-slate-700">Search</span>
            <Input v-model="search" placeholder="Database, owner, or remark" />
          </label>
          <label class="flex items-end gap-2 pb-2 text-sm text-slate-700">
            <input v-model="savedOnly" type="checkbox" class="h-4 w-4 rounded border-slate-300">
            Saved metadata only
          </label>
        </div>
      </CardContent>
    </Card>

    <div class="grid gap-6 xl:grid-cols-[minmax(0,1fr)_minmax(20rem,0.34fr)]">
      <Card class="border-slate-200">
        <CardHeader>
          <CardTitle class="flex items-center gap-2">
            <Database class="h-5 w-5" />
            Databases
          </CardTitle>
          <CardDescription>
            {{ filteredDatabases.length }} shown for {{ selectedInstance?.instance_name || 'no instance' }}
          </CardDescription>
        </CardHeader>
        <CardContent class="p-0">
          <div v-if="loadingDatabases" class="p-6 text-sm text-slate-500">
            Loading databases...
          </div>
          <div v-else-if="filteredDatabases.length === 0" class="p-6 text-sm text-slate-500">
            No databases match the current filters.
          </div>
          <div v-else class="overflow-x-auto">
            <table class="min-w-full divide-y divide-slate-200 text-sm">
              <thead class="bg-slate-50">
                <tr>
                  <th class="px-4 py-3 text-left font-medium text-slate-600">Database</th>
                  <th class="px-4 py-3 text-left font-medium text-slate-600">Owner</th>
                  <th class="px-4 py-3 text-left font-medium text-slate-600">Rows</th>
                  <th class="px-4 py-3 text-left font-medium text-slate-600">Data</th>
                  <th class="px-4 py-3 text-left font-medium text-slate-600">Remark</th>
                  <th class="px-4 py-3 text-right font-medium text-slate-600">Actions</th>
                </tr>
              </thead>
              <tbody class="divide-y divide-slate-100 bg-white">
                <tr v-for="databaseRecord in filteredDatabases" :key="databaseRecord.db_name">
                  <td class="px-4 py-3">
                    <div class="flex flex-wrap items-center gap-2">
                      <span class="font-medium text-slate-900">{{ databaseRecord.db_name }}</span>
                      <Badge v-if="databaseRecord.saved" variant="outline">Saved</Badge>
                    </div>
                  </td>
                  <td class="px-4 py-3 text-slate-600">{{ databaseRecord.owner_display || databaseRecord.owner || '-' }}</td>
                  <td class="px-4 py-3 text-slate-600">{{ cellValue(databaseRecord.table_rows) }}</td>
                  <td class="px-4 py-3 text-slate-600">{{ cellValue(databaseRecord.data_total) }}</td>
                  <td class="max-w-[20rem] truncate px-4 py-3 text-slate-600">{{ databaseRecord.remark || '-' }}</td>
                  <td class="px-4 py-3 text-right">
                    <Button variant="outline" type="button" size="sm" class="gap-2" @click="openEditForm(databaseRecord)">
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

      <Card v-if="activeFormMode" class="border-slate-200">
        <CardHeader>
          <CardTitle>{{ activeFormMode === 'create' ? 'Create database' : 'Edit metadata' }}</CardTitle>
          <CardDescription>
            {{ activeFormMode === 'create' ? 'Create a database and save ownership metadata.' : 'Update ownership metadata for an existing database.' }}
          </CardDescription>
        </CardHeader>
        <CardContent>
          <form class="grid gap-4" @submit.prevent="void submitDatabaseForm()">
            <label class="grid gap-2">
              <span class="text-sm font-medium text-slate-700">Database name</span>
              <Input v-model="form.dbName" :disabled="activeFormMode === 'edit'" placeholder="appdb" />
            </label>
            <label class="grid gap-2">
              <span class="text-sm font-medium text-slate-700">Owner username</span>
              <Input v-model="form.owner" placeholder="jane.doe" />
            </label>
            <label class="grid gap-2">
              <span class="text-sm font-medium text-slate-700">Remark</span>
              <Input v-model="form.remark" placeholder="Business owner, lifecycle, or notes" />
            </label>
            <div class="flex flex-wrap justify-end gap-2">
              <Button variant="outline" type="button" class="gap-2" @click="closeForm">
                <X class="h-4 w-4" />
                Cancel
              </Button>
              <Button type="submit" class="gap-2" :disabled="submitting">
                <Save class="h-4 w-4" />
                {{ submitting ? 'Saving...' : 'Save' }}
              </Button>
            </div>
          </form>
        </CardContent>
      </Card>
    </div>
  </section>
</template>
