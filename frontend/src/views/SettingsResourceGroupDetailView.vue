<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { RouterLink, useRoute, useRouter } from 'vue-router'
import { ArrowLeft, ChevronLeft, ChevronRight, ChevronsLeft, ChevronsRight, Save, Trash2 } from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import {
  createResourceGroup,
  deleteResourceGroup,
  fetchResourceGroup,
  fetchResourceGroupInstances,
  fetchResourceGroupUsers,
  updateResourceGroup,
  type ResourceGroupInstanceLookupRecord,
  type ResourceGroupUserLookupRecord,
} from '@/lib/api'
import { useAuthStore } from '@/stores/auth'

const authStore = useAuthStore()
const route = useRoute()
const router = useRouter()

const users = ref<ResourceGroupUserLookupRecord[]>([])
const instances = ref<ResourceGroupInstanceLookupRecord[]>([])
const selectedUserIds = ref<number[]>([])
const selectedInstanceIds = ref<number[]>([])
const groupName = ref('')

const availableUserFilter = ref('')
const selectedUserFilter = ref('')
const availableInstanceFilter = ref('')
const selectedInstanceFilter = ref('')

const availableUserSelection = ref<number[]>([])
const selectedUserSelection = ref<number[]>([])
const availableInstanceSelection = ref<number[]>([])
const selectedInstanceSelection = ref<number[]>([])

const isLoading = ref(false)
const isSaving = ref(false)
const isDeleting = ref(false)
const pageError = ref('')
const formError = ref('')
const formSuccess = ref('')

const isCreateMode = computed(() => route.name === 'settings-resource-groups-new')
const groupId = computed(() => {
  if (isCreateMode.value) {
    return null
  }
  const value = Number(route.params.groupId)
  return Number.isFinite(value) ? value : null
})

function hasPermission(permission: string) {
  if (authStore.currentUser?.is_superuser) {
    return true
  }
  return authStore.currentUser?.permissions.includes(permission) ?? false
}

const canAccessSettings = computed(() => hasPermission('sql.menu_system'))
const canViewResourceGroups = computed(() => canAccessSettings.value && hasPermission('sql.view_resourcegroup'))
const canCreateResourceGroups = computed(() => hasPermission('sql.menu_system') || hasPermission('sql.add_resourcegroup'))
const canEditResourceGroups = computed(() => hasPermission('sql.menu_system') || hasPermission('sql.change_resourcegroup'))
const canDeleteResourceGroups = computed(() => hasPermission('sql.menu_system') || hasPermission('sql.delete_resourcegroup'))
const canSave = computed(() => (isCreateMode.value ? canCreateResourceGroups.value : canEditResourceGroups.value))

const selectedUserSet = computed(() => new Set(selectedUserIds.value))
const selectedInstanceSet = computed(() => new Set(selectedInstanceIds.value))
const normalizedAvailableUserFilter = computed(() => availableUserFilter.value.trim().toLowerCase())
const normalizedSelectedUserFilter = computed(() => selectedUserFilter.value.trim().toLowerCase())
const normalizedAvailableInstanceFilter = computed(() => availableInstanceFilter.value.trim().toLowerCase())
const normalizedSelectedInstanceFilter = computed(() => selectedInstanceFilter.value.trim().toLowerCase())

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

function requireToken() {
  if (!authStore.accessToken) {
    throw new Error('Missing access token. Please login again.')
  }
  return authStore.accessToken
}

function userLabel(user: ResourceGroupUserLookupRecord) {
  return user.label || user.display || user.username
}

function serverLabel(instance: ResourceGroupInstanceLookupRecord) {
  return instance.label || `${instance.instance_name} | ${instance.db_type} | ${instance.host}`
}

function sortUsers(values: ResourceGroupUserLookupRecord[]) {
  return [...values].sort((left, right) =>
    userLabel(left).localeCompare(userLabel(right), undefined, {
      sensitivity: 'base',
      numeric: true,
    }),
  )
}

function sortInstances(values: ResourceGroupInstanceLookupRecord[]) {
  return [...values].sort((left, right) =>
    serverLabel(left).localeCompare(serverLabel(right), undefined, {
      sensitivity: 'base',
      numeric: true,
    }),
  )
}

function userMatches(user: ResourceGroupUserLookupRecord, filterValue: string) {
  if (!filterValue) {
    return true
  }

  const haystack = [user.display, user.username, user.label].join(' ').toLowerCase()
  return haystack.includes(filterValue)
}

function instanceMatches(instance: ResourceGroupInstanceLookupRecord, filterValue: string) {
  if (!filterValue) {
    return true
  }

  const haystack = [
    instance.instance_name,
    instance.db_type,
    instance.host,
    instance.label,
  ]
    .join(' ')
    .toLowerCase()
  return haystack.includes(filterValue)
}

const availableUsers = computed(() =>
  sortUsers(
    users.value
      .filter((user) => !selectedUserSet.value.has(user.id))
      .filter((user) => userMatches(user, normalizedAvailableUserFilter.value)),
  ),
)

const assignedUsers = computed(() =>
  sortUsers(
    users.value
      .filter((user) => selectedUserSet.value.has(user.id))
      .filter((user) => userMatches(user, normalizedSelectedUserFilter.value)),
  ),
)

const availableInstances = computed(() =>
  sortInstances(
    instances.value
      .filter((instance) => !selectedInstanceSet.value.has(instance.id))
      .filter((instance) => instanceMatches(instance, normalizedAvailableInstanceFilter.value)),
  ),
)

const assignedInstances = computed(() =>
  sortInstances(
    instances.value
      .filter((instance) => selectedInstanceSet.value.has(instance.id))
      .filter((instance) => instanceMatches(instance, normalizedSelectedInstanceFilter.value)),
  ),
)

function sortNumeric(values: number[]) {
  return [...new Set(values)].sort((left, right) => left - right)
}

function setSelectedUsers(userIds: number[]) {
  selectedUserIds.value = sortNumeric(userIds)
  formSuccess.value = ''
}

function addUsers(userIds: number[]) {
  if (userIds.length === 0) {
    return
  }
  setSelectedUsers([...selectedUserIds.value, ...userIds])
}

function removeUsers(userIds: number[]) {
  if (userIds.length === 0) {
    return
  }
  setSelectedUsers(selectedUserIds.value.filter((value) => !userIds.includes(value)))
}

function setSelectedInstances(instanceIds: number[]) {
  selectedInstanceIds.value = sortNumeric(instanceIds)
  formSuccess.value = ''
}

function addInstances(instanceIds: number[]) {
  if (instanceIds.length === 0) {
    return
  }
  setSelectedInstances([...selectedInstanceIds.value, ...instanceIds])
}

function removeInstances(instanceIds: number[]) {
  if (instanceIds.length === 0) {
    return
  }
  setSelectedInstances(selectedInstanceIds.value.filter((value) => !instanceIds.includes(value)))
}

function moveSelectedUsersToAssigned() {
  addUsers(availableUserSelection.value)
  availableUserSelection.value = []
}

function moveAllUsersToAssigned() {
  addUsers(availableUsers.value.map((user) => user.id))
  availableUserSelection.value = []
}

function moveSelectedUsersToAvailable() {
  removeUsers(selectedUserSelection.value)
  selectedUserSelection.value = []
}

function moveAllUsersToAvailable() {
  removeUsers(assignedUsers.value.map((user) => user.id))
  selectedUserSelection.value = []
}

function moveSelectedInstancesToAssigned() {
  addInstances(availableInstanceSelection.value)
  availableInstanceSelection.value = []
}

function moveAllInstancesToAssigned() {
  addInstances(availableInstances.value.map((instance) => instance.id))
  availableInstanceSelection.value = []
}

function moveSelectedInstancesToAvailable() {
  removeInstances(selectedInstanceSelection.value)
  selectedInstanceSelection.value = []
}

function moveAllInstancesToAvailable() {
  removeInstances(assignedInstances.value.map((instance) => instance.id))
  selectedInstanceSelection.value = []
}

function updateSelection(
  event: Event,
  target: 'available-users' | 'selected-users' | 'available-instances' | 'selected-instances',
) {
  const element = event.target as HTMLSelectElement
  const values = Array.from(element.selectedOptions)
    .map((option) => Number(option.value))
    .filter((value) => Number.isFinite(value))

  if (target === 'available-users') {
    availableUserSelection.value = values
    return
  }

  if (target === 'selected-users') {
    selectedUserSelection.value = values
    return
  }

  if (target === 'available-instances') {
    availableInstanceSelection.value = values
    return
  }

  selectedInstanceSelection.value = values
}

async function loadPage() {
  isLoading.value = true
  pageError.value = ''
  formError.value = ''
  formSuccess.value = ''
  groupName.value = ''
  selectedUserIds.value = []
  selectedInstanceIds.value = []
  availableUserSelection.value = []
  selectedUserSelection.value = []
  availableInstanceSelection.value = []
  selectedInstanceSelection.value = []

  try {
    await authStore.loadCurrentUser()

    if (!canViewResourceGroups.value && !canCreateResourceGroups.value && !canEditResourceGroups.value) {
      pageError.value = 'You do not have permission to access Datamingle resource group management.'
      return
    }

    const [userLookup, instanceLookup] = await Promise.all([
      fetchResourceGroupUsers(requireToken()),
      fetchResourceGroupInstances(requireToken()),
    ])

    users.value = userLookup
    instances.value = instanceLookup

    if (isCreateMode.value) {
      return
    }

    if (!groupId.value) {
      pageError.value = 'Invalid resource group identifier.'
      return
    }

    const resourceGroup = await fetchResourceGroup(groupId.value, requireToken())
    groupName.value = resourceGroup.group_name
    selectedUserIds.value = sortNumeric(resourceGroup.user_ids)
    selectedInstanceIds.value = sortNumeric(resourceGroup.instance_ids)
  } catch (errorValue) {
    pageError.value = toUserFacingMessage(errorValue, 'Failed to load the resource group editor.')
  } finally {
    isLoading.value = false
  }
}

async function saveResourceGroup() {
  if (!canSave.value) {
    formError.value = 'You do not have permission to save this resource group.'
    return
  }

  const trimmedName = groupName.value.trim()
  if (!trimmedName) {
    formError.value = 'Group name cannot be blank.'
    return
  }

  isSaving.value = true
  formError.value = ''
  formSuccess.value = ''

  try {
    const payload = {
      group_name: trimmedName,
      user_ids: sortNumeric(selectedUserIds.value),
      instance_ids: sortNumeric(selectedInstanceIds.value),
    }

    if (isCreateMode.value) {
      const createdGroup = await createResourceGroup(payload, requireToken())
      formSuccess.value = 'Resource group created successfully.'
      await router.replace(`/settings/resource-groups/${createdGroup.group_id}`)
      groupName.value = createdGroup.group_name
      selectedUserIds.value = sortNumeric(createdGroup.user_ids)
      selectedInstanceIds.value = sortNumeric(createdGroup.instance_ids)
      return
    }

    if (!groupId.value) {
      throw new Error('Missing resource group identifier.')
    }

    const updatedGroup = await updateResourceGroup(groupId.value, payload, requireToken())
    groupName.value = updatedGroup.group_name
    selectedUserIds.value = sortNumeric(updatedGroup.user_ids)
    selectedInstanceIds.value = sortNumeric(updatedGroup.instance_ids)
    formSuccess.value = 'Resource group updated successfully.'
  } catch (errorValue) {
    formError.value = toUserFacingMessage(errorValue, 'Failed to save the resource group.')
  } finally {
    isSaving.value = false
  }
}

async function removeResourceGroup() {
  if (isCreateMode.value || !groupId.value) {
    return
  }

  if (!canDeleteResourceGroups.value) {
    formError.value = 'You do not have permission to delete this resource group.'
    return
  }

  if (!window.confirm(`Delete the "${groupName.value}" resource group from Datamingle?`)) {
    return
  }

  isDeleting.value = true
  formError.value = ''
  formSuccess.value = ''

  try {
    await deleteResourceGroup(groupId.value, requireToken())
    await router.push('/settings/resource-groups')
  } catch (errorValue) {
    formError.value = toUserFacingMessage(errorValue, 'Failed to delete the resource group.')
  } finally {
    isDeleting.value = false
  }
}

onMounted(() => {
  void loadPage()
})

watch(
  () => route.fullPath,
  (currentPath, previousPath) => {
    if (currentPath !== previousPath) {
      void loadPage()
    }
  },
)
</script>

<template>
  <section class="grid gap-6">
    <div class="flex flex-wrap items-center justify-between gap-3">
      <Button as-child variant="ghost">
        <RouterLink to="/settings/resource-groups">
          <ArrowLeft class="h-4 w-4" />
          Back to resource groups
        </RouterLink>
      </Button>
    </div>

    <Card class="border-slate-200">
      <CardHeader>
        <CardTitle>{{ isCreateMode ? 'Create Resource Group' : 'Edit Resource Group' }}</CardTitle>
        <CardDescription>
          Assign users and servers with filterable dual-list selectors modeled after Permission Groups.
        </CardDescription>
      </CardHeader>
      <CardContent class="space-y-6">
        <div class="space-y-2">
          <label for="resource-group-name" class="text-sm font-medium text-slate-900">Name</label>
          <Input
            id="resource-group-name"
            v-model="groupName"
            :disabled="!canSave || isLoading"
            placeholder="e.g. production"
          />
        </div>

        <p v-if="pageError" class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
          {{ pageError }}
        </p>
        <p v-else-if="formError" class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
          {{ formError }}
        </p>
        <p
          v-else-if="formSuccess"
          class="rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700"
        >
          {{ formSuccess }}
        </p>

        <div class="space-y-4 rounded-2xl border border-slate-200 p-5">
          <div class="flex flex-wrap items-center justify-between gap-3">
            <div>
              <h3 class="text-lg font-semibold text-slate-900">Users</h3>
              <p class="text-sm text-slate-600">Choose which Datamingle users belong to this resource group.</p>
            </div>
            <div class="flex flex-wrap items-center gap-2">
              <Badge variant="secondary" class="bg-slate-100 text-slate-700">
                {{ availableUsers.length }} available
              </Badge>
              <Badge variant="secondary" class="bg-slate-100 text-slate-700">
                {{ assignedUsers.length }} assigned
              </Badge>
            </div>
          </div>

          <div class="grid gap-4 lg:grid-cols-[minmax(0,1fr)_auto_minmax(0,1fr)]">
            <div class="space-y-3">
              <label for="available-users-filter" class="text-sm font-medium text-slate-900">Available users</label>
              <Input
                id="available-users-filter"
                v-model="availableUserFilter"
                :disabled="isLoading"
                placeholder="Filter available users"
              />
              <select
                class="min-h-[18rem] w-full rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm focus:outline-none focus:ring-1 focus:ring-ring"
                multiple
                @change="updateSelection($event, 'available-users')"
              >
                <option v-for="user in availableUsers" :key="user.id" :value="user.id">
                  {{ userLabel(user) }}
                </option>
              </select>
            </div>

            <div class="flex flex-col items-center justify-center gap-2">
              <Button variant="outline" size="icon" :disabled="!canSave || availableUserSelection.length === 0" @click="moveSelectedUsersToAssigned">
                <ChevronRight class="h-4 w-4" />
              </Button>
              <Button variant="outline" size="icon" :disabled="!canSave || availableUsers.length === 0" @click="moveAllUsersToAssigned">
                <ChevronsRight class="h-4 w-4" />
              </Button>
              <Button variant="outline" size="icon" :disabled="!canSave || selectedUserSelection.length === 0" @click="moveSelectedUsersToAvailable">
                <ChevronLeft class="h-4 w-4" />
              </Button>
              <Button variant="outline" size="icon" :disabled="!canSave || assignedUsers.length === 0" @click="moveAllUsersToAvailable">
                <ChevronsLeft class="h-4 w-4" />
              </Button>
            </div>

            <div class="space-y-3">
              <label for="assigned-users-filter" class="text-sm font-medium text-slate-900">Assigned users</label>
              <Input
                id="assigned-users-filter"
                v-model="selectedUserFilter"
                :disabled="isLoading"
                placeholder="Filter assigned users"
              />
              <select
                class="min-h-[18rem] w-full rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm focus:outline-none focus:ring-1 focus:ring-ring"
                multiple
                @change="updateSelection($event, 'selected-users')"
              >
                <option v-for="user in assignedUsers" :key="user.id" :value="user.id">
                  {{ userLabel(user) }}
                </option>
              </select>
            </div>
          </div>
        </div>

        <div class="space-y-4 rounded-2xl border border-slate-200 p-5">
          <div class="flex flex-wrap items-center justify-between gap-3">
            <div>
              <h3 class="text-lg font-semibold text-slate-900">Servers</h3>
              <p class="text-sm text-slate-600">Choose which database servers belong to this resource group.</p>
            </div>
            <div class="flex flex-wrap items-center gap-2">
              <Badge variant="secondary" class="bg-slate-100 text-slate-700">
                {{ availableInstances.length }} available
              </Badge>
              <Badge variant="secondary" class="bg-slate-100 text-slate-700">
                {{ assignedInstances.length }} assigned
              </Badge>
            </div>
          </div>

          <div class="grid gap-4 lg:grid-cols-[minmax(0,1fr)_auto_minmax(0,1fr)]">
            <div class="space-y-3">
              <label for="available-servers-filter" class="text-sm font-medium text-slate-900">Available servers</label>
              <Input
                id="available-servers-filter"
                v-model="availableInstanceFilter"
                :disabled="isLoading"
                placeholder="Filter available servers"
              />
              <select
                class="min-h-[18rem] w-full rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm focus:outline-none focus:ring-1 focus:ring-ring"
                multiple
                @change="updateSelection($event, 'available-instances')"
              >
                <option v-for="instance in availableInstances" :key="instance.id" :value="instance.id">
                  {{ serverLabel(instance) }}
                </option>
              </select>
            </div>

            <div class="flex flex-col items-center justify-center gap-2">
              <Button variant="outline" size="icon" :disabled="!canSave || availableInstanceSelection.length === 0" @click="moveSelectedInstancesToAssigned">
                <ChevronRight class="h-4 w-4" />
              </Button>
              <Button variant="outline" size="icon" :disabled="!canSave || availableInstances.length === 0" @click="moveAllInstancesToAssigned">
                <ChevronsRight class="h-4 w-4" />
              </Button>
              <Button variant="outline" size="icon" :disabled="!canSave || selectedInstanceSelection.length === 0" @click="moveSelectedInstancesToAvailable">
                <ChevronLeft class="h-4 w-4" />
              </Button>
              <Button variant="outline" size="icon" :disabled="!canSave || assignedInstances.length === 0" @click="moveAllInstancesToAvailable">
                <ChevronsLeft class="h-4 w-4" />
              </Button>
            </div>

            <div class="space-y-3">
              <label for="assigned-servers-filter" class="text-sm font-medium text-slate-900">Assigned servers</label>
              <Input
                id="assigned-servers-filter"
                v-model="selectedInstanceFilter"
                :disabled="isLoading"
                placeholder="Filter assigned servers"
              />
              <select
                class="min-h-[18rem] w-full rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm focus:outline-none focus:ring-1 focus:ring-ring"
                multiple
                @change="updateSelection($event, 'selected-instances')"
              >
                <option v-for="instance in assignedInstances" :key="instance.id" :value="instance.id">
                  {{ serverLabel(instance) }}
                </option>
              </select>
            </div>
          </div>
        </div>
      </CardContent>
      <CardFooter class="justify-between border-t border-slate-200 pt-6">
        <div>
          <Button
            v-if="!isCreateMode && canDeleteResourceGroups"
            variant="destructive"
            :disabled="isDeleting"
            @click="removeResourceGroup"
          >
            <Trash2 class="h-4 w-4" />
            Delete
          </Button>
        </div>
        <Button :disabled="!canSave || isLoading || isSaving" @click="saveResourceGroup">
          <Save class="h-4 w-4" />
          {{ isSaving ? 'Saving…' : 'Save resource group' }}
        </Button>
      </CardFooter>
    </Card>
  </section>
</template>
