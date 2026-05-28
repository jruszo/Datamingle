<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { RouterLink, useRoute, useRouter } from 'vue-router'
import {
  ArrowLeft,
  ChevronLeft,
  ChevronRight,
  ChevronsLeft,
  ChevronsRight,
  Save,
  Trash2,
} from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import {
  createResourceGroup,
  deleteResourceGroup,
  fetchAccessRoles,
  fetchResourceGroup,
  fetchResourceGroupInstances,
  fetchResourceGroupUsers,
  updateResourceGroup,
  type AccessRoleRecord,
  type ResourceAccessRoleCode,
  type ResourceGroupInstanceLookupRecord,
  type ResourceGroupMembershipSource,
  type ResourceGroupUpsertPayload,
  type ResourceGroupUserLookupRecord,
} from '../api'
import { useAuthStore } from '@/stores/auth'

type UserAccessState = {
  access_role: ResourceAccessRoleCode | ''
  membership_source?: ResourceGroupMembershipSource
}

const authStore = useAuthStore()
const route = useRoute()
const router = useRouter()

const accessRoles = ref<AccessRoleRecord[]>([])
const users = ref<ResourceGroupUserLookupRecord[]>([])
const instances = ref<ResourceGroupInstanceLookupRecord[]>([])
const userAccessById = ref<Record<number, UserAccessState>>({})
const selectedInstanceIds = ref<number[]>([])
const groupName = ref('')

const userFilter = ref('')
const availableInstanceFilter = ref('')
const selectedInstanceFilter = ref('')

const availableInstanceSelection = ref<number[]>([])
const selectedInstanceSelection = ref<number[]>([])

const isLoading = ref(false)
const isSaving = ref(false)
const isDeleting = ref(false)
const pageNotice = ref('')
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
  return authStore.currentUser?.permissions?.includes(permission) ?? false
}

const canViewResourceGroups = computed(
  () =>
    hasPermission('sql.menu_system')
    || hasPermission('sql.view_resourcegroup')
    || hasPermission('sql.resource_group_owner'),
)
const canCreateResourceGroups = computed(
  () => hasPermission('sql.menu_system') || hasPermission('sql.add_resourcegroup'),
)
const canEditResourceGroups = computed(
  () =>
    hasPermission('sql.menu_system')
    || hasPermission('sql.change_resourcegroup')
    || hasPermission('sql.resource_group_owner'),
)
const canDeleteResourceGroups = computed(
  () => hasPermission('sql.menu_system') || hasPermission('sql.delete_resourcegroup'),
)
const canSave = computed(() => (isCreateMode.value ? canCreateResourceGroups.value : canEditResourceGroups.value))

const selectedInstanceSet = computed(() => new Set(selectedInstanceIds.value))
const normalizedUserFilter = computed(() => userFilter.value.trim().toLowerCase())
const normalizedAvailableInstanceFilter = computed(() => availableInstanceFilter.value.trim().toLowerCase())
const normalizedSelectedInstanceFilter = computed(() => selectedInstanceFilter.value.trim().toLowerCase())
const assignedUserCount = computed(
  () => Object.values(userAccessById.value).filter((row) => row.access_role).length,
)

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

  const haystack = [instance.instance_name, instance.db_type, instance.host, instance.label]
    .join(' ')
    .toLowerCase()
  return haystack.includes(filterValue)
}

const filteredUsers = computed(() =>
  sortUsers(users.value.filter((user) => userMatches(user, normalizedUserFilter.value))),
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

function isAccessRoleCode(value: string): value is ResourceAccessRoleCode {
  return accessRoles.value.some((role) => role.code === value)
}

function roleForUser(userId: number) {
  return userAccessById.value[userId]?.access_role ?? ''
}

function sourceForUser(userId: number) {
  return userAccessById.value[userId]?.membership_source
}

function updateUserRole(userId: number, event: Event) {
  const value = (event.target as HTMLSelectElement).value
  if (sourceForUser(userId) === 'workos_directory') {
    return
  }

  const nextAccess = { ...userAccessById.value }
  if (!value) {
    delete nextAccess[userId]
  } else if (isAccessRoleCode(value)) {
    nextAccess[userId] = {
      access_role: value,
      membership_source: nextAccess[userId]?.membership_source ?? 'datamingle',
    }
  }
  userAccessById.value = nextAccess
  formSuccess.value = ''
}

function userAccessRows() {
  return Object.entries(userAccessById.value)
    .map(([userId, row]) => ({
      user_id: Number(userId),
      access_role: row.access_role,
    }))
    .filter(
      (row): row is { user_id: number; access_role: ResourceAccessRoleCode } =>
        Number.isFinite(row.user_id) && isAccessRoleCode(row.access_role),
    )
    .sort((left, right) => left.user_id - right.user_id)
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

function updateInstanceSelection(event: Event, target: 'available-instances' | 'selected-instances') {
  const element = event.target as HTMLSelectElement
  const values = Array.from(element.selectedOptions)
    .map((option) => Number(option.value))
    .filter((value) => Number.isFinite(value))

  if (target === 'available-instances') {
    availableInstanceSelection.value = values
    return
  }

  selectedInstanceSelection.value = values
}

async function loadPage() {
  isLoading.value = true
  pageNotice.value = ''
  pageError.value = ''
  formError.value = ''
  formSuccess.value = ''
  groupName.value = ''
  userAccessById.value = {}
  selectedInstanceIds.value = []
  availableInstanceSelection.value = []
  selectedInstanceSelection.value = []

  try {
    await authStore.loadCurrentUser()

    if (isCreateMode.value && route.query.reason === 'inventory-requires-resource-group') {
      pageNotice.value = 'A resource group is required before you can add an instance.'
    }

    if (!canViewResourceGroups.value && !canCreateResourceGroups.value && !canEditResourceGroups.value) {
      pageError.value = 'You do not have permission to access Datamingle resource group management.'
      return
    }

    const [roles, userLookup, instanceLookup] = await Promise.all([
      fetchAccessRoles(requireToken()),
      fetchResourceGroupUsers(requireToken()),
      fetchResourceGroupInstances(requireToken()),
    ])

    accessRoles.value = roles
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
    selectedInstanceIds.value = sortNumeric(resourceGroup.instance_ids)
    const nextAccess: Record<number, UserAccessState> = {}
    for (const row of resourceGroup.user_access ?? []) {
      nextAccess[row.user_id] = {
        access_role: row.access_role,
        membership_source: row.membership_source ?? 'datamingle',
      }
    }
    userAccessById.value = nextAccess
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
    const payload: ResourceGroupUpsertPayload = {
      group_name: trimmedName,
      user_access: userAccessRows(),
      instance_ids: sortNumeric(selectedInstanceIds.value),
    }

    if (isCreateMode.value) {
      const createdGroup = await createResourceGroup(payload, requireToken())
      formSuccess.value = 'Resource group created successfully.'
      await router.replace(`/settings/resource-groups/${createdGroup.group_id}`)
      return
    }

    if (!groupId.value) {
      throw new Error('Missing resource group identifier.')
    }

    const updatedGroup = await updateResourceGroup(groupId.value, payload, requireToken())
    groupName.value = updatedGroup.group_name
    selectedInstanceIds.value = sortNumeric(updatedGroup.instance_ids)
    const nextAccess: Record<number, UserAccessState> = {}
    for (const row of updatedGroup.user_access ?? []) {
      nextAccess[row.user_id] = {
        access_role: row.access_role,
        membership_source: row.membership_source ?? 'datamingle',
      }
    }
    userAccessById.value = nextAccess
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
        <CardDescription>Manage scoped resource access and server membership.</CardDescription>
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

        <p v-if="pageError" class="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
          {{ pageError }}
        </p>
        <p
          v-else-if="pageNotice"
          class="rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800"
        >
          {{ pageNotice }}
        </p>
        <p v-else-if="formError" class="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
          {{ formError }}
        </p>
        <p
          v-else-if="formSuccess"
          class="rounded-lg border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700"
        >
          {{ formSuccess }}
        </p>

        <div class="space-y-4 rounded-lg border border-slate-200 p-5">
          <div class="flex flex-wrap items-center justify-between gap-3">
            <div>
              <h3 class="text-base font-semibold text-slate-900">User Access</h3>
            </div>
            <div class="flex flex-wrap items-center gap-2">
              <Badge variant="secondary" class="bg-slate-100 text-slate-700">
                {{ assignedUserCount }} assigned
              </Badge>
              <Badge variant="secondary" class="bg-slate-100 text-slate-700">
                {{ users.length }} users
              </Badge>
            </div>
          </div>

          <Input
            v-model="userFilter"
            :disabled="isLoading"
            placeholder="Filter users"
            aria-label="Filter users"
          />

          <div class="overflow-x-auto rounded-md border border-slate-200">
            <table class="min-w-full divide-y divide-slate-200 text-sm">
              <thead class="bg-slate-50 text-left text-xs font-medium uppercase text-slate-500">
                <tr>
                  <th class="px-4 py-3">User</th>
                  <th class="px-4 py-3">Access role</th>
                  <th class="px-4 py-3">Source</th>
                </tr>
              </thead>
              <tbody class="divide-y divide-slate-200 bg-white">
                <tr v-for="user in filteredUsers" :key="user.id">
                  <td class="px-4 py-3">
                    <div class="font-medium text-slate-900">{{ userLabel(user) }}</div>
                    <div class="mt-1 text-xs text-slate-500">{{ user.username }}</div>
                  </td>
                  <td class="px-4 py-3">
                    <select
                      class="w-full min-w-52 rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm focus:outline-none focus:ring-1 focus:ring-ring disabled:cursor-not-allowed disabled:opacity-50"
                      :value="roleForUser(user.id)"
                      :disabled="!canSave || sourceForUser(user.id) === 'workos_directory'"
                      @change="updateUserRole(user.id, $event)"
                    >
                      <option value="">No access</option>
                      <option v-for="role in accessRoles" :key="role.code" :value="role.code">
                        {{ role.label }}
                      </option>
                    </select>
                  </td>
                  <td class="px-4 py-3">
                    <Badge
                      v-if="sourceForUser(user.id) === 'workos_directory'"
                      variant="secondary"
                      class="bg-violet-100 text-violet-800"
                    >
                      WorkOS Directory
                    </Badge>
                    <span v-else-if="roleForUser(user.id)" class="text-xs text-slate-500">
                      Datamingle
                    </span>
                    <span v-else class="text-xs text-slate-400">None</span>
                  </td>
                </tr>
                <tr v-if="filteredUsers.length === 0">
                  <td colspan="3" class="px-4 py-8 text-center text-sm text-slate-500">
                    No users match the current filter.
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>

        <div class="space-y-4 rounded-lg border border-slate-200 p-5">
          <div class="flex flex-wrap items-center justify-between gap-3">
            <div>
              <h3 class="text-base font-semibold text-slate-900">Servers</h3>
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
                @change="updateInstanceSelection($event, 'available-instances')"
              >
                <option v-for="instance in availableInstances" :key="instance.id" :value="instance.id">
                  {{ serverLabel(instance) }}
                </option>
              </select>
            </div>

            <div class="flex flex-col items-center justify-center gap-2">
              <Button
                variant="outline"
                size="icon"
                :disabled="!canSave || availableInstanceSelection.length === 0"
                @click="moveSelectedInstancesToAssigned"
              >
                <ChevronRight class="h-4 w-4" />
              </Button>
              <Button
                variant="outline"
                size="icon"
                :disabled="!canSave || availableInstances.length === 0"
                @click="moveAllInstancesToAssigned"
              >
                <ChevronsRight class="h-4 w-4" />
              </Button>
              <Button
                variant="outline"
                size="icon"
                :disabled="!canSave || selectedInstanceSelection.length === 0"
                @click="moveSelectedInstancesToAvailable"
              >
                <ChevronLeft class="h-4 w-4" />
              </Button>
              <Button
                variant="outline"
                size="icon"
                :disabled="!canSave || assignedInstances.length === 0"
                @click="moveAllInstancesToAvailable"
              >
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
                @change="updateInstanceSelection($event, 'selected-instances')"
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
        <Button
          v-if="!isCreateMode"
          variant="destructive"
          :disabled="isDeleting || !canDeleteResourceGroups"
          @click="removeResourceGroup"
        >
          <Trash2 class="h-4 w-4" />
          Delete
        </Button>
        <span v-else />
        <div class="flex flex-wrap justify-end gap-3">
          <Button as-child variant="outline">
            <RouterLink to="/settings/resource-groups">Cancel</RouterLink>
          </Button>
          <Button :disabled="isLoading || isSaving || !canSave" @click="saveResourceGroup">
            <Save class="h-4 w-4" />
            Save
          </Button>
        </div>
      </CardFooter>
    </Card>
  </section>
</template>
