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
  createTeam,
  deleteTeam,
  fetchPermissionLevels,
  fetchTeam,
  fetchTeamInstances,
  fetchTeamNodes,
  fetchTeamUsers,
  updateTeam,
  type PermissionLevelId,
  type PermissionLevelRecord,
  type TeamInstanceLookupRecord,
  type TeamNodeLookupRecord,
  type TeamUpsertPayload,
  type TeamUserLookupRecord,
} from '../api'
import { useAuthStore } from '@/stores/auth'

type UserAccessState = {
  permission_level_id: PermissionLevelId | ''
}

const authStore = useAuthStore()
const route = useRoute()
const router = useRouter()

const accessRoles = ref<PermissionLevelRecord[]>([])
const users = ref<TeamUserLookupRecord[]>([])
const instances = ref<TeamInstanceLookupRecord[]>([])
const nodes = ref<TeamNodeLookupRecord[]>([])
const userAccessById = ref<Record<number, UserAccessState>>({})
const selectedInstanceIds = ref<number[]>([])
const selectedNodeIds = ref<number[]>([])
const groupName = ref('')

const userFilter = ref('')
const availableUserId = ref('')
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

const isCreateMode = computed(() => route.name === 'settings-teams-new')
const groupId = computed(() => {
  if (isCreateMode.value) {
    return null
  }
  const value = Number(route.params.teamId)
  return Number.isFinite(value) ? value : null
})

function hasPermission(permission: string) {
  if (authStore.currentUser?.is_superuser) {
    return true
  }
  return authStore.currentUser?.permissions?.includes(permission) ?? false
}

const canViewTeams = computed(
  () =>
    hasPermission('sql.menu_system')
    || hasPermission('sql.view_team')
    || hasPermission('sql.change_team'),
)
const canCreateTeams = computed(
  () => hasPermission('sql.menu_system') || hasPermission('sql.add_team'),
)
const canEditTeams = computed(
  () =>
    hasPermission('sql.menu_system')
    || hasPermission('sql.change_team'),
)
const canDeleteTeams = computed(
  () => hasPermission('sql.menu_system') || hasPermission('sql.delete_team'),
)
const canSave = computed(() => (isCreateMode.value ? canCreateTeams.value : canEditTeams.value))

const selectedInstanceSet = computed(() => new Set(selectedInstanceIds.value))
const normalizedUserFilter = computed(() => userFilter.value.trim().toLowerCase())
const normalizedAvailableInstanceFilter = computed(() => availableInstanceFilter.value.trim().toLowerCase())
const normalizedSelectedInstanceFilter = computed(() => selectedInstanceFilter.value.trim().toLowerCase())
const assignedUserCount = computed(
  () => Object.values(userAccessById.value).filter((row) => row.permission_level_id).length,
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

function userLabel(user: TeamUserLookupRecord) {
  return user.label || user.display || user.username
}

function serverLabel(instance: TeamInstanceLookupRecord) {
  return instance.label || `${instance.instance_name} | ${instance.db_type} | ${instance.host}`
}

function nodeLabel(node: TeamNodeLookupRecord) {
  return node.label || `${node.name} | ${node.address}`
}

function sortUsers(values: TeamUserLookupRecord[]) {
  return [...values].sort((left, right) =>
    userLabel(left).localeCompare(userLabel(right), undefined, {
      sensitivity: 'base',
      numeric: true,
    }),
  )
}

function sortInstances(values: TeamInstanceLookupRecord[]) {
  return [...values].sort((left, right) =>
    serverLabel(left).localeCompare(serverLabel(right), undefined, {
      sensitivity: 'base',
      numeric: true,
    }),
  )
}

function userMatches(user: TeamUserLookupRecord, filterValue: string) {
  if (!filterValue) {
    return true
  }

  const haystack = [user.display, user.username, user.label].join(' ').toLowerCase()
  return haystack.includes(filterValue)
}

function instanceMatches(instance: TeamInstanceLookupRecord, filterValue: string) {
  if (!filterValue) {
    return true
  }

  const haystack = [instance.instance_name, instance.db_type, instance.host, instance.label]
    .join(' ')
    .toLowerCase()
  return haystack.includes(filterValue)
}

const assignedUsers = computed(() =>
  sortUsers(
    users.value
      .filter((user) => userAccessById.value[user.id]?.permission_level_id)
      .filter((user) => userMatches(user, normalizedUserFilter.value)),
  ),
)

const availableUsers = computed(() =>
  sortUsers(users.value.filter((user) => !userAccessById.value[user.id])),
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

function permissionLevelId(value: string | number | '') {
  const levelId = Number(value)
  return accessRoles.value.some((level) => level.id === levelId) ? levelId : null
}

function roleForUser(userId: number) {
  return userAccessById.value[userId]?.permission_level_id ?? ''
}

function updateUserRole(userId: number, event: Event) {
  const value = (event.target as HTMLSelectElement).value
  const levelId = permissionLevelId(value)

  const nextAccess = { ...userAccessById.value }
  if (!value) {
    delete nextAccess[userId]
  } else if (levelId !== null) {
    nextAccess[userId] = { permission_level_id: levelId }
  }
  userAccessById.value = nextAccess
  formSuccess.value = ''
}

function addMember() {
  const userId = Number(availableUserId.value)
  const defaultLevel = accessRoles.value[0]?.id
  if (!Number.isFinite(userId) || !defaultLevel) {
    return
  }
  userAccessById.value = {
    ...userAccessById.value,
    [userId]: { permission_level_id: defaultLevel },
  }
  availableUserId.value = ''
  formSuccess.value = ''
}

function removeMember(userId: number) {
  const nextAccess = { ...userAccessById.value }
  delete nextAccess[userId]
  userAccessById.value = nextAccess
  formSuccess.value = ''
}

function userAccessRows() {
  return Object.entries(userAccessById.value)
    .map(([userId, row]) => ({
      user_id: Number(userId),
      permission_level_id: row.permission_level_id,
    }))
    .filter(
      (row): row is { user_id: number; permission_level_id: PermissionLevelId } =>
        Number.isFinite(row.user_id) && permissionLevelId(row.permission_level_id) !== null,
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

function updateNodeSelection(event: Event) {
  selectedNodeIds.value = Array.from((event.target as HTMLSelectElement).selectedOptions)
    .map((option) => Number(option.value))
    .filter((value) => Number.isFinite(value))
  formSuccess.value = ''
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
  selectedNodeIds.value = []
  availableInstanceSelection.value = []
  selectedInstanceSelection.value = []

  try {
    await authStore.loadCurrentUser()

    if (isCreateMode.value && route.query.reason === 'inventory-requires-team') {
      pageNotice.value = 'A team is required before you can add an instance.'
    }

    if (!canViewTeams.value && !canCreateTeams.value && !canEditTeams.value) {
      pageError.value = 'You do not have permission to access Datamingle team management.'
      return
    }

    const [roles, userLookup, nodeLookup, instanceLookup] = await Promise.all([
      fetchPermissionLevels(requireToken()),
      fetchTeamUsers(requireToken()),
      fetchTeamNodes(requireToken()),
      fetchTeamInstances(requireToken()),
    ])

    accessRoles.value = roles
    users.value = userLookup
    nodes.value = nodeLookup
    instances.value = instanceLookup

    if (isCreateMode.value) {
      return
    }

    if (!groupId.value) {
      pageError.value = 'Invalid team identifier.'
      return
    }

    const resourceGroup = await fetchTeam(groupId.value, requireToken())
    groupName.value = resourceGroup.team_name
    selectedNodeIds.value = sortNumeric(resourceGroup.node_ids)
    selectedInstanceIds.value = sortNumeric(resourceGroup.service_ids)
    const nextAccess: Record<number, UserAccessState> = {}
    for (const row of resourceGroup.user_access ?? []) {
      nextAccess[row.user_id] = {
        permission_level_id: row.permission_level_id,
      }
    }
    userAccessById.value = nextAccess
  } catch (errorValue) {
    pageError.value = toUserFacingMessage(errorValue, 'Failed to load the team editor.')
  } finally {
    isLoading.value = false
  }
}

async function saveTeam() {
  if (!canSave.value) {
    formError.value = 'You do not have permission to save this team.'
    return
  }

  const trimmedName = groupName.value.trim()
  if (!trimmedName) {
    formError.value = 'Team name cannot be blank.'
    return
  }

  isSaving.value = true
  formError.value = ''
  formSuccess.value = ''

  try {
    const payload: TeamUpsertPayload = {
      team_name: trimmedName,
      user_access: userAccessRows(),
      node_ids: sortNumeric(selectedNodeIds.value),
      service_ids: sortNumeric(selectedInstanceIds.value),
    }

    if (isCreateMode.value) {
      const createdGroup = await createTeam(payload, requireToken())
      formSuccess.value = 'Team created successfully.'
      await router.replace(`/settings/teams/${createdGroup.team_id}`)
      return
    }

    if (!groupId.value) {
      throw new Error('Missing team identifier.')
    }

    const updatedGroup = await updateTeam(groupId.value, payload, requireToken())
    groupName.value = updatedGroup.team_name
    selectedNodeIds.value = sortNumeric(updatedGroup.node_ids)
    selectedInstanceIds.value = sortNumeric(updatedGroup.service_ids)
    const nextAccess: Record<number, UserAccessState> = {}
    for (const row of updatedGroup.user_access ?? []) {
      nextAccess[row.user_id] = {
        permission_level_id: row.permission_level_id,
      }
    }
    userAccessById.value = nextAccess
    formSuccess.value = 'Team updated successfully.'
  } catch (errorValue) {
    formError.value = toUserFacingMessage(errorValue, 'Failed to save the team.')
  } finally {
    isSaving.value = false
  }
}

async function removeTeam() {
  if (isCreateMode.value || !groupId.value) {
    return
  }

  if (!canDeleteTeams.value) {
    formError.value = 'You do not have permission to delete this team.'
    return
  }

  if (!window.confirm(`Delete the "${groupName.value}" team from Datamingle?`)) {
    return
  }

  isDeleting.value = true
  formError.value = ''
  formSuccess.value = ''

  try {
    await deleteTeam(groupId.value, requireToken())
    await router.push('/settings/teams')
  } catch (errorValue) {
    formError.value = toUserFacingMessage(errorValue, 'Failed to delete the team.')
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
        <RouterLink to="/settings/teams">
          <ArrowLeft class="h-4 w-4" />
          Back to teams
        </RouterLink>
      </Button>
    </div>

    <Card class="border-slate-200" data-testid="team-detail">
      <CardHeader>
        <CardTitle>{{ isCreateMode ? 'Create Team' : 'Edit Team' }}</CardTitle>
        <CardDescription>Manage scoped resource access and server membership.</CardDescription>
      </CardHeader>
      <CardContent class="space-y-6">
        <div class="space-y-2">
          <label for="team-name" class="text-sm font-medium text-slate-900">Name</label>
          <Input
            id="team-name"
            v-model="groupName"
            data-testid="team-name"
            :disabled="!canSave || isLoading"
            placeholder="e.g. production"
          />
        </div>

        <p
          v-if="pageError"
          data-testid="team-page-error"
          class="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700"
        >
          {{ pageError }}
        </p>
        <p
          v-else-if="pageNotice"
          data-testid="team-page-notice"
          class="rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800"
        >
          {{ pageNotice }}
        </p>
        <p
          v-else-if="formError"
          data-testid="team-form-error"
          class="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700"
        >
          {{ formError }}
        </p>
        <p
          v-else-if="formSuccess"
          data-testid="team-feedback"
          class="rounded-lg border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700"
        >
          {{ formSuccess }}
        </p>

        <div class="space-y-4 rounded-lg border border-slate-200 p-5">
          <div class="flex flex-wrap items-center justify-between gap-3">
            <div>
              <h3 class="text-base font-semibold text-slate-900">Members</h3>
              <p class="mt-1 text-sm text-slate-500">
                Each member has one permission level in this team.
              </p>
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

          <div class="flex flex-col gap-2 sm:flex-row">
            <select
              v-model="availableUserId"
              data-testid="team-member-add-select"
              class="h-10 flex-1 rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-900"
              :disabled="!canSave || availableUsers.length === 0"
            >
              <option value="">Select a user to add</option>
              <option v-for="user in availableUsers" :key="user.id" :value="`${user.id}`">
                {{ userLabel(user) }} ({{ user.username }})
              </option>
            </select>
            <Button
              type="button"
              variant="outline"
              data-testid="team-member-add"
              :disabled="!canSave || !availableUserId"
              @click="addMember"
            >
              Add member
            </Button>
          </div>

          <Input
            v-model="userFilter"
            data-testid="team-member-filter"
            :disabled="isLoading"
            placeholder="Filter users"
            aria-label="Filter users"
          />

          <div class="overflow-x-auto rounded-md border border-slate-200">
            <table class="min-w-full divide-y divide-slate-200 text-sm">
              <thead class="bg-slate-50 text-left text-xs font-medium uppercase text-slate-500">
                <tr>
                  <th class="px-4 py-3">User</th>
                  <th class="px-4 py-3">Permission level</th>
                  <th class="px-4 py-3 text-right">Actions</th>
                </tr>
              </thead>
              <tbody class="divide-y divide-slate-200 bg-white">
                <tr v-for="user in assignedUsers" :key="user.id" data-testid="team-member-row">
                  <td class="px-4 py-3">
                    <div class="font-medium text-slate-900">{{ userLabel(user) }}</div>
                    <div class="mt-1 text-xs text-slate-500">{{ user.username }}</div>
                  </td>
                  <td class="px-4 py-3">
                    <select
                      data-testid="team-member-role"
                      class="w-full min-w-52 rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm focus:outline-none focus:ring-1 focus:ring-ring disabled:cursor-not-allowed disabled:opacity-50"
                      :value="roleForUser(user.id)"
                      :disabled="!canSave"
                      @change="updateUserRole(user.id, $event)"
                    >
                      <option value="">No access</option>
                      <option v-for="role in accessRoles" :key="role.id" :value="role.id">
                        {{ role.name }}
                      </option>
                    </select>
                  </td>
                  <td class="px-4 py-3 text-right">
                    <Button
                      type="button"
                      variant="ghost"
                      size="sm"
                      data-testid="team-member-remove"
                      :disabled="!canSave"
                      @click="removeMember(user.id)"
                    >
                      Remove
                    </Button>
                  </td>
                </tr>
                <tr v-if="assignedUsers.length === 0">
                  <td colspan="3" class="px-4 py-8 text-center text-sm text-slate-500">
                    No assigned members match the current filter.
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>

        <div class="space-y-4 rounded-lg border border-slate-200 p-5">
          <div>
            <h3 class="text-base font-semibold text-slate-900">Nodes</h3>
            <p class="mt-1 text-sm text-slate-500">
              Select every infrastructure node that belongs to this team.
            </p>
          </div>
          <select
            data-testid="team-node-select"
            class="min-h-48 w-full rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm focus:outline-none focus:ring-1 focus:ring-ring"
            multiple
            :disabled="!canSave"
            @change="updateNodeSelection"
          >
            <option
              v-for="node in nodes"
              :key="node.id"
              :value="node.id"
              :selected="selectedNodeIds.includes(node.id)"
            >
              {{ nodeLabel(node) }}
            </option>
          </select>
        </div>

        <div class="space-y-4 rounded-lg border border-slate-200 p-5">
          <div class="flex flex-wrap items-center justify-between gap-3">
            <div>
              <h3 class="text-base font-semibold text-slate-900">Services</h3>
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
                data-testid="team-service-available-filter"
                :disabled="isLoading"
                placeholder="Filter available servers"
              />
              <select
                data-testid="team-service-available-select"
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
                data-testid="team-service-add-selected"
                :disabled="!canSave || availableInstanceSelection.length === 0"
                @click="moveSelectedInstancesToAssigned"
              >
                <ChevronRight class="h-4 w-4" />
              </Button>
              <Button
                variant="outline"
                size="icon"
                data-testid="team-service-add-all"
                :disabled="!canSave || availableInstances.length === 0"
                @click="moveAllInstancesToAssigned"
              >
                <ChevronsRight class="h-4 w-4" />
              </Button>
              <Button
                variant="outline"
                size="icon"
                data-testid="team-service-remove-selected"
                :disabled="!canSave || selectedInstanceSelection.length === 0"
                @click="moveSelectedInstancesToAvailable"
              >
                <ChevronLeft class="h-4 w-4" />
              </Button>
              <Button
                variant="outline"
                size="icon"
                data-testid="team-service-remove-all"
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
                data-testid="team-service-assigned-filter"
                :disabled="isLoading"
                placeholder="Filter assigned servers"
              />
              <select
                data-testid="team-service-assigned-select"
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
          data-testid="team-delete"
          variant="destructive"
          :disabled="isDeleting || !canDeleteTeams"
          @click="removeTeam"
        >
          <Trash2 class="h-4 w-4" />
          Delete
        </Button>
        <span v-else />
        <div class="flex flex-wrap justify-end gap-3">
          <Button as-child variant="outline">
            <RouterLink to="/settings/teams">Cancel</RouterLink>
          </Button>
          <Button data-testid="team-save" :disabled="isLoading || isSaving || !canSave" @click="saveTeam">
            <Save class="h-4 w-4" />
            Save
          </Button>
        </div>
      </CardFooter>
    </Card>
  </section>
</template>
