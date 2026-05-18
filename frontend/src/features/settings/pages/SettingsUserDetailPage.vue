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
  deleteUser,
  fetchResourceGroups,
  fetchUser,
  updateUser,
  type ResourceGroupRecord,
  type UserManagementDetailRecord,
} from '../api'
import { useAuthStore } from '@/stores/auth'

const authStore = useAuthStore()
const route = useRoute()
const router = useRouter()

const resourceGroups = ref<ResourceGroupRecord[]>([])
const loadedUser = ref<UserManagementDetailRecord | null>(null)
const username = ref('')
const displayName = ref('')
const email = ref('')
const selectedResourceGroupIds = ref<number[]>([])
const availableFilter = ref('')
const selectedFilter = ref('')
const availableSelection = ref<number[]>([])
const selectedSelection = ref<number[]>([])
const isLoading = ref(false)
const isSaving = ref(false)
const isDeleting = ref(false)
const isTogglingStatus = ref(false)
const pageError = ref('')
const formError = ref('')
const formSuccess = ref('')

const userId = computed(() => {
  const value = Number(route.params.userId)
  return Number.isFinite(value) ? value : null
})
const canManageUsers = computed(() => authStore.currentUser?.is_superuser ?? false)
const isDirectoryManaged = computed(() => loadedUser.value?.is_directory_managed ?? false)
const selectedResourceGroupSet = computed(() => new Set(selectedResourceGroupIds.value))
const normalizedAvailableFilter = computed(() => availableFilter.value.trim().toLowerCase())
const normalizedSelectedFilter = computed(() => selectedFilter.value.trim().toLowerCase())

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

function applyUser(user: UserManagementDetailRecord) {
  loadedUser.value = user
  username.value = user.username
  displayName.value = user.display
  email.value = user.email
  selectedResourceGroupIds.value = [...user.resource_group_ids].sort((left, right) => left - right)
}

function sortGroups(values: ResourceGroupRecord[]) {
  return [...values].sort((left, right) =>
    left.group_name.localeCompare(right.group_name, undefined, {
      sensitivity: 'base',
      numeric: true,
    }),
  )
}

function groupMatches(group: ResourceGroupRecord, filterValue: string) {
  if (!filterValue) {
    return true
  }

  const haystack = `${group.group_name} ${group.group_id}`.toLowerCase()
  return haystack.includes(filterValue)
}

const availableGroups = computed(() =>
  sortGroups(
    resourceGroups.value
      .filter((group) => !selectedResourceGroupSet.value.has(group.group_id))
      .filter((group) => groupMatches(group, normalizedAvailableFilter.value)),
  ),
)

const assignedGroups = computed(() =>
  sortGroups(
    resourceGroups.value
      .filter((group) => selectedResourceGroupSet.value.has(group.group_id))
      .filter((group) => groupMatches(group, normalizedSelectedFilter.value)),
  ),
)

function setSelectedGroups(groupIds: number[]) {
  selectedResourceGroupIds.value = [...new Set(groupIds)].sort((left, right) => left - right)
  formSuccess.value = ''
}

function addGroups(groupIds: number[]) {
  if (isDirectoryManaged.value) {
    return
  }
  if (groupIds.length === 0) {
    return
  }
  setSelectedGroups([...selectedResourceGroupIds.value, ...groupIds])
}

function removeGroups(groupIds: number[]) {
  if (isDirectoryManaged.value) {
    return
  }
  if (groupIds.length === 0) {
    return
  }
  setSelectedGroups(selectedResourceGroupIds.value.filter((value) => !groupIds.includes(value)))
}

function moveSelectedToAssigned() {
  addGroups(availableSelection.value)
  availableSelection.value = []
}

function moveAllToAssigned() {
  addGroups(availableGroups.value.map((group) => group.group_id))
  availableSelection.value = []
}

function moveSelectedToAvailable() {
  removeGroups(selectedSelection.value)
  selectedSelection.value = []
}

function moveAllToAvailable() {
  removeGroups(assignedGroups.value.map((group) => group.group_id))
  selectedSelection.value = []
}

function updateSelection(event: Event, target: 'available' | 'selected') {
  const element = event.target as HTMLSelectElement
  const values = Array.from(element.selectedOptions)
    .map((option) => Number(option.value))
    .filter((value) => Number.isFinite(value))

  if (target === 'available') {
    availableSelection.value = values
    return
  }

  selectedSelection.value = values
}

async function loadAllGroups() {
  const collectedGroups: ResourceGroupRecord[] = []
  let page = 1
  let totalCount = 0

  while (page === 1 || collectedGroups.length < totalCount) {
    const response = await fetchResourceGroups(requireToken(), {
      page,
      size: 100,
      ordering: 'group_name',
    })
    collectedGroups.push(...response.results)
    totalCount = response.count

    if (!response.next || response.results.length === 0) {
      break
    }

    page += 1
  }

  resourceGroups.value = sortGroups(collectedGroups)
}

async function loadPage() {
  isLoading.value = true
  pageError.value = ''
  formError.value = ''
  formSuccess.value = ''
  loadedUser.value = null
  username.value = ''
  displayName.value = ''
  email.value = ''
  selectedResourceGroupIds.value = []
  availableSelection.value = []
  selectedSelection.value = []

  try {
    await authStore.loadCurrentUser()

    if (!canManageUsers.value) {
      pageError.value = 'Only superusers can access Datamingle user management.'
      return
    }

    await loadAllGroups()

    if (!userId.value) {
      pageError.value = 'Invalid user identifier.'
      return
    }

    const user = await fetchUser(userId.value, requireToken())
    applyUser(user)
  } catch (errorValue) {
    pageError.value = toUserFacingMessage(errorValue, 'Failed to load the user editor.')
  } finally {
    isLoading.value = false
  }
}

async function saveUser() {
  if (!canManageUsers.value) {
    formError.value = 'Only superusers can save Datamingle users.'
    return
  }

  isSaving.value = true
  formError.value = ''
  formSuccess.value = ''

  try {
    if (!userId.value) {
      throw new Error('Missing user identifier.')
    }
    if (isDirectoryManaged.value) {
      throw new Error(
        'Resource group membership for this user is managed by WorkOS Directory Sync.',
      )
    }

    const updatedUser = await updateUser(
      userId.value,
      {
        resource_group_ids: [...selectedResourceGroupIds.value].sort((left, right) => left - right),
        is_active: loadedUser.value?.is_active ?? true,
      },
      requireToken(),
    )
    applyUser(updatedUser)
    formSuccess.value = 'User updated successfully.'
  } catch (errorValue) {
    formError.value = toUserFacingMessage(errorValue, 'Failed to save the user.')
  } finally {
    isSaving.value = false
  }
}

async function toggleUserStatus() {
  if (!userId.value || !loadedUser.value) {
    return
  }

  const nextIsActive = !loadedUser.value.is_active
  const actionLabel = nextIsActive ? 'reactivate' : 'deactivate'

  if (
    !window.confirm(
      `${actionLabel[0]?.toUpperCase() ?? ''}${actionLabel.slice(1)} "${loadedUser.value.display || loadedUser.value.username}"?`,
    )
  ) {
    return
  }

  isTogglingStatus.value = true
  formError.value = ''
  formSuccess.value = ''

  try {
    const payload: { resource_group_ids?: number[]; is_active: boolean } = {
      is_active: nextIsActive,
    }
    if (!loadedUser.value.is_directory_managed) {
      payload.resource_group_ids = [...loadedUser.value.resource_group_ids].sort(
        (left, right) => left - right,
      )
    }

    const updatedUser = await updateUser(userId.value, payload, requireToken())
    applyUser(updatedUser)
    formSuccess.value = nextIsActive
      ? 'User reactivated successfully.'
      : 'User deactivated successfully.'
  } catch (errorValue) {
    formError.value = toUserFacingMessage(errorValue, `Failed to ${actionLabel} the user.`)
  } finally {
    isTogglingStatus.value = false
  }
}

async function removeUserAccount() {
  if (!userId.value || !loadedUser.value) {
    return
  }

  if (
    !window.confirm(
      `Delete "${loadedUser.value.display || loadedUser.value.username}" from Datamingle? This cannot be undone.`,
    )
  ) {
    return
  }

  isDeleting.value = true
  formError.value = ''
  formSuccess.value = ''

  try {
    await deleteUser(userId.value, requireToken())
    await router.push('/settings/users')
  } catch (errorValue) {
    formError.value = toUserFacingMessage(errorValue, 'Failed to delete the user.')
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
        <RouterLink to="/settings/users">
          <ArrowLeft class="h-4 w-4" />
          Back to users
        </RouterLink>
      </Button>
      <div v-if="loadedUser" class="flex flex-wrap gap-2">
        <Badge
          :variant="loadedUser.is_active ? 'secondary' : 'outline'"
          :class="loadedUser.is_active ? 'bg-emerald-100 text-emerald-800' : 'text-slate-600'"
        >
          {{ loadedUser.is_active ? 'Active' : 'Inactive' }}
        </Badge>
        <Badge
          v-if="loadedUser.is_superuser"
          variant="secondary"
          class="bg-amber-100 text-amber-800"
        >
          Superuser
        </Badge>
        <Badge v-if="loadedUser.is_staff" variant="secondary" class="bg-sky-100 text-sky-800">
          Staff
        </Badge>
        <Badge
          v-if="loadedUser.is_directory_managed"
          variant="secondary"
          class="bg-violet-100 text-violet-800"
        >
          WorkOS Directory
        </Badge>
      </div>
    </div>

    <Card class="border-slate-200">
      <CardHeader>
        <CardTitle>Edit User</CardTitle>
        <CardDescription>
          Review resource groups and manage account lifecycle from the Datamingle SPA.
        </CardDescription>
      </CardHeader>
      <CardContent class="space-y-6">
        <div
          class="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800"
        >
          <span v-if="isDirectoryManaged">
            Resource group membership is managed by WorkOS Directory Sync. Active status remains
            editable in Datamingle.
          </span>
          <span v-else>
            This user uses Datamingle-managed fallback resource groups until WorkOS Directory Sync
            manages their membership.
          </span>
        </div>

        <div class="grid gap-4 md:grid-cols-2">
          <div class="space-y-2">
            <label for="user-username" class="text-sm font-medium text-slate-900">Username</label>
            <Input id="user-username" v-model="username" :disabled="true" placeholder="e.g. jdoe" />
          </div>

          <div class="space-y-2">
            <label for="user-display" class="text-sm font-medium text-slate-900"
              >Display name</label
            >
            <Input
              id="user-display"
              v-model="displayName"
              :disabled="true"
              placeholder="e.g. Jane Doe"
            />
          </div>

          <div class="space-y-2">
            <label for="user-email" class="text-sm font-medium text-slate-900">Email</label>
            <Input
              id="user-email"
              v-model="email"
              :disabled="true"
              placeholder="jane.doe@example.com"
            />
          </div>
        </div>

        <p
          v-if="pageError"
          class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700"
        >
          {{ pageError }}
        </p>
        <p
          v-else-if="formError"
          class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700"
        >
          {{ formError }}
        </p>
        <p
          v-else-if="formSuccess"
          class="rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700"
        >
          {{ formSuccess }}
        </p>

        <div class="grid gap-4 lg:grid-cols-[minmax(0,1fr)_auto_minmax(0,1fr)]">
          <div class="space-y-3">
            <label for="available-groups-filter" class="text-sm font-medium text-slate-900"
              >Available resource groups</label
            >
            <Input
              id="available-groups-filter"
              v-model="availableFilter"
              :disabled="isLoading || isDirectoryManaged"
              placeholder="Filter available resource groups"
            />
            <select
              class="min-h-[22rem] w-full rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm focus:outline-none focus:ring-1 focus:ring-ring"
              multiple
              :disabled="isLoading || isDirectoryManaged"
              @change="updateSelection($event, 'available')"
            >
              <option
                v-for="group in availableGroups"
                :key="group.group_id"
                :value="group.group_id"
              >
                {{ group.group_name }}
              </option>
            </select>
          </div>

          <div class="flex flex-col items-center justify-center gap-2">
            <Button
              variant="outline"
              size="icon"
              :disabled="isLoading || isDirectoryManaged || availableSelection.length === 0"
              @click="moveSelectedToAssigned"
            >
              <ChevronRight class="h-4 w-4" />
            </Button>
            <Button
              variant="outline"
              size="icon"
              :disabled="isLoading || isDirectoryManaged || availableGroups.length === 0"
              @click="moveAllToAssigned"
            >
              <ChevronsRight class="h-4 w-4" />
            </Button>
            <Button
              variant="outline"
              size="icon"
              :disabled="isLoading || isDirectoryManaged || selectedSelection.length === 0"
              @click="moveSelectedToAvailable"
            >
              <ChevronLeft class="h-4 w-4" />
            </Button>
            <Button
              variant="outline"
              size="icon"
              :disabled="isLoading || isDirectoryManaged || assignedGroups.length === 0"
              @click="moveAllToAvailable"
            >
              <ChevronsLeft class="h-4 w-4" />
            </Button>
          </div>

          <div class="space-y-3">
            <label for="selected-groups-filter" class="text-sm font-medium text-slate-900"
              >Assigned resource groups</label
            >
            <Input
              id="selected-groups-filter"
              v-model="selectedFilter"
              :disabled="isLoading || isDirectoryManaged"
              placeholder="Filter assigned resource groups"
            />
            <select
              class="min-h-[22rem] w-full rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm focus:outline-none focus:ring-1 focus:ring-ring"
              multiple
              :disabled="isLoading || isDirectoryManaged"
              @change="updateSelection($event, 'selected')"
            >
              <option v-for="group in assignedGroups" :key="group.group_id" :value="group.group_id">
                {{ group.group_name }}
              </option>
            </select>
          </div>
        </div>
      </CardContent>
      <CardFooter class="justify-between border-t border-slate-200 pt-6">
        <div class="flex flex-wrap gap-2">
          <Button
            variant="outline"
            :disabled="isTogglingStatus || !loadedUser"
            @click="toggleUserStatus"
          >
            {{ loadedUser?.is_active ? 'Deactivate user' : 'Reactivate user' }}
          </Button>
          <Button variant="destructive" :disabled="isDeleting" @click="removeUserAccount">
            <Trash2 class="h-4 w-4" />
            Delete user
          </Button>
        </div>
        <Button
          :disabled="isLoading || isSaving || !canManageUsers || isDirectoryManaged"
          @click="saveUser"
        >
          <Save class="h-4 w-4" />
          Save
        </Button>
      </CardFooter>
    </Card>
  </section>
</template>
