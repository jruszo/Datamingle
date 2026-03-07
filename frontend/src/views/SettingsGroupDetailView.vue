<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { RouterLink, useRoute, useRouter } from 'vue-router'
import { ArrowLeft, ChevronLeft, ChevronRight, ChevronsLeft, ChevronsRight, Save, Trash2 } from 'lucide-vue-next'

import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import {
  createGroup,
  deleteGroup,
  fetchGroup,
  fetchPermissions,
  updateGroup,
  type PermissionRecord,
} from '@/lib/api'
import { useAuthStore } from '@/stores/auth'

const authStore = useAuthStore()
const route = useRoute()
const router = useRouter()

const permissions = ref<PermissionRecord[]>([])
const selectedPermissionIds = ref<number[]>([])
const groupName = ref('')
const availableFilter = ref('')
const selectedFilter = ref('')
const availableSelection = ref<number[]>([])
const selectedSelection = ref<number[]>([])
const isLoading = ref(false)
const isSaving = ref(false)
const isDeleting = ref(false)
const pageError = ref('')
const formError = ref('')
const formSuccess = ref('')

const isCreateMode = computed(() => route.name === 'settings-groups-new')
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
const canViewGroups = computed(() => canAccessSettings.value && hasPermission('auth.view_group'))
const canCreateGroups = computed(() => hasPermission('sql.menu_system') || hasPermission('auth.add_group'))
const canEditGroups = computed(() => hasPermission('sql.menu_system') || hasPermission('auth.change_group'))
const canDeleteGroups = computed(() => hasPermission('sql.menu_system') || hasPermission('auth.delete_group'))
const canSave = computed(() => (isCreateMode.value ? canCreateGroups.value : canEditGroups.value))
const selectedPermissionSet = computed(() => new Set(selectedPermissionIds.value))
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

function permissionLabel(permission: PermissionRecord) {
  return `${permission.app_label} | ${permission.model} | ${permission.name}`
}

function sortPermissions(values: PermissionRecord[]) {
  return [...values].sort((left, right) => {
    return permissionLabel(left).localeCompare(permissionLabel(right), undefined, {
      sensitivity: 'base',
      numeric: true,
    })
  })
}

function permissionMatches(permission: PermissionRecord, filterValue: string) {
  if (!filterValue) {
    return true
  }

  const haystack = [
    permission.name,
    permission.codename,
    permission.app_label,
    permission.model,
  ]
    .join(' ')
    .toLowerCase()

  return haystack.includes(filterValue)
}

const availablePermissions = computed(() =>
  sortPermissions(
    permissions.value
      .filter((permission) => !selectedPermissionSet.value.has(permission.id))
      .filter((permission) => permissionMatches(permission, normalizedAvailableFilter.value)),
  ),
)

const assignedPermissions = computed(() =>
  sortPermissions(
    permissions.value
      .filter((permission) => selectedPermissionSet.value.has(permission.id))
      .filter((permission) => permissionMatches(permission, normalizedSelectedFilter.value)),
  ),
)

function setSelectedPermissions(permissionIds: number[]) {
  selectedPermissionIds.value = [...new Set(permissionIds)].sort((left, right) => left - right)
  formSuccess.value = ''
}

function addPermissions(permissionIds: number[]) {
  if (permissionIds.length === 0) {
    return
  }
  setSelectedPermissions([...selectedPermissionIds.value, ...permissionIds])
}

function removePermissions(permissionIds: number[]) {
  if (permissionIds.length === 0) {
    return
  }
  setSelectedPermissions(selectedPermissionIds.value.filter((value) => !permissionIds.includes(value)))
}

function moveSelectedToAssigned() {
  addPermissions(availableSelection.value)
  availableSelection.value = []
}

function moveAllToAssigned() {
  addPermissions(availablePermissions.value.map((permission) => permission.id))
  availableSelection.value = []
}

function moveSelectedToAvailable() {
  removePermissions(selectedSelection.value)
  selectedSelection.value = []
}

function moveAllToAvailable() {
  removePermissions(assignedPermissions.value.map((permission) => permission.id))
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

async function loadPage() {
  isLoading.value = true
  pageError.value = ''
  formError.value = ''
  formSuccess.value = ''
  groupName.value = ''
  selectedPermissionIds.value = []
  availableSelection.value = []
  selectedSelection.value = []

  try {
    await authStore.loadCurrentUser()

    if (!canViewGroups.value && !canCreateGroups.value && !canEditGroups.value) {
      pageError.value = 'You do not have permission to access Datamingle group management.'
      return
    }

    const permissionCatalog = await fetchPermissions(requireToken())
    permissions.value = permissionCatalog

    if (isCreateMode.value) {
      groupName.value = ''
      selectedPermissionIds.value = []
      return
    }

    if (!groupId.value) {
      pageError.value = 'Invalid group identifier.'
      return
    }

    const group = await fetchGroup(groupId.value, requireToken())
    groupName.value = group.name
    selectedPermissionIds.value = [...group.permissions].sort((left, right) => left - right)
  } catch (errorValue) {
    pageError.value = toUserFacingMessage(errorValue, 'Failed to load the group editor.')
  } finally {
    isLoading.value = false
  }
}

async function saveGroup() {
  if (!canSave.value) {
    formError.value = 'You do not have permission to save this group.'
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
      name: trimmedName,
      permissions: [...selectedPermissionIds.value].sort((left, right) => left - right),
    }

    if (isCreateMode.value) {
      const createdGroup = await createGroup(payload, requireToken())
      formSuccess.value = 'Group created successfully.'
      await router.replace(`/settings/groups/${createdGroup.id}`)
      groupName.value = createdGroup.name
      selectedPermissionIds.value = [...createdGroup.permissions].sort((left, right) => left - right)
      return
    }

    if (!groupId.value) {
      throw new Error('Missing group identifier.')
    }

    const updatedGroup = await updateGroup(groupId.value, payload, requireToken())
    groupName.value = updatedGroup.name
    selectedPermissionIds.value = [...updatedGroup.permissions].sort((left, right) => left - right)
    formSuccess.value = 'Group updated successfully.'
  } catch (errorValue) {
    formError.value = toUserFacingMessage(errorValue, 'Failed to save the group.')
  } finally {
    isSaving.value = false
  }
}

async function removeGroup() {
  if (isCreateMode.value || !groupId.value) {
    return
  }

  if (!canDeleteGroups.value) {
    formError.value = 'You do not have permission to delete this group.'
    return
  }

  if (!window.confirm(`Delete the "${groupName.value}" group from Datamingle?`)) {
    return
  }

  isDeleting.value = true
  formError.value = ''
  formSuccess.value = ''

  try {
    await deleteGroup(groupId.value, requireToken())
    await router.push('/settings/groups')
  } catch (errorValue) {
    formError.value = toUserFacingMessage(errorValue, 'Failed to delete the group.')
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
        <RouterLink to="/settings/groups">
          <ArrowLeft class="h-4 w-4" />
          Back to groups
        </RouterLink>
      </Button>
    </div>

    <Card class="border-slate-200">
      <CardHeader>
        <CardTitle>{{ isCreateMode ? 'Create Group' : 'Edit Group' }}</CardTitle>
        <CardDescription>Assign permissions using the same two-list pattern as Django admin.</CardDescription>
      </CardHeader>
      <CardContent class="space-y-6">
        <div class="space-y-2">
          <label for="group-name" class="text-sm font-medium text-slate-900">Name</label>
          <Input
            id="group-name"
            v-model="groupName"
            :disabled="!canSave || isLoading"
            placeholder="e.g. DBA"
          />
        </div>

        <p v-if="pageError" class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
          {{ pageError }}
        </p>
        <p v-else-if="formError" class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
          {{ formError }}
        </p>
        <p v-else-if="formSuccess" class="rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700">
          {{ formSuccess }}
        </p>

        <div class="grid gap-4 lg:grid-cols-[minmax(0,1fr)_auto_minmax(0,1fr)]">
          <div class="space-y-3">
            <label for="available-filter" class="text-sm font-medium text-slate-900">Available permissions</label>
            <Input
              id="available-filter"
              v-model="availableFilter"
              :disabled="isLoading"
              placeholder="Filter available permissions"
            />
            <select
              class="min-h-[26rem] w-full rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm focus:outline-none focus:ring-1 focus:ring-ring"
              multiple
              @change="updateSelection($event, 'available')"
            >
              <option
                v-for="permission in availablePermissions"
                :key="permission.id"
                :value="permission.id"
              >
                {{ permissionLabel(permission) }}
              </option>
            </select>
          </div>

          <div class="flex flex-col items-center justify-center gap-2">
            <Button variant="outline" size="icon" :disabled="!canSave || availableSelection.length === 0" @click="moveSelectedToAssigned">
              <ChevronRight class="h-4 w-4" />
            </Button>
            <Button variant="outline" size="icon" :disabled="!canSave || availablePermissions.length === 0" @click="moveAllToAssigned">
              <ChevronsRight class="h-4 w-4" />
            </Button>
            <Button variant="outline" size="icon" :disabled="!canSave || selectedSelection.length === 0" @click="moveSelectedToAvailable">
              <ChevronLeft class="h-4 w-4" />
            </Button>
            <Button variant="outline" size="icon" :disabled="!canSave || assignedPermissions.length === 0" @click="moveAllToAvailable">
              <ChevronsLeft class="h-4 w-4" />
            </Button>
          </div>

          <div class="space-y-3">
            <label for="selected-filter" class="text-sm font-medium text-slate-900">Chosen permissions</label>
            <Input
              id="selected-filter"
              v-model="selectedFilter"
              :disabled="isLoading"
              placeholder="Filter chosen permissions"
            />
            <select
              class="min-h-[26rem] w-full rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm focus:outline-none focus:ring-1 focus:ring-ring"
              multiple
              @change="updateSelection($event, 'selected')"
            >
              <option
                v-for="permission in assignedPermissions"
                :key="permission.id"
                :value="permission.id"
              >
                {{ permissionLabel(permission) }}
              </option>
            </select>
          </div>
        </div>
      </CardContent>
      <CardFooter class="justify-between border-t border-slate-200 pt-6">
        <div>
          <Button
            v-if="!isCreateMode && canDeleteGroups"
            variant="destructive"
            :disabled="isDeleting"
            @click="removeGroup"
          >
            <Trash2 class="h-4 w-4" />
            Delete group
          </Button>
        </div>
        <Button :disabled="isLoading || isSaving || !canSave" @click="saveGroup">
          <Save class="h-4 w-4" />
          {{ isCreateMode ? 'Create group' : 'Save' }}
        </Button>
      </CardFooter>
    </Card>
  </section>
</template>
