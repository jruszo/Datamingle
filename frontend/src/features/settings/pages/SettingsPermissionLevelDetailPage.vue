<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { RouterLink, useRoute, useRouter } from 'vue-router'
import { ArrowLeft, Save, Trash2 } from 'lucide-vue-next'

import { Button } from '@/components/ui/button'
import { Card, CardContent, CardFooter, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import {
  createPermissionLevel,
  deletePermissionLevel,
  fetchAvailableTeamPermissions,
  fetchPermissionLevel,
  updatePermissionLevel,
  type PermissionCategoryRecord,
} from '../api'
import { useAuthStore } from '@/stores/auth'

const authStore = useAuthStore()
const route = useRoute()
const router = useRouter()

const name = ref('')
const selectedCodes = ref<string[]>([])
const categories = ref<PermissionCategoryRecord[]>([])
const membershipCount = ref(0)
const isLoading = ref(false)
const isSaving = ref(false)
const isDeleting = ref(false)
const error = ref('')
const feedback = ref('')

const isCreateMode = computed(() => route.name === 'settings-permission-levels-new')
const levelId = computed(() => {
  const value = Number(route.params.levelId)
  return Number.isFinite(value) ? value : null
})

function requireToken() {
  if (!authStore.accessToken) {
    throw new Error('Missing access token. Please login again.')
  }
  return authStore.accessToken
}

function togglePermission(code: string, checked: boolean) {
  selectedCodes.value = checked
    ? [...new Set([...selectedCodes.value, code])].sort()
    : selectedCodes.value.filter((value) => value !== code)
  feedback.value = ''
}

async function loadPage() {
  isLoading.value = true
  error.value = ''
  feedback.value = ''
  try {
    categories.value = await fetchAvailableTeamPermissions(requireToken())
    if (!isCreateMode.value && levelId.value) {
      const level = await fetchPermissionLevel(levelId.value, requireToken())
      name.value = level.name
      selectedCodes.value = [...level.permissions]
      membershipCount.value = level.membership_count
    }
  } catch (errorValue) {
    error.value = errorValue instanceof Error ? errorValue.message : 'Failed to load permission level.'
  } finally {
    isLoading.value = false
  }
}

async function saveLevel() {
  if (!name.value.trim()) {
    error.value = 'Permission level name is required.'
    return
  }
  isSaving.value = true
  error.value = ''
  try {
    const payload = { name: name.value.trim(), permission_codes: selectedCodes.value }
    const level = isCreateMode.value
      ? await createPermissionLevel(payload, requireToken())
      : await updatePermissionLevel(levelId.value!, payload, requireToken())
    name.value = level.name
    selectedCodes.value = [...level.permissions]
    membershipCount.value = level.membership_count
    feedback.value = 'Permission level saved.'
    if (isCreateMode.value) {
      await router.replace(`/settings/permission-levels/${level.id}`)
    }
  } catch (errorValue) {
    error.value = errorValue instanceof Error ? errorValue.message : 'Failed to save permission level.'
  } finally {
    isSaving.value = false
  }
}

async function removeLevel() {
  if (!levelId.value || !window.confirm(`Delete the "${name.value}" permission level?`)) {
    return
  }
  isDeleting.value = true
  error.value = ''
  try {
    await deletePermissionLevel(levelId.value, requireToken())
    await router.push('/settings/permission-levels')
  } catch (errorValue) {
    error.value = errorValue instanceof Error ? errorValue.message : 'Failed to delete permission level.'
  } finally {
    isDeleting.value = false
  }
}

onMounted(() => void loadPage())
watch(() => route.fullPath, () => void loadPage())
</script>

<template>
  <section class="grid gap-6">
    <Button as-child variant="ghost" class="w-fit">
      <RouterLink to="/settings/permission-levels">
        <ArrowLeft class="h-4 w-4" />
        Back to permission levels
      </RouterLink>
    </Button>

    <Card class="border-slate-200">
      <CardHeader>
        <CardTitle>{{ isCreateMode ? 'Create Permission Level' : 'Edit Permission Level' }}</CardTitle>
      </CardHeader>
      <CardContent class="space-y-6">
        <p v-if="error" class="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
          {{ error }}
        </p>
        <p v-else-if="feedback" class="rounded-lg border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700">
          {{ feedback }}
        </p>

        <div class="space-y-2">
          <label for="level-name" class="text-sm font-medium text-slate-900">Name</label>
          <Input id="level-name" v-model="name" :disabled="isLoading || isSaving" placeholder="e.g. Developer" />
          <p v-if="!isCreateMode" class="text-xs text-slate-500">{{ membershipCount }} active team memberships</p>
        </div>

        <div class="space-y-4">
          <div v-for="category in categories" :key="category.category" class="rounded-lg border border-slate-200 p-4">
            <h3 class="font-semibold text-slate-900">{{ category.category }}</h3>
            <div class="mt-3 grid gap-3 md:grid-cols-2">
              <label
                v-for="permission in category.permissions"
                :key="permission.code"
                class="flex gap-3 rounded-md border border-slate-200 p-3"
              >
                <input
                  type="checkbox"
                  :checked="selectedCodes.includes(permission.code)"
                  :disabled="isLoading || isSaving"
                  @change="togglePermission(permission.code, ($event.target as HTMLInputElement).checked)"
                />
                <span>
                  <span class="block text-sm font-medium text-slate-900">{{ permission.name }}</span>
                  <code class="mt-1 block text-xs text-slate-500">{{ permission.code }}</code>
                </span>
              </label>
            </div>
          </div>
        </div>
      </CardContent>
      <CardFooter class="justify-between border-t border-slate-200 pt-6">
        <Button
          v-if="!isCreateMode"
          variant="destructive"
          :disabled="isDeleting || membershipCount > 0"
          @click="removeLevel"
        >
          <Trash2 class="h-4 w-4" />
          Delete
        </Button>
        <span v-else />
        <Button :disabled="isLoading || isSaving" @click="saveLevel">
          <Save class="h-4 w-4" />
          Save
        </Button>
      </CardFooter>
    </Card>
  </section>
</template>
