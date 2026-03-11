<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { RouterLink, useRoute, useRouter } from 'vue-router'
import { ArrowLeft, Save } from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import {
  createInstanceTag,
  fetchInstanceTag,
  updateInstanceTag,
  type InstanceTagRecord,
} from '@/lib/api'
import { useAuthStore } from '@/stores/auth'

const authStore = useAuthStore()
const route = useRoute()
const router = useRouter()

const loadedTag = ref<InstanceTagRecord | null>(null)
const tagCode = ref('')
const tagName = ref('')
const isActive = ref(true)
const isLoading = ref(false)
const isSaving = ref(false)
const pageError = ref('')
const formError = ref('')
const formSuccess = ref('')

const isCreateMode = computed(() => route.name === 'settings-instance-tags-new')
const tagId = computed(() => {
  if (isCreateMode.value) {
    return null
  }
  const value = Number(route.params.tagId)
  return Number.isFinite(value) ? value : null
})
const returnTo = computed(() => {
  return typeof route.query.returnTo === 'string' && route.query.returnTo
    ? route.query.returnTo
    : '/settings/instance-tags'
})

function hasPermission(permission: string) {
  if (authStore.currentUser?.is_superuser) {
    return true
  }
  return authStore.currentUser?.permissions.includes(permission) ?? false
}

const canManageInstanceTags = computed(() => hasPermission('sql.menu_instance'))

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

function applyTag(tag: InstanceTagRecord) {
  loadedTag.value = tag
  tagCode.value = tag.tag_code
  tagName.value = tag.tag_name
  isActive.value = tag.active
}

async function loadPage() {
  isLoading.value = true
  pageError.value = ''
  formError.value = ''
  formSuccess.value = ''
  loadedTag.value = null
  tagCode.value = ''
  tagName.value = ''
  isActive.value = true

  try {
    await authStore.loadCurrentUser()

    if (!canManageInstanceTags.value) {
      pageError.value = 'You do not have permission to access Datamingle instance tag management.'
      return
    }

    if (isCreateMode.value) {
      isActive.value = true
      return
    }

    if (!tagId.value) {
      pageError.value = 'Invalid instance tag identifier.'
      return
    }

    const tag = await fetchInstanceTag(tagId.value, requireToken())
    applyTag(tag)
  } catch (errorValue) {
    pageError.value = toUserFacingMessage(errorValue, 'Failed to load the instance tag editor.')
  } finally {
    isLoading.value = false
  }
}

async function saveTag() {
  if (!canManageInstanceTags.value) {
    formError.value = 'You do not have permission to save instance tags.'
    return
  }

  const trimmedCode = tagCode.value.trim()
  const trimmedName = tagName.value.trim()

  if (isCreateMode.value && !trimmedCode) {
    formError.value = 'Tag code cannot be blank.'
    return
  }

  if (!trimmedName) {
    formError.value = 'Tag name cannot be blank.'
    return
  }

  isSaving.value = true
  formError.value = ''
  formSuccess.value = ''

  try {
    if (isCreateMode.value) {
      const createdTag = await createInstanceTag(
        {
          tag_code: trimmedCode,
          tag_name: trimmedName,
          active: isActive.value,
        },
        requireToken(),
      )
      applyTag(createdTag)
      formSuccess.value = 'Instance tag created successfully.'

      if (typeof route.query.returnTo === 'string' && route.query.returnTo) {
        await router.push(route.query.returnTo)
        return
      }

      await router.replace(`/settings/instance-tags/${createdTag.id}`)
      return
    }

    if (!tagId.value) {
      throw new Error('Missing instance tag identifier.')
    }

    const updatedTag = await updateInstanceTag(
      tagId.value,
      {
        tag_name: trimmedName,
        active: isActive.value,
      },
      requireToken(),
    )
    applyTag(updatedTag)
    formSuccess.value = 'Instance tag updated successfully.'
  } catch (errorValue) {
    formError.value = toUserFacingMessage(errorValue, 'Failed to save the instance tag.')
  } finally {
    isSaving.value = false
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
        <RouterLink :to="returnTo">
          <ArrowLeft class="h-4 w-4" />
          Back
        </RouterLink>
      </Button>
      <Badge
        v-if="loadedTag"
        :variant="loadedTag.active ? 'secondary' : 'outline'"
        :class="loadedTag.active ? 'bg-emerald-100 text-emerald-800' : 'text-slate-600'"
      >
        {{ loadedTag.active ? 'Active' : 'Inactive' }}
      </Badge>
    </div>

    <Card class="border-slate-200">
      <CardHeader>
        <CardTitle>{{ isCreateMode ? 'Create Instance Tag' : 'Edit Instance Tag' }}</CardTitle>
        <CardDescription>
          Use stable tag codes and control whether the tag is available in inventory assignment and filters.
        </CardDescription>
      </CardHeader>
      <CardContent class="space-y-6">
        <div class="grid gap-4 md:grid-cols-2">
          <div class="space-y-2">
            <label for="instance-tag-code" class="text-sm font-medium text-slate-900">Tag code</label>
            <Input
              id="instance-tag-code"
              v-model="tagCode"
              :disabled="isLoading || !isCreateMode"
              placeholder="e.g. can_read"
            />
            <p class="text-xs text-slate-500">Tag code stays read-only after creation to keep access rules stable.</p>
          </div>

          <div class="space-y-2">
            <label for="instance-tag-name" class="text-sm font-medium text-slate-900">Tag name</label>
            <Input
              id="instance-tag-name"
              v-model="tagName"
              :disabled="isLoading"
              placeholder="e.g. Can Read"
            />
          </div>
        </div>

        <label class="flex items-center gap-3 rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-700">
          <input v-model="isActive" :disabled="isLoading" class="rounded border-slate-300" type="checkbox">
          <span>Active tags are available in inventory filters and instance assignment.</span>
        </label>

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
      </CardContent>
      <CardFooter class="justify-end border-t border-slate-200 pt-6">
        <Button :disabled="isLoading || isSaving || !canManageInstanceTags" @click="saveTag">
          <Save class="h-4 w-4" />
          {{ isSaving ? 'Saving…' : isCreateMode ? 'Create tag' : 'Save changes' }}
        </Button>
      </CardFooter>
    </Card>
  </section>
</template>
