<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { Fingerprint, KeyRound, ShieldCheck, UserRound } from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { changeCurrentUserPassword, updateCurrentUserDisplay } from '@/lib/api'
import { useAuthStore } from '@/stores/auth'

const authStore = useAuthStore()

const isLoading = ref(false)
const isSavingProfile = ref(false)
const isChangingPassword = ref(false)

const pageError = ref('')
const profileError = ref('')
const profileSuccess = ref('')
const passwordError = ref('')
const passwordSuccess = ref('')

const profileForm = reactive({
  display: '',
})

const passwordForm = reactive({
  currentPassword: '',
  newPassword: '',
  newPasswordConfirm: '',
})

const currentUser = computed(() => authStore.currentUser)

const currentUserInitials = computed(() => {
  const source = currentUser.value?.display || currentUser.value?.username || 'U'
  const initials = source
    .split(/\s+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((segment) => segment[0]?.toUpperCase() ?? '')
    .join('')

  return initials || 'U'
})

const accountFacts = computed(() => {
  if (!currentUser.value) {
    return []
  }

  return [
    { label: 'Username', value: currentUser.value.username },
    { label: 'Email', value: currentUser.value.email || '-' },
    {
      label: 'Groups',
      value:
        currentUser.value.groups.length > 0
          ? currentUser.value.groups.map((group) => group.name).join(', ')
          : '-',
    },
    {
      label: 'Resource Groups',
      value:
        currentUser.value.resource_groups.length > 0
          ? currentUser.value.resource_groups.map((group) => group.group_name).join(', ')
          : '-',
    },
  ]
})

const twoFactorLabels = computed(() => {
  if (!currentUser.value || currentUser.value.two_factor_auth_types.length === 0) {
    return ['Not enabled']
  }

  return currentUser.value.two_factor_auth_types.map((value) => value.toUpperCase())
})

function toUserFacingMessage(error: unknown, fallback: string) {
  if (!(error instanceof Error)) {
    return fallback
  }

  const separator = '): '
  const separatorIndex = error.message.indexOf(separator)
  if (separatorIndex === -1) {
    return error.message
  }

  return error.message.slice(separatorIndex + separator.length)
}

function syncProfileForm() {
  profileForm.display = currentUser.value?.display || ''
}

function resetPasswordForm() {
  passwordForm.currentPassword = ''
  passwordForm.newPassword = ''
  passwordForm.newPasswordConfirm = ''
}

async function loadProfile(force = false) {
  if (!authStore.accessToken) {
    pageError.value = 'Missing access token. Please login again.'
    return
  }

  isLoading.value = true
  pageError.value = ''

  try {
    await authStore.loadCurrentUser(force)
    syncProfileForm()
  } catch (error) {
    pageError.value = toUserFacingMessage(error, 'Failed to load your profile.')
  } finally {
    isLoading.value = false
  }
}

async function saveDisplayName() {
  if (!authStore.accessToken) {
    profileError.value = 'Missing access token. Please login again.'
    return
  }

  const display = profileForm.display.trim()
  profileError.value = ''
  profileSuccess.value = ''

  if (!display) {
    profileError.value = 'Display name cannot be blank.'
    return
  }

  isSavingProfile.value = true

  try {
    const user = await updateCurrentUserDisplay(display, authStore.accessToken)
    authStore.setCurrentUser(user)
    syncProfileForm()
    profileSuccess.value = 'Display name updated.'
  } catch (error) {
    profileError.value = toUserFacingMessage(error, 'Failed to update display name.')
  } finally {
    isSavingProfile.value = false
  }
}

async function updatePassword() {
  if (!authStore.accessToken) {
    passwordError.value = 'Missing access token. Please login again.'
    return
  }

  passwordError.value = ''
  passwordSuccess.value = ''

  if (!passwordForm.currentPassword || !passwordForm.newPassword || !passwordForm.newPasswordConfirm) {
    passwordError.value = 'All password fields are required.'
    return
  }

  isChangingPassword.value = true

  try {
    const detail = await changeCurrentUserPassword(
      passwordForm.currentPassword,
      passwordForm.newPassword,
      passwordForm.newPasswordConfirm,
      authStore.accessToken,
    )
    resetPasswordForm()
    passwordSuccess.value = detail
  } catch (error) {
    passwordError.value = toUserFacingMessage(error, 'Failed to update password.')
  } finally {
    isChangingPassword.value = false
  }
}

onMounted(() => {
  void loadProfile()
})
</script>

<template>
  <section class="grid gap-6">
    <Card class="overflow-hidden border-slate-200">
      <CardContent class="p-0">
        <div class="grid gap-0 lg:grid-cols-[minmax(0,1.3fr)_minmax(0,0.9fr)]">
          <div class="bg-[radial-gradient(circle_at_top_left,_rgba(59,130,246,0.24),_transparent_48%),linear-gradient(135deg,#f8fafc_0%,#eef4ff_42%,#fef3c7_100%)] p-6 lg:p-8">
            <Badge variant="outline" class="border-slate-300 bg-white/70 text-slate-700">Profile Center</Badge>
            <div class="mt-5 flex flex-col gap-5 sm:flex-row sm:items-center">
              <div class="flex h-20 w-20 items-center justify-center rounded-3xl bg-slate-900 text-xl font-semibold text-white shadow-lg">
                {{ currentUserInitials }}
              </div>
              <div>
                <h2 class="text-2xl font-semibold text-slate-900">
                  {{ currentUser?.display || currentUser?.username || 'User profile' }}
                </h2>
                <p class="mt-1 text-sm text-slate-600">
                  Manage the account details shown across Datamingle and rotate your password without leaving the SPA.
                </p>
              </div>
            </div>

            <div class="mt-6 flex flex-wrap gap-2">
              <Badge variant="secondary" class="bg-white/80 text-slate-800">
                <UserRound class="h-3.5 w-3.5" />
                {{ currentUser?.username || 'Account' }}
              </Badge>
              <Badge variant="secondary" class="bg-white/80 text-slate-800">
                <ShieldCheck class="h-3.5 w-3.5" />
                {{ currentUser?.is_superuser ? 'Admin access' : 'Standard access' }}
              </Badge>
              <Badge
                v-for="label in twoFactorLabels"
                :key="label"
                variant="secondary"
                class="bg-white/80 text-slate-800"
              >
                <Fingerprint class="h-3.5 w-3.5" />
                2FA {{ label }}
              </Badge>
            </div>
          </div>

          <div class="border-t border-slate-200 bg-white p-6 lg:border-l lg:border-t-0 lg:p-8">
            <div class="grid gap-4 sm:grid-cols-2 lg:grid-cols-1">
              <div v-for="fact in accountFacts" :key="fact.label" class="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                <p class="text-xs font-medium uppercase tracking-[0.2em] text-slate-500">{{ fact.label }}</p>
                <p class="mt-2 break-words text-sm font-medium text-slate-900">{{ fact.value }}</p>
              </div>
            </div>
          </div>
        </div>
      </CardContent>
    </Card>

    <div v-if="pageError" class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
      {{ pageError }}
    </div>

    <div class="grid gap-6 xl:grid-cols-2">
      <Card class="border-slate-200">
        <CardHeader>
          <CardTitle>Display Name</CardTitle>
          <CardDescription>
            This is the human-readable name used across workflow and query history.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <form class="space-y-4" @submit.prevent="saveDisplayName">
            <div class="space-y-2">
              <label class="text-sm font-medium text-slate-700" for="display-name">Display name</label>
              <Input
                id="display-name"
                v-model="profileForm.display"
                placeholder="Enter a display name"
                autocomplete="name"
                :disabled="isLoading || isSavingProfile"
              />
            </div>

            <div v-if="profileError" class="rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">
              {{ profileError }}
            </div>
            <div
              v-else-if="profileSuccess"
              class="rounded-lg border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-700"
            >
              {{ profileSuccess }}
            </div>

            <Button type="submit" :disabled="isLoading || isSavingProfile">
              {{ isSavingProfile ? 'Saving...' : 'Save display name' }}
            </Button>
          </form>
        </CardContent>
      </Card>

      <Card class="border-slate-200">
        <CardHeader>
          <CardTitle>Password</CardTitle>
          <CardDescription>
            Confirm your current password, then choose a new one that passes backend validation.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <form class="space-y-4" @submit.prevent="updatePassword">
            <div class="space-y-2">
              <label class="text-sm font-medium text-slate-700" for="current-password">Current password</label>
              <Input
                id="current-password"
                v-model="passwordForm.currentPassword"
                type="password"
                autocomplete="current-password"
                :disabled="isChangingPassword"
              />
            </div>

            <div class="grid gap-4 md:grid-cols-2">
              <div class="space-y-2">
                <label class="text-sm font-medium text-slate-700" for="new-password">New password</label>
                <Input
                  id="new-password"
                  v-model="passwordForm.newPassword"
                  type="password"
                  autocomplete="new-password"
                  :disabled="isChangingPassword"
                />
              </div>

              <div class="space-y-2">
                <label class="text-sm font-medium text-slate-700" for="confirm-password">Confirm password</label>
                <Input
                  id="confirm-password"
                  v-model="passwordForm.newPasswordConfirm"
                  type="password"
                  autocomplete="new-password"
                  :disabled="isChangingPassword"
                />
              </div>
            </div>

            <div v-if="passwordError" class="rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">
              {{ passwordError }}
            </div>
            <div
              v-else-if="passwordSuccess"
              class="rounded-lg border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-700"
            >
              {{ passwordSuccess }}
            </div>

            <Button type="submit" :disabled="isChangingPassword" class="gap-2">
              <KeyRound class="h-4 w-4" />
              {{ isChangingPassword ? 'Updating...' : 'Change password' }}
            </Button>
          </form>
        </CardContent>
      </Card>
    </div>
  </section>
</template>
