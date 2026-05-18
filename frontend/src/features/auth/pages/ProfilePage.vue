<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { Clock3, LogOut, RefreshCw, Save, ShieldCheck, UserRound } from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { useAuthStore } from '@/stores/auth'
import {
  fetchWorkosProfile,
  fetchWorkosSessions,
  revokeWorkosSession,
  updateWorkosProfile,
  type WorkosProfile,
  type WorkosSessionRecord,
} from '../api'

const authStore = useAuthStore()

const isLoading = ref(false)
const pageError = ref('')
const workosError = ref('')
const profileFeedback = ref('')
const sessionFeedback = ref('')
const isWorkosProfileLoading = ref(false)
const isWorkosSessionLoading = ref(false)
const isSavingProfile = ref(false)
const revokingSessionId = ref('')
const workosProfile = ref<WorkosProfile | null>(null)
const workosSessions = ref<WorkosSessionRecord[]>([])
const profileForm = ref({
  first_name: '',
  last_name: '',
})

const currentUser = computed(() => authStore.currentUser)
const isWorkosManagedUser = computed(() => currentUser.value?.is_workos_managed ?? false)
const currentUserAvatarUrl = computed(() => {
  return workosProfile.value?.profile_picture_url || currentUser.value?.avatar_url?.trim() || ''
})

const currentUserInitials = computed(() => {
  const source = workosProfile.value?.display_name || currentUser.value?.display || currentUser.value?.username || 'U'
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
    { label: 'Email', value: workosProfile.value?.email || currentUser.value.email || '-' },
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

function requireToken() {
  if (!authStore.accessToken) {
    throw new Error('Missing access token. Please login again.')
  }
  return authStore.accessToken
}

function applyWorkosProfile(profile: WorkosProfile) {
  workosProfile.value = profile
  profileForm.value = {
    first_name: profile.first_name,
    last_name: profile.last_name,
  }
}

function formatDate(value: string) {
  if (!value) {
    return '-'
  }

  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return value
  }

  return new Intl.DateTimeFormat(undefined, {
    dateStyle: 'medium',
    timeStyle: 'short',
  }).format(date)
}

function authMethodLabel(value: string) {
  const labels: Record<string, string> = {
    sso: 'SSO',
    password: 'Email and password',
    magic_code: 'Magic code',
    oauth: 'OAuth',
    passkey: 'Passkey',
    external_auth: 'External auth',
    cross_app_auth: 'Cross-app auth',
    impersonation: 'Impersonation',
    migrated_session: 'Migrated session',
    unknown: 'Unknown',
  }

  return labels[value] || value || 'Unknown'
}

async function loadWorkosProfile() {
  isWorkosProfileLoading.value = true
  workosError.value = ''

  try {
    applyWorkosProfile(await fetchWorkosProfile(requireToken()))
  } catch (error) {
    workosError.value = toUserFacingMessage(error, 'Failed to load your WorkOS profile.')
  } finally {
    isWorkosProfileLoading.value = false
  }
}

async function loadWorkosSessions() {
  isWorkosSessionLoading.value = true
  workosError.value = ''

  try {
    workosSessions.value = await fetchWorkosSessions(requireToken())
  } catch (error) {
    workosError.value = toUserFacingMessage(error, 'Failed to load your WorkOS sessions.')
  } finally {
    isWorkosSessionLoading.value = false
  }
}

async function loadWorkosData() {
  if (!isWorkosManagedUser.value) {
    workosProfile.value = null
    workosSessions.value = []
    return
  }

  await Promise.all([loadWorkosProfile(), loadWorkosSessions()])
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
    await loadWorkosData()
  } catch (error) {
    pageError.value = toUserFacingMessage(error, 'Failed to load your profile.')
  } finally {
    isLoading.value = false
  }
}

async function saveWorkosProfile() {
  if (!isWorkosManagedUser.value) {
    return
  }

  profileFeedback.value = ''
  workosError.value = ''
  isSavingProfile.value = true

  try {
    const profile = await updateWorkosProfile(
      {
        first_name: profileForm.value.first_name.trim(),
        last_name: profileForm.value.last_name.trim(),
      },
      requireToken(),
    )
    applyWorkosProfile(profile)
    await authStore.loadCurrentUser(true)
    profileFeedback.value = 'Profile updated in WorkOS.'
  } catch (error) {
    workosError.value = toUserFacingMessage(error, 'Failed to update your WorkOS profile.')
  } finally {
    isSavingProfile.value = false
  }
}

async function revokeSession(session: WorkosSessionRecord) {
  if (session.is_current) {
    sessionFeedback.value = 'Use logout to end the current browser session.'
    return
  }

  if (!window.confirm('End this WorkOS session? The device will need to sign in again.')) {
    return
  }

  revokingSessionId.value = session.id
  workosError.value = ''
  sessionFeedback.value = ''

  try {
    sessionFeedback.value = await revokeWorkosSession(session.id, requireToken())
    await loadWorkosSessions()
  } catch (error) {
    workosError.value = toUserFacingMessage(error, 'Failed to revoke the WorkOS session.')
  } finally {
    revokingSessionId.value = ''
  }
}

onMounted(() => {
  void loadProfile()
})
</script>

<template>
  <section class="grid gap-6">
    <div v-if="isLoading" class="flex items-center justify-center py-20 text-sm text-slate-500">
      Loading profile...
    </div>

    <template v-else>
      <Card class="overflow-hidden border-slate-200">
        <CardContent class="p-0">
          <div class="grid gap-0 lg:grid-cols-[minmax(0,1.3fr)_minmax(0,0.9fr)]">
            <div class="bg-[radial-gradient(circle_at_top_left,_rgba(59,130,246,0.24),_transparent_48%),linear-gradient(135deg,#f8fafc_0%,#eef4ff_42%,#fef3c7_100%)] p-6 lg:p-8">
              <Badge variant="outline" class="border-slate-300 bg-white/70 text-slate-700">Profile Center</Badge>
              <div class="mt-5 flex flex-col gap-5 sm:flex-row sm:items-center">
                <div class="flex h-20 w-20 items-center justify-center overflow-hidden rounded-3xl bg-slate-900 text-xl font-semibold text-white shadow-lg">
                  <img
                    v-if="currentUserAvatarUrl"
                    :src="currentUserAvatarUrl"
                    :alt="`${workosProfile?.display_name || currentUser?.display || currentUser?.username || 'User'} avatar`"
                    class="h-full w-full object-cover"
                    referrerpolicy="no-referrer"
                  />
                  <span v-else>{{ currentUserInitials }}</span>
                </div>
                <div>
                  <h2 class="text-2xl font-semibold text-slate-900">
                    {{ workosProfile?.display_name || currentUser?.display || currentUser?.username || 'User profile' }}
                  </h2>
                  <p class="mt-1 text-sm text-slate-600">
                    {{
                      isWorkosManagedUser
                        ? 'This account is linked to WorkOS. Profile details and sessions are managed through WorkOS-backed controls below.'
                        : 'This local record is used for Datamingle permissions and will be linked when the matching WorkOS user signs in.'
                    }}
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
      <div v-if="workosError" class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
        {{ workosError }}
      </div>

      <div v-if="isWorkosManagedUser" class="grid gap-6 xl:grid-cols-[minmax(0,0.8fr)_minmax(0,1.2fr)]">
        <Card class="border-slate-200">
          <CardHeader>
            <CardTitle>WorkOS Profile</CardTitle>
            <CardDescription>
              Update the personal name WorkOS returns to Datamingle.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <form class="grid gap-4" @submit.prevent="saveWorkosProfile">
              <div class="grid gap-2">
                <label for="workos-first-name" class="text-sm font-medium text-slate-900">First name</label>
                <Input
                  id="workos-first-name"
                  v-model="profileForm.first_name"
                  :disabled="isWorkosProfileLoading || isSavingProfile"
                />
              </div>
              <div class="grid gap-2">
                <label for="workos-last-name" class="text-sm font-medium text-slate-900">Last name</label>
                <Input
                  id="workos-last-name"
                  v-model="profileForm.last_name"
                  :disabled="isWorkosProfileLoading || isSavingProfile"
                />
              </div>
              <div class="flex flex-wrap items-center gap-3">
                <Button type="submit" class="gap-2" :disabled="isWorkosProfileLoading || isSavingProfile">
                  <Save class="h-4 w-4" />
                  {{ isSavingProfile ? 'Saving...' : 'Save profile' }}
                </Button>
                <Button type="button" variant="outline" class="gap-2" :disabled="isWorkosProfileLoading" @click="loadWorkosProfile">
                  <RefreshCw class="h-4 w-4" />
                  Refresh
                </Button>
              </div>
              <p v-if="profileFeedback" class="rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700">
                {{ profileFeedback }}
              </p>
            </form>
          </CardContent>
        </Card>

        <Card class="border-slate-200">
          <CardHeader>
            <div class="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
              <div>
                <CardTitle>WorkOS Sessions</CardTitle>
                <CardDescription>
                  Review active WorkOS sessions and end sessions on other devices.
                </CardDescription>
              </div>
              <Button type="button" variant="outline" class="gap-2" :disabled="isWorkosSessionLoading" @click="loadWorkosSessions">
                <RefreshCw class="h-4 w-4" />
                Refresh
              </Button>
            </div>
          </CardHeader>
          <CardContent class="space-y-4">
            <p v-if="sessionFeedback" class="rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700">
              {{ sessionFeedback }}
            </p>
            <div v-if="isWorkosSessionLoading" class="py-8 text-center text-sm text-slate-500">
              Loading WorkOS sessions...
            </div>
            <div v-else-if="workosSessions.length === 0" class="rounded-xl border border-slate-200 bg-slate-50 px-4 py-6 text-sm text-slate-600">
              No active WorkOS sessions were returned.
            </div>
            <div v-else class="overflow-hidden rounded-xl border border-slate-200">
              <div
                v-for="session in workosSessions"
                :key="session.id"
                class="grid gap-4 border-b border-slate-200 p-4 last:border-b-0 lg:grid-cols-[minmax(0,1fr)_auto]"
              >
                <div class="min-w-0 space-y-2">
                  <div class="flex flex-wrap items-center gap-2">
                    <Badge
                      :variant="session.status === 'active' ? 'secondary' : 'outline'"
                      :class="session.status === 'active' ? 'bg-emerald-100 text-emerald-800' : 'text-slate-600'"
                    >
                      {{ session.status || 'unknown' }}
                    </Badge>
                    <Badge v-if="session.is_current" variant="secondary" class="bg-sky-100 text-sky-800">
                      Current browser
                    </Badge>
                    <Badge variant="outline" class="text-slate-600">
                      {{ authMethodLabel(session.auth_method) }}
                    </Badge>
                  </div>
                  <p class="break-words text-sm font-medium text-slate-900">
                    {{ session.user_agent || 'Unknown device' }}
                  </p>
                  <div class="flex flex-wrap gap-x-5 gap-y-1 text-xs text-slate-500">
                    <span>{{ session.ip_address || 'No IP address' }}</span>
                    <span class="inline-flex items-center gap-1">
                      <Clock3 class="h-3.5 w-3.5" />
                      Expires {{ formatDate(session.expires_at) }}
                    </span>
                  </div>
                </div>
                <div class="flex items-center lg:justify-end">
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    class="gap-2"
                    :disabled="session.is_current || revokingSessionId === session.id"
                    @click="revokeSession(session)"
                  >
                    <LogOut class="h-4 w-4" />
                    {{ revokingSessionId === session.id ? 'Ending...' : 'End session' }}
                  </Button>
                </div>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card class="border-slate-200 xl:col-span-2">
          <CardHeader>
            <CardTitle>Security</CardTitle>
            <CardDescription>
              Datamingle does not store WorkOS passwords, MFA factors, or SSO credentials.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div class="rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-700">
              Password policy, MFA enrollment, and SSO enforcement stay in WorkOS and the organization identity provider. Use the session list above for device-level control, and use logout for this browser session.
            </div>
          </CardContent>
        </Card>
      </div>

      <Card v-else class="border-slate-200">
        <CardHeader>
          <CardTitle>Identity</CardTitle>
          <CardDescription>
            This local account will be linked when a matching WorkOS user signs in.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div class="rounded-lg border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-700">
            Local changes are limited to Datamingle access and lifecycle settings until the account is connected to WorkOS.
          </div>
        </CardContent>
      </Card>
    </template>
  </section>
</template>
