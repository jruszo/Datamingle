<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { RouterLink, RouterView, useRoute, useRouter } from 'vue-router'
import {
  ChevronDown,
  ChevronRight,
  LogOut,
  PanelLeftClose,
  PanelLeftOpen,
  Settings,
} from 'lucide-vue-next'

import { Button } from '@/components/ui/button'
import { publicApiUrl } from '@/shared/api/http'
import { getVisibleNavigationItems, matchesNavigationItem } from '@/app/feature-registry'
import { useAuthStore } from '@/stores/auth'

const authStore = useAuthStore()
const router = useRouter()
const route = useRoute()

const showAppShell = computed(() => authStore.isAuthenticated)
const isSidebarCollapsed = ref(false)
const isSettingsMenuOpen = ref(route.path.startsWith('/settings'))
const settingsSubmenuId = 'settings-submenu'

const visiblePrimaryNavigation = computed(() =>
  getVisibleNavigationItems('primary', authStore.currentUser),
)
const visibleSettingsNavigation = computed(() =>
  getVisibleNavigationItems('settings', authStore.currentUser),
)
const hasSettingsNavigation = computed(() => visibleSettingsNavigation.value.length > 0)
const isSettingsRouteActive = computed(() => route.path.startsWith('/settings'))

function navigationItemClass(isActive: boolean) {
  const baseClass = 'flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium transition'
  if (isActive) {
    return `${baseClass} bg-slate-100 text-slate-900`
  }

  return `${baseClass} text-slate-600 hover:bg-slate-100 hover:text-slate-900`
}

function isNavigationItemActive(to: string, matchPrefix?: string) {
  return matchesNavigationItem({ to, matchPrefix, label: '', section: 'primary' }, route.path)
}

const pageTitle = computed(() => {
  if (typeof route.meta.title === 'string') {
    return route.meta.title
  }

  const matchedItem = visiblePrimaryNavigation.value.find((item) =>
    matchesNavigationItem(item, route.path),
  )

  return matchedItem?.label ?? 'Datamingle'
})

const currentUserName = computed(() => {
  return authStore.currentUser?.display || authStore.currentUser?.username || 'User'
})

const currentUserSubtitle = computed(() => {
  return authStore.currentUser?.email || authStore.currentUser?.username || 'Profile'
})

const currentUserAvatarUrl = computed(() => {
  const avatarUrl = authStore.currentUser?.avatar_url?.trim()
  return avatarUrl || ''
})

const currentUserInitials = computed(() => {
  const source = authStore.currentUser?.display || authStore.currentUser?.username || 'U'
  const initials = source
    .split(/\s+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((segment) => segment[0]?.toUpperCase() ?? '')
    .join('')

  return initials || 'U'
})

async function loadCurrentUser(force = false) {
  if (!authStore.isAuthenticated) {
    return
  }

  try {
    await authStore.loadCurrentUser(force)
  } catch {
    // Page-level requests will surface errors if user loading fails.
  }
}

function toggleSidebar() {
  isSidebarCollapsed.value = !isSidebarCollapsed.value
}

function toggleSettingsMenu() {
  if (isSidebarCollapsed.value) {
    isSidebarCollapsed.value = false
    isSettingsMenuOpen.value = true
    return
  }

  isSettingsMenuOpen.value = !isSettingsMenuOpen.value
}

async function logout() {
  authStore.clearTokens()

  const cachedAuthMode = authStore.authMode
  if (cachedAuthMode === 'workos') {
    window.location.assign(publicApiUrl('/auth/workos/logout/'))
    return
  }

  try {
    const resolvedAuthMode = await Promise.race([
      authStore.loadAuthConfig(),
      new Promise<typeof cachedAuthMode>((resolve) => {
        window.setTimeout(() => resolve(cachedAuthMode), 400)
      }),
    ])

    if (resolvedAuthMode === 'workos') {
      window.location.assign(publicApiUrl('/auth/workos/logout/'))
      return
    }
  } catch {
    // Fall back to local sign-out if auth config cannot be loaded quickly.
  }

  await router.push('/login')
}

onMounted(() => {
  void authStore.loadAuthConfig()
  void loadCurrentUser()
})

watch(
  () => authStore.accessToken,
  (token, previousToken) => {
    if (token && token !== previousToken) {
      void loadCurrentUser(true)
    }
  },
)

watch(
  () => route.path,
  (path) => {
    if (path.startsWith('/settings')) {
      isSettingsMenuOpen.value = true
    }
  },
)
</script>

<template>
  <div class="min-h-screen bg-slate-100 text-slate-900">
    <div v-if="showAppShell" class="flex min-h-screen">
      <aside
        :class="isSidebarCollapsed ? 'w-20' : 'w-64'"
        class="flex min-h-screen flex-col border-r border-slate-200 bg-white transition-all duration-200"
      >
        <div class="flex h-16 items-center gap-3 border-b border-slate-200 px-4">
          <div
            class="flex h-9 w-9 items-center justify-center rounded-lg bg-gradient-to-br from-violet-500 to-fuchsia-500 text-sm font-bold text-white"
          >
            D
          </div>
          <div v-if="!isSidebarCollapsed">
            <p class="text-sm font-semibold tracking-wide">Datamingle</p>
            <p class="text-xs text-slate-500">SQL Platform</p>
          </div>
        </div>

        <nav class="flex-1 space-y-1 p-3">
          <RouterLink
            v-for="item in visiblePrimaryNavigation"
            :key="item.to"
            :to="item.to"
            :title="isSidebarCollapsed ? item.label : undefined"
            :class="navigationItemClass(isNavigationItemActive(item.to, item.matchPrefix))"
          >
            <component :is="item.icon" class="h-4 w-4 shrink-0" />
            <span v-if="!isSidebarCollapsed">{{ item.label }}</span>
          </RouterLink>

          <div v-if="hasSettingsNavigation" class="space-y-1">
            <button
              :aria-controls="settingsSubmenuId"
              :aria-expanded="isSettingsMenuOpen"
              :class="
                isSettingsRouteActive
                  ? 'bg-slate-100 text-slate-900'
                  : 'text-slate-600 hover:bg-slate-100 hover:text-slate-900'
              "
              :title="isSidebarCollapsed ? 'Settings' : undefined"
              class="flex w-full items-center gap-3 rounded-md px-3 py-2 text-sm font-medium transition"
              type="button"
              @click="toggleSettingsMenu"
            >
              <Settings class="h-4 w-4 shrink-0" />
              <template v-if="!isSidebarCollapsed">
                <span class="flex-1 text-left">Settings</span>
                <ChevronDown v-if="isSettingsMenuOpen" class="h-4 w-4" />
                <ChevronRight v-else class="h-4 w-4" />
              </template>
            </button>

            <div
              v-if="!isSidebarCollapsed && isSettingsMenuOpen"
              :id="settingsSubmenuId"
              aria-label="Settings submenu"
              class="space-y-1 pl-10"
              role="region"
            >
              <RouterLink
                v-for="item in visibleSettingsNavigation"
                :key="item.to"
                :to="item.to"
                class="flex rounded-md px-3 py-2 text-sm text-slate-600 transition hover:bg-slate-100 hover:text-slate-900"
                active-class="bg-slate-100 font-medium text-slate-900"
              >
                {{ item.label }}
              </RouterLink>
            </div>
          </div>
        </nav>
      </aside>

      <div class="flex min-h-screen flex-1 flex-col">
        <header class="flex h-16 items-center justify-between border-b border-slate-200 bg-white px-4 lg:px-6">
          <div class="flex items-center gap-3">
            <Button variant="ghost" size="icon" @click="toggleSidebar">
              <PanelLeftOpen v-if="isSidebarCollapsed" class="h-4 w-4" />
              <PanelLeftClose v-else class="h-4 w-4" />
            </Button>
            <div>
              <p class="text-xs uppercase tracking-wide text-slate-500">Workspace</p>
              <h1 class="text-sm font-semibold text-slate-900">{{ pageTitle }}</h1>
            </div>
          </div>

          <div class="flex items-center gap-3">
            <RouterLink to="/profile" class="flex items-center gap-3 rounded-full transition hover:opacity-90">
              <div class="hidden text-right sm:block">
                <p class="text-sm font-semibold">{{ currentUserName }}</p>
                <p class="text-xs text-slate-500">{{ currentUserSubtitle }}</p>
              </div>
              <div
                class="flex h-9 w-9 items-center justify-center overflow-hidden rounded-full bg-slate-900 text-xs font-semibold text-white"
              >
                <img
                  v-if="currentUserAvatarUrl"
                  :src="currentUserAvatarUrl"
                  :alt="`${currentUserName} avatar`"
                  class="h-full w-full object-cover"
                  referrerpolicy="no-referrer"
                />
                <span v-else>{{ currentUserInitials }}</span>
              </div>
            </RouterLink>
            <Button variant="ghost" size="icon" title="Logout" @click="logout">
              <LogOut class="h-4 w-4" />
            </Button>
          </div>
        </header>

        <main class="flex-1 p-4 lg:p-6">
          <RouterView />
        </main>
      </div>
    </div>

    <main v-else class="mx-auto flex min-h-screen w-full max-w-md items-center px-6 py-8">
      <RouterView />
    </main>
  </div>
</template>
