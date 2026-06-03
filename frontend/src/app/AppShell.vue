<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import type { Component } from 'vue'
import { onClickOutside } from '@vueuse/core'
import { RouterLink, RouterView, useRoute, useRouter } from 'vue-router'
import {
  Bell,
  CheckCheck,
  ChevronDown,
  ChevronRight,
  ExternalLink,
  LogOut,
  PanelLeftClose,
  PanelLeftOpen,
  Settings,
} from 'lucide-vue-next'

import { Button } from '@/components/ui/button'
import { publicApiUrl } from '@/shared/api/http'
import { getVisibleNavigationItems, matchesNavigationItem } from '@/app/feature-registry'
import type { FeatureNavigationItem } from '@/app/feature-contract'
import { useAuthStore } from '@/stores/auth'
import { useMailboxStore } from '@/stores/mailbox'

type PrimaryNavigationEntry =
  | {
      type: 'item'
      key: string
      order: number
      item: FeatureNavigationItem
    }
  | {
      type: 'group'
      key: string
      id: string
      label: string
      icon?: Component
      order: number
      items: FeatureNavigationItem[]
    }

const authStore = useAuthStore()
const mailboxStore = useMailboxStore()
const router = useRouter()
const route = useRoute()

const showAppShell = computed(() => authStore.isAuthenticated)
const isSidebarCollapsed = ref(false)
const openNavigationGroups = ref<Record<string, boolean>>({
  database: true,
  infrastructure: true,
})
const isSettingsMenuOpen = ref(route.path.startsWith('/settings'))
const settingsSubmenuId = 'settings-submenu'
const isMailboxMenuOpen = ref(false)
const mailboxMenuRef = ref<HTMLElement | null>(null)

const visiblePrimaryNavigation = computed(() =>
  getVisibleNavigationItems('primary', authStore.currentUser),
)
const primaryNavigationEntries = computed(() =>
  buildPrimaryNavigationEntries(visiblePrimaryNavigation.value),
)
const visibleSettingsNavigation = computed(() =>
  getVisibleNavigationItems('settings', authStore.currentUser),
)
const hasSettingsNavigation = computed(() => visibleSettingsNavigation.value.length > 0)
const isSettingsRouteActive = computed(() => route.path.startsWith('/settings'))

function navigationItemClass(isActive: boolean) {
  const baseClass =
    'flex items-center gap-2.5 rounded-md px-2.5 py-1.5 text-sm font-medium transition'
  if (isActive) {
    return `${baseClass} bg-slate-100 text-slate-950`
  }

  return `${baseClass} text-slate-600 hover:bg-slate-100 hover:text-slate-950`
}

function navigationSubItemClass(isActive: boolean) {
  const baseClass = 'flex items-center gap-2 rounded-md px-2.5 py-1.5 text-sm transition'
  if (isActive) {
    return `${baseClass} bg-slate-100 font-medium text-slate-950`
  }

  return `${baseClass} text-slate-600 hover:bg-slate-100 hover:text-slate-950`
}

function navigationGroupClass(isActive: boolean) {
  const baseClass =
    'flex w-full items-center gap-2.5 rounded-md px-2.5 py-1.5 text-sm font-medium transition'
  if (isActive) {
    return `${baseClass} bg-slate-100 text-slate-950`
  }

  return `${baseClass} text-slate-600 hover:bg-slate-100 hover:text-slate-950`
}

function buildPrimaryNavigationEntries(items: FeatureNavigationItem[]) {
  const entries: PrimaryNavigationEntry[] = []
  const groupedEntries = new Map<string, Extract<PrimaryNavigationEntry, { type: 'group' }>>()

  items.forEach((item) => {
    if (!item.group) {
      entries.push({
        type: 'item',
        key: item.to,
        order: item.order ?? 0,
        item,
      })
      return
    }

    const existingGroup = groupedEntries.get(item.group.id)
    if (existingGroup) {
      existingGroup.items.push(item)
      existingGroup.order = Math.min(existingGroup.order, item.group.order ?? item.order ?? 0)
      return
    }

    const groupEntry: Extract<PrimaryNavigationEntry, { type: 'group' }> = {
      type: 'group',
      key: `group-${item.group.id}`,
      id: item.group.id,
      label: item.group.label,
      icon: item.group.icon,
      order: item.group.order ?? item.order ?? 0,
      items: [item],
    }
    groupedEntries.set(item.group.id, groupEntry)
    entries.push(groupEntry)
  })

  entries.forEach((entry) => {
    if (entry.type === 'group') {
      entry.items.sort((left, right) => (left.order ?? 0) - (right.order ?? 0))
    }
  })

  return entries.sort((left, right) => left.order - right.order)
}

function isNavigationItemActive(to: string, matchPrefix?: string) {
  return matchesNavigationItem({ to, matchPrefix, label: '', section: 'primary' }, route.path)
}

function isNavigationGroupActive(items: FeatureNavigationItem[]) {
  return items.some((item) => matchesNavigationItem(item, route.path))
}

function isNavigationGroupOpen(entry: Extract<PrimaryNavigationEntry, { type: 'group' }>) {
  return openNavigationGroups.value[entry.id] || isNavigationGroupActive(entry.items)
}

function toggleNavigationGroup(entry: Extract<PrimaryNavigationEntry, { type: 'group' }>) {
  if (isSidebarCollapsed.value) {
    isSidebarCollapsed.value = false
    openNavigationGroups.value = {
      ...openNavigationGroups.value,
      [entry.id]: true,
    }
    return
  }

  openNavigationGroups.value = {
    ...openNavigationGroups.value,
    [entry.id]: !isNavigationGroupOpen(entry),
  }
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

const mailboxPreviewItems = computed(() => mailboxStore.summary.items)
const mailboxUnreadCount = computed(() => mailboxStore.unreadCount)
const mailboxUnreadBadge = computed(() =>
  mailboxUnreadCount.value > 99 ? '99+' : `${mailboxUnreadCount.value}`,
)

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

function formatMailboxTime(value: string | null) {
  if (!value) {
    return 'Now'
  }
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return value
  }
  return date.toLocaleString()
}

function mailboxCategoryClass(category: string) {
  if (category === 'approval_needed') {
    return 'border-amber-200 bg-amber-50 text-amber-700'
  }
  if (category === 'execution_needed') {
    return 'border-sky-200 bg-sky-50 text-sky-700'
  }
  return 'border-emerald-200 bg-emerald-50 text-emerald-700'
}

async function toggleMailboxMenu() {
  isMailboxMenuOpen.value = !isMailboxMenuOpen.value
  if (isMailboxMenuOpen.value) {
    await mailboxStore.refreshSummary()
  }
}

async function openMailboxItem(actionPath: string, itemId: number, isUnread: boolean) {
  isMailboxMenuOpen.value = false
  if (isUnread) {
    void mailboxStore.markRead(itemId).catch((error) => {
      console.error('Failed to mark mailbox item as read.', error)
    })
  }
  await router.push(actionPath)
}

async function openMailboxPage() {
  isMailboxMenuOpen.value = false
  await router.push('/mailbox')
}

async function logout() {
  mailboxStore.stopPolling()
  mailboxStore.reset()
  authStore.clearTokens()
  window.location.assign(publicApiUrl('/auth/workos/logout/'))
}

onMounted(() => {
  void loadCurrentUser()
  if (authStore.isAuthenticated) {
    mailboxStore.startPolling()
  }
})

onClickOutside(mailboxMenuRef, () => {
  isMailboxMenuOpen.value = false
})

watch(
  () => authStore.accessToken,
  (token, previousToken) => {
    if (token && token !== previousToken) {
      void loadCurrentUser(true)
      mailboxStore.startPolling()
      return
    }

    if (!token) {
      mailboxStore.stopPolling()
      mailboxStore.reset()
      isMailboxMenuOpen.value = false
    }
  },
)

watch(
  () => route.path,
  (path, previousPath) => {
    if (path.startsWith('/settings')) {
      isSettingsMenuOpen.value = true
    }
    isMailboxMenuOpen.value = false
    if (
      previousPath?.startsWith('/workflows/') ||
      previousPath?.startsWith('/archives/') ||
      previousPath?.startsWith('/permission-management')
    ) {
      void mailboxStore.refreshSummary()
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

        <nav class="flex-1 space-y-0.5 p-3">
          <template v-for="entry in primaryNavigationEntries" :key="entry.key">
            <RouterLink
              v-if="entry.type === 'item'"
              :to="entry.item.to"
              :title="isSidebarCollapsed ? entry.item.label : undefined"
              :class="
                navigationItemClass(isNavigationItemActive(entry.item.to, entry.item.matchPrefix))
              "
            >
              <component :is="entry.item.icon" v-if="entry.item.icon" class="h-4 w-4 shrink-0" />
              <span v-if="!isSidebarCollapsed">{{ entry.item.label }}</span>
            </RouterLink>

            <div v-else class="space-y-0.5">
              <button
                :aria-controls="`primary-navigation-group-${entry.id}`"
                :aria-expanded="isNavigationGroupOpen(entry)"
                :class="navigationGroupClass(isNavigationGroupActive(entry.items))"
                :title="isSidebarCollapsed ? entry.label : undefined"
                class="group"
                type="button"
                @click="toggleNavigationGroup(entry)"
              >
                <component :is="entry.icon" v-if="entry.icon" class="h-4 w-4 shrink-0" />
                <template v-if="!isSidebarCollapsed">
                  <span class="flex-1 text-left">{{ entry.label }}</span>
                  <ChevronDown v-if="isNavigationGroupOpen(entry)" class="h-4 w-4 text-slate-400" />
                  <ChevronRight v-else class="h-4 w-4 text-slate-400" />
                </template>
              </button>

              <div
                v-if="!isSidebarCollapsed && isNavigationGroupOpen(entry)"
                :id="`primary-navigation-group-${entry.id}`"
                :aria-label="`${entry.label} submenu`"
                class="space-y-0.5 pl-6"
                role="region"
              >
                <RouterLink
                  v-for="item in entry.items"
                  :key="item.to"
                  :to="item.to"
                  :class="navigationSubItemClass(isNavigationItemActive(item.to, item.matchPrefix))"
                >
                  <component :is="item.icon" v-if="item.icon" class="h-3.5 w-3.5 shrink-0" />
                  <span>{{ item.label }}</span>
                </RouterLink>
              </div>
            </div>
          </template>

          <div v-if="hasSettingsNavigation" class="space-y-1">
            <button
              :aria-controls="settingsSubmenuId"
              :aria-expanded="isSettingsMenuOpen"
              :class="
                isSettingsRouteActive
                  ? 'bg-slate-100 text-slate-950'
                  : 'text-slate-600 hover:bg-slate-100 hover:text-slate-950'
              "
              :title="isSidebarCollapsed ? 'Settings' : undefined"
              class="flex w-full items-center gap-2.5 rounded-md px-2.5 py-1.5 text-sm font-medium transition"
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
              class="space-y-0.5 pl-6"
              role="region"
            >
              <RouterLink
                v-for="item in visibleSettingsNavigation"
                :key="item.to"
                :to="item.to"
                class="flex rounded-md px-2.5 py-1.5 text-sm text-slate-600 transition hover:bg-slate-100 hover:text-slate-950"
                active-class="bg-slate-100 font-medium text-slate-950"
              >
                {{ item.label }}
              </RouterLink>
            </div>
          </div>
        </nav>
      </aside>

      <div class="flex min-h-screen flex-1 flex-col">
        <header
          class="flex h-16 items-center justify-between border-b border-slate-200 bg-white px-4 lg:px-6"
        >
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
            <div ref="mailboxMenuRef" class="relative">
              <Button
                variant="ghost"
                size="icon"
                title="Mailbox"
                data-testid="app-mailbox-button"
                class="relative"
                @click="void toggleMailboxMenu()"
              >
                <Bell class="h-4 w-4" />
                <span
                  v-if="mailboxUnreadCount > 0"
                  data-testid="app-mailbox-badge"
                  class="absolute -right-1 -top-1 inline-flex min-w-5 items-center justify-center rounded-full bg-rose-500 px-1.5 py-0.5 text-[10px] font-semibold text-white"
                >
                  {{ mailboxUnreadBadge }}
                </span>
              </Button>

              <div
                v-if="isMailboxMenuOpen"
                data-testid="app-mailbox-menu"
                class="absolute right-0 top-12 z-30 w-[24rem] rounded-2xl border border-slate-200 bg-white p-3 shadow-xl"
              >
                <div
                  class="flex items-center justify-between gap-3 border-b border-slate-100 px-1 pb-3"
                >
                  <div>
                    <p class="text-sm font-semibold text-slate-900">Mailbox</p>
                    <p class="text-xs text-slate-500">
                      {{ mailboxUnreadCount }} unread notifications
                    </p>
                  </div>
                  <Button
                    v-if="mailboxUnreadCount > 0"
                    variant="ghost"
                    type="button"
                    data-testid="app-mailbox-mark-all-read"
                    class="h-8 px-2 text-xs"
                    @click="
                      void mailboxStore
                        .markAllRead()
                        .catch((error) =>
                          console.error('Failed to mark all mailbox items as read.', error),
                        )
                    "
                  >
                    <CheckCheck class="mr-1 h-3.5 w-3.5" />
                    Mark all read
                  </Button>
                </div>

                <div
                  v-if="mailboxPreviewItems.length === 0"
                  class="px-1 py-6 text-center text-sm text-slate-500"
                >
                  No notifications right now.
                </div>

                <div v-else class="max-h-[24rem] space-y-2 overflow-y-auto py-3">
                  <button
                    v-for="item in mailboxPreviewItems"
                    :key="item.id"
                    type="button"
                    :data-testid="`app-mailbox-preview-item-${item.id}`"
                    class="grid w-full gap-2 rounded-xl border border-slate-200 px-3 py-3 text-left transition hover:border-slate-300 hover:bg-slate-50"
                    @click="void openMailboxItem(item.action_path, item.id, item.is_unread)"
                  >
                    <div class="flex items-start justify-between gap-3">
                      <div class="space-y-1">
                        <div class="flex flex-wrap items-center gap-2">
                          <span
                            class="inline-flex rounded-full border px-2 py-0.5 text-[11px] font-medium"
                            :class="mailboxCategoryClass(item.category)"
                          >
                            {{ item.category_label }}
                          </span>
                          <span
                            v-if="item.is_unread"
                            class="inline-flex rounded-full bg-slate-900 px-2 py-0.5 text-[11px] font-medium text-white"
                          >
                            Unread
                          </span>
                        </div>
                        <p class="text-sm font-medium text-slate-900">{{ item.title }}</p>
                        <p class="text-sm text-slate-600">{{ item.body }}</p>
                      </div>
                      <ExternalLink class="mt-0.5 h-4 w-4 shrink-0 text-slate-400" />
                    </div>
                    <p class="text-xs text-slate-500">{{ formatMailboxTime(item.create_time) }}</p>
                  </button>
                </div>

                <div class="border-t border-slate-100 px-1 pt-3">
                  <Button
                    variant="outline"
                    type="button"
                    data-testid="app-mailbox-view-all"
                    class="w-full justify-between"
                    @click="void openMailboxPage()"
                  >
                    View full mailbox
                    <ExternalLink class="h-4 w-4" />
                  </Button>
                </div>
              </div>
            </div>
            <RouterLink
              to="/profile"
              class="flex items-center gap-3 rounded-full transition hover:opacity-90"
            >
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
