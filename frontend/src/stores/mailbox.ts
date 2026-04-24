import { computed, ref } from 'vue'
import { defineStore } from 'pinia'

import {
  fetchMailboxItems,
  fetchMailboxSummary,
  markAllMailboxItemsRead,
  markMailboxItemRead,
  type MailboxCategory,
  type MailboxItem,
  type MailboxReadState,
  type MailboxSourceType,
  type MailboxSummary,
  type PaginatedResponse,
} from '@/lib/api'
import { useAuthStore } from '@/stores/auth'

const DEFAULT_PAGE_SIZE = 20

type MailboxListFilters = {
  page: number
  size: number
  state: MailboxReadState
  category: MailboxCategory | ''
  source_type: MailboxSourceType | ''
}

function emptySummary(): MailboxSummary {
  return {
    unread_count: 0,
    items: [],
  }
}

function emptyPage(): PaginatedResponse<MailboxItem> {
  return {
    count: 0,
    next: null,
    previous: null,
    results: [],
  }
}

export const useMailboxStore = defineStore('mailbox', () => {
  const authStore = useAuthStore()

  const summary = ref<MailboxSummary>(emptySummary())
  const summaryLoading = ref(false)
  const itemsLoading = ref(false)
  const itemsPage = ref<PaginatedResponse<MailboxItem>>(emptyPage())
  const listFilters = ref<MailboxListFilters>({
    page: 1,
    size: DEFAULT_PAGE_SIZE,
    state: 'all',
    category: '',
    source_type: '',
  })

  const unreadCount = computed(() => summary.value.unread_count)
  const hasUnread = computed(() => unreadCount.value > 0)

  let pollingHandle: number | null = null

  function reset() {
    stopPolling()
    summary.value = emptySummary()
    itemsPage.value = emptyPage()
    listFilters.value = {
      page: 1,
      size: DEFAULT_PAGE_SIZE,
      state: 'all',
      category: '',
      source_type: '',
    }
  }

  function requireToken() {
    if (!authStore.accessToken) {
      throw new Error('Missing access token. Please login again.')
    }
    return authStore.accessToken
  }

  async function refreshSummary() {
    if (!authStore.accessToken) {
      reset()
      return summary.value
    }

    summaryLoading.value = true
    try {
      summary.value = await fetchMailboxSummary(requireToken())
      return summary.value
    } finally {
      summaryLoading.value = false
    }
  }

  async function loadItems(overrides: Partial<MailboxListFilters> = {}) {
    if (!authStore.accessToken) {
      itemsPage.value = emptyPage()
      return itemsPage.value
    }

    const mergedFilters = {
      ...listFilters.value,
      ...overrides,
    }
    itemsLoading.value = true
    try {
      itemsPage.value = await fetchMailboxItems(requireToken(), mergedFilters)
      listFilters.value = mergedFilters
      return itemsPage.value
    } finally {
      itemsLoading.value = false
    }
  }

  async function refreshItems() {
    return loadItems({})
  }

  function updatePageItem(updatedItem: MailboxItem) {
    itemsPage.value = {
      ...itemsPage.value,
      results: itemsPage.value.results.map((item) =>
        item.id === updatedItem.id ? updatedItem : item,
      ),
    }
  }

  function removeReadItemsFromUnreadPage() {
    const readItemCount = itemsPage.value.results.filter((item) => !item.is_unread).length
    itemsPage.value = {
      ...itemsPage.value,
      count: Math.max(0, itemsPage.value.count - readItemCount),
      results: itemsPage.value.results.filter((item) => item.is_unread),
    }
  }

  async function markRead(itemId: number) {
    const updatedItem = await markMailboxItemRead(itemId, requireToken())
    updatePageItem(updatedItem)
    if (listFilters.value.state === 'unread') {
      removeReadItemsFromUnreadPage()
    }
    await refreshSummary()
    return updatedItem
  }

  async function markAllRead() {
    await markAllMailboxItemsRead(requireToken())
    itemsPage.value = {
      ...itemsPage.value,
      results: itemsPage.value.results.map((item) => ({
        ...item,
        is_unread: false,
        read_at: item.read_at || new Date().toISOString(),
      })),
    }
    if (listFilters.value.state === 'unread') {
      removeReadItemsFromUnreadPage()
    }
    await refreshSummary()
  }

  function startPolling() {
    stopPolling()
    if (!authStore.accessToken) {
      return
    }
    void refreshSummary()
    pollingHandle = window.setInterval(() => {
      void refreshSummary()
    }, 60_000)
  }

  function stopPolling() {
    if (pollingHandle !== null) {
      window.clearInterval(pollingHandle)
      pollingHandle = null
    }
  }

  return {
    summary,
    summaryLoading,
    itemsLoading,
    itemsPage,
    listFilters,
    unreadCount,
    hasUnread,
    reset,
    refreshSummary,
    loadItems,
    refreshItems,
    markRead,
    markAllRead,
    startPolling,
    stopPolling,
  }
})
