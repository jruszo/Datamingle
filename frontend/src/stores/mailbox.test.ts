import { beforeEach, describe, expect, it, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'

vi.mock('@/lib/api', () => ({
  fetchMailboxSummary: vi.fn(),
  fetchMailboxItems: vi.fn(),
  markMailboxItemRead: vi.fn(),
  markAllMailboxItemsRead: vi.fn(),
}))

import {
  fetchMailboxItems,
  fetchMailboxSummary,
  markAllMailboxItemsRead,
  markMailboxItemRead,
} from '@/lib/api'
import { useAuthStore } from '@/stores/auth'
import { useMailboxStore } from '@/stores/mailbox'

const localStorageMock = {
  getItem: vi.fn(() => ''),
  setItem: vi.fn(),
  removeItem: vi.fn(),
}

Object.defineProperty(globalThis, 'localStorage', {
  value: localStorageMock,
  configurable: true,
})

Object.defineProperty(globalThis, 'window', {
  value: globalThis,
  configurable: true,
})

describe('useMailboxStore', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.useRealTimers()
    setActivePinia(createPinia())
  })

  it('refreshes summary and exposes unread count', async () => {
    vi.mocked(fetchMailboxSummary).mockResolvedValue({
      unread_count: 3,
      items: [],
    })

    const authStore = useAuthStore()
    authStore.syncTokens('access-token', 'refresh-token')

    const mailboxStore = useMailboxStore()
    await mailboxStore.refreshSummary()

    expect(fetchMailboxSummary).toHaveBeenCalledWith('access-token')
    expect(mailboxStore.unreadCount).toBe(3)
    expect(mailboxStore.hasUnread).toBe(true)
  })

  it('loads items and marks them read', async () => {
    vi.mocked(fetchMailboxItems).mockResolvedValue({
      count: 1,
      next: null,
      previous: null,
      results: [
        {
          id: 1,
          category: 'approval_needed',
          category_label: 'Approval needed',
          source_type: 'sql_workflow',
          source_type_label: 'SQL Workflow',
          source_id: 42,
          title: 'Approval needed: Example',
          body: 'Review this workflow.',
          action_path: '/workflows/42',
          is_unread: true,
          read_at: null,
          resolved_at: null,
          metadata: {},
          create_time: '2026-04-21T09:00:00',
          sys_time: '2026-04-21T09:00:00',
        },
      ],
    })
    vi.mocked(markMailboxItemRead).mockResolvedValue({
      id: 1,
      category: 'approval_needed',
      category_label: 'Approval needed',
      source_type: 'sql_workflow',
      source_type_label: 'SQL Workflow',
      source_id: 42,
      title: 'Approval needed: Example',
      body: 'Review this workflow.',
      action_path: '/workflows/42',
      is_unread: false,
      read_at: '2026-04-21T09:05:00',
      resolved_at: null,
      metadata: {},
      create_time: '2026-04-21T09:00:00',
      sys_time: '2026-04-21T09:05:00',
    })
    vi.mocked(fetchMailboxSummary).mockResolvedValue({
      unread_count: 0,
      items: [],
    })
    vi.mocked(markAllMailboxItemsRead).mockResolvedValue({ updated: 1 })

    const authStore = useAuthStore()
    authStore.syncTokens('access-token', 'refresh-token')

    const mailboxStore = useMailboxStore()
    await mailboxStore.loadItems({ state: 'unread', page: 1, size: 20 })
    await mailboxStore.markRead(1)
    await mailboxStore.markAllRead()

    expect(fetchMailboxItems).toHaveBeenCalledWith('access-token', {
      page: 1,
      size: 20,
      state: 'unread',
      category: '',
      source_type: '',
    })
    expect(markMailboxItemRead).toHaveBeenCalledWith(1, 'access-token')
    expect(markAllMailboxItemsRead).toHaveBeenCalledWith('access-token')
    expect(mailboxStore.itemsPage.results).toEqual([])
    expect(mailboxStore.itemsPage.count).toBe(0)
  })

  it('keeps existing filters when loading items fails', async () => {
    vi.mocked(fetchMailboxItems)
      .mockResolvedValueOnce({
        count: 1,
        next: null,
        previous: null,
        results: [],
      })
      .mockRejectedValueOnce(new Error('load failed'))

    const authStore = useAuthStore()
    authStore.syncTokens('access-token', 'refresh-token')

    const mailboxStore = useMailboxStore()
    await mailboxStore.loadItems({ state: 'unread', page: 2 })

    await expect(mailboxStore.loadItems({ state: 'read' })).rejects.toThrow(
      'load failed',
    )
    expect(mailboxStore.listFilters.state).toBe('unread')
    expect(mailboxStore.listFilters.page).toBe(2)
  })

  it('reset stops polling', async () => {
    vi.useFakeTimers()
    vi.mocked(fetchMailboxSummary).mockResolvedValue({
      unread_count: 1,
      items: [],
    })

    const clearIntervalSpy = vi.spyOn(window, 'clearInterval')
    const authStore = useAuthStore()
    authStore.syncTokens('access-token', 'refresh-token')

    const mailboxStore = useMailboxStore()
    mailboxStore.startPolling()
    await Promise.resolve()

    mailboxStore.reset()

    expect(clearIntervalSpy).toHaveBeenCalled()
    await vi.advanceTimersByTimeAsync(60_000)
    expect(fetchMailboxSummary).toHaveBeenCalledTimes(1)
  })
})
