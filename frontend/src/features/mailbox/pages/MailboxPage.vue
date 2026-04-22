<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { CheckCheck, ExternalLink, RefreshCw } from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { useMailboxStore } from '@/stores/mailbox'
import type { MailboxCategory, MailboxItem, MailboxReadState, MailboxSourceType } from '../api'

const router = useRouter()
const mailboxStore = useMailboxStore()

const stateFilter = ref<MailboxReadState>('all')
const categoryFilter = ref<MailboxCategory | ''>('')
const sourceTypeFilter = ref<MailboxSourceType | ''>('')

const items = computed(() => mailboxStore.itemsPage.results)
const currentPage = computed(() => mailboxStore.listFilters.page)
const totalCount = computed(() => mailboxStore.itemsPage.count)

const selectClass =
  'block h-10 rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-700 shadow-sm outline-none transition focus:border-slate-400 disabled:cursor-not-allowed disabled:bg-slate-100'

function formatDateTime(value: string | null) {
  if (!value) {
    return 'Not yet'
  }
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return value
  }
  return date.toLocaleString()
}

function badgeClassForCategory(category: MailboxCategory) {
  switch (category) {
    case 'approval_needed':
      return 'border-amber-200 bg-amber-50 text-amber-700'
    case 'execution_needed':
      return 'border-sky-200 bg-sky-50 text-sky-700'
    case 'execution_finished':
      return 'border-emerald-200 bg-emerald-50 text-emerald-700'
    default:
      return 'border-slate-200 bg-slate-100 text-slate-600'
  }
}

function badgeClassForSource(sourceType: MailboxSourceType) {
  switch (sourceType) {
    case 'sql_workflow':
      return 'border-violet-200 bg-violet-50 text-violet-700'
    case 'archive':
      return 'border-indigo-200 bg-indigo-50 text-indigo-700'
    case 'permission_request':
      return 'border-cyan-200 bg-cyan-50 text-cyan-700'
    default:
      return 'border-slate-200 bg-slate-100 text-slate-600'
  }
}

async function loadPage(page = 1) {
  await mailboxStore.loadItems({
    page,
    state: stateFilter.value,
    category: categoryFilter.value,
    source_type: sourceTypeFilter.value,
  })
}

async function openItem(item: MailboxItem) {
  if (item.is_unread) {
    await mailboxStore.markRead(item.id)
  }
  await router.push(item.action_path)
}

async function markItemRead(item: MailboxItem) {
  if (!item.is_unread) {
    return
  }
  await mailboxStore.markRead(item.id)
}

watch([stateFilter, categoryFilter, sourceTypeFilter], () => {
  void loadPage(1)
})

onMounted(async () => {
  await Promise.all([
    mailboxStore.refreshSummary(),
    loadPage(),
  ])
})
</script>

<template>
  <section class="grid gap-6">
    <Card>
      <CardHeader class="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
        <div class="space-y-1">
          <CardTitle>Mailbox</CardTitle>
          <CardDescription>
            Review approval requests, execution actions, and execution results in one place.
          </CardDescription>
        </div>
        <div class="flex flex-wrap items-center gap-2">
          <Button
            variant="outline"
            type="button"
            class="gap-2"
            :disabled="!mailboxStore.hasUnread"
            @click="void mailboxStore.markAllRead()"
          >
            <CheckCheck class="h-4 w-4" />
            Mark all read
          </Button>
          <Button
            variant="outline"
            type="button"
            class="gap-2"
            :disabled="mailboxStore.itemsLoading"
            @click="void loadPage(currentPage)"
          >
            <RefreshCw class="h-4 w-4" :class="{ 'animate-spin': mailboxStore.itemsLoading }" />
            Refresh
          </Button>
        </div>
      </CardHeader>
      <CardContent class="grid gap-4">
        <div class="flex flex-wrap items-center gap-3">
          <select v-model="stateFilter" :class="selectClass">
            <option value="all">All items</option>
            <option value="unread">Unread only</option>
            <option value="read">Read only</option>
          </select>
          <select v-model="categoryFilter" :class="selectClass">
            <option value="">All categories</option>
            <option value="approval_needed">Approval needed</option>
            <option value="execution_needed">Execution needed</option>
            <option value="execution_finished">Execution finished</option>
          </select>
          <select v-model="sourceTypeFilter" :class="selectClass">
            <option value="">All sources</option>
            <option value="sql_workflow">SQL workflows</option>
            <option value="archive">Archives</option>
            <option value="permission_request">Permission requests</option>
          </select>
          <p class="text-sm text-slate-500">{{ totalCount }} items</p>
        </div>

        <div v-if="mailboxStore.itemsLoading" class="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-10 text-center text-sm text-slate-500">
          Loading mailbox items…
        </div>

        <div v-else-if="items.length === 0" class="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-10 text-center text-sm text-slate-500">
          No mailbox items match the current filters.
        </div>

        <div v-else class="grid gap-3">
          <button
            v-for="item in items"
            :key="item.id"
            type="button"
            class="grid gap-3 rounded-2xl border border-slate-200 bg-white p-4 text-left shadow-sm transition hover:border-slate-300 hover:bg-slate-50"
            @click="void openItem(item)"
          >
            <div class="flex flex-wrap items-start justify-between gap-3">
              <div class="space-y-2">
                <div class="flex flex-wrap items-center gap-2">
                  <Badge :class="badgeClassForCategory(item.category)">{{ item.category_label }}</Badge>
                  <Badge :class="badgeClassForSource(item.source_type)">{{ item.source_type_label }}</Badge>
                  <Badge v-if="item.is_unread" class="border-slate-900 bg-slate-900 text-white">Unread</Badge>
                  <Badge v-if="item.resolved_at" variant="outline">Resolved</Badge>
                </div>
                <div class="space-y-1">
                  <p class="text-sm font-semibold text-slate-900">{{ item.title }}</p>
                  <p class="text-sm text-slate-600">{{ item.body }}</p>
                </div>
              </div>
              <div class="flex items-center gap-2">
                <Button
                  v-if="item.is_unread"
                  variant="ghost"
                  type="button"
                  class="h-8 px-3 text-xs"
                  @click.stop="void markItemRead(item)"
                >
                  Mark read
                </Button>
                <ExternalLink class="h-4 w-4 text-slate-400" />
              </div>
            </div>
            <div class="flex flex-wrap items-center justify-between gap-2 text-xs text-slate-500">
              <span>Created {{ formatDateTime(item.create_time) }}</span>
              <span v-if="item.read_at">Read {{ formatDateTime(item.read_at) }}</span>
              <span v-else-if="item.resolved_at">Resolved {{ formatDateTime(item.resolved_at) }}</span>
            </div>
          </button>
        </div>

        <div class="flex items-center justify-between gap-3">
          <Button
            variant="outline"
            type="button"
            :disabled="!mailboxStore.itemsPage.previous || mailboxStore.itemsLoading"
            @click="void loadPage(Math.max(1, currentPage - 1))"
          >
            Previous
          </Button>
          <p class="text-sm text-slate-500">Page {{ currentPage }}</p>
          <Button
            variant="outline"
            type="button"
            :disabled="!mailboxStore.itemsPage.next || mailboxStore.itemsLoading"
            @click="void loadPage(currentPage + 1)"
          >
            Next
          </Button>
        </div>
      </CardContent>
    </Card>
  </section>
</template>
