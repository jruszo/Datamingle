<script setup lang="ts">
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { useDebounceFn } from '@vueuse/core'
import {
  CheckCircle2,
  Clock3,
  ExternalLink,
  Plus,
  RefreshCw,
  ShieldPlus,
  Trash2,
  X,
  XCircle,
} from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import {
  createPermissionRequest,
  fetchPermissionGrants,
  fetchPermissionInstancesLookup,
  fetchPermissionRequestDetail,
  fetchPermissionRequests,
  fetchPermissionResourceGroupsLookup,
  reviewPermissionRequest,
  revokePermissionGrant,
  type PaginatedResponse,
  type PermissionGrantRecord,
  type PermissionInstanceAccessLevel,
  type PermissionInstanceLookupRecord,
  type PermissionRequestDetailRecord,
  type PermissionRequestRecord,
  type PermissionRequestStatus,
  type PermissionRequestTarget,
  type PermissionResourceGroupLookupRecord,
} from '@/lib/api'
import { useAuthStore } from '@/stores/auth'

const REQUEST_PAGE_SIZE = 8
const GRANT_PAGE_SIZE = 10
const UNLIMITED_VALID_DATE = '2099-12-31'

const authStore = useAuthStore()

const activeSection = ref<'requests' | 'grants'>('requests')

const requestError = ref('')
const detailError = ref('')
const grantError = ref('')
const lookupsError = ref('')
const formError = ref('')
const feedback = ref('')

const lookupsLoading = ref(false)
const requestsLoading = ref(false)
const detailLoading = ref(false)
const grantsLoading = ref(false)
const createSubmitting = ref(false)
const reviewSubmitting = ref(false)
const revokingGrantKey = ref('')

const isCreateDialogOpen = ref(false)
const isDetailDialogOpen = ref(false)

const resourceGroups = ref<PermissionResourceGroupLookupRecord[]>([])
const instances = ref<PermissionInstanceLookupRecord[]>([])

const requestsPage = ref<PaginatedResponse<PermissionRequestRecord>>({
  count: 0,
  next: null,
  previous: null,
  results: [],
})

const grantsPage = ref<PaginatedResponse<PermissionGrantRecord>>({
  count: 0,
  next: null,
  previous: null,
  results: [],
})

const requestSearch = ref('')
const grantSearch = ref('')
const requestPage = ref(1)
const grantPage = ref(1)
const selectedRequestId = ref<number | null>(null)
const selectedRequestDetail = ref<PermissionRequestDetailRecord | null>(null)

const createForm = reactive({
  title: '',
  reason: '',
  target_type: 'resource_group' as PermissionRequestTarget,
  resource_group_id: '',
  instance_id: '',
  access_level: 'query' as PermissionInstanceAccessLevel,
  valid_date: defaultValidDate(7),
})

const reviewForm = reactive({
  audit_remark: '',
})

let requestLoadCounter = 0
let detailLoadCounter = 0
let grantLoadCounter = 0

const selectClass =
  'block h-10 w-full rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-700 shadow-sm outline-none transition focus:border-slate-400 disabled:cursor-not-allowed disabled:bg-slate-100'
const textareaClass =
  'block min-h-[7.5rem] w-full rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700 shadow-sm outline-none transition focus:border-slate-400 disabled:cursor-not-allowed disabled:bg-slate-100'

function defaultValidDate(daysFromNow: number) {
  const date = new Date()
  date.setDate(date.getDate() + daysFromNow)
  return date.toISOString().slice(0, 10)
}

function hasPermission(permission: string) {
  if (authStore.currentUser?.is_superuser) {
    return true
  }
  return authStore.currentUser?.permissions.includes(permission) ?? false
}

function requireToken() {
  if (!authStore.accessToken) {
    throw new Error('Missing access token. Please login again.')
  }
  return authStore.accessToken
}

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

function formatDate(value: string) {
  if (value >= UNLIMITED_VALID_DATE) {
    return 'Unlimited'
  }
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return value
  }
  return date.toLocaleDateString()
}

function formatDateTime(value: string) {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return value
  }
  return date.toLocaleString()
}

function statusLabel(status: PermissionRequestStatus) {
  switch (status) {
    case 0:
      return 'Pending review'
    case 1:
      return 'Approved'
    case 2:
      return 'Rejected'
    case 3:
      return 'Canceled'
    default:
      return 'Unknown'
  }
}

function statusClass(status: PermissionRequestStatus) {
  switch (status) {
    case 0:
      return 'border-amber-200 bg-amber-50 text-amber-700'
    case 1:
      return 'border-emerald-200 bg-emerald-50 text-emerald-700'
    case 2:
      return 'border-rose-200 bg-rose-50 text-rose-700'
    case 3:
      return 'border-slate-200 bg-slate-100 text-slate-600'
    default:
      return 'border-slate-200 bg-slate-100 text-slate-600'
  }
}

function targetLabel(targetType: PermissionRequestTarget) {
  return targetType === 'resource_group' ? 'Resource group' : 'Instance'
}

function grantTypeLabel(grantType: PermissionGrantRecord['grant_type']) {
  return grantType === 'resource_group' ? 'Group access' : 'Instance access'
}

function accessLevelLabel(level: PermissionInstanceAccessLevel | '') {
  switch (level) {
    case 'query':
      return 'Query only'
    case 'query_dml':
      return 'Query + DML'
    case 'query_dml_ddl':
      return 'Query + DML + DDL'
    default:
      return 'Inherited from group access'
  }
}

function setQuickValidDate(daysFromNow: number) {
  createForm.valid_date = defaultValidDate(daysFromNow)
}

function setUnlimitedValidDate() {
  createForm.valid_date = UNLIMITED_VALID_DATE
}

function resetCreateForm() {
  createForm.title = ''
  createForm.reason = ''
  createForm.target_type = 'resource_group'
  createForm.resource_group_id = ''
  createForm.instance_id = ''
  createForm.access_level = 'query'
  createForm.valid_date = defaultValidDate(7)
}

function closeCreateDialog() {
  isCreateDialogOpen.value = false
  formError.value = ''
}

function openCreateDialog() {
  formError.value = ''
  lookupsError.value = ''
  isCreateDialogOpen.value = true
  if (resourceGroups.value.length === 0 && !lookupsLoading.value) {
    void loadLookups()
  }
}

function closeDetailDialog() {
  isDetailDialogOpen.value = false
  detailError.value = ''
  reviewForm.audit_remark = ''
}

const canViewPermissionManagement = computed(() => hasPermission('sql.menu_queryapplylist'))
const canCreateRequests = computed(() => hasPermission('sql.query_applypriv'))
const canReviewRequests = computed(() => hasPermission('sql.query_review'))
const canManageGrants = computed(() => hasPermission('sql.query_mgtpriv'))

const filteredInstances = computed(() => {
  const resourceGroupId = Number(createForm.resource_group_id)
  if (!resourceGroupId) {
    return instances.value
  }

  return instances.value.filter((instance) =>
    instance.resource_groups.some((group) => group.group_id === resourceGroupId),
  )
})

const selectedRequestSummary = computed(() => {
  if (selectedRequestDetail.value) {
    return selectedRequestDetail.value
  }
  if (selectedRequestId.value === null) {
    return null
  }
  return requestsPage.value.results.find((item) => item.request_id === selectedRequestId.value) ?? null
})

watch(
  () => createForm.target_type,
  (targetType) => {
    if (targetType === 'resource_group') {
      createForm.instance_id = ''
      createForm.access_level = 'query'
    }
  },
)

watch(
  () => createForm.resource_group_id,
  () => {
    if (createForm.target_type !== 'instance' || !createForm.instance_id) {
      return
    }

    const currentInstanceId = Number(createForm.instance_id)
    const stillAvailable = filteredInstances.value.some((instance) => instance.id === currentInstanceId)
    if (!stillAvailable) {
      createForm.instance_id = ''
    }
  },
)

async function loadLookups() {
  if (!canCreateRequests.value) {
    return
  }

  lookupsLoading.value = true
  lookupsError.value = ''

  try {
    const [resourceGroupRows, instanceRows] = await Promise.all([
      fetchPermissionResourceGroupsLookup(requireToken()),
      fetchPermissionInstancesLookup(requireToken()),
    ])
    resourceGroups.value = resourceGroupRows
    instances.value = instanceRows
  } catch (errorValue) {
    lookupsError.value = toUserFacingMessage(errorValue, 'Failed to load request form options.')
  } finally {
    lookupsLoading.value = false
  }
}

async function loadRequestDetail(requestId: number) {
  const loadId = detailLoadCounter + 1
  detailLoadCounter = loadId
  selectedRequestId.value = requestId
  detailLoading.value = true
  detailError.value = ''
  isDetailDialogOpen.value = true

  try {
    const detail = await fetchPermissionRequestDetail(requestId, requireToken())
    if (loadId !== detailLoadCounter) {
      return
    }
    selectedRequestDetail.value = detail
  } catch (errorValue) {
    if (loadId !== detailLoadCounter) {
      return
    }
    detailError.value = toUserFacingMessage(errorValue, 'Failed to load request detail.')
    selectedRequestDetail.value = null
  } finally {
    if (loadId === detailLoadCounter) {
      detailLoading.value = false
    }
  }
}

async function loadRequests(options: { focusRequestId?: number; openDetail?: boolean } = {}) {
  if (!canViewPermissionManagement.value) {
    return
  }

  const loadId = requestLoadCounter + 1
  requestLoadCounter = loadId
  requestsLoading.value = true
  requestError.value = ''

  try {
    const page = await fetchPermissionRequests(requireToken(), {
      page: requestPage.value,
      size: REQUEST_PAGE_SIZE,
      search: requestSearch.value,
    })

    if (loadId !== requestLoadCounter) {
      return
    }

    requestsPage.value = page

    if (options.focusRequestId) {
      selectedRequestId.value = options.focusRequestId
      if (options.openDetail) {
        void loadRequestDetail(options.focusRequestId)
      }
      return
    }

    if (
      selectedRequestId.value !== null
      && !page.results.some((item) => item.request_id === selectedRequestId.value)
    ) {
      selectedRequestId.value = null
      selectedRequestDetail.value = null
      isDetailDialogOpen.value = false
    }
  } catch (errorValue) {
    if (loadId !== requestLoadCounter) {
      return
    }
    requestError.value = toUserFacingMessage(errorValue, 'Failed to load permission requests.')
  } finally {
    if (loadId === requestLoadCounter) {
      requestsLoading.value = false
    }
  }
}

async function loadGrants() {
  if (!canViewPermissionManagement.value) {
    return
  }

  const loadId = grantLoadCounter + 1
  grantLoadCounter = loadId
  grantsLoading.value = true
  grantError.value = ''

  try {
    const page = await fetchPermissionGrants(requireToken(), {
      page: grantPage.value,
      size: GRANT_PAGE_SIZE,
      search: grantSearch.value,
    })

    if (loadId !== grantLoadCounter) {
      return
    }

    grantsPage.value = page
  } catch (errorValue) {
    if (loadId !== grantLoadCounter) {
      return
    }
    grantError.value = toUserFacingMessage(errorValue, 'Failed to load active grants.')
  } finally {
    if (loadId === grantLoadCounter) {
      grantsLoading.value = false
    }
  }
}

async function submitRequest() {
  if (!canCreateRequests.value) {
    return
  }

  formError.value = ''
  feedback.value = ''

  const title = createForm.title.trim()
  const resourceGroupId = Number(createForm.resource_group_id)
  const instanceId = Number(createForm.instance_id)

  if (!title) {
    formError.value = 'A short title is required.'
    return
  }
  if (!resourceGroupId) {
    formError.value = 'Choose a resource group first.'
    return
  }
  if (!createForm.valid_date) {
    formError.value = 'Choose a valid end date.'
    return
  }
  if (createForm.target_type === 'instance' && !instanceId) {
    formError.value = 'Choose an instance for instance access requests.'
    return
  }

  createSubmitting.value = true

  try {
    const createdRequest = await createPermissionRequest(
      {
        title,
        reason: createForm.reason.trim(),
        target_type: createForm.target_type,
        resource_group_id: resourceGroupId,
        instance_id: createForm.target_type === 'instance' ? instanceId : undefined,
        access_level: createForm.target_type === 'instance' ? createForm.access_level : undefined,
        valid_date: createForm.valid_date,
      },
      requireToken(),
    )

    feedback.value = 'Permission request submitted.'
    activeSection.value = 'requests'
    reviewForm.audit_remark = ''
    resetCreateForm()
    closeCreateDialog()
    requestPage.value = 1
    await loadRequests({ focusRequestId: createdRequest.request_id, openDetail: true })
  } catch (errorValue) {
    formError.value = toUserFacingMessage(errorValue, 'Failed to submit the permission request.')
  } finally {
    createSubmitting.value = false
  }
}

async function submitReview(auditStatus: 1 | 2) {
  if (!selectedRequestId.value) {
    return
  }

  reviewSubmitting.value = true
  detailError.value = ''
  feedback.value = ''

  try {
    await reviewPermissionRequest(
      selectedRequestId.value,
      {
        audit_status: auditStatus,
        audit_remark: reviewForm.audit_remark.trim(),
      },
      requireToken(),
    )

    feedback.value = auditStatus === 1 ? 'Request approved.' : 'Request rejected.'
    reviewForm.audit_remark = ''

    await Promise.all([
      loadRequests({ focusRequestId: selectedRequestId.value }),
      loadRequestDetail(selectedRequestId.value),
      loadGrants(),
    ])
  } catch (errorValue) {
    detailError.value = toUserFacingMessage(errorValue, 'Failed to submit the review action.')
  } finally {
    reviewSubmitting.value = false
  }
}

async function revokeGrant(grant: PermissionGrantRecord) {
  const grantKey = `${grant.grant_type}-${grant.grant_id}`
  revokingGrantKey.value = grantKey
  grantError.value = ''
  feedback.value = ''

  try {
    await revokePermissionGrant(grant.grant_type, grant.grant_id, requireToken())
    feedback.value = 'Grant revoked.'
    await loadGrants()
  } catch (errorValue) {
    grantError.value = toUserFacingMessage(errorValue, 'Failed to revoke the selected grant.')
  } finally {
    if (revokingGrantKey.value === grantKey) {
      revokingGrantKey.value = ''
    }
  }
}

function openRequestFromGrant(requestId: number | null) {
  if (!requestId) {
    return
  }

  activeSection.value = 'requests'
  void loadRequestDetail(requestId)
}

const debouncedLoadRequests = useDebounceFn(() => {
  void loadRequests()
}, 250)

const debouncedLoadGrants = useDebounceFn(() => {
  void loadGrants()
}, 250)

watch(requestPage, () => {
  void loadRequests()
})

watch(grantPage, () => {
  void loadGrants()
})

watch(requestSearch, () => {
  if (requestPage.value !== 1) {
    requestPage.value = 1
    return
  }
  debouncedLoadRequests()
})

watch(grantSearch, () => {
  if (grantPage.value !== 1) {
    grantPage.value = 1
    return
  }
  debouncedLoadGrants()
})

watch(activeSection, (section) => {
  if (section === 'grants' && grantsPage.value.count === 0 && !grantsLoading.value && !grantError.value) {
    void loadGrants()
  }
})

onMounted(async () => {
  await authStore.loadCurrentUser()

  if (!canViewPermissionManagement.value) {
    return
  }

  await Promise.all([
    loadRequests(),
    loadGrants(),
    loadLookups(),
  ])
})
</script>

<template>
  <section class="grid gap-6">
    <div class="flex flex-col gap-4 rounded-2xl border border-slate-200 bg-white p-5 shadow-sm lg:flex-row lg:items-center lg:justify-between">
      <div class="space-y-1">
        <h1 class="text-2xl font-semibold text-slate-900">Permission Management</h1>
        <p class="text-sm text-slate-500">
          Request access, review approvals, and manage active temporary grants.
        </p>
      </div>

      <div class="flex flex-wrap items-center gap-2">
        <Button
          :variant="activeSection === 'requests' ? 'default' : 'outline'"
          type="button"
          @click="activeSection = 'requests'"
        >
          Requests
        </Button>
        <Button
          :variant="activeSection === 'grants' ? 'default' : 'outline'"
          type="button"
          @click="activeSection = 'grants'"
        >
          Active Access
        </Button>
        <Button
          v-if="canCreateRequests"
          type="button"
          class="gap-2"
          @click="openCreateDialog"
        >
          <Plus class="h-4 w-4" />
          Request access
        </Button>
      </div>
    </div>

    <p
      v-if="feedback"
      class="rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700"
    >
      {{ feedback }}
    </p>

    <Card v-if="!canViewPermissionManagement" class="border-red-200">
      <CardHeader>
        <CardTitle>Access denied</CardTitle>
        <CardDescription>
          `sql.menu_queryapplylist` is required to use the permission management SPA.
        </CardDescription>
      </CardHeader>
    </Card>

    <template v-else-if="activeSection === 'requests'">
      <Card class="border-slate-200">
        <CardHeader class="gap-4">
          <div class="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
            <div>
              <CardTitle>Requests</CardTitle>
              <CardDescription>
                Your requests plus anything currently assigned to you for review.
              </CardDescription>
            </div>
            <div class="flex flex-wrap items-center gap-2">
              <Badge variant="outline" class="border-slate-200 bg-slate-50 text-slate-600">
                {{ requestsPage.count }} total
              </Badge>
              <Button
                v-if="canCreateRequests"
                variant="outline"
                type="button"
                class="gap-2"
                @click="openCreateDialog"
              >
                <ShieldPlus class="h-4 w-4" />
                New request
              </Button>
              <Button variant="outline" type="button" class="gap-2" @click="void loadRequests()">
                <RefreshCw class="h-4 w-4" />
                Refresh
              </Button>
            </div>
          </div>
          <p
            v-if="!canCreateRequests"
            class="rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-600"
          >
            `sql.query_applypriv` is required to submit a new request.
          </p>
          <div class="grid gap-3 lg:grid-cols-[minmax(0,1fr)_auto]">
            <Input
              v-model="requestSearch"
              placeholder="Search title, requester, group, or instance"
            />
            <Button variant="outline" type="button" class="gap-2" @click="void loadRequests()">
              <RefreshCw class="h-4 w-4" />
              Refresh
            </Button>
          </div>
        </CardHeader>
        <CardContent class="space-y-4">
          <p
            v-if="requestError"
            class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700"
          >
            {{ requestError }}
          </p>

          <div
            v-if="requestsLoading"
            class="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-10 text-sm text-slate-500"
          >
            Loading requests...
          </div>

          <div
            v-else-if="requestsPage.results.length === 0"
            class="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-10 text-sm text-slate-500"
          >
            No permission requests match the current filters.
          </div>

          <div v-else class="grid gap-3">
            <button
              v-for="requestItem in requestsPage.results"
              :key="requestItem.request_id"
              type="button"
              class="grid gap-3 rounded-2xl border border-slate-200 bg-white p-4 text-left transition hover:border-slate-300 hover:bg-slate-50"
              @click="void loadRequestDetail(requestItem.request_id)"
            >
              <div class="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
                <div class="space-y-2">
                  <div class="flex flex-wrap items-center gap-2">
                    <p class="font-medium text-slate-900">{{ requestItem.title }}</p>
                    <Badge variant="outline" :class="statusClass(requestItem.status)">
                      {{ statusLabel(requestItem.status) }}
                    </Badge>
                    <Badge variant="outline" class="border-slate-200 bg-slate-50 text-slate-600">
                      {{ targetLabel(requestItem.target_type) }}
                    </Badge>
                  </div>
                  <p class="text-sm text-slate-500">
                    {{ requestItem.resource_group_name }}
                    <span v-if="requestItem.instance_name"> / {{ requestItem.instance_name }}</span>
                    <span v-if="requestItem.access_level"> / {{ accessLevelLabel(requestItem.access_level) }}</span>
                  </p>
                </div>
                <div class="flex items-center gap-2 text-sm text-slate-500">
                  <span>{{ requestItem.user_display }}</span>
                  <span>•</span>
                  <span>{{ formatDate(requestItem.valid_date) }}</span>
                  <ExternalLink class="h-4 w-4" />
                </div>
              </div>
              <p v-if="requestItem.reason" class="line-clamp-2 text-sm text-slate-600">
                {{ requestItem.reason }}
              </p>
            </button>
          </div>

          <div class="flex items-center justify-between border-t border-slate-100 pt-4 text-sm text-slate-500">
            <span>
              Page {{ requestPage }}
              <span v-if="requestsPage.count > 0">
                of {{ Math.max(1, Math.ceil(requestsPage.count / REQUEST_PAGE_SIZE)) }}
              </span>
            </span>
            <div class="flex gap-2">
              <Button
                variant="outline"
                size="sm"
                type="button"
                :disabled="!requestsPage.previous"
                @click="requestPage = Math.max(1, requestPage - 1)"
              >
                Previous
              </Button>
              <Button
                variant="outline"
                size="sm"
                type="button"
                :disabled="!requestsPage.next"
                @click="requestPage += 1"
              >
                Next
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>
    </template>

    <template v-else>
      <Card class="border-slate-200">
        <CardHeader class="gap-4">
          <div class="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
            <div>
              <CardTitle>Active Access</CardTitle>
              <CardDescription>
                Review currently active temporary grants and revoke them when needed.
              </CardDescription>
            </div>
            <Badge variant="outline" class="border-slate-200 bg-slate-50 text-slate-600">
              {{ grantsPage.count }} active
            </Badge>
          </div>
          <div class="grid gap-3 lg:grid-cols-[minmax(0,1fr)_auto]">
            <Input
              v-model="grantSearch"
              placeholder="Search user, group, instance, or access level"
            />
            <Button variant="outline" type="button" class="gap-2" @click="void loadGrants()">
              <RefreshCw class="h-4 w-4" />
              Refresh
            </Button>
          </div>
        </CardHeader>
        <CardContent class="space-y-4">
          <p
            v-if="grantError"
            class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700"
          >
            {{ grantError }}
          </p>

          <div
            v-if="grantsLoading"
            class="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-10 text-sm text-slate-500"
          >
            Loading active grants...
          </div>

          <div
            v-else-if="grantsPage.results.length === 0"
            class="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-10 text-sm text-slate-500"
          >
            No active grants match the current filters.
          </div>

          <div v-else class="grid gap-3">
            <div
              v-for="grant in grantsPage.results"
              :key="`${grant.grant_type}-${grant.grant_id}`"
              class="grid gap-3 rounded-2xl border border-slate-200 bg-white p-4"
            >
              <div class="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
                <div class="space-y-2">
                  <div class="flex flex-wrap items-center gap-2">
                    <p class="font-medium text-slate-900">{{ grant.user_display }}</p>
                    <Badge variant="outline" class="border-slate-200 bg-slate-50 text-slate-600">
                      {{ grantTypeLabel(grant.grant_type) }}
                    </Badge>
                    <Badge
                      v-if="grant.access_level"
                      variant="outline"
                      class="border-sky-200 bg-sky-50 text-sky-700"
                    >
                      {{ accessLevelLabel(grant.access_level) }}
                    </Badge>
                  </div>
                  <p class="text-sm text-slate-500">
                    {{ grant.resource_group_name }}
                    <span v-if="grant.instance_name"> / {{ grant.instance_name }}</span>
                  </p>
                </div>
                <div class="text-sm text-slate-500">
                  Valid until {{ formatDate(grant.valid_date) }}
                </div>
              </div>

              <div class="flex flex-wrap items-center gap-2">
                <Button
                  v-if="grant.source_request_id"
                  variant="outline"
                  size="sm"
                  type="button"
                  class="gap-2"
                  @click="openRequestFromGrant(grant.source_request_id)"
                >
                  <ExternalLink class="h-4 w-4" />
                  View request
                </Button>
                <Button
                  v-if="canManageGrants"
                  variant="outline"
                  size="sm"
                  type="button"
                  class="gap-2 border-rose-200 text-rose-700 hover:bg-rose-50 hover:text-rose-700"
                  :disabled="revokingGrantKey === `${grant.grant_type}-${grant.grant_id}`"
                  @click="void revokeGrant(grant)"
                >
                  <Trash2 class="h-4 w-4" />
                  Revoke
                </Button>
              </div>
            </div>
          </div>

          <div class="flex items-center justify-between border-t border-slate-100 pt-4 text-sm text-slate-500">
            <span>
              Page {{ grantPage }}
              <span v-if="grantsPage.count > 0">
                of {{ Math.max(1, Math.ceil(grantsPage.count / GRANT_PAGE_SIZE)) }}
              </span>
            </span>
            <div class="flex gap-2">
              <Button
                variant="outline"
                size="sm"
                type="button"
                :disabled="!grantsPage.previous"
                @click="grantPage = Math.max(1, grantPage - 1)"
              >
                Previous
              </Button>
              <Button
                variant="outline"
                size="sm"
                type="button"
                :disabled="!grantsPage.next"
                @click="grantPage += 1"
              >
                Next
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>
    </template>

    <div
      v-if="isCreateDialogOpen"
      class="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/45 p-4"
      @click.self="closeCreateDialog"
    >
      <div class="max-h-[90vh] w-full max-w-3xl overflow-y-auto rounded-3xl bg-white shadow-2xl">
        <div class="sticky top-0 flex items-start justify-between gap-4 border-b border-slate-200 bg-white px-6 py-5">
          <div>
            <h2 class="text-xl font-semibold text-slate-900">Request permission</h2>
            <p class="mt-1 text-sm text-slate-500">
              Create a temporary access request for a resource group or a single instance.
            </p>
          </div>
          <Button variant="ghost" size="icon" type="button" @click="closeCreateDialog">
            <X class="h-4 w-4" />
          </Button>
        </div>

        <div class="space-y-5 px-6 py-6">
          <p
            v-if="formError"
            class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700"
          >
            {{ formError }}
          </p>
          <p
            v-else-if="lookupsError"
            class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700"
          >
            {{ lookupsError }}
          </p>

          <div class="grid gap-4 md:grid-cols-2">
            <div class="space-y-2">
              <label class="text-sm font-medium text-slate-700" for="request-title">Title</label>
              <Input
                id="request-title"
                v-model="createForm.title"
                :disabled="createSubmitting || lookupsLoading"
                placeholder="Short summary of the access need"
              />
            </div>
            <div class="space-y-2">
              <label class="text-sm font-medium text-slate-700" for="request-target-type">Target type</label>
              <select
                id="request-target-type"
                v-model="createForm.target_type"
                :class="selectClass"
                :disabled="createSubmitting || lookupsLoading"
              >
                <option value="resource_group">Resource group</option>
                <option value="instance">Instance</option>
              </select>
            </div>
          </div>

          <div class="grid gap-4 md:grid-cols-2">
            <div class="space-y-2">
              <label class="text-sm font-medium text-slate-700" for="request-resource-group">Resource group</label>
              <select
                id="request-resource-group"
                v-model="createForm.resource_group_id"
                :class="selectClass"
                :disabled="createSubmitting || lookupsLoading"
              >
                <option value="">Select a resource group</option>
                <option
                  v-for="resourceGroup in resourceGroups"
                  :key="resourceGroup.group_id"
                  :value="`${resourceGroup.group_id}`"
                >
                  {{ resourceGroup.group_name }}
                </option>
              </select>
            </div>
            <div class="space-y-2">
              <label class="text-sm font-medium text-slate-700" for="request-valid-date">Valid until</label>
              <Input
                id="request-valid-date"
                v-model="createForm.valid_date"
                :disabled="createSubmitting || lookupsLoading"
                type="date"
              />
            </div>
          </div>

                <div class="flex flex-wrap gap-2">
                  <Button variant="outline" size="sm" type="button" @click="setQuickValidDate(1)">1 day</Button>
                  <Button variant="outline" size="sm" type="button" @click="setQuickValidDate(7)">7 days</Button>
                  <Button variant="outline" size="sm" type="button" @click="setQuickValidDate(30)">30 days</Button>
                  <Button variant="outline" size="sm" type="button" @click="setQuickValidDate(365)">1 year</Button>
                  <Button variant="outline" size="sm" type="button" @click="setUnlimitedValidDate()">Unlimited</Button>
                </div>

          <div v-if="createForm.target_type === 'instance'" class="grid gap-4 md:grid-cols-2">
            <div class="space-y-2">
              <label class="text-sm font-medium text-slate-700" for="request-instance">Instance</label>
              <select
                id="request-instance"
                v-model="createForm.instance_id"
                :class="selectClass"
                :disabled="createSubmitting || lookupsLoading"
              >
                <option value="">Select an instance</option>
                <option
                  v-for="instance in filteredInstances"
                  :key="instance.id"
                  :value="`${instance.id}`"
                >
                  {{ instance.instance_name }} · {{ instance.host }}
                </option>
              </select>
            </div>
            <div class="space-y-2">
              <label class="text-sm font-medium text-slate-700" for="request-access-level">Access level</label>
              <select
                id="request-access-level"
                v-model="createForm.access_level"
                :class="selectClass"
                :disabled="createSubmitting || lookupsLoading"
              >
                <option value="query">Query only</option>
                <option value="query_dml">Query + DML</option>
                <option value="query_dml_ddl">Query + DML + DDL</option>
              </select>
            </div>
          </div>

          <div class="space-y-2">
            <label class="text-sm font-medium text-slate-700" for="request-reason">Reason</label>
            <textarea
              id="request-reason"
              v-model="createForm.reason"
              :class="textareaClass"
              :disabled="createSubmitting || lookupsLoading"
              placeholder="Why is the access needed and what work will it support?"
            />
          </div>
        </div>

        <div class="sticky bottom-0 flex items-center justify-end gap-3 border-t border-slate-200 bg-white px-6 py-4">
          <Button variant="outline" type="button" @click="closeCreateDialog">Cancel</Button>
          <Button type="button" :disabled="createSubmitting || lookupsLoading" @click="void submitRequest()">
            Submit request
          </Button>
        </div>
      </div>
    </div>

    <div
      v-if="isDetailDialogOpen"
      class="fixed inset-0 z-50 flex justify-end bg-slate-950/45"
      @click.self="closeDetailDialog"
    >
      <div class="flex h-full w-full max-w-2xl flex-col bg-white shadow-2xl">
        <div class="flex items-start justify-between gap-4 border-b border-slate-200 px-6 py-5">
          <div>
            <h2 class="text-xl font-semibold text-slate-900">Approval flow</h2>
            <p class="mt-1 text-sm text-slate-500">
              Review request details, approval stages, and audit history.
            </p>
          </div>
          <Button variant="ghost" size="icon" type="button" @click="closeDetailDialog">
            <X class="h-4 w-4" />
          </Button>
        </div>

        <div class="flex-1 overflow-y-auto px-6 py-6">
          <p
            v-if="detailError"
            class="mb-4 rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700"
          >
            {{ detailError }}
          </p>

          <div
            v-if="detailLoading"
            class="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-10 text-sm text-slate-500"
          >
            Loading request detail...
          </div>

          <div
            v-else-if="!selectedRequestSummary"
            class="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-10 text-sm text-slate-500"
          >
            Select a request to inspect its approval flow.
          </div>

          <div v-else class="space-y-6">
            <div class="space-y-3 rounded-2xl border border-slate-200 bg-slate-50 p-4">
              <div class="flex flex-wrap items-center gap-2">
                <p class="text-lg font-semibold text-slate-900">{{ selectedRequestSummary.title }}</p>
                <Badge variant="outline" :class="statusClass(selectedRequestSummary.status)">
                  {{ statusLabel(selectedRequestSummary.status) }}
                </Badge>
              </div>
              <div class="grid gap-3 text-sm text-slate-600 md:grid-cols-2">
                <div>
                  <p class="text-slate-400">Requester</p>
                  <p>{{ selectedRequestSummary.user_display }}</p>
                </div>
                <div>
                  <p class="text-slate-400">Target</p>
                  <p>{{ targetLabel(selectedRequestSummary.target_type) }}</p>
                </div>
                <div>
                  <p class="text-slate-400">Resource group</p>
                  <p>{{ selectedRequestSummary.resource_group_name }}</p>
                </div>
                <div>
                  <p class="text-slate-400">Instance</p>
                  <p>{{ selectedRequestSummary.instance_name || 'Not applicable' }}</p>
                </div>
                <div>
                  <p class="text-slate-400">Access level</p>
                  <p>{{ accessLevelLabel(selectedRequestSummary.access_level) }}</p>
                </div>
                <div>
                  <p class="text-slate-400">Valid until</p>
                  <p>{{ formatDate(selectedRequestSummary.valid_date) }}</p>
                </div>
              </div>
              <div v-if="selectedRequestSummary.reason" class="text-sm text-slate-600">
                <p class="mb-1 text-slate-400">Reason</p>
                <p>{{ selectedRequestSummary.reason }}</p>
              </div>
            </div>

            <div class="space-y-3">
              <div class="flex items-center gap-2">
                <Clock3 class="h-4 w-4 text-slate-500" />
                <h3 class="font-medium text-slate-900">Approval flow</h3>
              </div>
              <div class="space-y-3">
                <div
                  v-for="(node, index) in selectedRequestDetail?.review_info ?? []"
                  :key="`${node.group_name}-${index}`"
                  class="rounded-2xl border p-4"
                  :class="
                    node.is_current_node
                      ? 'border-amber-200 bg-amber-50'
                      : node.is_passed_node
                        ? 'border-emerald-200 bg-emerald-50'
                        : 'border-slate-200 bg-white'
                  "
                >
                  <div class="flex items-center justify-between gap-3">
                    <p class="font-medium text-slate-900">{{ node.group_name }}</p>
                    <Badge
                      variant="outline"
                      :class="
                        node.is_current_node
                          ? 'border-amber-200 bg-amber-100 text-amber-700'
                          : node.is_passed_node
                            ? 'border-emerald-200 bg-emerald-100 text-emerald-700'
                            : 'border-slate-200 bg-slate-50 text-slate-600'
                      "
                    >
                      {{ node.is_current_node ? 'Current' : node.is_passed_node ? 'Passed' : 'Pending' }}
                    </Badge>
                  </div>
                </div>
              </div>
            </div>

            <div class="space-y-3">
              <div class="flex items-center gap-2">
                <CheckCircle2 class="h-4 w-4 text-slate-500" />
                <h3 class="font-medium text-slate-900">Audit history</h3>
              </div>
              <div
                v-if="(selectedRequestDetail?.logs?.length ?? 0) === 0"
                class="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-8 text-sm text-slate-500"
              >
                No audit log entries yet.
              </div>
              <div v-else class="space-y-3">
                <div
                  v-for="(log, index) in selectedRequestDetail?.logs ?? []"
                  :key="`${log.operation_time}-${index}`"
                  class="rounded-2xl border border-slate-200 bg-white p-4"
                >
                  <div class="flex flex-wrap items-center justify-between gap-2">
                    <p class="font-medium text-slate-900">{{ log.operation_type_desc }}</p>
                    <span class="text-xs text-slate-400">{{ formatDateTime(log.operation_time) }}</span>
                  </div>
                  <p class="mt-2 text-sm text-slate-600">{{ log.operation_info }}</p>
                  <p class="mt-2 text-xs uppercase tracking-wide text-slate-400">
                    {{ log.operator_display }}
                  </p>
                </div>
              </div>
            </div>

            <div
              v-if="selectedRequestDetail?.is_can_review && canReviewRequests"
              class="space-y-3 rounded-2xl border border-slate-200 bg-slate-50 p-4"
            >
              <h3 class="font-medium text-slate-900">Review action</h3>
              <textarea
                v-model="reviewForm.audit_remark"
                :class="textareaClass"
                :disabled="reviewSubmitting"
                placeholder="Add an approval note"
              />
              <div class="flex flex-wrap gap-2">
                <Button
                  type="button"
                  class="gap-2"
                  :disabled="reviewSubmitting"
                  @click="void submitReview(1)"
                >
                  <CheckCircle2 class="h-4 w-4" />
                  Approve
                </Button>
                <Button
                  variant="outline"
                  type="button"
                  class="gap-2 border-rose-200 text-rose-700 hover:bg-rose-50 hover:text-rose-700"
                  :disabled="reviewSubmitting"
                  @click="void submitReview(2)"
                >
                  <XCircle class="h-4 w-4" />
                  Reject
                </Button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </section>
</template>
