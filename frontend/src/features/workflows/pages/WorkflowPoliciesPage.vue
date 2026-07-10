<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import {
  ArrowDown,
  ArrowRight,
  ArrowUp,
  Check,
  CheckCircle2,
  GitBranch,
  Plus,
  Save,
  Search,
  Trash2,
  UserRound,
  UsersRound,
} from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { useAuthStore } from '@/stores/auth'
import {
  createWorkflowPolicy,
  deleteWorkflowPolicy,
  fetchWorkflowPolicies,
  fetchWorkflowPolicyMetadata,
  updateWorkflowPolicy,
  type WorkflowPolicyRecord,
} from '../api'

type EditorStep = {
  permission_group: number | null
}

const authStore = useAuthStore()
const policies = ref<WorkflowPolicyRecord[]>([])
const permissionGroups = ref<Array<{ id: number; name: string }>>([])
const selectedPolicyId = ref<number | null>(null)
const loading = ref(false)
const saving = ref(false)
const deleting = ref(false)
const error = ref('')
const feedback = ref('')
const search = ref('')

const form = reactive({
  name: '',
  description: '',
  is_active: true,
  steps: [] as EditorStep[],
})

const selectedPolicy = computed(() => {
  return policies.value.find((policy) => policy.id === selectedPolicyId.value) ?? null
})

const filteredPolicies = computed(() => {
  const needle = search.value.trim().toLowerCase()
  if (!needle) {
    return policies.value
  }
  return policies.value.filter((policy) =>
    `${policy.name} ${policy.description}`.toLowerCase().includes(needle),
  )
})

const canEdit = computed(() => {
  return !selectedPolicy.value || selectedPolicy.value.can_edit
})

const activePolicyCount = computed(() => {
  return policies.value.filter((policy) => policy.is_active).length
})

function requireToken() {
  if (!authStore.accessToken) {
    throw new Error('Missing access token. Please login again.')
  }
  return authStore.accessToken
}

function toUserFacingMessage(errorValue: unknown, fallback: string) {
  return errorValue instanceof Error ? errorValue.message : fallback
}

function roleName(roleId: number | null) {
  if (!roleId) {
    return 'Select role'
  }
  return permissionGroups.value.find((group) => group.id === roleId)?.name ?? `Role ${roleId}`
}

function resetForm() {
  selectedPolicyId.value = null
  form.name = ''
  form.description = ''
  form.is_active = true
  form.steps = [{ permission_group: null }]
  feedback.value = ''
  error.value = ''
}

function selectPolicy(policy: WorkflowPolicyRecord) {
  selectedPolicyId.value = policy.id
  form.name = policy.name
  form.description = policy.description
  form.is_active = policy.is_active
  form.steps = policy.steps.map((step) => ({ permission_group: step.permission_group }))
  if (form.steps.length === 0) {
    form.steps = [{ permission_group: null }]
  }
  feedback.value = ''
  error.value = ''
}

function addStep() {
  form.steps.push({ permission_group: null })
}

function removeStep(index: number) {
  form.steps.splice(index, 1)
  if (form.steps.length === 0) {
    addStep()
  }
}

function moveStep(index: number, direction: -1 | 1) {
  const target = index + direction
  if (target < 0 || target >= form.steps.length) {
    return
  }
  const [step] = form.steps.splice(index, 1)
  if (!step) {
    return
  }
  form.steps.splice(target, 0, step)
}

async function loadData() {
  loading.value = true
  error.value = ''
  try {
    const [policyPayload, metadata] = await Promise.all([
      fetchWorkflowPolicies(requireToken()),
      fetchWorkflowPolicyMetadata(requireToken()),
    ])
    policies.value = policyPayload.results
    permissionGroups.value = metadata.permission_groups
    if (selectedPolicyId.value) {
      const refreshed = policies.value.find((policy) => policy.id === selectedPolicyId.value)
      if (refreshed) {
        selectPolicy(refreshed)
      } else {
        const firstPolicy = policies.value[0]
        if (firstPolicy) {
          selectPolicy(firstPolicy)
        } else {
          resetForm()
        }
      }
    } else {
      const firstPolicy = policies.value[0]
      if (firstPolicy) {
        selectPolicy(firstPolicy)
      } else {
        resetForm()
      }
    }
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to load workflow policies.')
  } finally {
    loading.value = false
  }
}

async function savePolicy() {
  if (!form.name.trim()) {
    error.value = 'Policy name is required.'
    return
  }
  const steps = form.steps
    .filter(
      (step): step is EditorStep & { permission_group: number } => step.permission_group !== null,
    )
    .map((step, index) => ({ order: index + 1, permission_group: step.permission_group }))
  if (steps.length === 0) {
    error.value = 'Add at least one approval role.'
    return
  }
  saving.value = true
  error.value = ''
  try {
    const payload = {
      name: form.name.trim(),
      description: form.description.trim(),
      is_active: form.is_active,
      steps,
    }
    const saved = selectedPolicyId.value
      ? await updateWorkflowPolicy(selectedPolicyId.value, payload, requireToken())
      : await createWorkflowPolicy(payload, requireToken())
    selectedPolicyId.value = saved.id
    await loadData()
    feedback.value = 'Policy saved.'
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to save workflow policy.')
  } finally {
    saving.value = false
  }
}

async function deletePolicyAction() {
  if (!selectedPolicyId.value) {
    return
  }
  if (!window.confirm('Delete this workflow policy?')) {
    return
  }
  deleting.value = true
  error.value = ''
  try {
    await deleteWorkflowPolicy(selectedPolicyId.value, requireToken())
    selectedPolicyId.value = null
    await loadData()
    feedback.value = 'Policy deleted.'
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to delete workflow policy.')
  } finally {
    deleting.value = false
  }
}

onMounted(() => {
  void loadData()
})
</script>

<template>
  <main class="mx-auto flex w-full max-w-[1500px] flex-col gap-5 px-4 py-6 sm:px-6 lg:px-8">
    <div class="flex flex-wrap items-end justify-between gap-4">
      <div>
        <p class="mb-1 text-xs font-semibold uppercase tracking-wider text-slate-500">
          Access control
        </p>
        <h1 class="text-2xl font-semibold tracking-tight text-slate-950">Workflow policies</h1>
        <p class="mt-1 text-sm text-slate-500">
          Define who reviews SQL requests, and in what order.
        </p>
      </div>
      <Button type="button" class="shadow-sm" @click="resetForm">
        <Plus class="h-4 w-4" />
        New policy
      </Button>
    </div>

    <p
      v-if="error"
      class="rounded-md border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700"
    >
      {{ error }}
    </p>
    <p
      v-if="feedback"
      class="rounded-md border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700"
    >
      {{ feedback }}
    </p>

    <div
      class="grid min-h-[700px] overflow-hidden rounded-xl border border-slate-200 bg-white shadow-sm lg:grid-cols-[320px_minmax(0,1fr)]"
    >
      <aside
        class="flex min-h-0 flex-col border-b border-slate-200 bg-slate-50/70 lg:border-b-0 lg:border-r"
      >
        <div class="border-b border-slate-200 p-4">
          <div class="mb-3 flex items-center justify-between gap-3">
            <div>
              <h2 class="text-sm font-semibold text-slate-950">All policies</h2>
              <p class="mt-0.5 text-xs text-slate-500">
                {{ policies.length }} total · {{ activePolicyCount }} active
              </p>
            </div>
          </div>
          <div class="relative">
            <Search
              class="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-400"
            />
            <Input
              v-model="search"
              class="bg-white pl-9"
              placeholder="Search by name or description"
            />
          </div>
        </div>
        <div class="max-h-[320px] flex-1 overflow-y-auto p-2 lg:max-h-none">
          <p v-if="loading" class="px-3 py-10 text-center text-sm text-slate-500">
            Loading policies…
          </p>
          <button
            v-for="policy in filteredPolicies"
            :key="policy.id"
            type="button"
            class="group mb-1 flex w-full items-start gap-3 rounded-lg border px-3 py-3 text-left transition"
            :class="
              selectedPolicyId === policy.id
                ? 'border-slate-300 bg-white shadow-sm'
                : 'border-transparent hover:border-slate-200 hover:bg-white/80'
            "
            @click="selectPolicy(policy)"
          >
            <span
              class="mt-1.5 h-2 w-2 shrink-0 rounded-full"
              :class="policy.is_active ? 'bg-emerald-500' : 'bg-slate-300'"
              aria-hidden="true"
            />
            <span class="min-w-0 flex-1">
              <span class="block truncate text-sm font-semibold text-slate-900">{{
                policy.name
              }}</span>
              <span class="mt-1 line-clamp-2 block text-xs leading-5 text-slate-500">
                {{ policy.description || 'No description provided.' }}
              </span>
              <span class="mt-2 flex items-center gap-1.5 text-xs font-medium text-slate-600">
                <UsersRound class="h-3.5 w-3.5" />
                {{ policy.steps.length }} approval
                {{ policy.steps.length === 1 ? 'step' : 'steps' }}
              </span>
            </span>
            <Check
              v-if="selectedPolicyId === policy.id"
              class="mt-0.5 h-4 w-4 shrink-0 text-slate-900"
            />
          </button>
          <div v-if="!loading && filteredPolicies.length === 0" class="px-4 py-12 text-center">
            <Search class="mx-auto h-5 w-5 text-slate-400" />
            <p class="mt-2 text-sm font-medium text-slate-700">No policies found</p>
            <p class="mt-1 text-xs text-slate-500">Try a different search.</p>
          </div>
        </div>
      </aside>

      <section class="min-w-0 bg-white">
        <div
          class="flex flex-wrap items-center justify-between gap-4 border-b border-slate-200 px-5 py-4 sm:px-6"
        >
          <div class="min-w-0">
            <div class="flex items-center gap-2">
              <h2 class="truncate text-lg font-semibold text-slate-950">
                {{ form.name || 'Create policy' }}
              </h2>
              <Badge :variant="form.is_active ? 'default' : 'outline'">
                {{ form.is_active ? 'Active' : 'Inactive' }}
              </Badge>
            </div>
            <p class="mt-1 text-sm text-slate-500">
              {{
                selectedPolicyId
                  ? 'Review the flow and update its configuration.'
                  : 'Set up a new approval path for SQL requests.'
              }}
            </p>
          </div>
          <div class="flex items-center gap-2">
            <Button
              v-if="selectedPolicyId"
              type="button"
              variant="outline"
              :disabled="!canEdit || deleting"
              @click="deletePolicyAction"
            >
              <Trash2 class="h-4 w-4" />
              Delete
            </Button>
            <Button type="button" :disabled="!canEdit || saving" @click="savePolicy">
              <Save class="h-4 w-4" />
              {{ saving ? 'Saving…' : selectedPolicyId ? 'Save changes' : 'Create policy' }}
            </Button>
          </div>
        </div>

        <div
          v-if="!canEdit"
          class="border-b border-amber-200 bg-amber-50 px-6 py-2.5 text-sm text-amber-800"
        >
          You can view this policy, but you don’t have permission to edit it.
        </div>

        <div class="grid gap-8 p-5 sm:p-6 xl:grid-cols-[minmax(0,1fr)_360px]">
          <div class="min-w-0 space-y-8">
            <section>
              <div class="mb-3 flex items-end justify-between gap-4">
                <div>
                  <h3 class="text-sm font-semibold text-slate-900">Approval path</h3>
                  <p class="mt-1 text-xs text-slate-500">
                    Requests move from left to right through every reviewer.
                  </p>
                </div>
                <span class="text-xs font-medium text-slate-500">
                  {{ form.steps.length }} {{ form.steps.length === 1 ? 'reviewer' : 'reviewers' }}
                </span>
              </div>
              <div class="rounded-xl border border-slate-200 bg-slate-50/70 p-4">
                <div class="flex w-full flex-wrap items-center gap-2">
                  <div
                    class="flex h-20 min-w-24 flex-1 flex-col items-center justify-center gap-2 rounded-lg border border-slate-200 bg-white px-2 shadow-sm"
                  >
                    <UserRound class="h-5 w-5 text-slate-500" />
                    <span class="text-sm font-medium text-slate-800">Submitter</span>
                  </div>
                  <template v-for="(step, index) in form.steps" :key="index">
                    <div class="flex w-5 shrink-0 items-center justify-center text-slate-400">
                      <ArrowRight class="h-4 w-4" />
                    </div>
                    <div
                      class="relative flex h-20 min-w-28 flex-1 flex-col items-center justify-center gap-1 rounded-lg border bg-white px-2 text-center shadow-sm"
                      :class="
                        step.permission_group ? 'border-sky-300' : 'border-dashed border-amber-300'
                      "
                    >
                      <span
                        class="absolute -top-2.5 rounded-full bg-slate-800 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-white"
                      >
                        Step {{ index + 1 }}
                      </span>
                      <GitBranch class="h-4 w-4 text-sky-600" />
                      <span class="max-w-full truncate text-sm font-semibold text-slate-900">{{
                        roleName(step.permission_group)
                      }}</span>
                    </div>
                  </template>
                  <div class="flex w-5 shrink-0 items-center justify-center text-slate-400">
                    <ArrowRight class="h-4 w-4" />
                  </div>
                  <div
                    class="flex h-20 min-w-24 flex-1 flex-col items-center justify-center gap-2 rounded-lg border border-emerald-200 bg-emerald-50 px-2 text-emerald-800"
                  >
                    <CheckCircle2 class="h-5 w-5" />
                    <span class="text-sm font-semibold">Approved</span>
                  </div>
                </div>
              </div>
            </section>

            <section>
              <div class="mb-3 flex items-center justify-between gap-3">
                <div>
                  <h3 class="text-sm font-semibold text-slate-900">Approval steps</h3>
                  <p class="mt-1 text-xs text-slate-500">
                    Choose a role for each stage and arrange the review order.
                  </p>
                </div>
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  :disabled="!canEdit"
                  @click="addStep"
                >
                  <Plus class="h-4 w-4" />
                  Add step
                </Button>
              </div>
              <div class="space-y-2">
                <div
                  v-for="(step, index) in form.steps"
                  :key="index"
                  class="flex flex-wrap items-center gap-3 rounded-lg border border-slate-200 bg-white p-3"
                >
                  <span
                    class="flex h-8 w-8 shrink-0 items-center justify-center rounded-full bg-slate-100 text-xs font-bold text-slate-700"
                  >
                    {{ index + 1 }}
                  </span>
                  <div class="min-w-[180px] flex-1">
                    <label :for="`approval-step-${index}`" class="sr-only"
                      >Role for step {{ index + 1 }}</label
                    >
                    <select
                      :id="`approval-step-${index}`"
                      v-model.number="step.permission_group"
                      :disabled="!canEdit"
                      class="block h-10 w-full rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-800 outline-none focus:border-slate-400 disabled:bg-slate-100"
                    >
                      <option :value="null">Select an approval role</option>
                      <option v-for="group in permissionGroups" :key="group.id" :value="group.id">
                        {{ group.name }}
                      </option>
                    </select>
                  </div>
                  <div class="flex items-center gap-1">
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      :disabled="!canEdit || index === 0"
                      :aria-label="`Move step ${index + 1} up`"
                      @click="moveStep(index, -1)"
                    >
                      <ArrowUp class="h-4 w-4" />
                    </Button>
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      :disabled="!canEdit || index === form.steps.length - 1"
                      :aria-label="`Move step ${index + 1} down`"
                      @click="moveStep(index, 1)"
                    >
                      <ArrowDown class="h-4 w-4" />
                    </Button>
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      :disabled="!canEdit"
                      :aria-label="`Remove step ${index + 1}`"
                      @click="removeStep(index)"
                    >
                      <Trash2 class="h-4 w-4" />
                    </Button>
                  </div>
                </div>
              </div>
            </section>
          </div>

          <aside
            class="order-first h-fit rounded-xl border border-slate-200 bg-slate-50/60 p-4 sm:p-5 xl:order-last"
          >
            <div class="mb-5 flex items-center gap-2">
              <div
                class="flex h-8 w-8 items-center justify-center rounded-lg bg-white shadow-sm ring-1 ring-slate-200"
              >
                <GitBranch class="h-4 w-4 text-slate-600" />
              </div>
              <div>
                <h3 class="text-sm font-semibold text-slate-900">Policy details</h3>
                <p class="text-xs text-slate-500">Shown to requesters and reviewers.</p>
              </div>
            </div>
            <div class="grid gap-5">
              <label class="grid gap-2">
                <span class="text-sm font-medium text-slate-700">Policy name</span>
                <Input
                  v-model="form.name"
                  :disabled="!canEdit"
                  placeholder="e.g. Production database changes"
                />
              </label>
              <label class="grid gap-2">
                <span class="text-sm font-medium text-slate-700"
                  >Description <span class="font-normal text-slate-400">(optional)</span></span
                >
                <textarea
                  v-model="form.description"
                  :disabled="!canEdit"
                  placeholder="Explain when this policy should be used."
                  class="min-h-28 resize-y rounded-md border border-slate-200 bg-white px-3 py-2 text-sm leading-6 outline-none placeholder:text-slate-400 focus:border-slate-400 disabled:bg-slate-100"
                />
              </label>
              <label
                class="flex cursor-pointer items-start justify-between gap-4 rounded-lg border border-slate-200 bg-white p-3"
              >
                <span>
                  <span class="block text-sm font-medium text-slate-800">Policy active</span>
                  <span class="mt-0.5 block text-xs leading-5 text-slate-500"
                    >Allow this policy to be used for new requests.</span
                  >
                </span>
                <input
                  v-model="form.is_active"
                  :disabled="!canEdit"
                  type="checkbox"
                  class="mt-1 h-4 w-4 rounded border-slate-300 accent-slate-900"
                />
              </label>
            </div>
          </aside>
        </div>
      </section>
    </div>
  </main>
</template>
