<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { ArrowDown, ArrowUp, GitBranch, Plus, Save, Trash2 } from 'lucide-vue-next'

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
    .filter((step): step is EditorStep & { permission_group: number } => step.permission_group !== null)
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
  <main class="mx-auto flex w-full max-w-7xl flex-col gap-4 px-4 py-6 sm:px-6 lg:px-8">
    <div class="flex flex-wrap items-center justify-between gap-3">
      <div>
        <h1 class="text-2xl font-semibold tracking-normal text-slate-950">Workflow policies</h1>
        <p class="text-sm text-slate-500">Create reusable SQL approval flows from team roles.</p>
      </div>
      <Button type="button" @click="resetForm">
        <Plus class="h-4 w-4" />
        New policy
      </Button>
    </div>

    <p v-if="error" class="rounded-md border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
      {{ error }}
    </p>
    <p v-if="feedback" class="rounded-md border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700">
      {{ feedback }}
    </p>

    <div class="grid min-h-[680px] gap-4 lg:grid-cols-[280px_minmax(0,1fr)_320px]">
      <aside class="rounded-lg border border-slate-200 bg-white">
        <div class="border-b border-slate-200 p-3">
          <Input v-model="search" placeholder="Search policies" />
        </div>
        <div class="max-h-[620px] overflow-y-auto p-2">
          <p v-if="loading" class="px-3 py-8 text-sm text-slate-500">Loading policies...</p>
          <button
            v-for="policy in filteredPolicies"
            :key="policy.id"
            type="button"
            class="mb-2 flex w-full flex-col items-start gap-1 rounded-md border px-3 py-3 text-left text-sm transition"
            :class="selectedPolicyId === policy.id ? 'border-slate-900 bg-slate-50' : 'border-slate-200 bg-white hover:border-slate-300'"
            @click="selectPolicy(policy)"
          >
            <span class="font-medium text-slate-950">{{ policy.name }}</span>
            <span class="line-clamp-2 text-xs text-slate-500">{{ policy.description || 'No description' }}</span>
            <Badge variant="outline" class="border-slate-200 bg-white text-slate-600">
              {{ policy.steps.length }} steps
            </Badge>
          </button>
        </div>
      </aside>

      <section class="rounded-lg border border-slate-200 bg-white p-4">
        <div class="mb-4 flex items-center justify-between gap-3">
          <div>
            <h2 class="text-base font-semibold text-slate-950">{{ form.name || 'New policy' }}</h2>
            <p class="text-sm text-slate-500">Submitter moves through each approval node in order.</p>
          </div>
          <Badge :variant="form.is_active ? 'default' : 'outline'">
            {{ form.is_active ? 'Active' : 'Inactive' }}
          </Badge>
        </div>

        <div class="overflow-x-auto rounded-lg border border-slate-100 bg-slate-50 p-5">
          <div class="flex min-w-max items-center gap-3">
            <div class="flex h-24 w-36 items-center justify-center rounded-md border border-slate-300 bg-white text-sm font-medium text-slate-700">
              Submitter
            </div>
            <template v-for="(step, index) in form.steps" :key="index">
              <div class="text-xl text-slate-400">→</div>
              <div
                class="flex h-24 w-40 flex-col items-center justify-center gap-2 rounded-md border bg-white px-3 text-center transition"
                :class="step.permission_group ? 'border-teal-600 text-slate-950' : 'border-dashed border-slate-300 text-slate-500'"
              >
                <GitBranch class="h-4 w-4" />
                <span class="text-sm font-medium">{{ roleName(step.permission_group) }}</span>
                <span class="text-xs text-slate-500">Step {{ index + 1 }}</span>
              </div>
            </template>
            <div class="text-xl text-slate-400">→</div>
            <div class="flex h-24 w-36 items-center justify-center rounded-md border border-emerald-300 bg-emerald-50 text-sm font-medium text-emerald-700">
              Approved
            </div>
            <Button type="button" variant="outline" size="sm" :disabled="!canEdit" aria-label="Add approval step" @click="addStep">
              <Plus class="h-4 w-4" />
            </Button>
          </div>
        </div>
      </section>

      <aside class="rounded-lg border border-slate-200 bg-white p-4">
        <div class="mb-4 flex items-center justify-between gap-2">
          <h2 class="text-base font-semibold text-slate-950">Inspector</h2>
          <span v-if="!canEdit" class="text-xs text-slate-500">Read only</span>
        </div>

        <div class="grid gap-4">
          <label class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">Name</span>
            <Input v-model="form.name" :disabled="!canEdit" />
          </label>
          <label class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">Description</span>
            <textarea
              v-model="form.description"
              :disabled="!canEdit"
              class="min-h-24 rounded-md border border-slate-200 px-3 py-2 text-sm outline-none focus:border-slate-400 disabled:bg-slate-100"
            />
          </label>
          <label class="flex items-center gap-2 text-sm text-slate-700">
            <input v-model="form.is_active" :disabled="!canEdit" type="checkbox" class="h-4 w-4 rounded border-slate-300" />
            Active
          </label>

          <div class="grid gap-3">
            <div class="flex items-center justify-between">
              <span class="text-sm font-medium text-slate-700">Approval steps</span>
              <Button type="button" variant="outline" size="sm" :disabled="!canEdit" aria-label="Add approval step" @click="addStep">
                <Plus class="h-4 w-4" />
              </Button>
            </div>
            <div v-for="(step, index) in form.steps" :key="index" class="rounded-md border border-slate-200 p-3">
              <div class="mb-2 flex items-center justify-between">
                <span class="text-xs font-medium uppercase text-slate-500">Step {{ index + 1 }}</span>
                <div class="flex gap-1">
                  <Button type="button" variant="outline" size="sm" :disabled="!canEdit || index === 0" aria-label="Move step up" @click="moveStep(index, -1)">
                    <ArrowUp class="h-4 w-4" />
                  </Button>
                  <Button type="button" variant="outline" size="sm" :disabled="!canEdit || index === form.steps.length - 1" aria-label="Move step down" @click="moveStep(index, 1)">
                    <ArrowDown class="h-4 w-4" />
                  </Button>
                  <Button type="button" variant="outline" size="sm" :disabled="!canEdit" aria-label="Remove approval step" @click="removeStep(index)">
                    <Trash2 class="h-4 w-4" />
                  </Button>
                </div>
              </div>
              <select
                v-model.number="step.permission_group"
                :disabled="!canEdit"
                class="block h-10 w-full rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-700 outline-none focus:border-slate-400 disabled:bg-slate-100"
              >
                <option :value="null">Select role</option>
                <option v-for="group in permissionGroups" :key="group.id" :value="group.id">
                  {{ group.name }}
                </option>
              </select>
            </div>
          </div>

          <div class="flex flex-wrap justify-end gap-2 border-t border-slate-200 pt-4">
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
              {{ saving ? 'Saving...' : 'Save' }}
            </Button>
          </div>
        </div>
      </aside>
    </div>
  </main>
</template>
