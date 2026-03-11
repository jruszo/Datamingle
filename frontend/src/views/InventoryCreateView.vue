<script setup lang="ts">
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { RouterLink, useRoute, useRouter } from 'vue-router'
import { ArrowLeft, Save } from 'lucide-vue-next'

import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import {
  createInstance,
  fetchInstance,
  fetchInstanceInventoryMetadata,
  testDraftInstanceConnection,
  updateInstance,
  type InstanceCreatePayload,
  type InstanceEditorRecord,
  type InstanceInventoryMetadata,
} from '@/lib/api'
import { useAuthStore } from '@/stores/auth'

const authStore = useAuthStore()
const route = useRoute()
const router = useRouter()

const metadata = ref<InstanceInventoryMetadata | null>(null)
const isLoading = ref(false)
const isSaving = ref(false)
const isTestingConnection = ref(false)
const needsResourceGroupDialog = ref(false)
const pageError = ref('')
const formError = ref('')
const connectionTestMessage = ref('')
const connectionTestTone = ref<'success' | 'error' | ''>('')

const isCreateMode = computed(() => route.name === 'inventory-new')
const instanceId = computed(() => {
  if (isCreateMode.value) {
    return null
  }
  const value = Number(route.params.instanceId)
  return Number.isFinite(value) ? value : null
})

const form = reactive({
  instance_name: '',
  type: 'master',
  db_type: 'mysql',
  host: '',
  port: 3306,
  user: '',
  password: '',
  is_ssl: false,
  verify_ssl: true,
  db_name: '',
  show_db_name_regex: '',
  denied_db_name_regex: '',
  charset: '',
  service_name: '',
  sid: '',
  tunnel_id: '',
  resource_group_ids: [] as number[],
  instance_tag_ids: [] as number[],
})

function resetForm() {
  form.instance_name = ''
  form.type = 'master'
  form.db_type = 'mysql'
  form.host = ''
  form.port = 3306
  form.user = ''
  form.password = ''
  form.is_ssl = false
  form.verify_ssl = true
  form.db_name = ''
  form.show_db_name_regex = ''
  form.denied_db_name_regex = ''
  form.charset = ''
  form.service_name = ''
  form.sid = ''
  form.tunnel_id = ''
  form.resource_group_ids = []
  form.instance_tag_ids = []
}

function applyInstance(instance: InstanceEditorRecord) {
  form.instance_name = instance.instance_name
  form.type = instance.type
  form.db_type = instance.db_type
  form.host = instance.host
  form.port = instance.port
  form.user = instance.user
  form.password = ''
  form.is_ssl = instance.is_ssl
  form.verify_ssl = instance.verify_ssl
  form.db_name = instance.db_name
  form.show_db_name_regex = instance.show_db_name_regex
  form.denied_db_name_regex = instance.denied_db_name_regex
  form.charset = instance.charset
  form.service_name = instance.service_name ?? ''
  form.sid = instance.sid ?? ''
  form.tunnel_id = instance.tunnel_id ? `${instance.tunnel_id}` : ''
  form.resource_group_ids = [...instance.resource_group_ids]
  form.instance_tag_ids = [...instance.instance_tag_ids]
}

const selectClass =
  'block h-10 w-full rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-700 shadow-sm outline-none transition focus:border-slate-400'
const multiSelectClass =
  'block min-h-[10rem] w-full rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700 shadow-sm outline-none transition focus:border-slate-400'
const textAreaClass =
  'block min-h-[7rem] w-full rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700 shadow-sm outline-none transition focus:border-slate-400'

function hasPermission(permission: string) {
  if (authStore.currentUser?.is_superuser) {
    return true
  }
  return authStore.currentUser?.permissions.includes(permission) ?? false
}

const canCreateInstances = computed(() => hasPermission('sql.menu_instance'))
const canEditInstances = computed(() => hasPermission('sql.menu_instance'))
const canManageInstance = computed(() => (isCreateMode.value ? canCreateInstances.value : canEditInstances.value))
const showOracleFields = computed(() => form.db_type === 'oracle')

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

function requireToken() {
  if (!authStore.accessToken) {
    throw new Error('Missing access token. Please login again.')
  }
  return authStore.accessToken
}

function updateNumericSelections(
  event: Event,
  target: 'resource_group_ids' | 'instance_tag_ids',
) {
  const element = event.target as HTMLSelectElement
  form[target] = Array.from(element.selectedOptions)
    .map((option) => Number(option.value))
    .filter((value) => Number.isFinite(value))
}

function resetConnectionTestResult() {
  connectionTestMessage.value = ''
  connectionTestTone.value = ''
}

async function goToResourceGroupCreation() {
  await router.push({
    name: 'settings-resource-groups-new',
    query: { reason: 'inventory-requires-resource-group' },
  })
}

async function goBackToInventory() {
  await router.push({ name: 'inventory' })
}

async function loadPage() {
  isLoading.value = true
  needsResourceGroupDialog.value = false
  pageError.value = ''
  formError.value = ''
  resetConnectionTestResult()
  resetForm()

  try {
    await authStore.loadCurrentUser()

    if (!canManageInstance.value) {
      pageError.value = `You do not have permission to ${isCreateMode.value ? 'create' : 'edit'} Datamingle instances.`
      return
    }

    metadata.value = await fetchInstanceInventoryMetadata(requireToken())
    if (metadata.value.resource_groups.length === 0) {
      needsResourceGroupDialog.value = true
      return
    }

    if (!metadata.value.db_types.find((item) => item.value === form.db_type)) {
      form.db_type = metadata.value.db_types[0]?.value || 'mysql'
    }

    if (!isCreateMode.value) {
      if (!instanceId.value) {
        pageError.value = 'Invalid instance identifier.'
        return
      }

      const instance = await fetchInstance(instanceId.value, requireToken())
      applyInstance(instance)
    }
  } catch (errorValue) {
    pageError.value = toUserFacingMessage(
      errorValue,
      `Failed to load the instance ${isCreateMode.value ? 'create' : 'edit'} form.`,
    )
  } finally {
    isLoading.value = false
  }
}

function buildInstancePayload(): InstanceCreatePayload | null {
  const instanceName = form.instance_name.trim()
  const host = form.host.trim()

  if (!instanceName) {
    formError.value = 'Instance name cannot be blank.'
    return null
  }
  if (!host) {
    formError.value = 'Host cannot be blank.'
    return null
  }
  if (!Number.isFinite(form.port) || form.port <= 0) {
    formError.value = 'Port must be a positive integer.'
    return null
  }

  formError.value = ''
  return {
    instance_name: instanceName,
    type: form.type,
    db_type: form.db_type,
    host,
    port: form.port,
    user: form.user.trim(),
    password: form.password,
    is_ssl: form.is_ssl,
    verify_ssl: form.verify_ssl,
    db_name: form.db_name.trim(),
    show_db_name_regex: form.show_db_name_regex.trim(),
    denied_db_name_regex: form.denied_db_name_regex.trim(),
    charset: form.charset.trim(),
    service_name: form.service_name.trim(),
    sid: form.sid.trim(),
    tunnel_id: form.tunnel_id ? Number(form.tunnel_id) : null,
    resource_group_ids: [...form.resource_group_ids],
    instance_tag_ids: [...form.instance_tag_ids],
  }
}

async function testConnection() {
  if (!canManageInstance.value) {
    formError.value = `You do not have permission to ${isCreateMode.value ? 'create' : 'edit'} Datamingle instances.`
    return
  }

  const payload = buildInstancePayload()
  if (!payload) {
    return
  }

  isTestingConnection.value = true
  resetConnectionTestResult()

  try {
    connectionTestMessage.value = await testDraftInstanceConnection(payload, requireToken())
    connectionTestTone.value = 'success'
  } catch (errorValue) {
    connectionTestMessage.value = toUserFacingMessage(errorValue, 'Failed to test the instance connection.')
    connectionTestTone.value = 'error'
  } finally {
    isTestingConnection.value = false
  }
}

async function saveInstance() {
  if (!canManageInstance.value) {
    formError.value = `You do not have permission to ${isCreateMode.value ? 'create' : 'edit'} Datamingle instances.`
    return
  }

  const payload = buildInstancePayload()
  if (!payload) {
    return
  }

  isSaving.value = true

  try {
    if (isCreateMode.value) {
      await createInstance(payload, requireToken())
      await router.push({
        name: 'inventory',
        query: { created: payload.instance_name },
      })
      return
    }

    if (!instanceId.value) {
      throw new Error('Missing instance identifier.')
    }

    await updateInstance(instanceId.value, payload, requireToken())
    await router.push({
      name: 'inventory',
      query: { edited: payload.instance_name },
    })
  } catch (errorValue) {
    formError.value = toUserFacingMessage(
      errorValue,
      `Failed to ${isCreateMode.value ? 'create' : 'update'} the instance.`,
    )
  } finally {
    isSaving.value = false
  }
}

onMounted(() => {
  void loadPage()
})

watch(
  () => route.fullPath,
  (currentPath, previousPath) => {
    if (currentPath !== previousPath) {
      void loadPage()
    }
  },
)

watch(form, () => {
  resetConnectionTestResult()
}, { deep: true })
</script>

<template>
  <section class="grid gap-6">
    <AlertDialog :open="needsResourceGroupDialog">
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle>Resource group required</AlertDialogTitle>
          <AlertDialogDescription>
            You need at least one resource group before creating an instance. Create the resource group first, then come back to inventory and add the instance.
          </AlertDialogDescription>
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel @click="void goBackToInventory()">Back to inventory</AlertDialogCancel>
          <AlertDialogAction @click="void goToResourceGroupCreation()">Create resource group</AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>

    <div class="flex flex-wrap items-center justify-between gap-3">
      <div class="space-y-1">
        <h2 class="text-2xl font-semibold text-slate-900">{{ isCreateMode ? 'Add Instance' : 'Edit Instance' }}</h2>
        <p class="text-sm text-slate-600">
          {{
            isCreateMode
              ? 'Create a new Datamingle inventory record with the connection settings and relationships needed by the SPA.'
              : 'Adjust the saved Datamingle inventory record, then test or save the updated connection settings.'
          }}
        </p>
      </div>
      <Button variant="outline" as-child>
        <RouterLink to="/inventory">
          <ArrowLeft class="h-4 w-4" />
          Back to inventory
        </RouterLink>
      </Button>
    </div>

    <Card class="border-slate-200">
      <CardHeader>
        <CardTitle>Instance Configuration</CardTitle>
        <CardDescription>
          {{
            isCreateMode
              ? 'Fill in the core connection details first, then optionally attach tags, resource groups, and a tunnel.'
              : 'Update the core connection details, then optionally adjust tags, resource groups, and the tunnel.'
          }}
        </CardDescription>
      </CardHeader>
      <CardContent class="space-y-6">
        <p v-if="pageError" class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
          {{ pageError }}
        </p>
        <p v-else-if="formError" class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
          {{ formError }}
        </p>
        <p
          v-if="connectionTestMessage"
          :class="connectionTestTone === 'success'
            ? 'rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700'
            : 'rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700'"
        >
          {{ connectionTestMessage }}
        </p>

        <div
          v-if="!pageError && !needsResourceGroupDialog"
          class="grid gap-6 xl:grid-cols-[minmax(0,1.2fr)_minmax(0,0.8fr)]"
        >
          <div class="space-y-6">
            <div class="grid gap-4 md:grid-cols-2">
              <div class="grid min-w-0 gap-2">
                <span class="text-xs font-semibold uppercase tracking-[0.24em] text-slate-500">Instance Name</span>
                <Input v-model="form.instance_name" placeholder="analytics-primary" />
              </div>

              <div class="grid min-w-0 gap-2">
                <span class="text-xs font-semibold uppercase tracking-[0.24em] text-slate-500">Host</span>
                <Input v-model="form.host" placeholder="db.internal.example.com" />
              </div>

              <div class="grid min-w-0 gap-2">
                <span class="text-xs font-semibold uppercase tracking-[0.24em] text-slate-500">Instance Type</span>
                <select v-model="form.type" :class="selectClass">
                  <option
                    v-for="item in metadata?.instance_types ?? []"
                    :key="item.value"
                    :value="item.value"
                  >
                    {{ item.label }}
                  </option>
                </select>
              </div>

              <div class="grid min-w-0 gap-2">
                <span class="text-xs font-semibold uppercase tracking-[0.24em] text-slate-500">Database Type</span>
                <select v-model="form.db_type" :class="selectClass">
                  <option
                    v-for="item in metadata?.db_types ?? []"
                    :key="item.value"
                    :value="item.value"
                  >
                    {{ item.label }}
                  </option>
                </select>
              </div>

              <div class="grid min-w-0 gap-2">
                <span class="text-xs font-semibold uppercase tracking-[0.24em] text-slate-500">Port</span>
                <Input v-model.number="form.port" type="number" min="1" step="1" />
              </div>

              <div class="grid min-w-0 gap-2">
                <span class="text-xs font-semibold uppercase tracking-[0.24em] text-slate-500">Tunnel</span>
                <select v-model="form.tunnel_id" :class="selectClass">
                  <option value="">No tunnel</option>
                  <option
                    v-for="item in metadata?.tunnels ?? []"
                    :key="item.id"
                    :value="`${item.id}`"
                  >
                    {{ item.label }}
                  </option>
                </select>
              </div>

              <div class="grid min-w-0 gap-2">
                <span class="text-xs font-semibold uppercase tracking-[0.24em] text-slate-500">User</span>
                <Input v-model="form.user" placeholder="readonly_user" />
              </div>

              <div class="grid min-w-0 gap-2">
                <span class="text-xs font-semibold uppercase tracking-[0.24em] text-slate-500">Password</span>
                <Input v-model="form.password" type="password" placeholder="Optional" />
              </div>
            </div>

            <div class="grid gap-4 md:grid-cols-2">
              <div class="grid min-w-0 gap-2">
                <span class="text-xs font-semibold uppercase tracking-[0.24em] text-slate-500">Default Database</span>
                <Input v-model="form.db_name" placeholder="Optional" />
              </div>

              <div class="grid min-w-0 gap-2">
                <span class="text-xs font-semibold uppercase tracking-[0.24em] text-slate-500">Charset</span>
                <Input v-model="form.charset" placeholder="utf8mb4" />
              </div>
            </div>

            <div v-if="showOracleFields" class="grid gap-4 md:grid-cols-2">
              <div class="grid min-w-0 gap-2">
                <span class="text-xs font-semibold uppercase tracking-[0.24em] text-slate-500">Oracle Service Name</span>
                <Input v-model="form.service_name" placeholder="Optional" />
              </div>

              <div class="grid min-w-0 gap-2">
                <span class="text-xs font-semibold uppercase tracking-[0.24em] text-slate-500">Oracle SID</span>
                <Input v-model="form.sid" placeholder="Optional" />
              </div>
            </div>

            <div class="grid gap-4 md:grid-cols-2">
              <div class="grid min-w-0 gap-2">
                <span class="text-xs font-semibold uppercase tracking-[0.24em] text-slate-500">Visible DB Regex</span>
                <textarea
                  v-model="form.show_db_name_regex"
                  :class="textAreaClass"
                  placeholder="^(app_db|analytics_.*)$"
                />
              </div>

              <div class="grid min-w-0 gap-2">
                <span class="text-xs font-semibold uppercase tracking-[0.24em] text-slate-500">Hidden DB Regex</span>
                <textarea
                  v-model="form.denied_db_name_regex"
                  :class="textAreaClass"
                  placeholder="^(mysql|sys)$"
                />
              </div>
            </div>

            <div class="grid gap-4 md:grid-cols-2">
              <label class="flex items-center gap-3 rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-700">
                <input v-model="form.is_ssl" class="rounded border-slate-300" type="checkbox">
                <span>Enable SSL for this instance</span>
              </label>

              <label class="flex items-center gap-3 rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-700">
                <input
                  v-model="form.verify_ssl"
                  :disabled="!form.is_ssl"
                  class="rounded border-slate-300"
                  type="checkbox"
                >
                <span>Verify the server certificate</span>
              </label>
            </div>
          </div>

          <div class="space-y-6">
            <div class="grid min-w-0 gap-2">
              <span class="text-xs font-semibold uppercase tracking-[0.24em] text-slate-500">Resource Groups</span>
              <select :class="multiSelectClass" multiple @change="updateNumericSelections($event, 'resource_group_ids')">
                <option
                  v-for="item in metadata?.resource_groups ?? []"
                  :key="item.group_id"
                  :selected="form.resource_group_ids.includes(item.group_id)"
                  :value="item.group_id"
                >
                  {{ item.group_name }}
                </option>
              </select>
              <span class="text-xs text-slate-500">Hold Command/Ctrl to select multiple resource groups.</span>
            </div>

            <div class="grid min-w-0 gap-2">
              <div class="flex items-center justify-between gap-3">
                <span class="text-xs font-semibold uppercase tracking-[0.24em] text-slate-500">Instance Tags</span>
                <Button as-child size="sm" variant="outline">
                  <RouterLink
                    :to="{
                      name: 'settings-instance-tags-new',
                      query: {
                        reason: 'inventory-tag-shortcut',
                        returnTo: route.fullPath,
                      },
                    }"
                  >
                    Add tag
                  </RouterLink>
                </Button>
              </div>
              <select :class="multiSelectClass" multiple @change="updateNumericSelections($event, 'instance_tag_ids')">
                <option
                  v-for="item in metadata?.tags ?? []"
                  :key="item.id"
                  :selected="form.instance_tag_ids.includes(item.id)"
                  :value="item.id"
                >
                  {{ item.tag_name }}
                </option>
              </select>
              <span class="text-xs text-slate-500">Assign the tags used by the legacy inventory filters and query access rules.</span>
            </div>
          </div>
        </div>
      </CardContent>
      <CardFooter class="flex flex-wrap items-center justify-between gap-3 border-t border-slate-200">
        <p class="text-sm text-slate-500">
          {{ isCreateMode
            ? 'Create keeps advanced cloud/RDS configuration out of scope; manage those relations separately if needed.'
            : 'Editing keeps advanced cloud/RDS configuration out of scope; manage those relations separately if needed.' }}
        </p>
        <div class="flex flex-wrap items-center gap-3">
          <Button
            variant="outline"
            :disabled="isLoading || isSaving || isTestingConnection || !!pageError"
            @click="void testConnection()"
          >
            {{ isTestingConnection ? 'Testing…' : 'Test connection' }}
          </Button>
          <Button :disabled="isLoading || isSaving || isTestingConnection || !!pageError" @click="void saveInstance()">
            <Save class="h-4 w-4" />
            {{ isSaving ? 'Saving…' : isCreateMode ? 'Create instance' : 'Save changes' }}
          </Button>
        </div>
      </CardFooter>
    </Card>
  </section>
</template>
