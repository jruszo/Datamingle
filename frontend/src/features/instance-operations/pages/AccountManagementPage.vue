<script setup lang="ts">
import { computed, onMounted, reactive, ref, watch } from 'vue'
import {
  Edit3,
  KeyRound,
  Lock,
  Plus,
  RefreshCw,
  Save,
  ShieldCheck,
  Trash2,
  Unlock,
  UserRound,
  X,
} from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import {
  createInstanceOperationAccount,
  deleteInstanceOperationAccount,
  fetchInstanceOperationAccountInstances,
  fetchInstanceOperationAccounts,
  grantInstanceOperationAccount,
  resetInstanceOperationAccountPassword,
  updateInstanceOperationAccount,
  updateInstanceOperationAccountLock,
  type InstanceOperationAccountInstance,
  type InstanceOperationAccountRecord,
} from '../api'
import { useAuthStore } from '@/stores/auth'

type FormMode = 'create' | 'edit' | 'password' | 'grant'

const authStore = useAuthStore()

const instances = ref<InstanceOperationAccountInstance[]>([])
const accounts = ref<InstanceOperationAccountRecord[]>([])
const selectedInstanceId = ref<number | null>(null)
const savedOnly = ref(false)
const search = ref('')

const loadingInstances = ref(false)
const loadingAccounts = ref(false)
const submitting = ref(false)
const error = ref('')
const feedback = ref('')
const activeFormMode = ref<FormMode | null>(null)
const selectedAccount = ref<InstanceOperationAccountRecord | null>(null)

const accountForm = reactive({
  user: '',
  host: '%',
  dbName: '',
  password: '',
  remark: '',
})

const grantForm = reactive({
  operation: '0',
  scope: '0',
  privileges: 'SELECT',
  dbName: '',
  tableName: '',
  columns: '',
})

const selectClass =
  'block h-10 w-full rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-700 shadow-sm outline-none transition focus:border-slate-400'

function requireToken() {
  if (!authStore.accessToken) {
    throw new Error('Missing access token. Please login again.')
  }
  return authStore.accessToken
}

function hasPermission(permission: string) {
  if (authStore.currentUser?.is_superuser) {
    return true
  }
  return authStore.currentUser?.permissions?.includes(permission) ?? false
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

const canViewAccounts = computed(() => hasPermission('sql.menu_instance_account'))
const canManageAccounts = computed(() => hasPermission('sql.instance_account_manage'))

const selectedInstance = computed(() =>
  instances.value.find((instance) => instance.id === selectedInstanceId.value) ?? null,
)

const filteredAccounts = computed(() => {
  const query = search.value.trim().toLowerCase()
  if (!query) {
    return accounts.value
  }

  return accounts.value.filter((account) =>
    [
      account.user,
      account.host,
      account.db_name,
      account.user_host,
      account.db_name_user,
      account.remark,
    ]
      .filter(Boolean)
      .join(' ')
      .toLowerCase()
      .includes(query),
  )
})

const gridColsClass = computed(() =>
  activeFormMode.value ? 'xl:grid-cols-[minmax(0,1fr)_minmax(21rem,0.34fr)]' : 'xl:grid-cols-1',
)

function splitList(value: string) {
  return value
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean)
}

function accountIdentity(account: InstanceOperationAccountRecord) {
  if (selectedInstance.value?.db_type === 'mongo') {
    return account.db_name_user || `${account.db_name ?? ''}.${account.user}`
  }
  return account.user_host || `\`${account.user}\`@\`${account.host ?? ''}\``
}

function rawAccountIdentity(account: InstanceOperationAccountRecord) {
  if (selectedInstance.value?.db_type === 'mongo') {
    return account.db_name_user || `${account.db_name ?? ''}.${account.user}`
  }
  return account.user_host || `${account.user}@${account.host ?? ''}`
}

function privilegeText(account: InstanceOperationAccountRecord) {
  if (Array.isArray(account.privileges)) {
    return account.privileges.join(', ')
  }
  if (account.privileges && typeof account.privileges === 'object') {
    return JSON.stringify(account.privileges)
  }
  return '-'
}

function resetForms() {
  accountForm.user = ''
  accountForm.host = '%'
  accountForm.dbName = ''
  accountForm.password = ''
  accountForm.remark = ''
  grantForm.operation = '0'
  grantForm.scope = '0'
  grantForm.privileges = 'SELECT'
  grantForm.dbName = ''
  grantForm.tableName = ''
  grantForm.columns = ''
  selectedAccount.value = null
}

function openCreateForm() {
  resetForms()
  activeFormMode.value = 'create'
  feedback.value = ''
}

function openAccountForm(mode: Exclude<FormMode, 'create'>, account: InstanceOperationAccountRecord) {
  resetForms()
  selectedAccount.value = account
  accountForm.user = account.user
  accountForm.host = account.host || '%'
  accountForm.dbName = account.db_name || ''
  accountForm.remark = account.remark || ''
  activeFormMode.value = mode
  feedback.value = ''
}

function closeForm() {
  resetForms()
  activeFormMode.value = null
}

async function loadInstances() {
  loadingInstances.value = true
  error.value = ''

  try {
    instances.value = await fetchInstanceOperationAccountInstances(requireToken())
    const firstInstance = instances.value[0]
    if (!selectedInstanceId.value && firstInstance) {
      selectedInstanceId.value = firstInstance.id
    }
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to load account-management instances.')
  } finally {
    loadingInstances.value = false
  }
}

async function loadAccounts() {
  if (!selectedInstanceId.value) {
    accounts.value = []
    return
  }

  loadingAccounts.value = true
  error.value = ''

  try {
    const response = await fetchInstanceOperationAccounts(requireToken(), {
      instance_id: selectedInstanceId.value,
      saved: savedOnly.value,
    })
    accounts.value = response.results
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to load accounts.')
  } finally {
    loadingAccounts.value = false
  }
}

async function refreshAccounts() {
  feedback.value = ''
  const priorInstanceId = selectedInstanceId.value
  await loadInstances()
  if (selectedInstanceId.value && selectedInstanceId.value === priorInstanceId) {
    await loadAccounts()
  }
}

function accountPayload() {
  if (!selectedInstanceId.value) {
    throw new Error('Select an instance first.')
  }

  return {
    instance_id: selectedInstanceId.value,
    db_name: accountForm.dbName.trim(),
    user: accountForm.user.trim(),
    host: accountForm.host.trim(),
    password: accountForm.password,
    remark: accountForm.remark.trim(),
  }
}

async function submitAccountForm() {
  if (!activeFormMode.value || !['create', 'edit'].includes(activeFormMode.value)) {
    return
  }
  if (!accountForm.user.trim()) {
    error.value = 'User is required.'
    return
  }
  if (selectedInstance.value?.db_type === 'mysql' && !accountForm.host.trim()) {
    error.value = 'Host is required for MySQL accounts.'
    return
  }
  if (selectedInstance.value?.db_type === 'mongo' && !accountForm.dbName.trim()) {
    error.value = 'Database is required for Mongo accounts.'
    return
  }

  submitting.value = true
  error.value = ''
  feedback.value = ''

  try {
    const payload = accountPayload()
    if (activeFormMode.value === 'create') {
      if (!payload.password) {
        error.value = 'Password is required for new accounts.'
        return
      }
      await createInstanceOperationAccount(payload, requireToken())
      feedback.value = `Account "${payload.user}" created.`
    } else {
      await updateInstanceOperationAccount(payload, requireToken())
      feedback.value = `Account "${payload.user}" metadata updated.`
    }
    closeForm()
    await loadAccounts()
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to save account.')
  } finally {
    submitting.value = false
  }
}

async function submitPasswordReset() {
  if (!selectedInstanceId.value || !selectedAccount.value) {
    return
  }
  if (!accountForm.password) {
    error.value = 'New password is required.'
    return
  }

  submitting.value = true
  error.value = ''
  feedback.value = ''

  try {
    await resetInstanceOperationAccountPassword(
      {
        instance_id: selectedInstanceId.value,
        db_name: accountForm.dbName.trim(),
        db_name_user: selectedAccount.value.db_name_user,
        user_host: selectedAccount.value.user_host,
        user: accountForm.user.trim(),
        host: accountForm.host.trim(),
        password: accountForm.password,
      },
      requireToken(),
    )
    feedback.value = `Password reset for "${accountForm.user}".`
    closeForm()
    await loadAccounts()
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to reset password.')
  } finally {
    submitting.value = false
  }
}

async function submitGrant() {
  if (!selectedInstanceId.value || !selectedAccount.value) {
    return
  }

  submitting.value = true
  error.value = ''
  feedback.value = ''

  const scope = Number(grantForm.scope) as 0 | 1 | 2 | 3
  const privilegeKey = scope === 0 ? 'global_privs' : scope === 1 ? 'db_privs' : scope === 2 ? 'tb_privs' : 'col_privs'

  try {
    const result = await grantInstanceOperationAccount(
      {
        instance_id: selectedInstanceId.value,
        user_host: selectedAccount.value.user_host,
        db_name_user: selectedAccount.value.db_name_user,
        op_type: Number(grantForm.operation) as 0 | 1,
        priv_type: scope,
        privs: { [privilegeKey]: splitList(grantForm.privileges) },
        db_name: grantForm.dbName.trim(),
        db_names: splitList(grantForm.dbName),
        tb_name: grantForm.tableName.trim(),
        tb_names: splitList(grantForm.tableName),
        col_names: splitList(grantForm.columns),
      },
      requireToken(),
    )
    feedback.value = result.grant_sql ? `Privileges updated: ${result.grant_sql}` : 'Privileges updated.'
    closeForm()
    await loadAccounts()
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to update privileges.')
  } finally {
    submitting.value = false
  }
}

async function toggleLock(account: InstanceOperationAccountRecord) {
  if (!selectedInstanceId.value) {
    return
  }
  const locked = account.is_locked !== 'Y'

  submitting.value = true
  error.value = ''
  feedback.value = ''

  try {
    await updateInstanceOperationAccountLock(
      {
        instance_id: selectedInstanceId.value,
        user_host: rawAccountIdentity(account),
        locked,
      },
      requireToken(),
    )
    feedback.value = locked ? 'Account locked.' : 'Account unlocked.'
    await loadAccounts()
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to change lock state.')
  } finally {
    submitting.value = false
  }
}

async function removeAccount(account: InstanceOperationAccountRecord) {
  if (!selectedInstanceId.value || !window.confirm(`Delete account ${accountIdentity(account)}?`)) {
    return
  }

  submitting.value = true
  error.value = ''
  feedback.value = ''

  try {
    await deleteInstanceOperationAccount(
      {
        instance_id: selectedInstanceId.value,
        db_name: account.db_name || '',
        db_name_user: account.db_name_user,
        user_host: account.user_host,
        user: account.user,
        host: account.host || '',
      },
      requireToken(),
    )
    feedback.value = 'Account deleted.'
    await loadAccounts()
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to delete account.')
  } finally {
    submitting.value = false
  }
}

onMounted(async () => {
  await authStore.loadCurrentUser()

  if (!canViewAccounts.value) {
    error.value = 'You do not have permission to view instance accounts.'
    return
  }

  await refreshAccounts()
})

watch([selectedInstanceId, savedOnly], () => {
  if (!canViewAccounts.value) {
    return
  }
  void loadAccounts()
})
</script>

<template>
  <section class="grid gap-6">
    <div class="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
      <div class="space-y-1">
        <div class="flex flex-wrap items-center gap-2">
          <RouterLink to="/inventory/data-dictionary" class="text-sm font-medium text-slate-500 hover:text-slate-900">
            Data Dictionary
          </RouterLink>
          <span class="text-slate-300">/</span>
          <span class="text-sm font-semibold text-slate-900">Accounts</span>
        </div>
        <h2 class="text-2xl font-semibold text-slate-900">Instance Accounts</h2>
        <p class="text-sm text-slate-600">
          Manage live instance accounts and saved metadata with operational permissions.
        </p>
      </div>
      <div class="flex flex-wrap gap-2">
        <Button variant="outline" type="button" class="gap-2" :disabled="loadingInstances || loadingAccounts" @click="void refreshAccounts()">
          <RefreshCw class="h-4 w-4" />
          Refresh
        </Button>
        <Button type="button" class="gap-2" :disabled="!selectedInstanceId || !canManageAccounts" @click="openCreateForm">
          <Plus class="h-4 w-4" />
          New account
        </Button>
      </div>
    </div>

    <p v-if="error" class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
      {{ error }}
    </p>
    <p
      v-else-if="feedback"
      class="rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700"
    >
      {{ feedback }}
    </p>

    <Card class="border-slate-200">
      <CardHeader>
        <CardTitle>Scope</CardTitle>
        <CardDescription>Select an operational instance before changing account metadata or grants.</CardDescription>
      </CardHeader>
      <CardContent>
        <div class="grid gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(0,0.8fr)_minmax(0,0.8fr)]">
          <label class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">Instance</span>
            <select v-model.number="selectedInstanceId" :class="selectClass" :disabled="loadingInstances">
              <option v-if="instances.length === 0" :value="null">No available instances</option>
              <option v-for="instance in instances" :key="instance.id" :value="instance.id">
                {{ instance.label }}
              </option>
            </select>
          </label>
          <label class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">Search</span>
            <Input v-model="search" placeholder="User, host, database, or remark" />
          </label>
          <label class="flex items-end gap-2 pb-2 text-sm text-slate-700">
            <input v-model="savedOnly" type="checkbox" class="h-4 w-4 rounded border-slate-300">
            Saved metadata only
          </label>
        </div>
      </CardContent>
    </Card>

    <div :class="['grid gap-6', gridColsClass]">
      <Card class="border-slate-200">
        <CardHeader>
          <CardTitle class="flex items-center gap-2">
            <UserRound class="h-5 w-5" />
            Accounts
          </CardTitle>
          <CardDescription>
            {{ filteredAccounts.length }} shown for {{ selectedInstance?.instance_name || 'no instance' }}
          </CardDescription>
        </CardHeader>
        <CardContent class="p-0">
          <div v-if="loadingAccounts" class="p-6 text-sm text-slate-500">
            Loading accounts...
          </div>
          <div v-else-if="filteredAccounts.length === 0" class="p-6 text-sm text-slate-500">
            No accounts match the current filters.
          </div>
          <div v-else class="overflow-x-auto">
            <table class="min-w-full divide-y divide-slate-200 text-sm">
              <thead class="bg-slate-50">
                <tr>
                  <th class="px-4 py-3 text-left font-medium text-slate-600">Account</th>
                  <th class="px-4 py-3 text-left font-medium text-slate-600">Database</th>
                  <th class="px-4 py-3 text-left font-medium text-slate-600">Privileges</th>
                  <th class="px-4 py-3 text-left font-medium text-slate-600">Remark</th>
                  <th class="px-4 py-3 text-right font-medium text-slate-600">Actions</th>
                </tr>
              </thead>
              <tbody class="divide-y divide-slate-100 bg-white">
                <tr v-for="account in filteredAccounts" :key="accountIdentity(account)">
                  <td class="px-4 py-3">
                    <div class="flex flex-wrap items-center gap-2">
                      <span class="font-medium text-slate-900">{{ accountIdentity(account) }}</span>
                      <Badge v-if="account.saved" variant="outline">Saved</Badge>
                      <Badge v-if="account.is_locked === 'Y'" variant="secondary">Locked</Badge>
                    </div>
                  </td>
                  <td class="px-4 py-3 text-slate-600">{{ account.db_name || '-' }}</td>
                  <td class="max-w-[18rem] truncate px-4 py-3 text-slate-600">{{ privilegeText(account) }}</td>
                  <td class="max-w-[16rem] truncate px-4 py-3 text-slate-600">{{ account.remark || '-' }}</td>
                  <td class="px-4 py-3 text-right">
                    <div class="flex flex-wrap justify-end gap-2">
                      <Button variant="outline" type="button" size="sm" class="gap-2" :disabled="!canManageAccounts" @click="openAccountForm('edit', account)">
                        <Edit3 class="h-4 w-4" />
                        Edit
                      </Button>
                      <Button variant="outline" type="button" size="sm" class="gap-2" :disabled="!canManageAccounts" @click="openAccountForm('password', account)">
                        <KeyRound class="h-4 w-4" />
                        Password
                      </Button>
                      <Button variant="outline" type="button" size="sm" class="gap-2" :disabled="!canManageAccounts" @click="openAccountForm('grant', account)">
                        <ShieldCheck class="h-4 w-4" />
                        Grants
                      </Button>
                      <Button
                        v-if="selectedInstance?.db_type === 'mysql'"
                        variant="outline"
                        type="button"
                        size="sm"
                        class="gap-2"
                        :disabled="!canManageAccounts || submitting"
                        @click="void toggleLock(account)"
                      >
                        <Unlock v-if="account.is_locked === 'Y'" class="h-4 w-4" />
                        <Lock v-else class="h-4 w-4" />
                        {{ account.is_locked === 'Y' ? 'Unlock' : 'Lock' }}
                      </Button>
                      <Button variant="destructive" type="button" size="sm" class="gap-2" :disabled="!canManageAccounts || submitting" @click="void removeAccount(account)">
                        <Trash2 class="h-4 w-4" />
                        Delete
                      </Button>
                    </div>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>

      <Card v-if="activeFormMode" class="border-slate-200">
        <CardHeader>
          <CardTitle>
            {{ activeFormMode === 'create' ? 'Create account' : activeFormMode === 'edit' ? 'Edit metadata' : activeFormMode === 'password' ? 'Reset password' : 'Change grants' }}
          </CardTitle>
          <CardDescription>
            {{ selectedAccount ? accountIdentity(selectedAccount) : selectedInstance?.instance_name }}
          </CardDescription>
        </CardHeader>
        <CardContent>
          <form v-if="activeFormMode === 'create' || activeFormMode === 'edit'" class="grid gap-4" @submit.prevent="void submitAccountForm()">
            <label class="grid gap-2">
              <span class="text-sm font-medium text-slate-700">User</span>
              <Input v-model="accountForm.user" :disabled="activeFormMode === 'edit'" placeholder="app_user" />
            </label>
            <label v-if="selectedInstance?.db_type === 'mysql'" class="grid gap-2">
              <span class="text-sm font-medium text-slate-700">Host</span>
              <Input v-model="accountForm.host" :disabled="activeFormMode === 'edit'" placeholder="%" />
            </label>
            <label v-else class="grid gap-2">
              <span class="text-sm font-medium text-slate-700">Database</span>
              <Input v-model="accountForm.dbName" :disabled="activeFormMode === 'edit'" placeholder="appdb" />
            </label>
            <label class="grid gap-2">
              <span class="text-sm font-medium text-slate-700">Password</span>
              <Input v-model="accountForm.password" type="password" :placeholder="activeFormMode === 'create' ? 'Required' : 'Optional metadata update'" />
            </label>
            <label class="grid gap-2">
              <span class="text-sm font-medium text-slate-700">Remark</span>
              <Input v-model="accountForm.remark" placeholder="Owner, lifecycle, or notes" />
            </label>
            <div class="flex flex-wrap justify-end gap-2">
              <Button variant="outline" type="button" class="gap-2" @click="closeForm">
                <X class="h-4 w-4" />
                Cancel
              </Button>
              <Button type="submit" class="gap-2" :disabled="submitting">
                <Save class="h-4 w-4" />
                {{ submitting ? 'Saving...' : 'Save' }}
              </Button>
            </div>
          </form>

          <form v-else-if="activeFormMode === 'password'" class="grid gap-4" @submit.prevent="void submitPasswordReset()">
            <label class="grid gap-2">
              <span class="text-sm font-medium text-slate-700">New password</span>
              <Input v-model="accountForm.password" type="password" placeholder="New password" />
            </label>
            <div class="flex flex-wrap justify-end gap-2">
              <Button variant="outline" type="button" class="gap-2" @click="closeForm">
                <X class="h-4 w-4" />
                Cancel
              </Button>
              <Button type="submit" class="gap-2" :disabled="submitting">
                <KeyRound class="h-4 w-4" />
                Reset
              </Button>
            </div>
          </form>

          <form v-else class="grid gap-4" @submit.prevent="void submitGrant()">
            <label class="grid gap-2">
              <span class="text-sm font-medium text-slate-700">Operation</span>
              <select v-model="grantForm.operation" :class="selectClass">
                <option value="0">Grant</option>
                <option value="1">Revoke</option>
              </select>
            </label>
            <label class="grid gap-2">
              <span class="text-sm font-medium text-slate-700">Scope</span>
              <select v-model="grantForm.scope" :class="selectClass">
                <option value="0">Global</option>
                <option value="1">Database</option>
                <option value="2">Table</option>
                <option value="3">Column</option>
              </select>
            </label>
            <label class="grid gap-2">
              <span class="text-sm font-medium text-slate-700">Privileges</span>
              <Input v-model="grantForm.privileges" placeholder="SELECT, INSERT" />
            </label>
            <label v-if="grantForm.scope !== '0'" class="grid gap-2">
              <span class="text-sm font-medium text-slate-700">Database</span>
              <Input v-model="grantForm.dbName" placeholder="appdb" />
            </label>
            <label v-if="grantForm.scope === '2' || grantForm.scope === '3'" class="grid gap-2">
              <span class="text-sm font-medium text-slate-700">Table</span>
              <Input v-model="grantForm.tableName" placeholder="orders" />
            </label>
            <label v-if="grantForm.scope === '3'" class="grid gap-2">
              <span class="text-sm font-medium text-slate-700">Columns</span>
              <Input v-model="grantForm.columns" placeholder="id, email" />
            </label>
            <div class="flex flex-wrap justify-end gap-2">
              <Button variant="outline" type="button" class="gap-2" @click="closeForm">
                <X class="h-4 w-4" />
                Cancel
              </Button>
              <Button type="submit" class="gap-2" :disabled="submitting">
                <ShieldCheck class="h-4 w-4" />
                Apply
              </Button>
            </div>
          </form>
        </CardContent>
      </Card>
    </div>
  </section>
</template>
