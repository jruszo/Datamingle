<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { onBeforeRouteLeave } from 'vue-router'
import { RefreshCw, Save, TestTube2 } from 'lucide-vue-next'

import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import {
  fetchSystemSettings,
  testSystemSettingsEmail,
  testSystemSettingsGoInception,
  testSystemSettingsStorage,
  type SystemSettings,
  type SystemSettingsOptions,
  type SystemSettingsValue,
  updateSystemSettings,
} from '../api'
import {
  createInitialSystemSettings,
  systemSettingsSections,
  type SystemSettingsFieldDefinition,
} from '../system-settings'
import { useAuthStore } from '@/stores/auth'

const authStore = useAuthStore()

const settings = reactive<SystemSettings>(createInitialSystemSettings())
const options = ref<SystemSettingsOptions | null>(null)
const isLoading = ref(false)
const isSaving = ref(false)
const activeSectionTest = ref<string | null>(null)
const pageError = ref('')
const feedback = ref('')
const initialSnapshot = ref('')

const numberListKeys = new Set(['api_user_whitelist'])
const textInputClass =
  'flex h-9 w-full rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm transition-colors placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50'
const textAreaClass =
  'flex min-h-[7rem] w-full rounded-md border border-input bg-transparent px-3 py-2 text-sm shadow-sm transition-colors placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50'
const selectClass =
  'flex min-h-9 w-full rounded-md border border-input bg-transparent px-3 py-2 text-sm shadow-sm transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50'

const canAccessSystemSettings = computed(() => {
  return authStore.currentUser?.is_superuser || authStore.currentUser?.is_staff || false
})

const isDirty = computed(() => {
  if (!initialSnapshot.value) {
    return false
  }
  return initialSnapshot.value !== snapshotSettings()
})

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

function cloneValue(value: SystemSettingsValue | undefined): SystemSettingsValue {
  if (Array.isArray(value)) {
    return [...value]
  }
  return value ?? null
}

function snapshotSettings() {
  const normalized = Object.keys(settings)
    .sort()
    .reduce<Record<string, SystemSettingsValue>>((accumulator, key) => {
      accumulator[key] = cloneValue(settings[key])
      return accumulator
    }, {})
  return JSON.stringify(normalized)
}

function applySettings(nextSettings: SystemSettings) {
  const initial = createInitialSystemSettings()
  for (const [key, value] of Object.entries(initial)) {
    const nextValue = nextSettings[key] ?? value
    settings[key] = cloneValue(nextValue)
  }
}

function fieldValue(key: string) {
  return settings[key] ?? null
}

function stringValue(key: string) {
  const value = fieldValue(key)
  return typeof value === 'string' ? value : ''
}

function numberValue(key: string) {
  const value = fieldValue(key)
  return typeof value === 'number' ? `${value}` : ''
}

function booleanValue(key: string) {
  return fieldValue(key) === true
}

function arrayValue(key: string) {
  const value = fieldValue(key)
  return Array.isArray(value) ? (value as Array<string | number>) : []
}

function updateTextValue(key: string, value: string) {
  settings[key] = value
}

function updateNumberValue(key: string, value: string) {
  settings[key] = value.trim() === '' ? null : Number(value)
}

function updateBooleanValue(key: string, value: boolean) {
  settings[key] = value
}

function updateSelectValue(key: string, value: string) {
  settings[key] = value
}

function updateMultiSelectValue(key: string, event: Event) {
  const element = event.target as HTMLSelectElement
  const values = Array.from(element.selectedOptions, (option) => option.value)
  settings[key] = numberListKeys.has(key)
    ? values.map((value) => Number(value))
    : values
}

function optionsForField(field: SystemSettingsFieldDefinition) {
  if (!field.optionSource || !options.value) {
    return []
  }
  return options.value[field.optionSource] ?? []
}

function isFieldVisible(field: SystemSettingsFieldDefinition) {
  if (!field.showWhen) {
    return true
  }
  return settings[field.showWhen.key] === field.showWhen.equals
}

function fieldSpanClass(field: SystemSettingsFieldDefinition) {
  if (field.input === 'textarea' || field.input === 'multiselect') {
    return 'md:col-span-2'
  }
  return ''
}

function fieldId(sectionId: string, key: string) {
  return `${sectionId}-${key}`
}

async function loadPage() {
  isLoading.value = true
  pageError.value = ''

  try {
    await authStore.loadCurrentUser()
    if (!canAccessSystemSettings.value) {
      pageError.value = 'You do not have permission to manage Datamingle system settings.'
      return
    }

    const payload = await fetchSystemSettings(requireToken())
    options.value = payload.options
    applySettings(payload.settings)
    initialSnapshot.value = snapshotSettings()
  } catch (errorValue) {
    pageError.value = toUserFacingMessage(errorValue, 'Failed to load system settings.')
  } finally {
    isLoading.value = false
  }
}

async function saveSettings() {
  isSaving.value = true
  pageError.value = ''
  feedback.value = ''

  try {
    const payload = await updateSystemSettings(
      Object.keys(settings).reduce<SystemSettings>((accumulator, key) => {
        accumulator[key] = cloneValue(settings[key])
        return accumulator
      }, {}),
      requireToken(),
    )
    options.value = payload.options
    applySettings(payload.settings)
    initialSnapshot.value = snapshotSettings()
    feedback.value = 'System settings saved.'
  } catch (errorValue) {
    pageError.value = toUserFacingMessage(errorValue, 'Failed to save system settings.')
  } finally {
    isSaving.value = false
  }
}

function buildPayload(keys: string[]) {
  return keys.reduce<Record<string, unknown>>((accumulator, key) => {
    accumulator[key] = cloneValue(settings[key])
    return accumulator
  }, {})
}

async function runSectionTest(action: 'goInception' | 'email' | 'storage') {
  pageError.value = ''
  feedback.value = ''
  activeSectionTest.value = action

  try {
    if (action === 'goInception') {
      feedback.value = await testSystemSettingsGoInception(
        buildPayload([
          'go_inception_host',
          'go_inception_port',
          'go_inception_user',
          'go_inception_password',
          'inception_remote_backup_host',
          'inception_remote_backup_port',
          'inception_remote_backup_user',
          'inception_remote_backup_password',
        ]),
        requireToken(),
      )
      return
    }

    if (action === 'email') {
      feedback.value = await testSystemSettingsEmail(
        buildPayload([
          'mail',
          'mail_ssl',
          'mail_smtp_server',
          'mail_smtp_port',
          'mail_smtp_user',
          'mail_smtp_password',
        ]),
        requireToken(),
      )
      return
    }

    feedback.value = await testSystemSettingsStorage(
      buildPayload([
        'storage_type',
        'max_export_rows',
        'sftp_host',
        'sftp_port',
        'sftp_user',
        'sftp_password',
        'sftp_path',
        'sftp_custom_params',
        's3c_access_key_id',
        's3c_access_key_secret',
        's3c_endpoint',
        's3c_region',
        's3c_bucket_name',
        's3c_path',
        's3c_custom_params',
        'azure_account_name',
        'azure_account_key',
        'azure_container',
        'azure_path',
        'azure_custom_params',
      ]),
      requireToken(),
    )
  } catch (errorValue) {
    pageError.value = toUserFacingMessage(errorValue, 'Failed to run the settings connection test.')
  } finally {
    activeSectionTest.value = null
  }
}

onMounted(() => {
  void loadPage()
})

onBeforeRouteLeave(() => {
  if (!isDirty.value || isSaving.value) {
    return true
  }

  return window.confirm('Leave this page without saving your system settings changes?')
})
</script>

<template>
  <section class="grid gap-6">
    <div class="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
      <div class="space-y-1">
        <h2 data-testid="settings-system-title" class="text-2xl font-semibold text-slate-900">System Settings</h2>
        <p class="max-w-3xl text-sm text-slate-600">
          Manage runtime Datamingle settings from the SPA. This page is available only to staff members and superusers.
        </p>
      </div>
      <div class="flex flex-wrap items-center gap-3">
        <span
          class="rounded-full border px-3 py-1 text-xs font-medium"
          :class="isDirty ? 'border-amber-200 bg-amber-50 text-amber-700' : 'border-emerald-200 bg-emerald-50 text-emerald-700'"
        >
          {{ isDirty ? 'Unsaved changes' : 'Saved state' }}
        </span>
        <Button data-testid="settings-reload" variant="outline" :disabled="isLoading || isSaving" @click="loadPage">
          <RefreshCw class="h-4 w-4" />
          Reload
        </Button>
        <Button data-testid="settings-save" :disabled="isLoading || isSaving || !canAccessSystemSettings" @click="saveSettings">
          <Save class="h-4 w-4" />
          {{ isSaving ? 'Saving...' : 'Save settings' }}
        </Button>
      </div>
    </div>

    <p v-if="pageError" class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
      {{ pageError }}
    </p>
    <p v-else-if="feedback" class="rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700">
      {{ feedback }}
    </p>

    <Card class="border-slate-200">
      <CardContent class="flex flex-col gap-3 p-5 text-sm text-slate-600 lg:flex-row lg:items-center lg:justify-between">
        <div class="max-w-3xl">
          Save writes only the known Datamingle system-config keys used by the SPA. Workflow approval configuration remains outside this page for now.
        </div>
        <div class="rounded-xl bg-slate-100 px-3 py-2 text-xs font-medium uppercase tracking-wide text-slate-500">
          {{ options ? 'Settings payload loaded' : 'Loading settings payload' }}
        </div>
      </CardContent>
    </Card>

    <Card
      v-for="section in systemSettingsSections"
      :key="section.id"
      :data-testid="`settings-section-${section.id}`"
      class="border-slate-200"
    >
      <CardHeader class="gap-4 lg:flex-row lg:items-start lg:justify-between">
        <div class="space-y-1">
          <CardTitle>{{ section.title }}</CardTitle>
          <CardDescription>{{ section.description }}</CardDescription>
        </div>
        <Button
          v-if="section.testAction"
          :data-testid="`settings-test-${section.id}`"
          variant="outline"
          :disabled="isLoading || isSaving || activeSectionTest === section.testAction"
          @click="void runSectionTest(section.testAction)"
        >
          <TestTube2 class="h-4 w-4" />
          {{
            activeSectionTest === section.testAction
              ? 'Testing...'
              : 'Run test'
          }}
        </Button>
      </CardHeader>
      <CardContent class="space-y-5">
        <div
          v-if="section.id === 'storage' && settings.storage_type === 'local'"
          class="rounded-lg border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-600"
        >
          Local storage keeps export files under the Datamingle downloads directory. No extra connection fields are required.
        </div>

        <div class="grid gap-5 md:grid-cols-2">
          <div
            v-for="field in section.fields.filter(isFieldVisible)"
            :key="field.key"
            class="space-y-2"
            :class="fieldSpanClass(field)"
          >
            <label :for="fieldId(section.id, field.key)" class="text-sm font-medium text-slate-900">
              {{ field.label }}
            </label>
            <p v-if="field.description" class="text-xs leading-5 text-slate-500">
              {{ field.description }}
            </p>

            <div v-if="field.input === 'checkbox'" class="rounded-xl border border-slate-200 bg-slate-50 px-4 py-3">
              <label class="flex items-start gap-3 text-sm text-slate-700">
                <input
                  :id="fieldId(section.id, field.key)"
                  :data-testid="`settings-field-${field.key}`"
                  type="checkbox"
                  class="mt-0.5 h-4 w-4 rounded border-slate-300 text-slate-900 focus:ring-slate-400"
                  :checked="booleanValue(field.key)"
                  :disabled="isLoading || isSaving"
                  @change="updateBooleanValue(field.key, ($event.target as HTMLInputElement).checked)"
                >
                <span>{{ field.label }}</span>
              </label>
            </div>

            <input
              v-else-if="field.input === 'text' || field.input === 'password'"
              :id="fieldId(section.id, field.key)"
              :data-testid="`settings-field-${field.key}`"
              :type="field.input"
              :class="textInputClass"
              :placeholder="field.placeholder"
              :value="stringValue(field.key)"
              :disabled="isLoading || isSaving"
              @input="updateTextValue(field.key, ($event.target as HTMLInputElement).value)"
            >

            <input
              v-else-if="field.input === 'number'"
              :id="fieldId(section.id, field.key)"
              :data-testid="`settings-field-${field.key}`"
              type="number"
              :class="textInputClass"
              :placeholder="field.placeholder"
              :value="numberValue(field.key)"
              :disabled="isLoading || isSaving"
              @input="updateNumberValue(field.key, ($event.target as HTMLInputElement).value)"
            >

            <textarea
              v-else-if="field.input === 'textarea'"
              :id="fieldId(section.id, field.key)"
              :data-testid="`settings-field-${field.key}`"
              :class="textAreaClass"
              :rows="field.rows ?? 4"
              :placeholder="field.placeholder"
              :value="stringValue(field.key)"
              :disabled="isLoading || isSaving"
              @input="updateTextValue(field.key, ($event.target as HTMLTextAreaElement).value)"
            />

            <select
              v-else-if="field.input === 'select'"
              :id="fieldId(section.id, field.key)"
              :data-testid="`settings-field-${field.key}`"
              :class="selectClass"
              :value="String(fieldValue(field.key) ?? '')"
              :disabled="isLoading || isSaving"
              @change="updateSelectValue(field.key, ($event.target as HTMLSelectElement).value)"
            >
              <option
                v-for="option in optionsForField(field)"
                :key="`${field.key}-${option.value}`"
                :value="option.value"
              >
                {{ option.label }}
              </option>
            </select>

            <select
              v-else
              :id="fieldId(section.id, field.key)"
              :data-testid="`settings-field-${field.key}`"
              multiple
              :class="`${selectClass} min-h-[9rem]`"
              :disabled="isLoading || isSaving"
              @change="updateMultiSelectValue(field.key, $event)"
            >
              <option
                v-for="option in optionsForField(field)"
                :key="`${field.key}-${option.value}`"
                :value="option.value"
                :selected="arrayValue(field.key).includes(option.value)"
              >
                {{ option.label }}
              </option>
            </select>
          </div>
        </div>
      </CardContent>
    </Card>
  </section>
</template>
