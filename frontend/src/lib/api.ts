import { AuthSessionExpiredError, getUsableAccessToken, notifyUnauthorized, refreshAccessToken } from '@/shared/auth/auth'
import {
  apiDelete,
  apiGet,
  apiPatch,
  apiPost,
  apiPut,
  buildUrl,
  flattenErrorMessage,
  isRecord,
  publicApiUrl,
} from '@/shared/api/http'

export { publicApiUrl }

export type ApiEnvelope<T> = {
  detail: string
  data: T
}

export async function fetchSchemaInfo() {
  return apiGet<object>('/schema/')
}

type TokenPair = {
  access: string
  refresh: string
}

export type AuthMode = 'builtin' | 'workos'

export type AuthConfig = {
  mode: AuthMode
}

export type CurrentUserContext = {
  id: number
  username: string
  display: string
  email: string
  avatar_url: string
  is_workos_managed: boolean
  is_superuser: boolean
  is_staff: boolean
  is_active: boolean
  groups: Array<{ id: number; name: string }>
  resource_groups: Array<{ group_id: number; group_name: string }>
  permissions: string[]
  two_factor_auth_types: string[]
}

export type SystemSettingsValue =
  | string
  | number
  | boolean
  | Array<string | number>
  | null

export type SystemSettings = Record<string, SystemSettingsValue>

export type SystemSettingsOption = {
  value: string | number
  label: string
}

export type SystemSettingsOptions = {
  instance_tags: SystemSettingsOption[]
  auth_groups: SystemSettingsOption[]
  resource_groups: SystemSettingsOption[]
  users: SystemSettingsOption[]
  notify_phases: SystemSettingsOption[]
  auto_review_db_types: SystemSettingsOption[]
  storage_types: SystemSettingsOption[]
  sms_providers: SystemSettingsOption[]
  task_backends: SystemSettingsOption[]
  inventory_refresh_intervals: SystemSettingsOption[]
}

export type SystemSettingsPayload = {
  settings: SystemSettings
  options: SystemSettingsOptions
}

export type GroupRecord = {
  id: number
  name: string
  permissions: number[]
}

export type UserManagementGroupRecord = {
  id: number
  name: string
}

export type UserManagementRecord = {
  id: number
  username: string
  display: string
  email: string
  is_workos_managed: boolean
  is_active: boolean
  is_superuser: boolean
  is_staff: boolean
  groups: UserManagementGroupRecord[]
}

export type UserManagementDetailRecord = UserManagementRecord & {
  group_ids: number[]
}

export type UserManagementListOptions = {
  page?: number
  size?: number
  search?: string
  ordering?: string
}

export type CreateUserPayload = {
  username: string
  display: string
  email: string
  password: string
  group_ids: number[]
}

export type UpdateUserPayload = {
  display?: string
  email?: string
  password?: string
  group_ids?: number[]
  is_active?: boolean
}

export type InstanceTagRecord = {
  id: number
  tag_code: string
  tag_name: string
  active: boolean
  usage_count?: number
}

export type InstanceTagListOptions = {
  page?: number
  size?: number
  search?: string
  ordering?: string
}

export type CreateInstanceTagPayload = {
  tag_code: string
  tag_name: string
  active: boolean
}

export type UpdateInstanceTagPayload = {
  tag_name: string
  active: boolean
}

export type ResourceGroupRecord = {
  group_id: number
  group_name: string
  user_count: number
  instance_count: number
}

export type ResourceGroupDetailRecord = ResourceGroupRecord & {
  user_ids: number[]
  instance_ids: number[]
}

export type ResourceGroupUserLookupRecord = {
  id: number
  username: string
  display: string
  label: string
}

export type ResourceGroupInstanceLookupRecord = {
  id: number
  instance_name: string
  db_type: string
  host: string
  label: string
}

export type InstanceInventoryRecord = {
  id: number
  instance_name: string
  type: string
  db_type: string
  host: string
  port: number
  user: string
  is_ssl: boolean
  verify_ssl: boolean
  db_name: string
  charset: string
  service_name: string | null
  sid: string | null
  resource_group_ids: number[]
  instance_tag_ids: number[]
  inventory_status: 'never' | 'ok' | 'stale' | 'failed'
  inventory_detected_hostname: string
  inventory_detected_version: string
  inventory_last_refresh_at: string | null
}

export type InstanceOptionRecord = {
  value: string
  label: string
}

export type InstanceTagOptionRecord = {
  id: number
  tag_code?: string
  tag_name: string
  label: string
}

export type ResourceGroupOptionRecord = {
  group_id: number
  group_name: string
  label: string
}

export type InstanceInventoryMetadata = {
  instance_types: InstanceOptionRecord[]
  db_types: InstanceOptionRecord[]
  tags: InstanceTagOptionRecord[]
  resource_groups: ResourceGroupOptionRecord[]
}

export type InstanceInventoryFilters = {
  page?: number
  size?: number
  search?: string
  type?: string
  db_type?: string
  tag_ids?: number[]
  ordering?: string
}

export type InstanceCreatePayload = {
  instance_name: string
  type: string
  db_type: string
  host: string
  port: number
  user: string
  password: string
  is_ssl: boolean
  verify_ssl: boolean
  db_name: string
  show_db_name_regex: string
  denied_db_name_regex: string
  charset: string
  service_name: string
  sid: string
  resource_group_ids: number[]
  instance_tag_ids: number[]
}

export type InstanceEditorRecord = InstanceCreatePayload & {
  id: number
}

export type PermissionRecord = {
  id: number
  name: string
  codename: string
  app_label: string
  model: string
}

type DashboardNamedSeries = {
  labels: string[]
  values: number[]
}

type DashboardQueryActivitySeries = {
  labels: string[]
  scanned_rows: number[]
  query_count: number[]
}

type DashboardStackedSeries = {
  categories: string[]
  series: Array<{
    name: string
    values: number[]
  }>
}

export type DashboardPayload = {
  start_date: string
  end_date: string
  summary: {
    sql_workflow_count: number
    query_workflow_count: number
    active_user_count: number
    instance_count: number
  }
  charts: {
    workflow_by_date: DashboardNamedSeries
    workflow_by_group: DashboardNamedSeries
    workflow_by_user: DashboardNamedSeries
    workflow_status: DashboardNamedSeries
    syntax_type: DashboardNamedSeries
    query_activity: DashboardQueryActivitySeries
    query_rows_by_user: DashboardNamedSeries
    query_rows_by_db: DashboardNamedSeries
    instance_type_distribution: DashboardNamedSeries
    instance_env_distribution: DashboardStackedSeries
  }
}

function extractData<T>(payload: unknown): T {
  if (isRecord(payload) && 'data' in payload) {
    return payload.data as T
  }
  return payload as T
}

function extractDetail(payload: unknown, fallback: string): string {
  if (isRecord(payload) && typeof payload.detail === 'string') {
    return payload.detail
  }

  return fallback
}

function isTokenPair(value: unknown): value is TokenPair {
  if (!isRecord(value)) {
    return false
  }
  return typeof value.access === 'string' && typeof value.refresh === 'string'
}

function extractTokenPair(payload: unknown): TokenPair {
  if (isTokenPair(payload)) {
    return payload
  }

  if (isRecord(payload) && isTokenPair(payload.data)) {
    return payload.data
  }

  throw new Error('Token response did not include access/refresh fields')
}

export function login(
  username: string,
  password: string,
  authType?: 'totp' | 'sms',
  otp?: string,
) {
  return apiPost<unknown>('/auth/token/', {
    username,
    password,
    auth_type: authType,
    otp,
  }).then(extractTokenPair)
}

export function fetchAuthConfig() {
  return apiGet<unknown>('/auth/config/').then((payload) =>
    extractData<AuthConfig>(payload),
  )
}

export function exchangeWorkosCode(code: string) {
  return apiPost<unknown>('/auth/workos/exchange/', { code }).then(extractTokenPair)
}

export function fetchCurrentUserContext(token: string) {
  return apiGet<unknown>('/v1/me/', { token }).then((payload) =>
    extractData<CurrentUserContext>(payload),
  )
}

export function fetchSystemSettings(token: string) {
  return apiGet<unknown>('/v1/system-settings/', { token }).then((payload) =>
    extractData<SystemSettingsPayload>(payload),
  )
}

export function updateSystemSettings(settings: SystemSettings, token: string) {
  return apiPut<unknown>('/v1/system-settings/', settings, { token }).then((payload) =>
    extractData<SystemSettingsPayload>(payload),
  )
}

export function testSystemSettingsGoInception(payload: Record<string, unknown>, token: string) {
  return apiPost<unknown>('/v1/system-settings/tests/go-inception/', payload, { token }).then(
    (responsePayload) => extractDetail(responsePayload, 'goInception connection test succeeded.'),
  )
}

export function testSystemSettingsEmail(payload: Record<string, unknown>, token: string) {
  return apiPost<unknown>('/v1/system-settings/tests/email/', payload, { token }).then(
    (responsePayload) => extractDetail(responsePayload, 'Email connection test succeeded.'),
  )
}

export function testSystemSettingsStorage(payload: Record<string, unknown>, token: string) {
  return apiPost<unknown>('/v1/system-settings/tests/storage/', payload, { token }).then(
    (responsePayload) => extractDetail(responsePayload, 'Storage connection test succeeded.'),
  )
}

export function fetchUsers(token: string, options: UserManagementListOptions = {}) {
  const params = new URLSearchParams()
  if (options.page) {
    params.set('page', `${options.page}`)
  }
  if (options.size) {
    params.set('size', `${options.size}`)
  }
  if (options.search?.trim()) {
    params.set('search', options.search.trim())
  }
  if (options.ordering?.trim()) {
    params.set('ordering', options.ordering.trim())
  }

  const queryString = params.toString()
  const path = queryString ? `/v1/user/?${queryString}` : '/v1/user/'
  return apiGet<unknown>(path, { token }).then((payload) =>
    extractData<PaginatedResponse<UserManagementRecord>>(payload),
  )
}

export function fetchUser(userId: number, token: string) {
  return apiGet<unknown>(`/v1/user/${userId}/`, { token }).then((payload) =>
    extractData<UserManagementDetailRecord>(payload),
  )
}

export function createUser(payload: CreateUserPayload, token: string) {
  return apiPost<unknown>('/v1/user/', payload, { token }).then((responsePayload) =>
    extractData<UserManagementDetailRecord>(responsePayload),
  )
}

export function updateUser(userId: number, payload: UpdateUserPayload, token: string) {
  return apiPut<unknown>(`/v1/user/${userId}/`, payload, { token }).then((responsePayload) =>
    extractData<UserManagementDetailRecord>(responsePayload),
  )
}

export function deleteUser(userId: number, token: string) {
  return apiDelete<unknown>(`/v1/user/${userId}/`, { token }).then((payload) =>
    extractDetail(payload, 'User deleted successfully.'),
  )
}

export function fetchInstanceTags(token: string, options: InstanceTagListOptions = {}) {
  const params = new URLSearchParams()
  if (options.page) {
    params.set('page', `${options.page}`)
  }
  if (options.size) {
    params.set('size', `${options.size}`)
  }
  if (options.search?.trim()) {
    params.set('search', options.search.trim())
  }
  if (options.ordering?.trim()) {
    params.set('ordering', options.ordering.trim())
  }

  const queryString = params.toString()
  const path = queryString ? `/v1/instance/tag/?${queryString}` : '/v1/instance/tag/'
  return apiGet<unknown>(path, { token }).then((payload) =>
    extractData<PaginatedResponse<InstanceTagRecord>>(payload),
  )
}

export function fetchInstanceTag(tagId: number, token: string) {
  return apiGet<unknown>(`/v1/instance/tag/${tagId}/`, { token }).then((payload) =>
    extractData<InstanceTagRecord>(payload),
  )
}

export function createInstanceTag(payload: CreateInstanceTagPayload, token: string) {
  return apiPost<unknown>('/v1/instance/tag/', payload, { token }).then((responsePayload) =>
    extractData<InstanceTagRecord>(responsePayload),
  )
}

export function updateInstanceTag(tagId: number, payload: UpdateInstanceTagPayload, token: string) {
  return apiPut<unknown>(`/v1/instance/tag/${tagId}/`, payload, { token }).then((responsePayload) =>
    extractData<InstanceTagRecord>(responsePayload),
  )
}

export function updateCurrentUserDisplay(display: string, token: string) {
  return apiPatch<unknown>('/v1/me/', { display }, { token }).then((payload) =>
    extractData<CurrentUserContext>(payload),
  )
}

export function changeCurrentUserPassword(
  currentPassword: string,
  newPassword: string,
  newPasswordConfirm: string,
  token: string,
) {
  return apiPost<unknown>(
    '/v1/me/password/',
    {
      current_password: currentPassword,
      new_password: newPassword,
      new_password_confirm: newPasswordConfirm,
    },
    { token },
  ).then((payload) => extractDetail(payload, 'Password updated successfully.'))
}

export function fetchGroups(
  token: string,
  options: {
    page?: number
    size?: number
    search?: string
    ordering?: string
  } = {},
) {
  const params = new URLSearchParams()
  if (options.page) {
    params.set('page', `${options.page}`)
  }
  if (options.size) {
    params.set('size', `${options.size}`)
  }
  if (options.search?.trim()) {
    params.set('search', options.search.trim())
  }
  if (options.ordering?.trim()) {
    params.set('ordering', options.ordering.trim())
  }
  const queryString = params.toString()
  const path = queryString ? `/v1/user/group/?${queryString}` : '/v1/user/group/'
  return apiGet<unknown>(path, { token }).then((payload) =>
    extractData<PaginatedResponse<GroupRecord>>(payload),
  )
}

export function fetchGroup(groupId: number, token: string) {
  return apiGet<unknown>(`/v1/user/group/${groupId}/`, { token }).then((payload) =>
    extractData<GroupRecord>(payload),
  )
}

export function createGroup(payload: { name: string; permissions: number[] }, token: string) {
  return apiPost<unknown>('/v1/user/group/', payload, { token }).then((responsePayload) =>
    extractData<GroupRecord>(responsePayload),
  )
}

export function updateGroup(
  groupId: number,
  payload: { name: string; permissions: number[] },
  token: string,
) {
  return apiPut<unknown>(`/v1/user/group/${groupId}/`, payload, { token }).then((responsePayload) =>
    extractData<GroupRecord>(responsePayload),
  )
}

export function deleteGroup(groupId: number, token: string) {
  return apiDelete<unknown>(`/v1/user/group/${groupId}/`, { token }).then((payload) =>
    extractDetail(payload, 'Group deleted successfully.'),
  )
}

export function fetchPermissions(token: string) {
  return apiGet<unknown>('/v1/user/permission/', { token }).then((payload) =>
    extractData<PermissionRecord[]>(payload),
  )
}

export function fetchResourceGroups(
  token: string,
  options: {
    page?: number
    size?: number
    search?: string
    ordering?: string
  } = {},
) {
  const params = new URLSearchParams()
  if (options.page) {
    params.set('page', `${options.page}`)
  }
  if (options.size) {
    params.set('size', `${options.size}`)
  }
  if (options.search?.trim()) {
    params.set('search', options.search.trim())
  }
  if (options.ordering?.trim()) {
    params.set('ordering', options.ordering.trim())
  }
  const queryString = params.toString()
  const path = queryString ? `/v1/user/resourcegroup/?${queryString}` : '/v1/user/resourcegroup/'
  return apiGet<unknown>(path, { token }).then((payload) =>
    extractData<PaginatedResponse<ResourceGroupRecord>>(payload),
  )
}

export function fetchResourceGroup(resourceGroupId: number, token: string) {
  return apiGet<unknown>(`/v1/user/resourcegroup/${resourceGroupId}/`, { token }).then((payload) =>
    extractData<ResourceGroupDetailRecord>(payload),
  )
}

export function createResourceGroup(
  payload: { group_name: string; user_ids: number[]; instance_ids: number[] },
  token: string,
) {
  return apiPost<unknown>('/v1/user/resourcegroup/', payload, { token }).then((responsePayload) =>
    extractData<ResourceGroupDetailRecord>(responsePayload),
  )
}

export function updateResourceGroup(
  resourceGroupId: number,
  payload: { group_name: string; user_ids: number[]; instance_ids: number[] },
  token: string,
) {
  return apiPut<unknown>(`/v1/user/resourcegroup/${resourceGroupId}/`, payload, { token }).then(
    (responsePayload) => extractData<ResourceGroupDetailRecord>(responsePayload),
  )
}

export function deleteResourceGroup(resourceGroupId: number, token: string) {
  return apiDelete<unknown>(`/v1/user/resourcegroup/${resourceGroupId}/`, { token }).then((payload) =>
    extractDetail(payload, 'Resource group deleted successfully.'),
  )
}

export function fetchResourceGroupUsers(token: string) {
  return apiGet<unknown>('/v1/user/resourcegroup/users/lookup/', { token }).then((payload) =>
    extractData<ResourceGroupUserLookupRecord[]>(payload),
  )
}

export function fetchResourceGroupInstances(token: string) {
  return apiGet<unknown>('/v1/user/resourcegroup/instances/lookup/', { token }).then((payload) =>
    extractData<ResourceGroupInstanceLookupRecord[]>(payload),
  )
}

export function fetchInstanceInventory(
  token: string,
  options: InstanceInventoryFilters = {},
) {
  const params = new URLSearchParams()
  if (options.page) {
    params.set('page', `${options.page}`)
  }
  if (options.size) {
    params.set('size', `${options.size}`)
  }
  if (options.search?.trim()) {
    params.set('search', options.search.trim())
  }
  if (options.type?.trim()) {
    params.set('type', options.type.trim())
  }
  if (options.db_type?.trim()) {
    params.set('db_type', options.db_type.trim())
  }
  if (options.ordering?.trim()) {
    params.set('ordering', options.ordering.trim())
  }
  if (options.tag_ids?.length) {
    for (const tagId of options.tag_ids) {
      params.append('tags', `${tagId}`)
    }
  }

  const queryString = params.toString()
  const path = queryString ? `/v1/instance/?${queryString}` : '/v1/instance/'
  return apiGet<unknown>(path, { token }).then((payload) =>
    extractData<PaginatedResponse<InstanceInventoryRecord>>(payload),
  )
}

export function fetchInstanceInventoryMetadata(token: string) {
  return apiGet<unknown>('/v1/instance/metadata/', { token }).then((payload) =>
    extractData<InstanceInventoryMetadata>(payload),
  )
}

export function createInstance(payload: InstanceCreatePayload, token: string) {
  return apiPost<unknown>('/v1/instance/', payload, { token }).then((responsePayload) =>
    extractData<InstanceInventoryRecord>(responsePayload),
  )
}

export function fetchInstance(instanceId: number, token: string) {
  return apiGet<unknown>(`/v1/instance/${instanceId}/`, { token }).then((payload) =>
    extractData<InstanceEditorRecord>(payload),
  )
}

export function updateInstance(instanceId: number, payload: InstanceCreatePayload, token: string) {
  return apiPut<unknown>(`/v1/instance/${instanceId}/`, payload, { token }).then((responsePayload) =>
    extractData<InstanceEditorRecord>(responsePayload),
  )
}

export function testDraftInstanceConnection(payload: InstanceCreatePayload, token: string) {
  return apiPost<unknown>('/v1/instance/test-connection/', payload, { token }).then((responsePayload) =>
    extractDetail(responsePayload, 'Connection successful.'),
  )
}

export function testInstanceConnection(instanceId: number, token: string) {
  return apiPost<unknown>(`/v1/instance/${instanceId}/test-connection/`, {}, { token }).then((payload) =>
    extractDetail(payload, 'Connection successful.'),
  )
}

export function fetchDashboard(startDate: string, endDate: string, token: string) {
  const params = new URLSearchParams({
    start_date: startDate,
    end_date: endDate,
  })
  return apiGet<unknown>(`/v1/dashboard/?${params.toString()}`, { token }).then((payload) =>
    extractData<DashboardPayload>(payload),
  )
}

export type PaginatedResponse<T> = {
  count: number
  next: string | null
  previous: string | null
  results: T[]
}

export type MailboxCategory = 'approval_needed' | 'execution_needed' | 'execution_finished'
export type MailboxSourceType = 'sql_workflow' | 'archive' | 'permission_request'
export type MailboxReadState = 'all' | 'unread' | 'read'

export type MailboxItem = {
  id: number
  category: MailboxCategory
  category_label: string
  source_type: MailboxSourceType
  source_type_label: string
  source_id: number
  title: string
  body: string
  action_path: string
  is_unread: boolean
  read_at: string | null
  resolved_at: string | null
  metadata: Record<string, unknown>
  create_time: string
  sys_time: string
}

export type MailboxSummary = {
  unread_count: number
  items: MailboxItem[]
}

export type MailboxListFilters = {
  page?: number
  size?: number
  state?: MailboxReadState
  category?: MailboxCategory | ''
  source_type?: MailboxSourceType | ''
}

export type WorkflowSyntaxType = 0 | 1 | 2 | 3

export type WorkflowResourceGroupLookupRecord = {
  group_id: number
  group_name: string
}

export type WorkflowInstanceLookupRecord = {
  id: number
  instance_name: string
  db_type: string
  type: string
  host: string
  label: string
  resource_groups: WorkflowResourceGroupLookupRecord[]
}

export type WorkflowMetadataRecord = {
  allow_backup_toggle: boolean
  manual_execution_enabled: boolean
  resource_groups: WorkflowResourceGroupLookupRecord[]
  instances: WorkflowInstanceLookupRecord[]
}

export type WorkflowSummaryRecord = {
  id: number
  workflow_name: string
  demand_url: string
  group_id: number
  group_name: string
  instance_id: number
  instance_name: string
  instance_db_type: string
  db_name: string
  schema_name: string
  syntax_type: WorkflowSyntaxType
  syntax_type_label: string
  is_offline_export: boolean | number
  export_format: string | null
  file_name: string | null
  download_available: boolean
  status: string
  status_label: string
  is_backup: boolean
  engineer: string
  engineer_display: string
  run_date_start: string | null
  run_date_end: string | null
  create_time: string
  finish_time: string | null
}

export type WorkflowReviewNode = {
  group_name: string
  is_current_node: boolean
  is_passed_node: boolean
}

export type WorkflowCurrentReviewer = {
  id: number
  username: string
  display: string
}

export type WorkflowLogRecord = {
  operation_type_desc: string
  operation_info: string
  operator_display: string
  operation_time: string
}

export type WorkflowResultRow = Record<string, unknown>

export type WorkflowExecutorOption = {
  id: string
  label: string
  kind: 'online' | 'direct'
}

export type WorkflowDetailRecord = WorkflowSummaryRecord & {
  sql_content: string
  review_rows: WorkflowResultRow[]
  execute_rows: WorkflowResultRow[]
  review_info: WorkflowReviewNode[]
  current_reviewers: WorkflowCurrentReviewer[]
  logs: WorkflowLogRecord[]
  last_operation_info: string
  scheduled_run_date: string | null
  scheduled_executor: string | null
  available_executors: WorkflowExecutorOption[]
  executor_blockers: Record<string, string>
  is_can_review: boolean
  is_can_reject: boolean
  is_can_execute: boolean
  is_can_schedule: boolean
  is_can_cancel: boolean
  is_can_abort: boolean
  is_can_rollback: boolean
  is_can_manual_execute: boolean
  is_can_edit_execution_window: boolean
  manual_execution_enabled: boolean
}

export function fetchWorkflowMetadata(token: string) {
  return apiGet<unknown>('/v1/workflow/metadata/', { token }).then((payload) =>
    extractData<WorkflowMetadataRecord>(payload),
  )
}

export type QueryableInstance = {
  id: number
  instance_name: string
  db_type: string
  type: string
}

export type InstanceResourceType = 'database' | 'schema' | 'table' | 'column'

export type InstanceResourceList = {
  count: number
  result: string[]
}

export type DataDictionaryInstance = {
  id: number
  instance_name: string
  db_type: string
  label: string
}

export type DataDictionaryDatabaseList = {
  count: number
  result: string[]
}

export type DataDictionaryTableGroup = {
  group: string
  tables: Array<[string, string]>
}

export type DataDictionaryTableGroupList = {
  count: number
  result: DataDictionaryTableGroup[]
}

export type DataDictionaryResultSet = {
  column_list?: string[]
  rows: unknown
}

export type DataDictionaryTableDetail = {
  meta_data: DataDictionaryResultSet
  desc: DataDictionaryResultSet
  index: DataDictionaryResultSet
  create_sql?: unknown
}

export type DataDictionaryExportResult =
  | {
    mode: 'blob'
    data: Blob
    filename: string
  }
  | {
    mode: 'message'
    detail: string
  }

export type AuditListFilters = {
  page?: number
  size?: number
  search?: string
  start_date?: string
  end_date?: string
  action?: string
  status?: string
  syntax_type?: string
  group_id?: string
  instance_id?: string
  instance_name?: string
  username?: string
}

export type GeneralAuditLogRecord = {
  id?: number
  user_id: number | null
  user_name: string
  user_display: string
  action: string
  extra_info: string
  action_time: string
}

export type QueryAuditLogRecord = {
  id: number
  instance_name: string
  db_name: string
  sqllog: string
  effect_row: number
  cost_time: string
  username: string
  user_display: string
  priv_check: boolean
  hit_rule: boolean
  masking: boolean
  favorite: boolean
  alias: string
  create_time: string
}

export type SqlWorkflowAuditLogRecord = {
  id: number
  workflow_name: string
  demand_url: string
  group_id: number
  group_name: string
  instance_id: number
  instance_name: string
  db_name: string
  schema_name: string
  syntax_type: number
  syntax_type_label: string
  is_backup: boolean
  engineer: string
  engineer_display: string
  status: string
  status_label: string
  run_date_start: string | null
  run_date_end: string | null
  create_time: string
  finish_time: string | null
  is_offline_export: number
  export_format: string | null
}

export type WorkflowOperationAuditLogRecord = {
  id: number
  audit_id: number
  operation_type: number
  operation_type_desc: string
  operation_info: string
  operator: string
  operator_display: string
  operation_time: string
}

export type InstanceOperationDatabaseInstance = {
  id: number
  instance_name: string
  db_type: string
  label: string
}

export type InstanceOperationDatabaseRecord = {
  id?: number
  db_name: string
  owner?: string
  owner_display?: string
  remark?: string
  saved: boolean
  sys_time?: string
  table_rows?: unknown
  data_length?: unknown
  index_length?: unknown
  data_total?: unknown
}

export type InstanceOperationDatabaseList = {
  count: number
  results: InstanceOperationDatabaseRecord[]
}

export type InstanceOperationDatabasePayload = {
  instance_id: number
  db_name: string
  owner?: string
  remark?: string
}

export type InstanceOperationAccountInstance = InstanceOperationDatabaseInstance

export type InstanceOperationAccountRecord = {
  id?: number
  user: string
  host?: string
  db_name?: string
  user_host?: string
  db_name_user?: string
  roles?: unknown
  privileges?: unknown
  is_locked?: string | null
  remark?: string
  saved: boolean
  sys_time?: string
}

export type InstanceOperationAccountList = {
  count: number
  results: InstanceOperationAccountRecord[]
}

export type InstanceOperationAccountPayload = {
  instance_id: number
  db_name?: string
  user: string
  host?: string
  password?: string
  remark?: string
}

export type InstanceOperationAccountPasswordPayload = {
  instance_id: number
  db_name?: string
  db_name_user?: string
  user_host?: string
  user: string
  host?: string
  password: string
}

export type InstanceOperationAccountLockPayload = {
  instance_id: number
  user_host: string
  locked: boolean
}

export type InstanceOperationAccountDeletePayload = {
  instance_id: number
  db_name?: string
  db_name_user?: string
  user_host?: string
  user: string
  host?: string
}

export type InstanceOperationAccountGrantPayload = {
  instance_id: number
  user_host?: string
  db_name_user?: string
  op_type?: 0 | 1
  priv_type?: 0 | 1 | 2 | 3
  privs?: Record<string, string[]> | string[]
  db_name?: string
  db_names?: string[]
  tb_name?: string
  tb_names?: string[]
  col_names?: string[]
  roles?: unknown[]
}

export type InstanceOperationAccountGrantResult = {
  grant_sql: string
}

export type InstanceOperationParamInstance = InstanceOperationDatabaseInstance

export type InstanceOperationParamRecord = {
  id?: number
  variable_name: string
  runtime_value: string | null
  default_value?: string
  valid_values?: string
  description?: string
  editable: boolean
  configured: boolean
}

export type InstanceOperationParamList = {
  count: number
  results: InstanceOperationParamRecord[]
}

export type InstanceOperationParamHistoryRecord = {
  instance_name: string
  variable_name: string
  old_var: string
  new_var: string
  set_sql: string
  user_name: string
  user_display: string
  create_time: string
}

export type InstanceOperationParamHistoryList = {
  count: number
  results: InstanceOperationParamHistoryRecord[]
}

export type InstanceOperationParamEditPayload = {
  instance_id: number
  variable_name: string
  runtime_value: string
}

export type InstanceOperationDiagnosticInstance = InstanceOperationDatabaseInstance

export type InstanceOperationDiagnosticRow = Record<string, unknown>

export type InstanceOperationDiagnosticList = {
  count: number
  results: InstanceOperationDiagnosticRow[]
}

export type InstanceOperationDiagnosticKillPayload = {
  instance_id: number
  thread_ids: number[]
}

export type InstanceOperationDiagnosticKillPreview = {
  kill_sql: string
}

export type QueryResultPayload = {
  full_sql: string
  is_execute: boolean
  checked: string | null
  is_masked: boolean
  query_time: string | number
  mask_rule_hit: boolean
  mask_time: string | number
  warning: string | null
  error: string | null
  is_critical: boolean
  rows: Array<Record<string, unknown>>
  column_list: string[]
  column_type: string[]
  status: number | null
  affected_rows: number
  seconds_behind_master?: string | number | null
}

export type QueryDescribePayload = {
  display_mode: 'ddl' | 'table'
  full_sql: string
  rows: Array<Record<string, unknown>>
  column_list: string[]
  column_type?: string[]
  affected_rows: number
  error: string | null
}

export type QueryLogRecord = {
  id: number
  instance_name: string
  db_name: string
  sqllog: string
  effect_row: number
  cost_time: string
  user_display: string
  favorite: boolean
  alias: string
  create_time: string
}

export type FavoriteQuery = {
  id: number
  alias: string
  instance_name: string
  db_name: string
  sqllog: string
  create_time: string
}

export type QueryExecuteRequest = {
  instance_name: string
  sql_content: string
  db_name: string
  schema_name?: string
  tb_name?: string
  limit_num: number
}

export type QueryDescribeRequest = {
  instance_id: number
  db_name: string
  schema_name?: string
  tb_name: string
}

export type QueryLogFilters = {
  page?: number
  size?: number
  search?: string
  star?: 'true' | 'false'
  query_log_id?: number
}

export function fetchQueryInstances(token: string) {
  return apiGet<unknown>('/v1/query/instance/', { token }).then((payload) =>
    extractData<QueryableInstance[]>(payload),
  )
}

export function fetchInstanceResources(
  instanceId: number,
  resourceType: InstanceResourceType,
  token: string,
  options: {
    db_name?: string
    schema_name?: string
    tb_name?: string
  } = {},
) {
  const params = new URLSearchParams({
    instance_id: `${instanceId}`,
    resource_type: resourceType,
  })

  if (options.db_name) {
    params.set('db_name', options.db_name)
  }
  if (options.schema_name) {
    params.set('schema_name', options.schema_name)
  }
  if (options.tb_name) {
    params.set('tb_name', options.tb_name)
  }

  return apiGet<unknown>(`/v1/instance/resource/?${params.toString()}`, { token }).then((payload) =>
    extractData<InstanceResourceList>(payload),
  )
}

export function fetchDataDictionaryInstances(token: string) {
  return apiGet<unknown>('/v1/instance/data-dictionary/instances/', { token }).then((payload) =>
    extractData<DataDictionaryInstance[]>(payload),
  )
}

export function fetchDataDictionaryDatabases(instanceId: number, token: string) {
  const params = new URLSearchParams({ instance_id: `${instanceId}` })

  return apiGet<unknown>(`/v1/instance/data-dictionary/databases/?${params.toString()}`, { token }).then(
    (payload) => extractData<DataDictionaryDatabaseList>(payload),
  )
}

export function fetchDataDictionaryTables(instanceId: number, dbName: string, token: string) {
  const params = new URLSearchParams({
    instance_id: `${instanceId}`,
    db_name: dbName,
  })

  return apiGet<unknown>(`/v1/instance/data-dictionary/tables/?${params.toString()}`, { token }).then(
    (payload) => extractData<DataDictionaryTableGroupList>(payload),
  )
}

export function fetchDataDictionaryTableDetail(
  instanceId: number,
  dbName: string,
  tableName: string,
  token: string,
) {
  const params = new URLSearchParams({
    instance_id: `${instanceId}`,
    db_name: dbName,
    table_name: tableName,
  })

  return apiGet<unknown>(`/v1/instance/data-dictionary/table/?${params.toString()}`, { token }).then(
    (payload) => extractData<DataDictionaryTableDetail>(payload),
  )
}

export async function exportDataDictionary(
  instanceId: number,
  dbName: string,
  token: string,
  options: { skipAuthRetry?: boolean } = {},
): Promise<DataDictionaryExportResult> {
  let authorizationToken = ''

  try {
    authorizationToken = await getUsableAccessToken(token)
  } catch (error) {
    if (error instanceof AuthSessionExpiredError) {
      notifyUnauthorized(error.message)
      throw new Error(`GET /v1/instance/data-dictionary/export/ failed (401): ${error.message}`)
    }
    throw error
  }

  const params = new URLSearchParams({ instance_id: `${instanceId}` })
  if (dbName) {
    params.set('db_name', dbName)
  }

  const response = await fetch(buildUrl(`/v1/instance/data-dictionary/export/?${params.toString()}`), {
    method: 'GET',
    headers: {
      Accept: 'application/json',
      Authorization: `Bearer ${authorizationToken}`,
    },
  })

  if (!response.ok) {
    const body = await response.text()
    let message = body

    try {
      message = flattenErrorMessage(JSON.parse(body)) || body
    } catch {
      message = body
    }

    if (response.status === 401 && !options.skipAuthRetry) {
      try {
        const refreshedAccessToken = await refreshAccessToken()
        return exportDataDictionary(instanceId, dbName, refreshedAccessToken, { skipAuthRetry: true })
      } catch (error) {
        if (error instanceof AuthSessionExpiredError) {
          notifyUnauthorized(error.message)
          throw new Error(`GET /v1/instance/data-dictionary/export/ failed (401): ${error.message}`)
        }
        throw error
      }
    }

    if (response.status === 401) {
      notifyUnauthorized(message)
    }

    throw new Error(`GET /v1/instance/data-dictionary/export/ failed (${response.status}): ${message}`)
  }

  const contentType = response.headers.get('content-type') || ''
  if (contentType.includes('application/json')) {
    const payload = await response.json()
    return {
      mode: 'message',
      detail: isRecord(payload) && typeof payload.detail === 'string' ? payload.detail : 'Export completed.',
    }
  }

  const blob = await response.blob()
  const disposition = response.headers.get('content-disposition') || ''
  const fileNameMatch = disposition.match(/filename="?([^"]+)"?$/i)
  const filename = fileNameMatch?.[1] || `data-dictionary-${instanceId}.html`

  return {
    mode: 'blob',
    data: blob,
    filename,
  }
}

function buildAuditQueryString(filters: AuditListFilters = {}) {
  const params = new URLSearchParams()

  for (const [key, value] of Object.entries(filters)) {
    if (value !== undefined && value !== null && `${value}` !== '') {
      params.set(key, `${value}`)
    }
  }

  return params.toString()
}

export function fetchGeneralAuditLogs(filters: AuditListFilters, token: string) {
  const query = buildAuditQueryString(filters)
  return apiGet<unknown>(`/v1/audit/general/?${query}`, { token }).then((payload) =>
    extractData<PaginatedResponse<GeneralAuditLogRecord>>(payload),
  )
}

export function fetchQueryAuditLogs(filters: AuditListFilters, token: string) {
  const query = buildAuditQueryString(filters)
  return apiGet<unknown>(`/v1/audit/query/?${query}`, { token }).then((payload) =>
    extractData<PaginatedResponse<QueryAuditLogRecord>>(payload),
  )
}

export function fetchSqlWorkflowAuditLogs(filters: AuditListFilters, token: string) {
  const query = buildAuditQueryString(filters)
  return apiGet<unknown>(`/v1/audit/sql-workflow/?${query}`, { token }).then((payload) =>
    extractData<PaginatedResponse<SqlWorkflowAuditLogRecord>>(payload),
  )
}

export function fetchWorkflowOperationAuditLogs(
  token: string,
  options: { audit_id?: number; workflow_id?: number; workflow_type?: number },
) {
  const params = new URLSearchParams()
  if (options.audit_id != null) {
    params.set('audit_id', `${options.audit_id}`)
  }
  if (options.workflow_id != null) {
    params.set('workflow_id', `${options.workflow_id}`)
  }
  if (options.workflow_type != null) {
    params.set('workflow_type', `${options.workflow_type}`)
  }

  return apiGet<unknown>(`/v1/audit/workflow-log/?${params.toString()}`, { token }).then((payload) =>
    extractData<{ count: number; results: WorkflowOperationAuditLogRecord[] }>(payload),
  )
}

export function fetchInstanceOperationDatabaseInstances(token: string) {
  return apiGet<unknown>('/v1/instance-operations/database/instances/', { token }).then((payload) =>
    extractData<InstanceOperationDatabaseInstance[]>(payload),
  )
}

export function fetchInstanceOperationDatabases(
  token: string,
  options: { instance_id: number; saved?: boolean },
) {
  const params = new URLSearchParams({ instance_id: `${options.instance_id}` })
  if (options.saved !== undefined) {
    params.set('saved', `${options.saved}`)
  }

  return apiGet<unknown>(`/v1/instance-operations/database/?${params.toString()}`, { token }).then((payload) =>
    extractData<InstanceOperationDatabaseList>(payload),
  )
}

export function createInstanceOperationDatabase(payload: InstanceOperationDatabasePayload, token: string) {
  return apiPost<unknown>('/v1/instance-operations/database/', payload, { token }).then((responsePayload) =>
    extractData<InstanceOperationDatabaseRecord>(responsePayload),
  )
}

export function updateInstanceOperationDatabase(payload: InstanceOperationDatabasePayload, token: string) {
  return apiPut<unknown>('/v1/instance-operations/database/metadata/', payload, { token }).then((responsePayload) =>
    extractData<InstanceOperationDatabaseRecord>(responsePayload),
  )
}

export function fetchInstanceOperationAccountInstances(token: string) {
  return apiGet<unknown>('/v1/instance-operations/account/instances/', { token }).then((payload) =>
    extractData<InstanceOperationAccountInstance[]>(payload),
  )
}

export function fetchInstanceOperationAccounts(
  token: string,
  options: { instance_id: number; saved?: boolean },
) {
  const params = new URLSearchParams({ instance_id: `${options.instance_id}` })
  if (options.saved !== undefined) {
    params.set('saved', `${options.saved}`)
  }

  return apiGet<unknown>(`/v1/instance-operations/account/?${params.toString()}`, { token }).then((payload) =>
    extractData<InstanceOperationAccountList>(payload),
  )
}

export function createInstanceOperationAccount(payload: InstanceOperationAccountPayload, token: string) {
  return apiPost<unknown>('/v1/instance-operations/account/', payload, { token }).then((responsePayload) =>
    extractData<InstanceOperationAccountRecord>(responsePayload),
  )
}

export function updateInstanceOperationAccount(payload: InstanceOperationAccountPayload, token: string) {
  return apiPut<unknown>('/v1/instance-operations/account/metadata/', payload, { token }).then((responsePayload) =>
    extractData<InstanceOperationAccountRecord>(responsePayload),
  )
}

export function resetInstanceOperationAccountPassword(payload: InstanceOperationAccountPasswordPayload, token: string) {
  return apiPost<unknown>('/v1/instance-operations/account/password/', payload, { token }).then((responsePayload) =>
    extractData<InstanceOperationAccountRecord>(responsePayload),
  )
}

export function updateInstanceOperationAccountLock(payload: InstanceOperationAccountLockPayload, token: string) {
  return apiPost<unknown>('/v1/instance-operations/account/lock/', payload, { token }).then((responsePayload) =>
    extractData<Record<string, never>>(responsePayload),
  )
}

export function deleteInstanceOperationAccount(payload: InstanceOperationAccountDeletePayload, token: string) {
  return apiDelete<unknown>('/v1/instance-operations/account/delete/', { token, body: payload }).then((responsePayload) =>
    extractData<Record<string, never>>(responsePayload),
  )
}

export function grantInstanceOperationAccount(payload: InstanceOperationAccountGrantPayload, token: string) {
  return apiPost<unknown>('/v1/instance-operations/account/grant/', payload, { token }).then((responsePayload) =>
    extractData<InstanceOperationAccountGrantResult>(responsePayload),
  )
}

export function fetchInstanceOperationParamInstances(token: string) {
  return apiGet<unknown>('/v1/instance-operations/param/instances/', { token }).then((payload) =>
    extractData<InstanceOperationParamInstance[]>(payload),
  )
}

export function fetchInstanceOperationParams(
  token: string,
  options: { instance_id: number; editable?: boolean; search?: string },
) {
  const params = new URLSearchParams({ instance_id: `${options.instance_id}` })
  if (options.editable !== undefined) {
    params.set('editable', `${options.editable}`)
  }
  if (options.search) {
    params.set('search', options.search)
  }

  return apiGet<unknown>(`/v1/instance-operations/param/?${params.toString()}`, { token }).then((payload) =>
    extractData<InstanceOperationParamList>(payload),
  )
}

export function fetchInstanceOperationParamHistory(
  token: string,
  options: { instance_id: number; search?: string; page?: number; size?: number },
) {
  const params = new URLSearchParams({ instance_id: `${options.instance_id}` })
  if (options.search) {
    params.set('search', options.search)
  }
  if (options.page) {
    params.set('page', `${options.page}`)
  }
  if (options.size) {
    params.set('size', `${options.size}`)
  }

  return apiGet<unknown>(`/v1/instance-operations/param/history/?${params.toString()}`, { token }).then((payload) =>
    extractData<InstanceOperationParamHistoryList>(payload),
  )
}

export function editInstanceOperationParam(payload: InstanceOperationParamEditPayload, token: string) {
  return apiPost<unknown>('/v1/instance-operations/param/edit/', payload, { token }).then((responsePayload) =>
    extractData<Record<string, never>>(responsePayload),
  )
}

export function fetchInstanceOperationDiagnosticInstances(token: string) {
  return apiGet<unknown>('/v1/instance-operations/diagnostic/instances/', { token }).then((payload) =>
    extractData<InstanceOperationDiagnosticInstance[]>(payload),
  )
}

export function fetchInstanceOperationDiagnosticProcesses(
  token: string,
  options: { instance_id: number; command_type?: string },
) {
  const params = new URLSearchParams({ instance_id: `${options.instance_id}` })
  if (options.command_type) {
    params.set('command_type', options.command_type)
  }
  return apiGet<unknown>(`/v1/instance-operations/diagnostic/processes/?${params.toString()}`, { token }).then((payload) =>
    extractData<InstanceOperationDiagnosticList>(payload),
  )
}

export function previewInstanceOperationDiagnosticKill(payload: InstanceOperationDiagnosticKillPayload, token: string) {
  return apiPost<unknown>('/v1/instance-operations/diagnostic/kill/preview/', payload, { token }).then((responsePayload) =>
    extractData<InstanceOperationDiagnosticKillPreview>(responsePayload),
  )
}

export function killInstanceOperationDiagnosticSessions(payload: InstanceOperationDiagnosticKillPayload, token: string) {
  return apiPost<unknown>('/v1/instance-operations/diagnostic/kill/', payload, { token }).then((responsePayload) =>
    extractData<Record<string, never>>(responsePayload),
  )
}

export function fetchInstanceOperationDiagnosticTablespace(
  token: string,
  options: { instance_id: number; page?: number; size?: number },
) {
  const params = new URLSearchParams({ instance_id: `${options.instance_id}` })
  if (options.page) {
    params.set('page', `${options.page}`)
  }
  if (options.size) {
    params.set('size', `${options.size}`)
  }
  return apiGet<unknown>(`/v1/instance-operations/diagnostic/tablespace/?${params.toString()}`, { token }).then((payload) =>
    extractData<InstanceOperationDiagnosticList>(payload),
  )
}

export function fetchInstanceOperationDiagnosticTransactions(token: string, instanceId: number) {
  return apiGet<unknown>(`/v1/instance-operations/diagnostic/transactions/?instance_id=${instanceId}`, { token }).then((payload) =>
    extractData<InstanceOperationDiagnosticList>(payload),
  )
}

export function fetchInstanceOperationDiagnosticLocks(token: string, instanceId: number) {
  return apiGet<unknown>(`/v1/instance-operations/diagnostic/locks/?instance_id=${instanceId}`, { token }).then((payload) =>
    extractData<InstanceOperationDiagnosticList>(payload),
  )
}

export function executeQuery(request: QueryExecuteRequest, token: string) {
  return apiPost<unknown>('/v1/query/', request, { token }).then((payload) =>
    extractData<QueryResultPayload>(payload),
  )
}

export function describeQueryTable(request: QueryDescribeRequest, token: string) {
  return apiPost<unknown>('/v1/query/describe/', request, { token }).then((payload) =>
    extractData<QueryDescribePayload>(payload),
  )
}

export function fetchFavoriteQueries(token: string) {
  return apiGet<unknown>('/v1/query/favorite/', { token }).then((payload) =>
    extractData<FavoriteQuery[]>(payload),
  )
}

export function updateFavoriteQuery(
  queryLogId: number,
  star: boolean,
  alias: string,
  token: string,
) {
  return apiPost<unknown>(
    '/v1/query/favorite/',
    {
      query_log_id: queryLogId,
      star,
      alias,
    },
    { token },
  ).then((payload) => extractDetail(payload, 'Favorite updated.'))
}

export function fetchQueryLogs(filters: QueryLogFilters, token: string) {
  const params = new URLSearchParams()

  if (filters.page) {
    params.set('page', `${filters.page}`)
  }
  if (filters.size) {
    params.set('size', `${filters.size}`)
  }
  if (filters.search) {
    params.set('search', filters.search)
  }
  if (filters.star) {
    params.set('star', filters.star)
  }
  if (filters.query_log_id) {
    params.set('query_log_id', `${filters.query_log_id}`)
  }

  return apiGet<unknown>(`/v1/query/log/?${params.toString()}`, { token }).then((payload) =>
    extractData<PaginatedResponse<QueryLogRecord>>(payload),
  )
}

export type PermissionRequestTarget = 'resource_group' | 'instance'
export type PermissionRequestStatus = 0 | 1 | 2 | 3
export type PermissionInstanceAccessLevel = 'query' | 'query_dml' | 'query_dml_ddl'
export type PermissionGrantType = 'resource_group' | 'instance'

export type PermissionResourceGroupLookupRecord = {
  group_id: number
  group_name: string
  label: string
}

export type PermissionInstanceLookupRecord = {
  id: number
  instance_name: string
  db_type: string
  type: string
  host: string
  label: string
  resource_groups: PermissionResourceGroupLookupRecord[]
}

export type PermissionRequestRecord = {
  request_id: number
  title: string
  reason: string
  target_type: PermissionRequestTarget
  resource_group_id: number
  resource_group_name: string
  instance_id: number | null
  instance_name: string
  access_level: PermissionInstanceAccessLevel | ''
  valid_date: string
  user_name: string
  user_display: string
  status: PermissionRequestStatus
  create_time: string
}

export type PermissionRequestReviewNode = {
  group_name: string
  is_current_node: boolean
  is_passed_node: boolean
}

export type PermissionRequestLogRecord = {
  operation_type_desc: string
  operation_info: string
  operator_display: string
  operation_time: string
}

export type PermissionRequestDetailRecord = PermissionRequestRecord & {
  review_info: PermissionRequestReviewNode[]
  is_can_review: boolean
  logs: PermissionRequestLogRecord[]
}

export type PermissionGrantRecord = {
  grant_type: PermissionGrantType
  grant_id: number
  user_name: string
  user_display: string
  resource_group_id: number
  resource_group_name: string
  instance_id: number | null
  instance_name: string
  access_level: PermissionInstanceAccessLevel | ''
  valid_date: string
  source_request_id: number | null
  create_time: string
}

export type PermissionRequestListFilters = {
  page?: number
  size?: number
  search?: string
}

export type PermissionGrantListFilters = {
  page?: number
  size?: number
  search?: string
}

export type PermissionRequestCreatePayload = {
  title: string
  reason?: string
  target_type: PermissionRequestTarget
  resource_group_id: number
  instance_id?: number
  access_level?: PermissionInstanceAccessLevel
  valid_date: string
}

export type PermissionRequestCreateResult = {
  request_id: number
}

export type PermissionRequestReviewPayload = {
  audit_status: 1 | 2
  audit_remark?: string
}

function buildListQueryString(filters: { page?: number; size?: number; search?: string }) {
  const params = new URLSearchParams()

  if (filters.page) {
    params.set('page', `${filters.page}`)
  }
  if (filters.size) {
    params.set('size', `${filters.size}`)
  }
  if (filters.search?.trim()) {
    params.set('search', filters.search.trim())
  }

  return params.toString()
}

function buildMailboxQueryString(filters: MailboxListFilters) {
  const params = new URLSearchParams()

  if (filters.page) {
    params.set('page', `${filters.page}`)
  }
  if (filters.size) {
    params.set('size', `${filters.size}`)
  }
  if (filters.state && filters.state !== 'all') {
    params.set('state', filters.state)
  }
  if (filters.category) {
    params.set('category', filters.category)
  }
  if (filters.source_type) {
    params.set('source_type', filters.source_type)
  }

  return params.toString()
}

export function fetchMailboxSummary(token: string) {
  return apiGet<unknown>('/v1/mailbox/summary/', { token }).then((payload) =>
    extractData<MailboxSummary>(payload),
  )
}

export function fetchMailboxItems(token: string, filters: MailboxListFilters = {}) {
  const queryString = buildMailboxQueryString(filters)
  const path = queryString ? `/v1/mailbox/items/?${queryString}` : '/v1/mailbox/items/'
  return apiGet<unknown>(path, { token }).then((payload) =>
    extractData<PaginatedResponse<MailboxItem>>(payload),
  )
}

export function markMailboxItemRead(itemId: number, token: string) {
  return apiPost<unknown>(`/v1/mailbox/items/${itemId}/read/`, {}, { token }).then((payload) =>
    extractData<MailboxItem>(payload),
  )
}

export function markAllMailboxItemsRead(token: string) {
  return apiPost<unknown>('/v1/mailbox/items/read-all/', {}, { token }).then((payload) =>
    extractData<{ updated: number }>(payload),
  )
}

export function fetchPermissionResourceGroupsLookup(token: string) {
  return apiGet<unknown>('/v1/access/resource-groups/lookup/', { token }).then((payload) =>
    extractData<PermissionResourceGroupLookupRecord[]>(payload),
  )
}

export function fetchPermissionInstancesLookup(token: string) {
  return apiGet<unknown>('/v1/access/instances/lookup/', { token }).then((payload) =>
    extractData<PermissionInstanceLookupRecord[]>(payload),
  )
}

export function fetchPermissionRequests(
  token: string,
  filters: PermissionRequestListFilters = {},
) {
  const queryString = buildListQueryString(filters)
  const path = queryString ? `/v1/access/request/?${queryString}` : '/v1/access/request/'
  return apiGet<unknown>(path, { token }).then((payload) =>
    extractData<PaginatedResponse<PermissionRequestRecord>>(payload),
  )
}

export function createPermissionRequest(payload: PermissionRequestCreatePayload, token: string) {
  return apiPost<unknown>('/v1/access/request/', payload, { token }).then((responsePayload) =>
    extractData<PermissionRequestCreateResult>(responsePayload),
  )
}

export function fetchPermissionRequestDetail(requestId: number, token: string) {
  return apiGet<unknown>(`/v1/access/request/${requestId}/`, { token }).then((payload) =>
    extractData<PermissionRequestDetailRecord>(payload),
  )
}

export function reviewPermissionRequest(
  requestId: number,
  payload: PermissionRequestReviewPayload,
  token: string,
) {
  return apiPost<unknown>(`/v1/access/request/${requestId}/reviews/`, payload, { token }).then(
    (responsePayload) => extractDetail(responsePayload, 'Request reviewed successfully.'),
  )
}

export type WorkflowContentRecord = {
  source: 'review' | 'execution'
  rows: Array<Record<string, unknown>>
  column_list: string[]
}

export type WorkflowRollbackRecord = {
  rows: Array<[string, string]>
  download_content: string
}

export function fetchPermissionGrants(token: string, filters: PermissionGrantListFilters = {}) {
  const queryString = buildListQueryString(filters)
  const path = queryString ? `/v1/access/grant/?${queryString}` : '/v1/access/grant/'
  return apiGet<unknown>(path, { token }).then((payload) =>
    extractData<PaginatedResponse<PermissionGrantRecord>>(payload),
  )
}

export function revokePermissionGrant(
  grantType: PermissionGrantType,
  grantId: number,
  token: string,
) {
  return apiDelete<unknown>(`/v1/access/grant/${grantType}/${grantId}/`, { token }).then((payload) =>
    extractDetail(payload, 'Grant revoked successfully.'),
  )
}

export type WorkflowScope = 'all' | 'mine' | 'pending_review'
export type WorkflowExecutionMode = 'auto' | 'manual'

export type WorkflowSubmitResourceGroupRecord = {
  group_id: number
  group_name: string
  label: string
}

export type WorkflowSubmitInstanceRecord = {
  id: number
  instance_name: string
  db_type: string
  type: string
  group_ids: number[]
  group_names: string[]
  allowed_syntax_types: WorkflowSyntaxType[]
}

export type WorkflowSubmissionMetadata = {
  resource_groups: WorkflowSubmitResourceGroupRecord[]
  instances: WorkflowSubmitInstanceRecord[]
  enable_backup_switch: boolean
  manual_execution_enabled: boolean
}

export type WorkflowApprovalPreview = {
  group_id: number
  group_name: string
  audit_auth_groups: string
  display: string
  review_info: Array<{
    group_name: string
    is_auto_pass: boolean
    is_current_node: boolean
    is_passed_node: boolean
  }>
}

export type WorkflowCheckRequest = {
  instance_id: number
  db_name: string
  schema_name?: string
  full_sql: string
}

export type WorkflowParseRequest = {
  text: string
  db_type?: string
}

export type WorkflowParsedStatementRecord = {
  sql_id: string | number
  sql: string
  syntax_type: WorkflowSyntaxType | null
}

export type WorkflowParseSummary = {
  syntax_type: WorkflowSyntaxType | null
  has_mixed_syntax: boolean
  has_unknown_syntax: boolean
}

export type WorkflowParseResult = {
  total: number
  rows: WorkflowParsedStatementRecord[]
  summary: WorkflowParseSummary
}

export type WorkflowCheckResult = {
  is_execute: boolean
  checked: string | null
  warning: string | null
  error: string | null
  warning_count: number
  error_count: number
  is_critical: boolean
  syntax_type: number
  rows: Array<Record<string, unknown>>
  column_list: string[]
  status: string
  affected_rows: number
}

export type WorkflowCreatePayload = {
  workflow: {
    workflow_name: string
    demand_url?: string
    group_id: number
    db_name: string
    schema_name?: string | null
    instance: number
    is_backup?: boolean
    is_offline_export: 0 | 1
    export_format?: 'csv' | 'tsv' | 'sql' | 'xlsx'
    run_date_start?: string | null
    run_date_end?: string | null
  }
  sql_content: string
}

export type WorkflowDownloadResult =
  | {
      mode: 'redirect'
      url: string
    }
  | {
      mode: 'blob'
      data: Blob
      filename: string
    }

export type WorkflowCreateResult = {
  id: number
  workflow_id: number
  workflow: WorkflowSummaryRecord & {
    instance: number
  }
  sql_content: string
  review_content: string
  execute_result: string
}

export type WorkflowListFilters = {
  page?: number
  size?: number
  search?: string
  scope?: WorkflowScope
  status?: string
  syntax_type?: WorkflowSyntaxType | ''
  instance_id?: number | ''
  group_id?: number | ''
  start_date?: string
  end_date?: string
}

export type WorkflowReviewPayload = {
  workflow_type: 2
  audit_type: 'pass' | 'reject' | 'cancel'
  audit_remark: string
}

export type WorkflowExecutionPayload = {
  workflow_type: 2
  mode: WorkflowExecutionMode
  executor?: 'direct' | 'gh-ost' | 'pt-osc'
}

export type WorkflowSchedulePayload = {
  run_date: string
  executor?: 'direct' | 'gh-ost' | 'pt-osc'
}

export type WorkflowWindowPayload = {
  run_date_start?: string | null
  run_date_end?: string | null
}

export type ArchiveMethod = 'dml' | 'pt_archiver'
export type ArchiveExecutionMode = 'one_time' | 'scheduled'
export type ArchiveScheduleFrequency = 'daily' | 'weekly'
export type ArchiveWeekday = 'mon' | 'tue' | 'wed' | 'thu' | 'fri' | 'sat' | 'sun'

export type ArchiveResourceGroupRecord = {
  group_id: number
  group_name: string
  label: string
}

export type ArchiveInstanceRecord = {
  id: number
  instance_name: string
  db_type: string
  type: string
  label: string
  group_ids: number[]
  group_names: string[]
  available_archive_methods: ArchiveMethod[]
}

export type ArchiveMetadataRecord = {
  resource_groups: ArchiveResourceGroupRecord[]
  instances: ArchiveInstanceRecord[]
  schedule_frequencies: Array<{ value: ArchiveScheduleFrequency; label: string }>
  weekdays: Array<{ value: ArchiveWeekday; label: string }>
}

export type ArchiveApprovalPreview = {
  group_id: number
  group_name: string
  audit_auth_groups: string
  display: string
  review_info: Array<{
    group_name: string
    is_auto_pass: boolean
    is_current_node: boolean
    is_passed_node: boolean
  }>
}

export type ArchiveListRecord = {
  id: number
  title: string
  status: number
  status_label: string
  archive_method: ArchiveMethod
  execution_mode: ArchiveExecutionMode
  schedule_frequency: ArchiveScheduleFrequency | null
  state: boolean
  src_instance_name: string
  src_db_name: string
  src_table_name: string
  resource_group_name: string
  user_display: string
  create_time: string
  last_archive_time: string | null
  next_run_at: string | null
}

export type ArchiveLogRecord = {
  id: number
  cmd: string
  condition: string
  archive_method: ArchiveMethod
  mode: string
  success: boolean
  error_info: string
  select_cnt: number
  insert_cnt: number
  delete_cnt: number
  start_time: string
  end_time: string
  statistics: string
}

export type ArchiveDetailRecord = {
  id: number
  title: string
  status: number
  status_label: string
  execution_state_label: string
  archive_method: ArchiveMethod
  execution_mode: ArchiveExecutionMode
  schedule_frequency: ArchiveScheduleFrequency | null
  schedule_time: string | null
  schedule_weekdays: ArchiveWeekday[]
  next_run_at: string | null
  last_archive_time: string | null
  state: boolean
  resource_group: {
    group_id: number
    group_name: string
  }
  src_instance: {
    id: number
    instance_name: string
    db_type: string
  }
  src_db_name: string
  src_table_name: string
  condition: string
  sleep: number
  create_time: string
  user_name: string
  user_display: string
  review_info: Array<{
    group_name: string
    is_current_node: boolean
    is_passed_node: boolean
  }>
  current_reviewers: WorkflowCurrentReviewer[]
  logs: WorkflowLogRecord[]
  archive_logs: ArchiveLogRecord[]
  last_operation_info: string
  is_can_review: boolean
  is_can_cancel: boolean
  is_can_run_now: boolean
  is_can_enable: boolean
  is_can_disable: boolean
}

export type ArchiveCreatePayload = {
  title: string
  group_id: number
  instance_id: number
  db_name: string
  table_name: string
  condition: string
  archive_method: ArchiveMethod
  execution_mode: ArchiveExecutionMode
  schedule_frequency?: ArchiveScheduleFrequency | null
  schedule_time?: string | null
  schedule_weekdays?: ArchiveWeekday[]
  sleep?: number
}

export type ArchiveReviewPayload = {
  audit_type: 'pass' | 'reject' | 'cancel'
  audit_remark: string
}

export type ArchiveStatePayload = {
  enabled: boolean
}

export type ArchiveListFilters = {
  page?: number
  size?: number
  search?: string
  status?: number | ''
  execution_mode?: ArchiveExecutionMode | ''
  instance_id?: number | ''
  group_id?: number | ''
}

function buildWorkflowListQueryString(filters: WorkflowListFilters) {
  const params = new URLSearchParams()

  if (filters.page) {
    params.set('page', `${filters.page}`)
  }
  if (filters.size) {
    params.set('size', `${filters.size}`)
  }
  if (filters.search?.trim()) {
    params.set('search', filters.search.trim())
  }
  if (filters.scope && filters.scope !== 'all') {
    params.set('scope', filters.scope)
  }
  if (filters.status?.trim()) {
    params.set('status', filters.status.trim())
  }
  if (filters.syntax_type) {
    params.set('syntax_type', `${filters.syntax_type}`)
  }
  if (filters.instance_id) {
    params.set('instance_id', `${filters.instance_id}`)
  }
  if (filters.group_id) {
    params.set('group_id', `${filters.group_id}`)
  }
  if (filters.start_date?.trim()) {
    params.set('start_date', filters.start_date.trim())
  }
  if (filters.end_date?.trim()) {
    params.set('end_date', filters.end_date.trim())
  }

  return params.toString()
}

function buildArchiveListQueryString(filters: ArchiveListFilters) {
  const params = new URLSearchParams()

  if (filters.page) {
    params.set('page', `${filters.page}`)
  }
  if (filters.size) {
    params.set('size', `${filters.size}`)
  }
  if (filters.search?.trim()) {
    params.set('search', filters.search.trim())
  }
  if (filters.status !== undefined && filters.status !== '') {
    params.set('status', `${filters.status}`)
  }
  if (filters.execution_mode) {
    params.set('execution_mode', filters.execution_mode)
  }
  if (filters.instance_id) {
    params.set('instance_id', `${filters.instance_id}`)
  }
  if (filters.group_id) {
    params.set('group_id', `${filters.group_id}`)
  }

  return params.toString()
}
export function fetchWorkflowSubmissionMetadata(token: string) {
  return apiGet<unknown>('/v1/workflow/submission-metadata/', { token }).then((payload) =>
    extractData<WorkflowSubmissionMetadata>(payload),
  )
}

export function fetchWorkflowExportSubmissionMetadata(token: string) {
  return apiGet<unknown>('/v1/workflow/export/submission-metadata/', { token }).then((payload) =>
    extractData<WorkflowSubmissionMetadata>(payload),
  )
}

export function fetchWorkflowApprovalPreview(groupId: number, token: string) {
  return apiGet<unknown>(`/v1/workflow/approval-preview/?group_id=${groupId}`, { token }).then(
    (payload) => extractData<WorkflowApprovalPreview>(payload),
  )
}

export function fetchWorkflows(token: string, filters: WorkflowListFilters = {}) {
  const queryString = buildWorkflowListQueryString(filters)
  const path = queryString ? `/v1/workflow/?${queryString}` : '/v1/workflow/'
  return apiGet<unknown>(path, { token }).then((payload) =>
    extractData<PaginatedResponse<WorkflowSummaryRecord>>(payload),
  )
}

export function checkWorkflowSql(payload: WorkflowCheckRequest, token: string) {
  return apiPost<unknown>('/v1/workflow/sqlcheck/', payload, { token }).then((responsePayload) =>
    extractData<WorkflowCheckResult>(responsePayload),
  )
}

export function checkWorkflowExportSql(payload: WorkflowCheckRequest, token: string) {
  return apiPost<unknown>('/v1/workflow/export/sqlcheck/', payload, { token }).then(
    (responsePayload) => extractData<WorkflowCheckResult>(responsePayload),
  )
}

export function parseWorkflowSql(payload: WorkflowParseRequest, token: string) {
  return apiPost<unknown>('/v1/workflow/parse/', payload, { token }).then((responsePayload) =>
    extractData<WorkflowParseResult>(responsePayload),
  )
}

export function createWorkflow(payload: WorkflowCreatePayload, token: string) {
  return apiPost<unknown>('/v1/workflow/', payload, { token }).then((responsePayload) =>
    extractData<WorkflowCreateResult>(responsePayload),
  )
}

export function fetchWorkflowDetail(workflowId: number, token: string) {
  return apiGet<unknown>(`/v1/workflow/${workflowId}/`, { token }).then((payload) =>
    extractData<WorkflowDetailRecord>(payload),
  )
}

export async function downloadWorkflowExport(
  workflowId: number,
  token: string,
  options: { skipAuthRetry?: boolean } = {},
): Promise<WorkflowDownloadResult> {
  let authorizationToken = ''

  try {
    authorizationToken = await getUsableAccessToken(token)
  } catch (error) {
    if (error instanceof AuthSessionExpiredError) {
      notifyUnauthorized(error.message)
      throw new Error(`GET /v1/workflow/${workflowId}/download/ failed (401): ${error.message}`)
    }
    throw error
  }

  const response = await fetch(buildUrl(`/v1/workflow/${workflowId}/download/`), {
    method: 'GET',
    headers: {
      Accept: 'application/json',
      Authorization: `Bearer ${authorizationToken}`,
    },
  })

  if (!response.ok) {
    const body = await response.text()
    let message = body

    try {
      message = flattenErrorMessage(JSON.parse(body)) || body
    } catch {
      message = body
    }

    if (response.status === 401 && !options.skipAuthRetry) {
      try {
        const refreshedAccessToken = await refreshAccessToken()
        return downloadWorkflowExport(workflowId, refreshedAccessToken, { skipAuthRetry: true })
      } catch (error) {
        if (error instanceof AuthSessionExpiredError) {
          notifyUnauthorized(error.message)
          throw new Error(`GET /v1/workflow/${workflowId}/download/ failed (401): ${error.message}`)
        }
        throw error
      }
    }

    if (response.status === 401) {
      notifyUnauthorized(message)
    }

    throw new Error(`GET /v1/workflow/${workflowId}/download/ failed (${response.status}): ${message}`)
  }

  const contentType = response.headers.get('content-type') || ''
  if (contentType.includes('application/json')) {
    const payload = await response.json()
    const redirectUrl = isRecord(payload) && typeof payload.url === 'string'
      ? payload.url
      : isRecord(payload)
        && isRecord(payload.data)
        && typeof payload.data.url === 'string'
          ? payload.data.url
          : ''

    if (!redirectUrl) {
      throw new Error('Download response did not include a redirect URL.')
    }

    return {
      mode: 'redirect',
      url: redirectUrl,
    }
  }

  const blob = await response.blob()
  const disposition = response.headers.get('content-disposition') || ''
  const fileNameMatch = disposition.match(/filename="?([^"]+)"?$/i)
  const fileName = fileNameMatch?.[1] || `workflow-${workflowId}-export`

  return {
    mode: 'blob',
    data: blob,
    filename: fileName,
  }
}

export function fetchWorkflowContent(workflowId: number, token: string) {
  return apiGet<unknown>(`/v1/workflow/${workflowId}/content/`, { token }).then((payload) =>
    extractData<WorkflowContentRecord>(payload),
  )
}

export function fetchWorkflowRollback(workflowId: number, token: string) {
  return apiGet<unknown>(`/v1/workflow/${workflowId}/rollback/`, { token }).then((payload) =>
    extractData<WorkflowRollbackRecord>(payload),
  )
}

export function reviewWorkflow(
  workflowId: number,
  payload: WorkflowReviewPayload,
  token: string,
) {
  return apiPost<unknown>(`/v1/workflow/${workflowId}/reviews/`, payload, { token }).then(
    (responsePayload) => extractDetail(responsePayload, 'Workflow reviewed successfully.'),
  )
}

export function executeWorkflow(
  workflowId: number,
  payload: WorkflowExecutionPayload,
  token: string,
) {
  return apiPost<unknown>(`/v1/workflow/${workflowId}/executions/`, payload, { token }).then(
    (responsePayload) => extractDetail(responsePayload, 'Workflow execution started.'),
  )
}

export function scheduleWorkflow(
  workflowId: number,
  payload: WorkflowSchedulePayload,
  token: string,
) {
  return apiPost<unknown>(`/v1/workflow/${workflowId}/schedule/`, payload, { token }).then(
    (responsePayload) => extractDetail(responsePayload, 'Workflow scheduled for execution.'),
  )
}

export function fetchArchiveMetadata(token: string) {
  return apiGet<unknown>('/v1/archive/metadata/', { token }).then((payload) =>
    extractData<ArchiveMetadataRecord>(payload),
  )
}

export function fetchArchiveApprovalPreview(groupId: number, token: string) {
  return apiGet<unknown>(`/v1/archive/approval-preview/?group_id=${groupId}`, { token }).then(
    (payload) => extractData<ArchiveApprovalPreview>(payload),
  )
}

export function fetchArchives(token: string, filters: ArchiveListFilters = {}) {
  const queryString = buildArchiveListQueryString(filters)
  const path = queryString ? `/v1/archive/?${queryString}` : '/v1/archive/'
  return apiGet<unknown>(path, { token }).then((payload) =>
    extractData<PaginatedResponse<ArchiveListRecord>>(payload),
  )
}

export function createArchive(payload: ArchiveCreatePayload, token: string) {
  return apiPost<unknown>('/v1/archive/', payload, { token }).then((responsePayload) =>
    extractData<{ id: number }>(responsePayload),
  )
}

export function fetchArchiveDetail(archiveId: number, token: string) {
  return apiGet<unknown>(`/v1/archive/${archiveId}/`, { token }).then((payload) =>
    extractData<ArchiveDetailRecord>(payload),
  )
}

export function reviewArchive(
  archiveId: number,
  payload: ArchiveReviewPayload,
  token: string,
) {
  return apiPost<unknown>(`/v1/archive/${archiveId}/reviews/`, payload, { token }).then(
    (responsePayload) => extractDetail(responsePayload, 'Archive workflow reviewed successfully.'),
  )
}

export function runArchiveNow(archiveId: number, token: string) {
  return apiPost<unknown>(`/v1/archive/${archiveId}/run/`, {}, { token }).then(
    (responsePayload) => extractDetail(responsePayload, 'Archive execution queued.'),
  )
}

export function updateArchiveState(
  archiveId: number,
  payload: ArchiveStatePayload,
  token: string,
) {
  return apiPost<unknown>(`/v1/archive/${archiveId}/state/`, payload, { token }).then(
    (responsePayload) => extractDetail(responsePayload, 'Archive schedule updated.'),
  )
}

export function fetchArchiveLogs(archiveId: number, token: string, page = 1, size = 20) {
  return apiGet<unknown>(`/v1/archive/${archiveId}/logs/?page=${page}&size=${size}`, { token }).then(
    (payload) => extractData<PaginatedResponse<ArchiveLogRecord>>(payload),
  )
}

export function updateWorkflowExecutionWindow(
  workflowId: number,
  payload: WorkflowWindowPayload,
  token: string,
) {
  return apiPatch<unknown>(`/v1/workflow/${workflowId}/execution-window/`, payload, {
    token,
  }).then((responsePayload) =>
    extractDetail(responsePayload, 'Execution window updated successfully.'),
  )
}
