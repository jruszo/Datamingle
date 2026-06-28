import {
  AuthSessionExpiredError,
  getUsableAccessToken,
  notifyUnauthorized,
  refreshAccessToken,
} from '@/shared/auth/auth'
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
import { allauthHeadlessPath } from '@/shared/auth/allauth'

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

export type CurrentUserContext = {
  id: number
  username: string
  display: string
  email: string
  avatar_url: string
  is_superuser: boolean
  is_staff: boolean
  is_active: boolean
  groups: Array<{ id: number; name: string }>
  teams: Array<{ team_id: number; team_name: string }>
  permissions: string[]
}

export type SystemSettingsValue = string | number | boolean | Array<string | number> | null

export type SystemSettings = Record<string, SystemSettingsValue>

export type SystemSettingsOption = {
  value: string | number
  label: string
}

export type SystemSettingsOptions = {
  instance_tags: SystemSettingsOption[]
  auth_groups: SystemSettingsOption[]
  teams: SystemSettingsOption[]
  users: SystemSettingsOption[]
  notify_phases: SystemSettingsOption[]
  auto_review_db_types: SystemSettingsOption[]
  storage_types: SystemSettingsOption[]
  sms_providers: SystemSettingsOption[]
  inventory_refresh_intervals: SystemSettingsOption[]
}

export type SystemSettingsPayload = {
  settings: SystemSettings
  options: SystemSettingsOptions
}

export type PermissionLevelId = number

export type PermissionLevelRecord = {
  id: number
  name: string
  permissions: string[]
  membership_count: number
}

export type PermissionLevelPayload = {
  name: string
  permission_codes: string[]
}

export type AvailablePermissionRecord = {
  code: string
  codename: string
  name: string
}

export type PermissionCategoryRecord = {
  category: string
  permissions: AvailablePermissionRecord[]
}

export type WorkflowPolicyStepRecord = {
  id: number
  order: number
  permission_group: number
  permission_group_name: string
}

export type WorkflowPolicyRecord = {
  id: number
  name: string
  description: string
  is_active: boolean
  steps: WorkflowPolicyStepRecord[]
  created_by: string
  updated_by: string
  can_edit: boolean
  create_time?: string
  update_time?: string
}

export type WorkflowPolicyPayload = {
  name: string
  description: string
  is_active: boolean
  steps: Array<{
    order: number
    permission_group: number
  }>
}

export type WorkflowPolicyMetadata = {
  permission_groups: Array<{
    id: number
    name: string
  }>
}

export type TeamUserAccessRecord = {
  user_id: number
  username?: string
  display?: string
  permission_level_id: PermissionLevelId
  permission_level_name: string
}

export type UserManagementTeamRecord = {
  team_id: number
  team_name: string
  permission_level_id: PermissionLevelId
  permission_level_name: string
}

export type UserManagementRecord = {
  id: number
  username: string
  display: string
  email: string
  is_active: boolean
  is_superuser: boolean
  is_staff: boolean
  teams: UserManagementTeamRecord[]
}

export type UserManagementDetailRecord = UserManagementRecord & {
  team_ids: number[]
}

export type UserManagementListOptions = {
  page?: number
  size?: number
  search?: string
  ordering?: string
}

export type UpdateUserPayload = {
  is_active?: boolean
}

export type CreateUserPayload = {
  email: string
  display?: string
  password: string
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

export type TeamRecord = {
  team_id: number
  team_name: string
  user_count: number
  node_count: number
  service_count: number
}

export type TeamDetailRecord = TeamRecord & {
  user_access: TeamUserAccessRecord[]
  node_ids: number[]
  service_ids: number[]
}

export type TeamUserLookupRecord = {
  id: number
  username: string
  display: string
  label: string
}

export type TeamInstanceLookupRecord = {
  id: number
  instance_name: string
  db_type: string
  host: string
  label: string
}

export type TeamNodeLookupRecord = {
  id: number
  name: string
  address: string
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
  team_ids: number[]
  inventory_status: 'never' | 'ok' | 'stale' | 'failed'
  inventory_detected_hostname: string
  inventory_detected_version: string
  inventory_last_refresh_at: string | null
}

export type InstanceOptionRecord = {
  value: string
  label: string
}

export type TeamOptionRecord = {
  team_id: number
  team_name: string
  label: string
}

export type InstanceInventoryMetadata = {
  instance_types: InstanceOptionRecord[]
  db_types: InstanceOptionRecord[]
  teams: TeamOptionRecord[]
}

export type InstanceInventoryFilters = {
  page?: number
  size?: number
  search?: string
  type?: string
  db_type?: string
  ordering?: string
}

export type AgentStatus = 'pending' | 'online' | 'offline' | 'disabled' | 'revoked'

export type AgentRecord = {
  id: number
  organization_id: string
  name: string
  display_name: string
  status: AgentStatus
  hostname: string
  platform: string
  architecture: string
  agent_version: string
  last_seen_at: string | null
  last_connected_at: string | null
  last_disconnected_at: string | null
  last_config_revision: number
  desired_config_revision: number
  enabled: boolean
  local_node: number | null
  local_node_name: string
  assignment_count: number
  create_time: string
  update_time: string
}

export type AgentAssignmentRecord = {
  id: number
  instance: number
  node: number | null
  node_assignment: number | null
  local_node: number | null
  inherited: boolean
  instance_name: string
  db_type: string
  host: string
  port: number
  workflow_enabled: boolean
  enabled: boolean
  modules: string[]
  capabilities: string[]
  command_enabled: boolean
  metrics_enabled: boolean
  online_schema_enabled: boolean
  logs_enabled: boolean
  create_time: string
  update_time: string
}

export type AgentCommandSummaryRecord = {
  id: number
  instance: number
  instance_name: string
  workflow_type: string
  workflow_id: string
  command_type: string
  status: string
  queued_at: string
  started_at: string | null
  finished_at: string | null
  cancel_requested_at: string | null
  create_time: string
}

export type AgentCommandEventRecord = {
  id: number
  event_type: string
  message: string
  payload: Record<string, unknown>
  create_time: string
}

export type AgentCommandDetailRecord = AgentCommandSummaryRecord & {
  payload: Record<string, unknown>
  result: Record<string, unknown>
  error: Record<string, unknown>
  lease_owner: string
  lease_expires_at: string | null
  cancel_requested_at: string | null
  events: AgentCommandEventRecord[]
}

export type AgentDetailRecord = AgentRecord & {
  metadata: Record<string, unknown>
  assignments: AgentAssignmentRecord[]
  recent_commands: AgentCommandSummaryRecord[]
}

export type AgentCreatePayload = {
  name?: string
  display_name?: string
  node_name?: string
  local_node?: number | null
  monitoring_enabled?: boolean
  monitoring_collectors?: string[]
}

export type AgentCreateResponse = AgentDetailRecord & {
  api_key: string
  api_key_backend: string
  api_key_id?: string
  api_key_prefix?: string
  install_command: string
}

export type AgentAssignmentReplaceItem = {
  instance: number
  enabled: boolean
  modules: string[]
  capabilities: string[]
  command_enabled: boolean
  metrics_enabled: boolean
  online_schema_enabled: boolean
  logs_enabled: boolean
}

export type AgentAssignmentReplacePayload = {
  assignments: AgentAssignmentReplaceItem[]
}

export type AgentListOptions = {
  page?: number
  size?: number
  search?: string
}

export type AgentCommandListOptions = {
  page?: number
  size?: number
  search?: string
  status?: string
}

export type InstanceCreatePayload = {
  instance_name: string
  type: string
  db_type: string
  host: string
  port: number
  user: string
  password: string
  workflow_enabled: boolean
  workflow_policy?: number | null
  is_ssl: boolean
  verify_ssl: boolean
  db_name: string
  show_db_name_regex: string
  denied_db_name_regex: string
  charset: string
  service_name: string
  sid: string
  team_ids: number[]
}

export type InstanceEditorRecord = InstanceCreatePayload & {
  id: number
}

export type TeamUpsertPayload = {
  team_name: string
  user_access?: Array<{
    user_id: number
    permission_level_id: PermissionLevelId
  }>
  node_ids: number[]
  service_ids: number[]
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

function extractAllauthTokenPair(payload: unknown): TokenPair {
  const meta = isRecord(payload) && isRecord(payload.meta) ? payload.meta : null
  if (!meta) {
    throw new Error('Token response did not include authentication metadata')
  }
  if (typeof meta.access_token !== 'string' || typeof meta.refresh_token !== 'string') {
    throw new Error('Token response did not include access/refresh fields')
  }
  return {
    access: meta.access_token,
    refresh: meta.refresh_token,
  }
}

export function loginWithPassword(email: string, password: string) {
  return apiPost<unknown>(allauthHeadlessPath('/auth/login'), { email, password }).then(
    extractAllauthTokenPair,
  )
}

export function fetchCurrentUserContext(token: string) {
  return apiGet<unknown>('/v1/me/', { token }).then((payload) => {
    const user = extractData<CurrentUserContext>(payload)
    return {
      ...user,
      groups: Array.isArray(user.groups) ? user.groups : [],
      teams: Array.isArray(user.teams) ? user.teams : [],
      permissions: Array.isArray(user.permissions) ? user.permissions : [],
    }
  })
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

export function createUser(payload: CreateUserPayload, token: string) {
  return apiPost<unknown>('/v1/user/', payload, { token }).then((responsePayload) =>
    extractData<UserManagementRecord>(responsePayload),
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

export function fetchPermissionLevels(token: string) {
  return apiGet<unknown>('/v1/permission-levels/', { token }).then((payload) =>
    extractData<PermissionLevelRecord[]>(payload),
  )
}

export function fetchPermissionLevel(levelId: number, token: string) {
  return apiGet<unknown>(`/v1/permission-levels/${levelId}/`, { token }).then((payload) =>
    extractData<PermissionLevelRecord>(payload),
  )
}

export function fetchAvailableTeamPermissions(token: string) {
  return apiGet<unknown>('/v1/permission-levels/available-permissions/', { token }).then((payload) =>
    extractData<PermissionCategoryRecord[]>(payload),
  )
}

export function createPermissionLevel(payload: PermissionLevelPayload, token: string) {
  return apiPost<unknown>('/v1/permission-levels/', payload, { token }).then((responsePayload) =>
    extractData<PermissionLevelRecord>(responsePayload),
  )
}

export function updatePermissionLevel(
  levelId: number,
  payload: PermissionLevelPayload,
  token: string,
) {
  return apiPut<unknown>(`/v1/permission-levels/${levelId}/`, payload, { token }).then(
    (responsePayload) => extractData<PermissionLevelRecord>(responsePayload),
  )
}

export function deletePermissionLevel(levelId: number, token: string) {
  return apiDelete<unknown>(`/v1/permission-levels/${levelId}/`, { token }).then(
    (payload) => extractDetail(payload, 'Permission level deleted successfully.'),
  )
}

export function fetchTeams(
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
  const path = queryString ? `/v1/teams/?${queryString}` : '/v1/teams/'
  return apiGet<unknown>(path, { token }).then((payload) =>
    extractData<PaginatedResponse<TeamRecord>>(payload),
  )
}

export function fetchTeam(teamId: number, token: string) {
  return apiGet<unknown>(`/v1/teams/${teamId}/`, { token }).then((payload) =>
    extractData<TeamDetailRecord>(payload),
  )
}

export function createTeam(payload: TeamUpsertPayload, token: string) {
  return apiPost<unknown>('/v1/teams/', payload, { token }).then((responsePayload) =>
    extractData<TeamDetailRecord>(responsePayload),
  )
}

export function updateTeam(
  teamId: number,
  payload: TeamUpsertPayload,
  token: string,
) {
  return apiPut<unknown>(`/v1/teams/${teamId}/`, payload, { token }).then(
    (responsePayload) => extractData<TeamDetailRecord>(responsePayload),
  )
}

export function deleteTeam(teamId: number, token: string) {
  return apiDelete<unknown>(`/v1/teams/${teamId}/`, { token }).then(
    (payload) => extractDetail(payload, 'Team deleted successfully.'),
  )
}

export function fetchTeamUsers(token: string) {
  return apiGet<unknown>('/v1/teams/users/lookup/', { token }).then((payload) =>
    extractData<TeamUserLookupRecord[]>(payload),
  )
}

export function fetchTeamNodes(token: string) {
  return apiGet<unknown>('/v1/teams/nodes/lookup/', { token }).then((payload) =>
    extractData<TeamNodeLookupRecord[]>(payload),
  )
}

export function fetchTeamInstances(token: string) {
  return apiGet<unknown>('/v1/teams/services/lookup/', { token }).then((payload) =>
    extractData<TeamInstanceLookupRecord[]>(payload),
  )
}

export function fetchInstanceInventory(token: string, options: InstanceInventoryFilters = {}) {
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

export function fetchAgents(token: string, options: AgentListOptions = {}) {
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

  const queryString = params.toString()
  const path = queryString ? `/v1/agents/?${queryString}` : '/v1/agents/'
  return apiGet<unknown>(path, { token }).then((payload) =>
    extractData<PaginatedResponse<AgentRecord>>(payload),
  )
}

export function createAgent(payload: AgentCreatePayload, token: string) {
  return apiPost<unknown>('/v1/agents/', payload, { token }).then((responsePayload) =>
    extractData<AgentCreateResponse>(responsePayload),
  )
}

export function issueAgentInstallKey(agentId: number, token: string) {
  return apiPost<unknown>(`/v1/agents/${agentId}/install-key/`, {}, { token }).then(
    (responsePayload) => extractData<AgentCreateResponse>(responsePayload),
  )
}

export function fetchAgent(agentId: number, token: string) {
  return apiGet<unknown>(`/v1/agents/${agentId}/`, { token }).then((payload) =>
    extractData<AgentDetailRecord>(payload),
  )
}

export function updateAgent(
  agentId: number,
  payload: Partial<Pick<AgentDetailRecord, 'display_name' | 'enabled' | 'metadata'>>,
  token: string,
) {
  return apiPatch<unknown>(`/v1/agents/${agentId}/`, payload, { token }).then((responsePayload) =>
    extractData<AgentDetailRecord>(responsePayload),
  )
}

export function revokeAgent(agentId: number, token: string) {
  return apiDelete<unknown>(`/v1/agents/${agentId}/`, { token }).then((responsePayload) =>
    extractData<AgentDetailRecord>(responsePayload),
  )
}

export function replaceAgentAssignments(
  agentId: number,
  payload: AgentAssignmentReplacePayload,
  token: string,
) {
  return apiPut<unknown>(`/v1/agents/${agentId}/assignments/`, payload, { token }).then(
    (responsePayload) => extractData<AgentAssignmentRecord[]>(responsePayload),
  )
}

export function fetchAgentCommands(
  agentId: number,
  token: string,
  options: AgentCommandListOptions = {},
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
  if (options.status?.trim()) {
    params.set('status', options.status.trim())
  }

  const queryString = params.toString()
  const path = queryString
    ? `/v1/agents/${agentId}/commands/?${queryString}`
    : `/v1/agents/${agentId}/commands/`
  return apiGet<unknown>(path, { token }).then((payload) =>
    extractData<PaginatedResponse<AgentCommandSummaryRecord>>(payload),
  )
}

export function fetchAgentCommand(agentId: number, commandId: number, token: string) {
  return apiGet<unknown>(`/v1/agents/${agentId}/commands/${commandId}/`, { token }).then(
    (payload) => extractData<AgentCommandDetailRecord>(payload),
  )
}

export function cancelAgentCommand(agentId: number, commandId: number, token: string) {
  return apiPost<unknown>(
    `/v1/agents/${agentId}/commands/${commandId}/cancel/`,
    {},
    { token },
  ).then((responsePayload) => extractData<AgentCommandDetailRecord>(responsePayload))
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
  return apiPut<unknown>(`/v1/instance/${instanceId}/`, payload, { token }).then(
    (responsePayload) => extractData<InstanceEditorRecord>(responsePayload),
  )
}

export function testDraftInstanceConnection(payload: InstanceCreatePayload, token: string) {
  return apiPost<unknown>('/v1/instance/test-connection/', payload, { token }).then(
    (responsePayload) => extractDetail(responsePayload, 'Connection successful.'),
  )
}

export function testInstanceConnection(instanceId: number, token: string) {
  return apiPost<unknown>(`/v1/instance/${instanceId}/test-connection/`, {}, { token }).then(
    (payload) => extractDetail(payload, 'Connection successful.'),
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

export type WorkflowTeamLookupRecord = {
  team_id: number
  team_name: string
}

export type WorkflowInstanceLookupRecord = {
  id: number
  instance_name: string
  db_type: string
  type: string
  host: string
  label: string
  teams: WorkflowTeamLookupRecord[]
}

export type WorkflowMetadataRecord = {
  manual_execution_enabled: boolean
  teams: WorkflowTeamLookupRecord[]
  instances: WorkflowInstanceLookupRecord[]
}

export type WorkflowSummaryRecord = {
  id: number
  workflow_name: string
  demand_url: string
  team_id: number
  team_name: string
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
  engineer: string
  engineer_display: string
  run_date_start: string | null
  run_date_end: string | null
  create_time: string
  finish_time: string | null
}

export type WorkflowReviewNode = {
  team_name: string
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
  team_id?: string
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
  team_id: number
  team_name: string
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

  return apiGet<unknown>(`/v1/instance/data-dictionary/databases/?${params.toString()}`, {
    token,
  }).then((payload) => extractData<DataDictionaryDatabaseList>(payload))
}

export function fetchDataDictionaryTables(instanceId: number, dbName: string, token: string) {
  const params = new URLSearchParams({
    instance_id: `${instanceId}`,
    db_name: dbName,
  })

  return apiGet<unknown>(`/v1/instance/data-dictionary/tables/?${params.toString()}`, {
    token,
  }).then((payload) => extractData<DataDictionaryTableGroupList>(payload))
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

  return apiGet<unknown>(`/v1/instance/data-dictionary/table/?${params.toString()}`, {
    token,
  }).then((payload) => extractData<DataDictionaryTableDetail>(payload))
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

  const response = await fetch(
    buildUrl(`/v1/instance/data-dictionary/export/?${params.toString()}`),
    {
      method: 'GET',
      headers: {
        Accept: 'application/json',
        Authorization: `Bearer ${authorizationToken}`,
      },
    },
  )

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
        return exportDataDictionary(instanceId, dbName, refreshedAccessToken, {
          skipAuthRetry: true,
        })
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

    throw new Error(
      `GET /v1/instance/data-dictionary/export/ failed (${response.status}): ${message}`,
    )
  }

  const contentType = response.headers.get('content-type') || ''
  if (contentType.includes('application/json')) {
    const payload = await response.json()
    return {
      mode: 'message',
      detail:
        isRecord(payload) && typeof payload.detail === 'string'
          ? payload.detail
          : 'Export completed.',
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

  return apiGet<unknown>(`/v1/audit/workflow-log/?${params.toString()}`, { token }).then(
    (payload) =>
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

  return apiGet<unknown>(`/v1/instance-operations/database/?${params.toString()}`, { token }).then(
    (payload) => extractData<InstanceOperationDatabaseList>(payload),
  )
}

export function createInstanceOperationDatabase(
  payload: InstanceOperationDatabasePayload,
  token: string,
) {
  return apiPost<unknown>('/v1/instance-operations/database/', payload, { token }).then(
    (responsePayload) => extractData<InstanceOperationDatabaseRecord>(responsePayload),
  )
}

export function updateInstanceOperationDatabase(
  payload: InstanceOperationDatabasePayload,
  token: string,
) {
  return apiPut<unknown>('/v1/instance-operations/database/metadata/', payload, { token }).then(
    (responsePayload) => extractData<InstanceOperationDatabaseRecord>(responsePayload),
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

  return apiGet<unknown>(`/v1/instance-operations/account/?${params.toString()}`, { token }).then(
    (payload) => extractData<InstanceOperationAccountList>(payload),
  )
}

export function createInstanceOperationAccount(
  payload: InstanceOperationAccountPayload,
  token: string,
) {
  return apiPost<unknown>('/v1/instance-operations/account/', payload, { token }).then(
    (responsePayload) => extractData<InstanceOperationAccountRecord>(responsePayload),
  )
}

export function updateInstanceOperationAccount(
  payload: InstanceOperationAccountPayload,
  token: string,
) {
  return apiPut<unknown>('/v1/instance-operations/account/metadata/', payload, { token }).then(
    (responsePayload) => extractData<InstanceOperationAccountRecord>(responsePayload),
  )
}

export function resetInstanceOperationAccountPassword(
  payload: InstanceOperationAccountPasswordPayload,
  token: string,
) {
  return apiPost<unknown>('/v1/instance-operations/account/password/', payload, { token }).then(
    (responsePayload) => extractData<InstanceOperationAccountRecord>(responsePayload),
  )
}

export function updateInstanceOperationAccountLock(
  payload: InstanceOperationAccountLockPayload,
  token: string,
) {
  return apiPost<unknown>('/v1/instance-operations/account/lock/', payload, { token }).then(
    (responsePayload) => extractData<Record<string, never>>(responsePayload),
  )
}

export function deleteInstanceOperationAccount(
  payload: InstanceOperationAccountDeletePayload,
  token: string,
) {
  return apiDelete<unknown>('/v1/instance-operations/account/delete/', {
    token,
    body: payload,
  }).then((responsePayload) => extractData<Record<string, never>>(responsePayload))
}

export function grantInstanceOperationAccount(
  payload: InstanceOperationAccountGrantPayload,
  token: string,
) {
  return apiPost<unknown>('/v1/instance-operations/account/grant/', payload, { token }).then(
    (responsePayload) => extractData<InstanceOperationAccountGrantResult>(responsePayload),
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

  return apiGet<unknown>(`/v1/instance-operations/param/?${params.toString()}`, { token }).then(
    (payload) => extractData<InstanceOperationParamList>(payload),
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

  return apiGet<unknown>(`/v1/instance-operations/param/history/?${params.toString()}`, {
    token,
  }).then((payload) => extractData<InstanceOperationParamHistoryList>(payload))
}

export function editInstanceOperationParam(
  payload: InstanceOperationParamEditPayload,
  token: string,
) {
  return apiPost<unknown>('/v1/instance-operations/param/edit/', payload, { token }).then(
    (responsePayload) => extractData<Record<string, never>>(responsePayload),
  )
}

export function fetchInstanceOperationDiagnosticInstances(token: string) {
  return apiGet<unknown>('/v1/instance-operations/diagnostic/instances/', { token }).then(
    (payload) => extractData<InstanceOperationDiagnosticInstance[]>(payload),
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
  return apiGet<unknown>(`/v1/instance-operations/diagnostic/processes/?${params.toString()}`, {
    token,
  }).then((payload) => extractData<InstanceOperationDiagnosticList>(payload))
}

export function previewInstanceOperationDiagnosticKill(
  payload: InstanceOperationDiagnosticKillPayload,
  token: string,
) {
  return apiPost<unknown>('/v1/instance-operations/diagnostic/kill/preview/', payload, {
    token,
  }).then((responsePayload) => extractData<InstanceOperationDiagnosticKillPreview>(responsePayload))
}

export function killInstanceOperationDiagnosticSessions(
  payload: InstanceOperationDiagnosticKillPayload,
  token: string,
) {
  return apiPost<unknown>('/v1/instance-operations/diagnostic/kill/', payload, { token }).then(
    (responsePayload) => extractData<Record<string, never>>(responsePayload),
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
  return apiGet<unknown>(`/v1/instance-operations/diagnostic/tablespace/?${params.toString()}`, {
    token,
  }).then((payload) => extractData<InstanceOperationDiagnosticList>(payload))
}

export function fetchInstanceOperationDiagnosticTransactions(token: string, instanceId: number) {
  return apiGet<unknown>(
    `/v1/instance-operations/diagnostic/transactions/?instance_id=${instanceId}`,
    { token },
  ).then((payload) => extractData<InstanceOperationDiagnosticList>(payload))
}

export function fetchInstanceOperationDiagnosticLocks(token: string, instanceId: number) {
  return apiGet<unknown>(`/v1/instance-operations/diagnostic/locks/?instance_id=${instanceId}`, {
    token,
  }).then((payload) => extractData<InstanceOperationDiagnosticList>(payload))
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

export type PermissionRequestTarget = 'team' | 'instance'
export type PermissionRequestStatus = 0 | 1 | 2 | 3
export type PermissionInstanceAccessLevel = 'query' | 'query_dml' | 'query_dml_ddl'
export type PermissionRequestSubject = 'user' | 'team'
export type PermissionRequestDuration = 'temporary' | 'permanent'
export type PermissionGrantType = 'team' | 'instance' | 'permanent_team'

export type PermissionTeamLookupRecord = {
  team_id: number
  team_name: string
  label: string
}

export type PermissionInstanceLookupRecord = {
  id: number
  instance_name: string
  db_type: string
  type: string
  host: string
  label: string
  teams: PermissionTeamLookupRecord[]
}

export type PermissionRequestRecord = {
  request_id: number
  title: string
  reason: string
  target_type: PermissionRequestTarget
  team_id: number
  team_name: string
  permission_level_id: number | null
  permission_level_name: string | null
  instance_id: number | null
  instance_name: string
  access_level: PermissionInstanceAccessLevel | ''
  subject_type: PermissionRequestSubject
  access_duration: PermissionRequestDuration
  valid_date: string
  user_name: string
  user_display: string
  status: PermissionRequestStatus
  create_time: string
}

export type PermissionRequestReviewNode = {
  team_name: string
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
  subject_type: PermissionRequestSubject
  user_name: string
  user_display: string
  team_id: number
  team_name: string
  permission_level_id: number | null
  permission_level_name: string
  instance_id: number | null
  instance_name: string
  access_level: PermissionInstanceAccessLevel | ''
  access_duration: PermissionRequestDuration
  valid_date: string | null
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
  subject_type?: PermissionRequestSubject
  access_duration?: PermissionRequestDuration
  team_id: number
  permission_level_id?: number
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

export function fetchPermissionTeamsLookup(token: string) {
  return apiGet<unknown>('/v1/access/teams/lookup/', { token }).then((payload) =>
    extractData<PermissionTeamLookupRecord[]>(payload),
  )
}

export function fetchPermissionInstancesLookup(token: string) {
  return apiGet<unknown>('/v1/access/instances/lookup/', { token }).then((payload) =>
    extractData<PermissionInstanceLookupRecord[]>(payload),
  )
}

export function fetchPermissionRequests(token: string, filters: PermissionRequestListFilters = {}) {
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
  return apiDelete<unknown>(`/v1/access/grant/${grantType}/${grantId}/`, { token }).then(
    (payload) => extractDetail(payload, 'Grant revoked successfully.'),
  )
}

export type WorkflowScope = 'all' | 'mine' | 'pending_review'
export type WorkflowExecutionMode = 'auto' | 'manual'

export type WorkflowSubmitTeamRecord = {
  team_id: number
  team_name: string
  label: string
}

export type WorkflowSubmitInstanceRecord = {
  id: number
  instance_name: string
  db_type: string
  type: string
  team_ids: number[]
  team_names: string[]
  workflow_policy_id: number | null
  workflow_policy_name: string
  allowed_syntax_types: WorkflowSyntaxType[]
}

export type WorkflowSubmissionMetadata = {
  teams: WorkflowSubmitTeamRecord[]
  instances: WorkflowSubmitInstanceRecord[]
  manual_execution_enabled: boolean
}

export type WorkflowApprovalPreview = {
  team_id: number
  team_name: string
  audit_auth_groups: string
  workflow_policy_id: number | null
  workflow_policy_name: string
  display: string
  review_info: Array<{
    team_name: string
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
    team_id: number
    db_name: string
    schema_name?: string | null
    instance: number
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
  team_id?: number | ''
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

export type ArchiveTeamRecord = {
  team_id: number
  team_name: string
  label: string
}

export type ArchiveInstanceRecord = {
  id: number
  instance_name: string
  db_type: string
  type: string
  label: string
  team_ids: number[]
  team_names: string[]
  available_archive_methods: ArchiveMethod[]
}

export type ArchiveMetadataRecord = {
  teams: ArchiveTeamRecord[]
  instances: ArchiveInstanceRecord[]
  schedule_frequencies: Array<{ value: ArchiveScheduleFrequency; label: string }>
  weekdays: Array<{ value: ArchiveWeekday; label: string }>
}

export type ArchiveApprovalPreview = {
  team_id: number
  team_name: string
  audit_auth_groups: string
  display: string
  review_info: Array<{
    team_name: string
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
  team_name: string
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
  team: {
    team_id: number
    team_name: string
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
    team_name: string
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
  team_id: number
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
  team_id?: number | ''
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
  if (filters.team_id) {
    params.set('team_id', `${filters.team_id}`)
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
  if (filters.team_id) {
    params.set('team_id', `${filters.team_id}`)
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

export function fetchWorkflowApprovalPreview(groupId: number, token: string, instanceId?: number) {
  const params = new URLSearchParams({ team_id: `${groupId}` })
  if (instanceId != null) {
    params.set('instance_id', `${instanceId}`)
  }
  return apiGet<unknown>(`/v1/workflow/approval-preview/?${params.toString()}`, { token }).then(
    (payload) => extractData<WorkflowApprovalPreview>(payload),
  )
}

export async function fetchWorkflowPolicies(token: string) {
  const size = 100
  let page = 1
  const results: WorkflowPolicyRecord[] = []
  let count = 0
  let next: string | null = null
  let previous: string | null = null

  do {
    const payload = await apiGet<unknown>(`/v1/workflow/policies/?page=${page}&size=${size}`, { token }).then(
      (responsePayload) => extractData<PaginatedResponse<WorkflowPolicyRecord>>(responsePayload),
    )
    count = payload.count
    next = payload.next
    previous = payload.previous
    results.push(...payload.results)
    page += 1
  } while (results.length < count && next)

  return { count, next, previous, results }
}

export function fetchWorkflowPolicyMetadata(token: string) {
  return apiGet<unknown>('/v1/workflow/policies/metadata/', { token }).then((payload) =>
    extractData<WorkflowPolicyMetadata>(payload),
  )
}

export function createWorkflowPolicy(payload: WorkflowPolicyPayload, token: string) {
  return apiPost<unknown>('/v1/workflow/policies/', payload, { token }).then((responsePayload) =>
    extractData<WorkflowPolicyRecord>(responsePayload),
  )
}

export function updateWorkflowPolicy(policyId: number, payload: Partial<WorkflowPolicyPayload>, token: string) {
  return apiPatch<unknown>(`/v1/workflow/policies/${policyId}/`, payload, { token }).then((responsePayload) =>
    extractData<WorkflowPolicyRecord>(responsePayload),
  )
}

export function deleteWorkflowPolicy(policyId: number, token: string) {
  return apiDelete(`/v1/workflow/policies/${policyId}/`, { token })
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

    throw new Error(
      `GET /v1/workflow/${workflowId}/download/ failed (${response.status}): ${message}`,
    )
  }

  const contentType = response.headers.get('content-type') || ''
  if (contentType.includes('application/json')) {
    const payload = await response.json()
    const redirectUrl =
      isRecord(payload) && typeof payload.url === 'string'
        ? payload.url
        : isRecord(payload) && isRecord(payload.data) && typeof payload.data.url === 'string'
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

export function reviewWorkflow(workflowId: number, payload: WorkflowReviewPayload, token: string) {
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
  return apiGet<unknown>(`/v1/archive/approval-preview/?team_id=${groupId}`, { token }).then(
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

export function reviewArchive(archiveId: number, payload: ArchiveReviewPayload, token: string) {
  return apiPost<unknown>(`/v1/archive/${archiveId}/reviews/`, payload, { token }).then(
    (responsePayload) => extractDetail(responsePayload, 'Archive workflow reviewed successfully.'),
  )
}

export function runArchiveNow(archiveId: number, token: string) {
  return apiPost<unknown>(`/v1/archive/${archiveId}/run/`, {}, { token }).then((responsePayload) =>
    extractDetail(responsePayload, 'Archive execution queued.'),
  )
}

export function updateArchiveState(archiveId: number, payload: ArchiveStatePayload, token: string) {
  return apiPost<unknown>(`/v1/archive/${archiveId}/state/`, payload, { token }).then(
    (responsePayload) => extractDetail(responsePayload, 'Archive schedule updated.'),
  )
}

export function fetchArchiveLogs(archiveId: number, token: string, page = 1, size = 20) {
  return apiGet<unknown>(`/v1/archive/${archiveId}/logs/?page=${page}&size=${size}`, {
    token,
  }).then((payload) => extractData<PaginatedResponse<ArchiveLogRecord>>(payload))
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
