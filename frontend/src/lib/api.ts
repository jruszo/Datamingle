import { AuthSessionExpiredError, getUsableAccessToken, notifyUnauthorized, refreshAccessToken } from '@/lib/auth'

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || '/api'

function buildUrl(path: string): string {
  return `${API_BASE_URL}${path.startsWith('/') ? path : `/${path}`}`
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}

function flattenErrorMessage(value: unknown): string {
  if (typeof value === 'string') {
    return value
  }

  if (Array.isArray(value)) {
    return value.map(flattenErrorMessage).filter(Boolean).join(', ')
  }

  if (isRecord(value)) {
    if (typeof value.errors === 'string') {
      return value.errors
    }

    if (typeof value.detail === 'string') {
      return value.detail
    }

    return Object.entries(value)
      .map(([field, fieldValue]) => `${field}: ${flattenErrorMessage(fieldValue)}`)
      .filter(Boolean)
      .join(' ')
  }

  return ''
}

type RequestOptions = {
  token?: string
  body?: unknown
}

type InternalRequestOptions = RequestOptions & {
  skipAuthRetry?: boolean
}

async function request<T>(
  method: 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE',
  path: string,
  options: InternalRequestOptions = {},
): Promise<T> {
  const requiresAuth = options.token !== undefined
  const headers: Record<string, string> = {
    Accept: 'application/json',
  }
  let authorizationToken = ''

  if (requiresAuth) {
    try {
      authorizationToken = await getUsableAccessToken(options.token)
    } catch (error) {
      if (error instanceof AuthSessionExpiredError) {
        notifyUnauthorized(error.message)
        throw new Error(`${method} ${path} failed (401): ${error.message}`)
      }

      throw error
    }
  }

  if (options.body !== undefined) {
    headers['Content-Type'] = 'application/json'
  }
  if (authorizationToken) {
    headers.Authorization = `Bearer ${authorizationToken}`
  }

  const requestInit: RequestInit = {
    method,
    headers: {
      ...headers,
    },
  }

  if (options.body !== undefined) {
    if (method === 'GET') {
      throw new Error(`GET ${path} cannot include a request body`)
    }
    requestInit.body = JSON.stringify(options.body)
  }

  const response = await fetch(buildUrl(path), requestInit)

  if (!response.ok) {
    const body = await response.text()
    let message = body

    try {
      message = flattenErrorMessage(JSON.parse(body)) || body
    } catch {
      message = body
    }

    if (response.status === 401 && requiresAuth && !options.skipAuthRetry) {
      try {
        const refreshedAccessToken = await refreshAccessToken()
        return request<T>(method, path, {
          ...options,
          token: refreshedAccessToken,
          skipAuthRetry: true,
        })
      } catch (error) {
        if (error instanceof AuthSessionExpiredError) {
          notifyUnauthorized(error.message)
          throw new Error(`${method} ${path} failed (401): ${error.message}`)
        }

        throw error
      }
    }

    if (response.status === 401 && requiresAuth) {
      notifyUnauthorized(message)
    }

    throw new Error(`${method} ${path} failed (${response.status}): ${message}`)
  }

  return response.json() as Promise<T>
}

export function apiGet<T>(path: string, options: RequestOptions = {}) {
  return request<T>('GET', path, options)
}

export function apiPost<T>(path: string, body: unknown, options: RequestOptions = {}) {
  return request<T>('POST', path, { ...options, body })
}

export function apiPatch<T>(path: string, body: unknown, options: RequestOptions = {}) {
  return request<T>('PATCH', path, { ...options, body })
}

export function apiPut<T>(path: string, body: unknown, options: RequestOptions = {}) {
  return request<T>('PUT', path, { ...options, body })
}

export function apiDelete<T>(path: string, options: RequestOptions = {}) {
  return request<T>('DELETE', path, options)
}

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
  is_superuser: boolean
  is_staff: boolean
  is_active: boolean
  groups: Array<{ id: number; name: string }>
  resource_groups: Array<{ group_id: number; group_name: string }>
  permissions: string[]
  two_factor_auth_types: string[]
}

export type GroupRecord = {
  id: number
  name: string
  permissions: number[]
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
  tunnel_id: number | null
  resource_group_ids: number[]
  instance_tag_ids: number[]
}

export type InstanceOptionRecord = {
  value: string
  label: string
}

export type InstanceTagOptionRecord = {
  id: number
  tag_name: string
  label: string
}

export type TunnelOptionRecord = {
  id: number
  tunnel_name: string
  host: string
  port: number
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
  tunnels: TunnelOptionRecord[]
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
  tunnel_id: number | null
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
    slow_query_by_db_user: DashboardNamedSeries
    slow_query_by_db: DashboardNamedSeries
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

export function fetchCurrentUserContext(token: string) {
  return apiGet<unknown>('/v1/me/', { token }).then((payload) =>
    extractData<CurrentUserContext>(payload),
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
