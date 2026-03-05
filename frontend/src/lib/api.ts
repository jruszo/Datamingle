const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || '/api'

function buildUrl(path: string): string {
  return `${API_BASE_URL}${path.startsWith('/') ? path : `/${path}`}`
}

type RequestOptions = {
  token?: string
  body?: unknown
}

async function request<T>(
  method: 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE',
  path: string,
  options: RequestOptions = {},
): Promise<T> {
  const headers: Record<string, string> = {
    Accept: 'application/json',
  }
  if (options.body !== undefined) {
    headers['Content-Type'] = 'application/json'
  }
  if (options.token) {
    headers.Authorization = `Bearer ${options.token}`
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
    throw new Error(`${method} ${path} failed (${response.status}): ${body}`)
  }

  return response.json() as Promise<T>
}

export function apiGet<T>(path: string, options: RequestOptions = {}) {
  return request<T>('GET', path, options)
}

export function apiPost<T>(path: string, body: unknown, options: RequestOptions = {}) {
  return request<T>('POST', path, { ...options, body })
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

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}

function extractData<T>(payload: unknown): T {
  if (isRecord(payload) && 'data' in payload) {
    return payload.data as T
  }
  return payload as T
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

export function fetchDashboard(startDate: string, endDate: string, token: string) {
  const params = new URLSearchParams({
    start_date: startDate,
    end_date: endDate,
  })
  return apiGet<unknown>(`/v1/dashboard/?${params.toString()}`, { token }).then((payload) =>
    extractData<DashboardPayload>(payload),
  )
}
