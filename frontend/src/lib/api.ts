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

export function fetchDashboard(startDate: string, endDate: string, token: string) {
  const params = new URLSearchParams({
    start_date: startDate,
    end_date: endDate,
  })
  return apiGet<unknown>(`/v1/dashboard/?${params.toString()}`, { token }).then((payload) =>
    extractData<DashboardPayload>(payload),
  )
}
