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

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
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
