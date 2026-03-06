export const ACCESS_TOKEN_KEY = 'archery.access_token'
export const REFRESH_TOKEN_KEY = 'archery.refresh_token'
export const AUTH_UNAUTHORIZED_EVENT = 'archery:auth-unauthorized'
export const AUTH_TOKENS_UPDATED_EVENT = 'archery:auth-tokens-updated'

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || '/api'

let refreshRequest: Promise<string> | null = null

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

    return Object.values(value).map(flattenErrorMessage).filter(Boolean).join(', ')
  }

  return ''
}

function parseResponseMessage(body: string): string {
  try {
    return flattenErrorMessage(JSON.parse(body)) || body
  } catch {
    return body
  }
}

export class AuthSessionExpiredError extends Error {}

function decodeBase64Url(input: string): string | null {
  try {
    const normalized = input.replace(/-/g, '+').replace(/_/g, '/')
    const padded = normalized.padEnd(Math.ceil(normalized.length / 4) * 4, '=')
    return window.atob(padded)
  } catch {
    return null
  }
}

function decodeJwtPayload(token: string): Record<string, unknown> | null {
  const segments = token.split('.')
  if (segments.length !== 3) {
    return null
  }

  const decoded = decodeBase64Url(segments[1] ?? '')
  if (!decoded) {
    return null
  }

  try {
    return JSON.parse(decoded) as Record<string, unknown>
  } catch {
    return null
  }
}

export function isAccessTokenExpired(token: string): boolean {
  const payload = decodeJwtPayload(token)
  if (!payload || typeof payload.exp !== 'number') {
    return true
  }

  return payload.exp * 1000 <= Date.now()
}

export function getStoredAccessToken(): string {
  return localStorage.getItem(ACCESS_TOKEN_KEY) || ''
}

export function getStoredRefreshToken(): string {
  return localStorage.getItem(REFRESH_TOKEN_KEY) || ''
}

export function setStoredTokens(access: string, refresh: string) {
  localStorage.setItem(ACCESS_TOKEN_KEY, access)
  localStorage.setItem(REFRESH_TOKEN_KEY, refresh)
  window.dispatchEvent(
    new CustomEvent(AUTH_TOKENS_UPDATED_EVENT, {
      detail: { access, refresh },
    }),
  )
}

export function clearStoredTokens() {
  localStorage.removeItem(ACCESS_TOKEN_KEY)
  localStorage.removeItem(REFRESH_TOKEN_KEY)
}

export function notifyUnauthorized(message: string) {
  window.dispatchEvent(
    new CustomEvent(AUTH_UNAUTHORIZED_EVENT, {
      detail: { message },
    }),
  )
}

function extractRefreshTokens(payload: unknown, fallbackRefresh: string) {
  const source =
    isRecord(payload) && isRecord(payload.data)
      ? payload.data
      : payload

  if (!isRecord(source) || typeof source.access !== 'string') {
    throw new Error('Refresh response did not include a new access token.')
  }

  return {
    access: source.access,
    refresh: typeof source.refresh === 'string' ? source.refresh : fallbackRefresh,
  }
}

export async function refreshAccessToken(): Promise<string> {
  const refreshToken = getStoredRefreshToken()

  if (!refreshToken || isAccessTokenExpired(refreshToken)) {
    throw new AuthSessionExpiredError('Your session expired. Sign in again.')
  }

  if (refreshRequest) {
    return refreshRequest
  }

  refreshRequest = (async () => {
    const response = await fetch(buildUrl('/auth/token/refresh/'), {
      method: 'POST',
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ refresh: refreshToken }),
    })

    const body = await response.text()

    if (!response.ok) {
      const message = parseResponseMessage(body)

      if (response.status === 401) {
        throw new AuthSessionExpiredError(message || 'Your session expired. Sign in again.')
      }

      throw new Error(message || 'Failed to refresh your session.')
    }

    const payload = body ? JSON.parse(body) : {}
    const nextTokens = extractRefreshTokens(payload, refreshToken)
    setStoredTokens(nextTokens.access, nextTokens.refresh)
    return nextTokens.access
  })().finally(() => {
    refreshRequest = null
  })

  return refreshRequest
}

export async function getUsableAccessToken(preferredToken = ''): Promise<string> {
  const accessToken = getStoredAccessToken() || preferredToken

  if (accessToken && !isAccessTokenExpired(accessToken)) {
    return accessToken
  }

  return refreshAccessToken()
}
