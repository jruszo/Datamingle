import {
  AuthSessionExpiredError,
  getUsableAccessToken,
  notifyUnauthorized,
  refreshAccessToken,
} from '@/shared/auth/auth'
export {
  buildUrl,
  flattenErrorMessage,
  isRecord,
  parseResponseMessage,
  publicApiUrl,
} from '@/shared/api/http-helpers'
import {
  buildUrl,
  flattenErrorMessage,
} from '@/shared/api/http-helpers'

type RequestOptions = {
  token?: string
  body?: unknown
}

type InternalRequestOptions = RequestOptions & {
  skipAuthRetry?: boolean
}

// Passing options.token enables authenticated request behavior because request()
// gates auth on options.token !== undefined. Callers must provide the token field
// to get Authorization headers plus the automatic refresh/retry flow.

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
