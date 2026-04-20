const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || '/api'

export function buildUrl(path: string): string {
  return `${API_BASE_URL}${path.startsWith('/') ? path : `/${path}`}`
}

export function publicApiUrl(path: string): string {
  return buildUrl(path)
}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}

export function flattenErrorMessage(value: unknown): string {
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

export function parseResponseMessage(body: string): string {
  try {
    return flattenErrorMessage(JSON.parse(body)) || body
  } catch {
    return body
  }
}
