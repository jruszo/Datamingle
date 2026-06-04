import {
  AuthSessionExpiredError,
  getUsableAccessToken,
  notifyUnauthorized,
  refreshAccessToken,
} from '@/shared/auth/auth'
import { buildUrl, parseResponseMessage } from '@/shared/api/http-helpers'

type MetricsEnvelope<T> = {
  status: 'success' | 'error'
  data?: T
  error?: string
}

type MetricsRequestOptions = {
  token: string
  params?: URLSearchParams
  skipAuthRetry?: boolean
}

export type PrometheusSeries = {
  metric: Record<string, string>
  values?: Array<[number, string]>
  value?: [number, string]
}

export type PrometheusRangeResult = {
  resultType: string
  result: PrometheusSeries[]
}

export type PrometheusInstantResult = {
  resultType: string
  result: PrometheusSeries[]
}

export type PrometheusSeriesSelector = Record<string, string>

export type PrometheusMetadata = Record<string, Array<{
  type?: string
  help?: string
  unit?: string
}>>

function metricsUrl(path: string, params?: URLSearchParams) {
  const query = params?.toString()
  return `${buildUrl(path)}${query ? `?${query}` : ''}`
}

async function metricsGet<T>(path: string, options: MetricsRequestOptions): Promise<T> {
  let authorizationToken = ''
  try {
    authorizationToken = await getUsableAccessToken(options.token)
  } catch (error) {
    if (error instanceof AuthSessionExpiredError) {
      notifyUnauthorized(error.message)
      throw new Error(`GET ${path} failed (401): ${error.message}`)
    }
    throw error
  }

  const response = await fetch(metricsUrl(path, options.params), {
    method: 'GET',
    headers: {
      Accept: 'application/json',
      Authorization: `Bearer ${authorizationToken}`,
    },
  })

  if (!response.ok) {
    const body = await response.text()
    const message = parseResponseMessage(body)

    if (response.status === 401 && !options.skipAuthRetry) {
      try {
        const refreshedAccessToken = await refreshAccessToken()
        return metricsGet<T>(path, {
          ...options,
          token: refreshedAccessToken,
          skipAuthRetry: true,
        })
      } catch (error) {
        if (error instanceof AuthSessionExpiredError) {
          notifyUnauthorized(error.message)
          throw new Error(`GET ${path} failed (401): ${error.message}`)
        }
        throw error
      }
    }

    if (response.status === 401) {
      notifyUnauthorized(message)
    }
    throw new Error(`GET ${path} failed (${response.status}): ${message}`)
  }

  const payload = (await response.json()) as MetricsEnvelope<T>
  if (payload.status !== 'success' || payload.data === undefined) {
    throw new Error(payload.error || 'Metrics request failed.')
  }
  return payload.data
}

export function fetchMetricNames(token: string) {
  return metricsGet<string[]>('/v1/metrics/label/__name__/values', { token })
}

export function fetchMetricLabelNames(token: string) {
  return metricsGet<string[]>('/v1/metrics/labels', { token })
}

export function fetchMetricLabelValues(labelName: string, token: string) {
  return metricsGet<string[]>(`/v1/metrics/label/${encodeURIComponent(labelName)}/values`, {
    token,
  })
}

export function fetchMetricMetadata(metricName: string, token: string) {
  const params = metricName ? new URLSearchParams({ metric: metricName }) : undefined
  return metricsGet<PrometheusMetadata>('/v1/metrics/metadata', { token, params })
}

export function fetchMetricSeries(metricName: string, token: string) {
  const params = new URLSearchParams({ 'match[]': metricName })
  return metricsGet<PrometheusSeriesSelector[]>('/v1/metrics/series', { token, params })
}

export function queryMetricRange(query: string, start: Date, end: Date, stepSeconds: number, token: string) {
  const params = new URLSearchParams({
    query,
    start: `${Math.floor(start.getTime() / 1000)}`,
    end: `${Math.floor(end.getTime() / 1000)}`,
    step: `${stepSeconds}`,
  })
  return metricsGet<PrometheusRangeResult>('/v1/metrics/query_range', { token, params })
}

export function queryMetricInstant(query: string, token: string, time?: Date) {
  const params = new URLSearchParams({ query })
  if (time) {
    params.set('time', `${Math.floor(time.getTime() / 1000)}`)
  }
  return metricsGet<PrometheusInstantResult>('/v1/metrics/query', { token, params })
}

export function formatPromQL(query: string, token: string) {
  return metricsGet<string>('/v1/metrics/format_query', {
    token,
    params: new URLSearchParams({ query }),
  })
}

export function parsePromQL(query: string, token: string) {
  return metricsGet<unknown>('/v1/metrics/parse_query', {
    token,
    params: new URLSearchParams({ query }),
  })
}
