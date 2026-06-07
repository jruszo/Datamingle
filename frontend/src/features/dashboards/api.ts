import {
  ApiRequestError,
  apiDelete,
  apiGet,
  apiGetBlob,
  apiPatch,
  apiPost,
  apiPostForm,
  isRecord,
} from '@/shared/api/http'

export type DashboardPanelLayout = {
  x: number
  y: number
  w: number
  h: number
}

export type DashboardPanel = {
  id: string
  title: string
  description: string
  queries: DashboardQuery[]
  step_seconds: number
  visualization: DashboardVisualization
  layout: DashboardPanelLayout
}

export type DashboardQuery = {
  ref_id: string
  query: string
  editor_mode: 'builder' | 'code'
  disabled: boolean
  legend: string
}

export type DashboardVisualizationType = 'time_series' | 'bar' | 'stat' | 'gauge' | 'table'

export type DashboardThreshold = {
  value: number
  color: string
}

export type DashboardVisualization = {
  type: DashboardVisualizationType
  unit: string
  decimals: number | null
  min: number | null
  max: number | null
  color_scheme: 'classic' | 'cool' | 'warm' | 'status'
  thresholds: DashboardThreshold[]
  legend_placement: 'bottom' | 'right' | 'hidden'
  tooltip_mode: 'single' | 'all'
  line_width: number
  fill_opacity: number
  stack: boolean
}

export type DashboardVariable = {
  name: string
  label: string
  metric: string
  label_name: string
  multi: boolean
  include_all: boolean
}

export type MetricsDashboard = {
  id: number
  name: string
  description: string
  created_by: {
    id: number
    username: string
    display: string
  } | null
  has_icon: boolean
  revision: number
  time_range_mode: 'relative' | 'absolute'
  time_range_seconds: number
  time_range_start: string
  time_range_end: string
  refresh_interval_seconds: number
  variables: DashboardVariable[]
  panels: DashboardPanel[]
  create_time: string
  update_time: string
}

export type DashboardRevisionSummary = {
  revision: number
  saved_by: MetricsDashboard['created_by']
  saved_at: string
  restored_from_revision: number | null
}

export type DashboardRevision = DashboardRevisionSummary & DashboardWritePayload

export type DashboardWritePayload = Pick<
  MetricsDashboard,
  | 'name'
  | 'description'
  | 'time_range_mode'
  | 'time_range_seconds'
  | 'time_range_start'
  | 'time_range_end'
  | 'refresh_interval_seconds'
  | 'variables'
  | 'panels'
>

type ApiEnvelope<T> = {
  detail: string
  data: T
}

function extractData<T>(payload: ApiEnvelope<T>) {
  return payload.data
}

export class DashboardConflictError extends Error {
  latest: MetricsDashboard

  constructor(latest: MetricsDashboard) {
    super('Dashboard was changed by another user.')
    this.name = 'DashboardConflictError'
    this.latest = latest
  }
}

export function listMetricsDashboards(token: string) {
  return apiGet<ApiEnvelope<MetricsDashboard[]>>('/v1/metrics/dashboards/', { token }).then(
    extractData,
  )
}

export function fetchMetricsDashboard(dashboardId: number, token: string) {
  return apiGet<ApiEnvelope<MetricsDashboard>>(`/v1/metrics/dashboards/${dashboardId}/`, {
    token,
  }).then(extractData)
}

export function fetchDashboardIcon(dashboardId: number, token: string) {
  return apiGetBlob(`/v1/metrics/dashboards/${dashboardId}/icon/`, { token })
}

export function uploadDashboardIcon(dashboardId: number, icon: File, token: string) {
  const form = new FormData()
  form.append('icon', icon)
  return apiPostForm<ApiEnvelope<MetricsDashboard>>(
    `/v1/metrics/dashboards/${dashboardId}/icon/`,
    form,
    { token },
  ).then(extractData)
}

export function removeDashboardIcon(dashboardId: number, token: string) {
  return apiDelete<ApiEnvelope<MetricsDashboard>>(
    `/v1/metrics/dashboards/${dashboardId}/icon/`,
    { token },
  ).then(extractData)
}

export function listDashboardRevisions(dashboardId: number, token: string) {
  return apiGet<ApiEnvelope<DashboardRevisionSummary[]>>(
    `/v1/metrics/dashboards/${dashboardId}/revisions/`,
    { token },
  ).then(extractData)
}

export function fetchDashboardRevision(
  dashboardId: number,
  revision: number,
  token: string,
) {
  return apiGet<ApiEnvelope<DashboardRevision>>(
    `/v1/metrics/dashboards/${dashboardId}/revisions/${revision}/`,
    { token },
  ).then(extractData)
}

export function createMetricsDashboard(payload: DashboardWritePayload, token: string) {
  return apiPost<ApiEnvelope<MetricsDashboard>>('/v1/metrics/dashboards/', payload, {
    token,
  }).then(extractData)
}

export async function updateMetricsDashboard(
  dashboardId: number,
  expectedRevision: number,
  payload: DashboardWritePayload,
  token: string,
) {
  try {
    return await apiPatch<ApiEnvelope<MetricsDashboard>>(
      `/v1/metrics/dashboards/${dashboardId}/`,
      { ...payload, expected_revision: expectedRevision },
      { token },
    ).then(extractData)
  } catch (error) {
    if (
      error instanceof ApiRequestError &&
      error.status === 409 &&
      isRecord(error.data) &&
      isRecord(error.data.data)
    ) {
      throw new DashboardConflictError(error.data.data as MetricsDashboard)
    }
    throw error
  }
}

export async function restoreDashboardRevision(
  dashboardId: number,
  revision: number,
  expectedRevision: number,
  token: string,
) {
  try {
    return await apiPost<ApiEnvelope<MetricsDashboard>>(
      `/v1/metrics/dashboards/${dashboardId}/revisions/${revision}/restore/`,
      { expected_revision: expectedRevision },
      { token },
    ).then(extractData)
  } catch (error) {
    if (
      error instanceof ApiRequestError &&
      error.status === 409 &&
      isRecord(error.data) &&
      isRecord(error.data.data)
    ) {
      throw new DashboardConflictError(error.data.data as MetricsDashboard)
    }
    throw error
  }
}

export function deleteMetricsDashboard(dashboardId: number, token: string) {
  return apiDelete<ApiEnvelope<Record<string, never>>>(`/v1/metrics/dashboards/${dashboardId}/`, {
    token,
  })
}

export function emptyDashboardPayload(name: string): DashboardWritePayload {
  return {
    name,
    description: '',
    time_range_mode: 'relative',
    time_range_seconds: 3600,
    time_range_start: '',
    time_range_end: '',
    refresh_interval_seconds: 0,
    variables: [],
    panels: [],
  }
}
