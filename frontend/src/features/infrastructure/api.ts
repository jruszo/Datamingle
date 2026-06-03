import {
  createAgent,
  fetchInstanceInventoryMetadata,
  issueAgentInstallKey,
  type AgentCreateResponse,
  type AgentStatus,
  type InstanceInventoryMetadata,
  type PaginatedResponse,
} from '@/lib/api'
import { apiGet, apiPatch, apiPost, isRecord } from '@/shared/api/http'

export {
  createAgent,
  fetchInstanceInventoryMetadata,
  issueAgentInstallKey,
  type AgentCreateResponse,
  type AgentStatus,
  type InstanceInventoryMetadata,
  type PaginatedResponse,
}

export type InfrastructureListOptions = {
  page?: number
  size?: number
  search?: string
}

export type InfrastructureNodePayload = {
  name: string
  address: string
  description: string
  metadata: Record<string, unknown>
  resource_group_ids?: number[]
}

export type InfrastructureNodeAgentRecord = {
  id: number
  status: AgentStatus
  hostname: string
  platform: string
  architecture: string
  agent_version: string
  last_seen_at: string | null
  last_websocket_pong_at: string | null
  last_connected_at: string | null
  last_disconnected_at: string | null
  last_config_revision: number
  desired_config_revision: number
  enabled: boolean
}

export type InfrastructureNodeRecord = InfrastructureNodePayload & {
  id: number
  agent: InfrastructureNodeAgentRecord | null
  agent_id: number | null
  agent_status: AgentStatus | null
  service_count: number
  recommendation_count: number
  create_time?: string
  update_time?: string
}

export type DatabaseServicePayload = {
  node_id: number
  service_name: string
  role: string
  engine: 'mysql' | 'pgsql'
  host: string
  port: number
  user: string
  password?: string
  is_ssl: boolean
  verify_ssl: boolean
  db_name: string
  show_db_name_regex: string
  denied_db_name_regex: string
  charset: string
  resource_group_ids: number[]
  service_tag_ids: number[]
  recommendation_id?: number
}

export type DatabaseServiceRecord = Omit<
  DatabaseServicePayload,
  'password' | 'recommendation_id'
> & {
  id: number
  inventory_status: 'never' | 'ok' | 'stale' | 'failed'
  inventory_detected_hostname?: string
  inventory_detected_version?: string
  inventory_last_refresh_at?: string | null
  create_time?: string
  update_time?: string
}

export type ServiceRecommendationRecord = {
  id: number
  node_id: number
  service_name: string
  engine: 'mysql' | 'pgsql'
  host: string
  port: number
  source: string
  confidence: number
  status: 'recommended' | 'ignored' | 'accepted'
  last_seen_at: string | null
}

export type InfrastructureNodeDetailRecord = InfrastructureNodeRecord & {
  services: DatabaseServiceRecord[]
  recommendations: ServiceRecommendationRecord[]
}

function extractData<T>(payload: unknown): T {
  if (isRecord(payload) && 'data' in payload) {
    return payload.data as T
  }
  return payload as T
}

function buildInfrastructureListPath(options: InfrastructureListOptions) {
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
  return queryString ? `/v1/infrastructure/nodes/?${queryString}` : '/v1/infrastructure/nodes/'
}

export function fetchInfrastructureNodes(token: string, options: InfrastructureListOptions = {}) {
  return apiGet<unknown>(buildInfrastructureListPath(options), { token }).then((payload) =>
    extractData<PaginatedResponse<InfrastructureNodeRecord>>(payload),
  )
}

export function fetchInfrastructureNode(nodeId: number, token: string) {
  return apiGet<unknown>(`/v1/infrastructure/nodes/${nodeId}/`, { token }).then((payload) =>
    extractData<InfrastructureNodeDetailRecord>(payload),
  )
}

export function updateInfrastructureNode(
  nodeId: number,
  payload: InfrastructureNodePayload,
  token: string,
) {
  return apiPatch<unknown>(`/v1/infrastructure/nodes/${nodeId}/`, payload, { token }).then(
    (responsePayload) => extractData<InfrastructureNodeDetailRecord>(responsePayload),
  )
}

export function createDatabaseService(payload: DatabaseServicePayload, token: string) {
  return apiPost<unknown>('/v1/infrastructure/services/', payload, { token }).then(
    (responsePayload) => extractData<DatabaseServiceRecord>(responsePayload),
  )
}

export function updateDatabaseService(
  serviceId: number,
  payload: DatabaseServicePayload,
  token: string,
) {
  return apiPatch<unknown>(`/v1/infrastructure/services/${serviceId}/`, payload, { token }).then(
    (responsePayload) => extractData<DatabaseServiceRecord>(responsePayload),
  )
}

export function discoverInfrastructureNodeServices(nodeId: number, token: string) {
  return apiPost<unknown>(`/v1/infrastructure/nodes/${nodeId}/discover/`, {}, { token }).then(
    (responsePayload) => extractData<Record<string, unknown>>(responsePayload),
  )
}

export function updateServiceRecommendationStatus(
  recommendationId: number,
  status: ServiceRecommendationRecord['status'],
  token: string,
) {
  return apiPatch<unknown>(
    `/v1/infrastructure/recommendations/${recommendationId}/`,
    { status },
    { token },
  ).then((responsePayload) => extractData<ServiceRecommendationRecord>(responsePayload))
}

export function testDatabaseServiceConnection(serviceId: number, token: string) {
  return apiPost<unknown>(`/v1/infrastructure/services/${serviceId}/test/`, {}, { token }).then(
    (responsePayload) => {
      const payload = extractData<unknown>(responsePayload)
      if (typeof payload === 'string') {
        return payload
      }
      if (isRecord(payload)) {
        if (typeof payload.message === 'string') {
          return payload.message
        }
        if (typeof payload.detail === 'string') {
          return payload.detail
        }
      }
      return 'Connection test completed.'
    },
  )
}
