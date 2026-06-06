import { apiGet, apiPost } from '@/shared/api/http'

type ApiEnvelope<T> = {
  detail: string
  data: T
}

export type PromQLAssistantSuggestion = {
  available: boolean
  query: string
  explanation: string
  assumptions: string[]
  warnings: string[]
}

export function fetchPromQLAssistantAvailability(token: string) {
  return apiGet<ApiEnvelope<{ available: boolean }>>('/v1/metrics/ai/availability', {
    token,
  }).then((payload) => payload.data.available)
}

export function requestPromQLAssistance(
  payload: {
    prompt: string
  },
  token: string,
) {
  return apiPost<ApiEnvelope<PromQLAssistantSuggestion>>('/v1/metrics/ai/assist', payload, {
    token,
  }).then((response) => response.data)
}
