import { beforeEach, describe, expect, it, vi } from 'vitest'

const mocks = vi.hoisted(() => ({
  apiPatch: vi.fn(),
}))

vi.mock('@/shared/api/http', () => {
  class ApiRequestError extends Error {
    status: number
    data: unknown

    constructor(message: string, status: number, data: unknown) {
      super(message)
      this.status = status
      this.data = data
    }
  }

  return {
    ApiRequestError,
    apiDelete: vi.fn(),
    apiGet: vi.fn(),
    apiPatch: mocks.apiPatch,
    apiPost: vi.fn(),
    isRecord: (value: unknown) => typeof value === 'object' && value !== null,
  }
})

import { ApiRequestError } from '@/shared/api/http'
import {
  emptyDashboardPayload,
  updateMetricsDashboard,
  type MetricsDashboard,
} from '@/features/dashboards/api'

const latest: MetricsDashboard = {
  id: 7,
  name: 'Shared',
  description: '',
  created_by: null,
  revision: 3,
  time_range_seconds: 3600,
      refresh_interval_seconds: 0,
      variables: [],
      panels: [],
  create_time: '2026-06-06T10:00:00Z',
  update_time: '2026-06-06T10:05:00Z',
}

describe('dashboard API', () => {
  beforeEach(() => {
    mocks.apiPatch.mockReset()
  })

  it('exposes the latest dashboard when an update conflicts', async () => {
    mocks.apiPatch.mockRejectedValue(
      new ApiRequestError('conflict', 409, {
        detail: 'Dashboard was changed by another user.',
        data: latest,
      }),
    )

    await expect(
      updateMetricsDashboard(7, 2, emptyDashboardPayload('Shared'), 'token'),
    ).rejects.toMatchObject({
      name: 'DashboardConflictError',
      latest,
    })
  })
})
