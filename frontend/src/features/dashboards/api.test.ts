import { beforeEach, describe, expect, it, vi } from 'vitest'

const mocks = vi.hoisted(() => ({
  apiGet: vi.fn(),
  apiPatch: vi.fn(),
  apiPost: vi.fn(),
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
    apiGet: mocks.apiGet,
    apiPatch: mocks.apiPatch,
    apiPost: mocks.apiPost,
    isRecord: (value: unknown) => typeof value === 'object' && value !== null,
  }
})

import { ApiRequestError } from '@/shared/api/http'
import {
  emptyDashboardPayload,
  fetchDashboardRevision,
  listDashboardRevisions,
  restoreDashboardRevision,
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
    mocks.apiGet.mockReset()
    mocks.apiPatch.mockReset()
    mocks.apiPost.mockReset()
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

  it('loads revision summaries and full revision details', async () => {
    mocks.apiGet
      .mockResolvedValueOnce({
        detail: '',
        data: [
          {
            revision: 3,
            saved_by: null,
            saved_at: '2026-06-06T10:05:00Z',
            restored_from_revision: null,
          },
        ],
      })
      .mockResolvedValueOnce({
        detail: '',
        data: {
          ...emptyDashboardPayload('Shared'),
          revision: 3,
          saved_by: null,
          saved_at: '2026-06-06T10:05:00Z',
          restored_from_revision: null,
        },
      })

    await expect(listDashboardRevisions(7, 'token')).resolves.toHaveLength(1)
    await expect(fetchDashboardRevision(7, 3, 'token')).resolves.toMatchObject({
      revision: 3,
      name: 'Shared',
    })
    expect(mocks.apiGet).toHaveBeenNthCalledWith(
      1,
      '/v1/metrics/dashboards/7/revisions/',
      { token: 'token' },
    )
  })

  it('exposes the latest dashboard when a restore conflicts', async () => {
    mocks.apiPost.mockRejectedValue(
      new ApiRequestError('conflict', 409, {
        detail: 'Dashboard was changed by another user.',
        data: latest,
      }),
    )

    await expect(restoreDashboardRevision(7, 1, 2, 'token')).rejects.toMatchObject({
      name: 'DashboardConflictError',
      latest,
    })
  })
})
