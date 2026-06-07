import { beforeEach, describe, expect, it, vi } from 'vitest'

const mocks = vi.hoisted(() => ({
  apiDelete: vi.fn(),
  apiGet: vi.fn(),
  apiGetBlob: vi.fn(),
  apiPatch: vi.fn(),
  apiPost: vi.fn(),
  apiPostForm: vi.fn(),
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
    apiDelete: mocks.apiDelete,
    apiGet: mocks.apiGet,
    apiGetBlob: mocks.apiGetBlob,
    apiPatch: mocks.apiPatch,
    apiPost: mocks.apiPost,
    apiPostForm: mocks.apiPostForm,
    isRecord: (value: unknown) => typeof value === 'object' && value !== null,
  }
})

import { ApiRequestError } from '@/shared/api/http'
import {
  emptyDashboardPayload,
  fetchDashboardIcon,
  fetchDashboardRevision,
  listDashboardRevisions,
  restoreDashboardRevision,
  uploadDashboardIcon,
  updateMetricsDashboard,
  type MetricsDashboard,
} from '@/features/dashboards/api'

const latest: MetricsDashboard = {
  id: 7,
  name: 'Shared',
  description: '',
  created_by: null,
  has_icon: false,
  revision: 3,
  time_range_mode: 'relative',
  time_range_seconds: 3600,
  time_range_start: '',
  time_range_end: '',
  refresh_interval_seconds: 0,
  variables: [],
  panels: [],
  create_time: '2026-06-06T10:00:00Z',
  update_time: '2026-06-06T10:05:00Z',
}

describe('dashboard API', () => {
  beforeEach(() => {
    mocks.apiDelete.mockReset()
    mocks.apiGet.mockReset()
    mocks.apiGetBlob.mockReset()
    mocks.apiPatch.mockReset()
    mocks.apiPost.mockReset()
    mocks.apiPostForm.mockReset()
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

  it('loads and uploads dashboard icons through authenticated endpoints', async () => {
    const blob = new Blob(['image'], { type: 'image/png' })
    mocks.apiGetBlob.mockResolvedValue(blob)
    mocks.apiPostForm.mockResolvedValue({
      detail: '',
      data: { ...latest, has_icon: true },
    })
    const file = new File(['image'], 'icon.png', { type: 'image/png' })

    await expect(fetchDashboardIcon(7, 'token')).resolves.toBe(blob)
    await expect(uploadDashboardIcon(7, file, 'token')).resolves.toMatchObject({
      has_icon: true,
    })

    expect(mocks.apiGetBlob).toHaveBeenCalledWith(
      '/v1/metrics/dashboards/7/icon/',
      { token: 'token' },
    )
    const form = mocks.apiPostForm.mock.calls[0]![1] as FormData
    expect(form.get('icon')).toBe(file)
  })
})
