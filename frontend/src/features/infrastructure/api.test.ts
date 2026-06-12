import { beforeEach, describe, expect, it, vi } from 'vitest'

const mocks = vi.hoisted(() => ({
  apiGet: vi.fn(),
  apiPatch: vi.fn(),
  apiPost: vi.fn(),
}))

vi.mock('@/shared/api/http', () => ({
  apiGet: mocks.apiGet,
  apiPatch: mocks.apiPatch,
  apiPost: mocks.apiPost,
  isRecord: (value: unknown) => typeof value === 'object' && value !== null,
}))

import {
  fetchInfrastructureNodeLabelValues,
  fetchInfrastructureNodes,
} from '@/features/infrastructure/api'

describe('infrastructure API', () => {
  beforeEach(() => {
    mocks.apiGet.mockReset()
    mocks.apiPatch.mockReset()
    mocks.apiPost.mockReset()
    mocks.apiGet.mockResolvedValue({
      data: { count: 0, next: null, previous: null, results: [] },
    })
  })

  it('sends include and exclude label filters with node list requests', async () => {
    await fetchInfrastructureNodes('token', {
      page: 2,
      search: 'db',
      labelFilters: [
        { label: 'environment', mode: 'include', values: ['prod', 'stage'] },
        { label: 'team', mode: 'exclude', values: ['legacy'] },
      ],
    })

    expect(mocks.apiGet).toHaveBeenCalledWith(
      '/v1/infrastructure/nodes/?page=2&search=db&lf.environment=prod&lf.environment=stage&lx.team=legacy',
      { token: 'token' },
    )
  })

  it('uses the remaining filters when loading autocomplete values', async () => {
    mocks.apiGet.mockResolvedValue({ data: ['platform'] })

    await expect(
      fetchInfrastructureNodeLabelValues('team', 'token', [
        { label: 'environment', mode: 'include', values: ['prod'] },
      ]),
    ).resolves.toEqual(['platform'])

    expect(mocks.apiGet).toHaveBeenCalledWith(
      '/v1/infrastructure/nodes/label/team/values/?lf.environment=prod',
      { token: 'token' },
    )
  })
})
