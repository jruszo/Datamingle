import { beforeEach, describe, expect, it, vi } from 'vitest'

const mocks = vi.hoisted(() => ({
  apiDelete: vi.fn(),
  apiGet: vi.fn(),
  apiPatch: vi.fn(),
  apiPost: vi.fn(),
  apiPut: vi.fn(),
}))

vi.mock('@/shared/api/http', () => ({
  apiDelete: mocks.apiDelete,
  apiGet: mocks.apiGet,
  apiPatch: mocks.apiPatch,
  apiPost: mocks.apiPost,
  apiPut: mocks.apiPut,
  buildUrl: (path: string) => path,
  flattenErrorMessage: () => '',
  isRecord: (value: unknown) => typeof value === 'object' && value !== null,
  publicApiUrl: (path: string) => path,
}))

import { exchangeWorkosCode, fetchCurrentUserContext } from '@/lib/api'

describe('core API authentication helpers', () => {
  beforeEach(() => {
    Object.values(mocks).forEach((mock) => mock.mockReset())
  })

  it('deduplicates concurrent WorkOS exchanges for the same one-time code', async () => {
    let resolveExchange!: (value: unknown) => void
    mocks.apiPost.mockReturnValue(
      new Promise((resolve) => {
        resolveExchange = resolve
      }),
    )

    const first = exchangeWorkosCode('exchange-code-1')
    const second = exchangeWorkosCode('exchange-code-1')
    resolveExchange({ data: { access: 'access-token', refresh: 'refresh-token' } })

    await expect(first).resolves.toEqual({
      access: 'access-token',
      refresh: 'refresh-token',
    })
    await expect(second).resolves.toEqual({
      access: 'access-token',
      refresh: 'refresh-token',
    })
    expect(mocks.apiPost).toHaveBeenCalledTimes(1)
  })

  it('normalizes missing current-user collections to empty arrays', async () => {
    mocks.apiGet.mockResolvedValue({
      data: {
        id: 1,
        username: 'user',
        display: 'User',
        email: 'user@example.com',
        avatar_url: '',
        is_workos_managed: true,
        is_superuser: false,
        is_staff: false,
        is_active: true,
      },
    })

    await expect(fetchCurrentUserContext('access-token')).resolves.toMatchObject({
      groups: [],
      teams: [],
      permissions: [],
    })
  })
})
