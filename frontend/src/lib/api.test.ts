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

import { fetchCurrentUserContext, loginWithPassword } from '@/lib/api'

describe('core API authentication helpers', () => {
  beforeEach(() => {
    Object.values(mocks).forEach((mock) => mock.mockReset())
  })

  it('extracts allauth login tokens from response metadata', async () => {
    mocks.apiPost.mockResolvedValue({
      meta: {
        access_token: 'access-token',
        refresh_token: 'refresh-token',
      },
    })

    await expect(loginWithPassword('person@example.com', 'password')).resolves.toEqual({
      access: 'access-token',
      refresh: 'refresh-token',
    })
    expect(mocks.apiPost).toHaveBeenCalledWith('/_allauth/app/v1/auth/login', {
      email: 'person@example.com',
      password: 'password',
    })
  })

  it('normalizes missing current-user collections to empty arrays', async () => {
    mocks.apiGet.mockResolvedValue({
      data: {
        id: 1,
        username: 'user',
        display: 'User',
        email: 'user@example.com',
        avatar_url: '',
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
