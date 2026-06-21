import { describe, expect, it } from 'vitest'

import { allauthHeadlessPath } from '@/shared/auth/allauth'

describe('allauth headless paths', () => {
  it('normalizes paths with or without a leading slash', () => {
    expect(allauthHeadlessPath('/auth/login')).toBe('/_allauth/app/v1/auth/login')
    expect(allauthHeadlessPath('auth/login')).toBe('/_allauth/app/v1/auth/login')
  })
})
