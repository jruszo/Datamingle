import { describe, expect, it } from 'vitest'

import {
  defaultTimeRange,
  effectiveQueryStep,
  formatTimeRange,
  parseRelativeDuration,
  resolveTimeRange,
} from '@/features/time-range/model'

describe('time range helpers', () => {
  it('resolves relative ranges against now', () => {
    const now = new Date('2026-06-06T12:00:00.000Z')
    const result = resolveTimeRange(
      { mode: 'relative', seconds: 3600, start: '', end: '' },
      now,
    )

    expect(result.start.toISOString()).toBe('2026-06-06T11:00:00.000Z')
    expect(result.end.toISOString()).toBe(now.toISOString())
  })

  it('resolves absolute ranges without changing their timestamps', () => {
    const result = resolveTimeRange({
      mode: 'absolute',
      seconds: 3600,
      start: '2026-06-06T08:15:00.000Z',
      end: '2026-06-06T10:45:00.000Z',
    })

    expect(result.start.toISOString()).toBe('2026-06-06T08:15:00.000Z')
    expect(result.end.toISOString()).toBe('2026-06-06T10:45:00.000Z')
  })

  it('labels common and custom relative ranges', () => {
    expect(formatTimeRange(defaultTimeRange())).toBe('Past 1 hour')
    expect(
      formatTimeRange({ mode: 'relative', seconds: 3 * 3600, start: '', end: '' }),
    ).toBe('Past 3 hours')
  })

  it('increases query step for long ranges', () => {
    const start = new Date('2026-05-07T12:00:00.000Z')
    const end = new Date('2026-06-06T12:00:00.000Z')

    expect(effectiveQueryStep(start, end, 60)).toBe(270)
    expect(effectiveQueryStep(start, end, 300)).toBe(300)
  })

  it('parses manual relative durations', () => {
    expect(parseRelativeDuration('4h')).toBe(14400)
    expect(parseRelativeDuration('1.5 d')).toBe(129600)
    expect(parseRelativeDuration('2w')).toBe(1209600)
    expect(parseRelativeDuration('45x')).toBeNull()
    expect(parseRelativeDuration('31d')).toBeNull()
  })
})
