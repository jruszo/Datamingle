import { describe, expect, it } from 'vitest'

import {
  applyMetricsFilters,
  metricsFiltersSelector,
  parseMetricsFilters,
  writeMetricsFilters,
} from './filters'

const filters = [
  { label: 'dm_environment', mode: 'include' as const, values: ['prod', 'staging'] },
  { label: 'dm_team', mode: 'exclude' as const, values: ['legacy'] },
]

describe('metrics filters', () => {
  it('builds Prometheus selectors with include and exclude matchers', () => {
    expect(metricsFiltersSelector(filters)).toBe(
      '{dm_environment=~"prod|staging",dm_team!="legacy"}',
    )
  })

  it('injects filters into every vector selector', () => {
    expect(applyMetricsFilters('sum(rate(foo[5m])) + bar{job="db"}', filters)).toBe(
      'sum(rate(foo{dm_environment=~"prod|staging",dm_team!="legacy"}[5m])) + bar{job="db",dm_environment=~"prod|staging",dm_team!="legacy"}',
    )
  })

  it('keeps explicit panel matchers authoritative', () => {
    expect(applyMetricsFilters('foo{dm_environment="dev"}', filters)).toBe(
      'foo{dm_environment="dev",dm_team!="legacy"}',
    )
  })

  it('round trips include and exclude filters through URL query values', () => {
    const query = writeMetricsFilters({ panel: 'abc' }, filters)
    expect(query).toEqual({
      panel: 'abc',
      'mf.dm_environment': ['prod', 'staging'],
      'mx.dm_team': ['legacy'],
    })
    expect(parseMetricsFilters(query)).toEqual(filters)
  })

  it('rejects malformed PromQL instead of applying partial filters', () => {
    expect(() => applyMetricsFilters('rate(foo{job="db"[5m])', filters)).toThrow(
      'invalid PromQL',
    )
  })
})
