import { describe, expect, it } from 'vitest'
import { reactive } from 'vue'

import {
  buildPromQL,
  clonePanel,
  createGraphPanel,
  createUuid,
  inferBuilderState,
  substituteDashboardVariables,
} from '@/features/graph-editor/model'

describe('graph editor model', () => {
  it('builds and infers a grouped aggregate', () => {
    const query = buildPromQL({
      metric: 'http_requests_total',
      operation: 'sum',
      range: '5m',
      groupBy: 'job, instance',
      matchers: [{ label: 'status', operator: '=~', value: '5..' }],
    })

    expect(query).toBe(
      'sum by (job, instance) (http_requests_total{status=~"5.."})',
    )
    expect(inferBuilderState(query)).toMatchObject({
      metric: 'http_requests_total',
      operation: 'sum',
      groupBy: 'job, instance',
    })
  })

  it('substitutes single and multi-value dashboard variables safely', () => {
    expect(
      substituteDashboardVariables(
        'up{instance=~"$instance",job="${job}"}',
        {
          instance: ['api-1', 'api.2'],
          job: ['backend'],
        },
      ),
    ).toBe('up{instance=~"api-1|api\\.2",job="backend"}')
  })

  it('keeps advanced PromQL in code mode', () => {
    expect(inferBuilderState('histogram_quantile(0.95, rate(bucket[5m]))')).toBeNull()
  })

  it('creates backend-compatible panel IDs', () => {
    expect(createUuid()).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
    )
  })

  it('clones Vue-reactive panel data without throwing', () => {
    const panel = reactive(createGraphPanel('up', 'Reactive panel'))

    const cloned = clonePanel(panel)

    expect(cloned).toEqual(panel)
    expect(cloned).not.toBe(panel)
    expect(cloned.queries).not.toBe(panel.queries)
  })
})
