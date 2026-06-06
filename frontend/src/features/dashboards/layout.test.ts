import { describe, expect, it } from 'vitest'

import type { DashboardPanel } from '@/features/dashboards/api'
import { nextDashboardPanelY, normalizeLegendLabels } from '@/features/dashboards/layout'
import { createGraphPanel } from '@/features/graph-editor/model'

function panel(y: number, h: number): DashboardPanel {
  const result = createGraphPanel('up', 'Panel')
  result.layout = { x: 0, y, w: 6, h }
  return result
}

describe('dashboard layout helpers', () => {
  it('places a new panel below the lowest existing panel', () => {
    expect(nextDashboardPanelY([panel(0, 4), panel(2, 5)])).toBe(7)
  })

  it('starts an empty dashboard at row zero', () => {
    expect(nextDashboardPanelY([])).toBe(0)
  })

  it('trims and de-duplicates legend labels', () => {
    expect(normalizeLegendLabels(' job, instance_name,job, , mode ')).toEqual([
      'job',
      'instance_name',
      'mode',
    ])
  })
})
