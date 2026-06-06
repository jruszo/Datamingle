import type { DashboardPanel } from '@/features/dashboards/api'

export function nextDashboardPanelY(panels: DashboardPanel[]) {
  return Math.max(0, ...panels.map((panel) => panel.layout.y + panel.layout.h))
}

export function normalizeLegendLabels(value: string) {
  return value
    .split(',')
    .map((item) => item.trim())
    .filter((item, index, values) => Boolean(item) && values.indexOf(item) === index)
}
