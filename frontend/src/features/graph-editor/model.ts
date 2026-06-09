import type {
  DashboardPanel,
  DashboardQuery,
  DashboardVisualization,
  DashboardVisualizationType,
} from '@/features/dashboards/api'

export const visualizationTypes: Array<{
  value: DashboardVisualizationType
  label: string
  description: string
}> = [
  { value: 'time_series', label: 'Time series', description: 'Values over time' },
  { value: 'bar', label: 'Bar chart', description: 'Compare series or categories' },
  { value: 'stat', label: 'Stat', description: 'Latest value at a glance' },
  { value: 'gauge', label: 'Gauge', description: 'Value against a range' },
  { value: 'table', label: 'Table', description: 'Inspect series and values' },
]

export function defaultVisualization(
  type: DashboardVisualizationType = 'time_series',
): DashboardVisualization {
  return {
    type,
    unit: '',
    decimals: null,
    min: null,
    max: null,
    color_scheme: 'classic',
    thresholds: [],
    legend_placement: 'bottom',
    tooltip_mode: 'all',
    line_width: 2,
    fill_opacity: 10,
    stack: false,
  }
}

export function createDashboardQuery(refId = 'A', query = ''): DashboardQuery {
  return {
    ref_id: refId,
    query,
    editor_mode: 'builder',
    disabled: false,
    legend: '',
  }
}

export function createUuid() {
  if (typeof globalThis.crypto?.randomUUID === 'function') {
    return globalThis.crypto.randomUUID()
  }

  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (character) => {
    const random = Math.floor(Math.random() * 16)
    const value = character === 'x' ? random : (random & 0x3) | 0x8
    return value.toString(16)
  })
}

export function createGraphPanel(query = '', title = 'New panel'): DashboardPanel {
  return {
    id: createUuid(),
    title,
    description: '',
    queries: [createDashboardQuery('A', query)],
    step_seconds: 60,
    visualization: defaultVisualization(),
    layout: { x: 0, y: 0, w: 6, h: 4 },
  }
}

export function nextQueryRef(queries: DashboardQuery[]) {
  const used = new Set(queries.map((query) => query.ref_id))
  for (let code = 65; code <= 90; code += 1) {
    const refId = String.fromCharCode(code)
    if (!used.has(refId)) {
      return refId
    }
  }
  return `Q${queries.length + 1}`
}

export function cloneDashboardData<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T
}

export function clonePanel(panel: DashboardPanel): DashboardPanel {
  return cloneDashboardData(panel)
}

export function substituteDashboardVariables(
  value: string,
  variables: Record<string, string[]>,
) {
  return value.replace(/\$([a-zA-Z_][a-zA-Z0-9_]*)|\$\{([a-zA-Z_][a-zA-Z0-9_]*)\}/g, (_match, a, b) => {
    const name = a || b
    const selected = variables[name] ?? []
    if (selected.length === 0) {
      return ''
    }
    if (selected.length === 1) {
      return escapePromQLValue(selected[0]!)
    }
    return selected.map(escapePromQLRegexValue).join('|')
  })
}

function escapePromQLValue(value: string) {
  return value.replace(/\\/g, '\\\\').replace(/"/g, '\\"')
}

function escapePromQLRegexValue(value: string) {
  return escapePromQLValue(value).replace(/[.*+?^${}()|[\]]/g, '\\$&')
}

export type QueryBuilderState = {
  metric: string
  operation: 'raw' | 'rate' | 'increase' | 'sum' | 'avg' | 'max' | 'min'
  range: string
  groupBy: string
  matchers: Array<{ label: string; operator: '=' | '!=' | '=~' | '!~'; value: string }>
}

export function defaultBuilderState(): QueryBuilderState {
  return {
    metric: '',
    operation: 'rate',
    range: '5m',
    groupBy: '',
    matchers: [],
  }
}

export function buildPromQL(state: QueryBuilderState) {
  const matcherText = state.matchers
    .filter((matcher) => matcher.label && matcher.value)
    .map(
      (matcher) =>
        `${matcher.label}${matcher.operator}"${matcher.value.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`,
    )
    .join(',')
  const selector = `${state.metric}${matcherText ? `{${matcherText}}` : ''}`
  if (!state.metric) {
    return ''
  }
  if (state.operation === 'raw') {
    return selector
  }
  if (state.operation === 'rate' || state.operation === 'increase') {
    return `${state.operation}(${selector}[${state.range || '5m'}])`
  }
  const grouping = state.groupBy.trim()
  return grouping
    ? `${state.operation} by (${grouping}) (${selector})`
    : `${state.operation}(${selector})`
}

export function inferBuilderState(query: string): QueryBuilderState | null {
  const trimmed = query.trim()
  const aggregate = trimmed.match(
    /^(sum|avg|max|min)(?:\s+by\s+\(([^)]*)\))?\s*\((.+)\)$/,
  )
  if (aggregate) {
    const inner = inferSelector(aggregate[3]!)
    return inner
      ? {
          ...inner,
          operation: aggregate[1] as QueryBuilderState['operation'],
          groupBy: aggregate[2]?.trim() ?? '',
        }
      : null
  }
  const rangeFunction = trimmed.match(/^(rate|increase)\((.+)\[([^\]]+)\]\)$/)
  if (rangeFunction) {
    const inner = inferSelector(rangeFunction[2]!)
    return inner
      ? {
          ...inner,
          operation: rangeFunction[1] as QueryBuilderState['operation'],
          range: rangeFunction[3]!,
        }
      : null
  }
  const selector = inferSelector(trimmed)
  return selector ? { ...selector, operation: 'raw' } : null
}

function inferSelector(value: string): QueryBuilderState | null {
  const match = value.trim().match(/^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})?$/)
  if (!match) {
    return null
  }
  const state = defaultBuilderState()
  state.metric = match[1]!
  state.matchers = (match[2] ?? '')
    .split(',')
    .map((part) => part.trim())
    .filter(Boolean)
    .map((part) => {
      const matcher = part.match(/^([a-zA-Z_][a-zA-Z0-9_]*)(=~|!~|!=|=)"(.*)"$/)
      return matcher
        ? {
            label: matcher[1]!,
            operator: matcher[2] as '=' | '!=' | '=~' | '!~',
            value: matcher[3]!,
          }
        : null
    })
    .filter((item): item is QueryBuilderState['matchers'][number] => item !== null)
  return state
}
