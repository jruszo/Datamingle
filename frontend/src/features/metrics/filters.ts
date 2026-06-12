import { parser } from '@prometheus-io/lezer-promql'
import type { LocationQuery, LocationQueryRaw } from 'vue-router'

import type { LabelFilter, LabelFilterMode } from '@/shared/filters/labelFilters'

export type MetricsFilterMode = LabelFilterMode
export type MetricsFilter = LabelFilter

const INCLUDE_PREFIX = 'mf.'
const EXCLUDE_PREFIX = 'mx.'

function queryValues(value: unknown) {
  const values = Array.isArray(value) ? value : [value]
  return values.filter((item): item is string => typeof item === 'string' && Boolean(item))
}

export function parseMetricsFilters(query: LocationQuery | LocationQueryRaw): MetricsFilter[] {
  const filters: MetricsFilter[] = []
  for (const [key, value] of Object.entries(query)) {
    const mode = key.startsWith(INCLUDE_PREFIX)
      ? 'include'
      : key.startsWith(EXCLUDE_PREFIX)
        ? 'exclude'
        : null
    if (!mode) continue
    const label = key.slice(3)
    const values = [...new Set(queryValues(value))].sort()
    if (label && values.length) filters.push({ label, mode, values })
  }
  return filters.sort(
    (left, right) => left.label.localeCompare(right.label) || left.mode.localeCompare(right.mode),
  )
}

export function writeMetricsFilters(
  query: LocationQuery,
  filters: MetricsFilter[],
): LocationQueryRaw {
  const next: LocationQueryRaw = {}
  for (const [key, value] of Object.entries(query)) {
    if (!key.startsWith(INCLUDE_PREFIX) && !key.startsWith(EXCLUDE_PREFIX)) {
      next[key] = value
    }
  }
  for (const filter of filters) {
    if (!filter.label || filter.values.length === 0) continue
    next[`${filter.mode === 'include' ? INCLUDE_PREFIX : EXCLUDE_PREFIX}${filter.label}`] = [
      ...new Set(filter.values),
    ].sort()
  }
  return next
}

function escapePrometheusString(value: string) {
  return value.replace(/\\/g, '\\\\').replace(/"/g, '\\"')
}

function escapePrometheusRegex(value: string) {
  return escapePrometheusString(value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'))
}

export function metricsFilterMatchers(filters: MetricsFilter[]) {
  return filters.flatMap((filter) => {
    if (!filter.label || filter.values.length === 0) return []
    const multiple = filter.values.length > 1
    const operator = filter.mode === 'include' ? (multiple ? '=~' : '=') : multiple ? '!~' : '!='
    const value = multiple
      ? filter.values.map(escapePrometheusRegex).join('|')
      : escapePrometheusString(filter.values[0]!)
    return [{ label: filter.label, text: `${filter.label}${operator}"${value}"` }]
  })
}

export function metricsFiltersSelector(filters: MetricsFilter[]) {
  const matchers = metricsFilterMatchers(filters)
  return matchers.length ? `{${matchers.map((matcher) => matcher.text).join(',')}}` : ''
}

export function applyMetricsFilters(query: string, filters: MetricsFilter[]) {
  const matchers = metricsFilterMatchers(filters)
  if (!query.trim() || matchers.length === 0) return query

  const tree = parser.parse(query)
  const cursor = tree.cursor()
  const selectors: Array<{
    from: number
    to: number
    matcherFrom: number | null
    matcherTo: number | null
    labels: Set<string>
  }> = []
  let invalid = false

  function walk() {
    if (cursor.type.isError) invalid = true
    if (cursor.name === 'VectorSelector') {
      const selector = {
        from: cursor.from,
        to: cursor.to,
        matcherFrom: null as number | null,
        matcherTo: null as number | null,
        labels: new Set<string>(),
      }
      if (cursor.firstChild()) {
        do {
          if (cursor.type.isError) invalid = true
          if (cursor.node.name === 'LabelMatchers') {
            selector.matcherFrom = cursor.from
            selector.matcherTo = cursor.to
            const matcherCursor = cursor.node.cursor()
            function collectLabels() {
              if (matcherCursor.type.isError) invalid = true
              if (matcherCursor.name === 'LabelName') {
                selector.labels.add(query.slice(matcherCursor.from, matcherCursor.to))
              }
              if (matcherCursor.firstChild()) {
                do collectLabels()
                while (matcherCursor.nextSibling())
                matcherCursor.parent()
              }
            }
            collectLabels()
          }
        } while (cursor.nextSibling())
        cursor.parent()
      }
      selectors.push(selector)
      return
    }
    if (cursor.firstChild()) {
      do walk()
      while (cursor.nextSibling())
      cursor.parent()
    }
  }

  walk()
  if (invalid) throw new Error('Global metrics filters cannot be applied to invalid PromQL.')

  const edits = selectors.flatMap((selector) => {
    const additions = matchers
      .filter((matcher) => !selector.labels.has(matcher.label))
      .map((matcher) => matcher.text)
    if (additions.length === 0) return []
    if (selector.matcherTo !== null && selector.matcherFrom !== null) {
      const existing = query.slice(selector.matcherFrom + 1, selector.matcherTo - 1).trim()
      return [
        {
          at: selector.matcherTo - 1,
          text: `${existing ? ',' : ''}${additions.join(',')}`,
        },
      ]
    }
    return [{ at: selector.to, text: `{${additions.join(',')}}` }]
  })

  let result = query
  for (const edit of edits.sort((left, right) => right.at - left.at)) {
    result = `${result.slice(0, edit.at)}${edit.text}${result.slice(edit.at)}`
  }
  return result
}

export function displayMetricsLabel(label: string) {
  return label.startsWith('dm_') ? label.slice(3) : label
}
