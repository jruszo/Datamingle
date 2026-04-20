import type { LocationQuery, LocationQueryRaw } from 'vue-router'

export function routeQueryValue(value: unknown) {
  return typeof value === 'string' ? value : ''
}

export function parsePositiveInteger(value: string, fallback: number) {
  const parsedValue = Number(value)
  return Number.isInteger(parsedValue) && parsedValue > 0 ? parsedValue : fallback
}

export function compactRouteQuery(query: LocationQueryRaw): LocationQueryRaw {
  const compacted: LocationQueryRaw = {}

  for (const [key, value] of Object.entries(query)) {
    if (value === undefined || value === null || value === '') {
      continue
    }

    compacted[key] = value
  }

  return compacted
}

function deepSortKeys(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((entry) => deepSortKeys(entry))
  }

  if (value && typeof value === 'object') {
    return Object.keys(value)
      .sort()
      .reduce<Record<string, unknown>>((sorted, key) => {
        sorted[key] = deepSortKeys((value as Record<string, unknown>)[key])
        return sorted
      }, {})
  }

  return value
}

export function routeQueriesMatch(
  routeQuery: LocationQuery,
  currentQuery: LocationQueryRaw,
) {
  const normalizedRouteQuery = deepSortKeys(compactRouteQuery(routeQuery as LocationQueryRaw))
  const normalizedCurrentQuery = deepSortKeys(compactRouteQuery(currentQuery))
  return JSON.stringify(normalizedRouteQuery) === JSON.stringify(normalizedCurrentQuery)
}
