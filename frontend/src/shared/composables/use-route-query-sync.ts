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

export function routeQueriesMatch(
  routeQuery: LocationQuery,
  currentQuery: LocationQueryRaw,
) {
  const normalizedRouteQuery = compactRouteQuery(routeQuery as LocationQueryRaw)
  const normalizedCurrentQuery = compactRouteQuery(currentQuery)
  return JSON.stringify(normalizedRouteQuery) === JSON.stringify(normalizedCurrentQuery)
}
