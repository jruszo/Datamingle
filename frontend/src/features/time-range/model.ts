export type TimeRangeValue = {
  mode: 'relative' | 'absolute'
  seconds: number
  start: string
  end: string
}

export const quickTimeRanges = [
  { label: 'Past 15 minutes', shortLabel: '15m', seconds: 15 * 60 },
  { label: 'Past 30 minutes', shortLabel: '30m', seconds: 30 * 60 },
  { label: 'Past 1 hour', shortLabel: '1h', seconds: 60 * 60 },
  { label: 'Past 4 hours', shortLabel: '4h', seconds: 4 * 60 * 60 },
  { label: 'Past 1 day', shortLabel: '1d', seconds: 24 * 60 * 60 },
  { label: 'Past 2 days', shortLabel: '2d', seconds: 2 * 24 * 60 * 60 },
  { label: 'Past 1 week', shortLabel: '1w', seconds: 7 * 24 * 60 * 60 },
  { label: 'Past 30 days', shortLabel: '30d', seconds: 30 * 24 * 60 * 60 },
] as const

export function defaultTimeRange(): TimeRangeValue {
  return { mode: 'relative', seconds: 3600, start: '', end: '' }
}

export function parseRelativeDuration(value: string) {
  const match = value.trim().toLowerCase().match(/^(\d+(?:\.\d+)?)\s*(m|h|d|w)$/)
  if (!match) return null
  const amount = Number(match[1])
  const unit = match[2] as 'm' | 'h' | 'd' | 'w'
  const multiplier = { m: 60, h: 3600, d: 86400, w: 7 * 86400 }[unit]
  const seconds = Math.round(amount * multiplier)
  return Number.isFinite(seconds) && seconds >= 60 && seconds <= 30 * 86400
    ? seconds
    : null
}

export function compactTimeRange(value: TimeRangeValue) {
  if (value.mode === 'relative') {
    return quickTimeRanges.find((item) => item.seconds === value.seconds)?.shortLabel
      ?? compactDuration(value.seconds)
  }
  return formatTimeRange(value)
}

export function resolveTimeRange(value: TimeRangeValue, now = new Date()) {
  if (value.mode === 'absolute' && value.start && value.end) {
    const start = new Date(value.start)
    const end = new Date(value.end)
    if (!Number.isNaN(start.getTime()) && !Number.isNaN(end.getTime()) && end > start) {
      return { start, end }
    }
  }
  return {
    start: new Date(now.getTime() - value.seconds * 1000),
    end: now,
  }
}

export function effectiveQueryStep(start: Date, end: Date, configuredStep: number) {
  const durationSeconds = Math.max(1, (end.getTime() - start.getTime()) / 1000)
  const adaptiveStep = Math.ceil(durationSeconds / 10_000 / 15) * 15
  return Math.max(15, configuredStep, adaptiveStep)
}

export function formatTimeRange(value: TimeRangeValue) {
  if (value.mode === 'relative') {
    return quickTimeRanges.find((item) => item.seconds === value.seconds)?.label
      ?? `Past ${formatDuration(value.seconds)}`
  }
  const { start, end } = resolveTimeRange(value)
  const sameDay = start.toDateString() === end.toDateString()
  const date = new Intl.DateTimeFormat(undefined, {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  })
  const dateTime = new Intl.DateTimeFormat(undefined, {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
  })
  const time = new Intl.DateTimeFormat(undefined, {
    hour: 'numeric',
    minute: '2-digit',
  })
  return sameDay
    ? `${date.format(start)}, ${time.format(start)} - ${time.format(end)}`
    : `${dateTime.format(start)} - ${dateTime.format(end)}`
}

export function formatDuration(seconds: number) {
  if (seconds % 86400 === 0) return `${seconds / 86400} days`
  if (seconds % 3600 === 0) return `${seconds / 3600} hours`
  return `${Math.max(1, Math.round(seconds / 60))} minutes`
}

function compactDuration(seconds: number) {
  if (seconds % 604800 === 0) return `${seconds / 604800}w`
  if (seconds % 86400 === 0) return `${seconds / 86400}d`
  if (seconds % 3600 === 0) return `${seconds / 3600}h`
  return `${Math.max(1, Math.round(seconds / 60))}m`
}

export function toLocalDateTime(value: string) {
  if (!value) return ''
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return ''
  const offset = date.getTimezoneOffset() * 60_000
  return new Date(date.getTime() - offset).toISOString().slice(0, 16)
}

export function fromLocalDateTime(value: string) {
  return value ? new Date(value).toISOString() : ''
}
