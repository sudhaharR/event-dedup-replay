// Small display helpers. Times from the API are UTC (the service stores everything in UTC).

export function time(value) {
  if (!value) return '–'
  const s = String(value)
  const m = s.match(/T?(\d{2}:\d{2}:\d{2})/)
  return m ? `${m[1]} UTC` : s
}

export function money(value) {
  if (value === null || value === undefined) return '–'
  const n = Number(value)
  return Number.isFinite(n) ? n.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : String(value)
}

export function ms(value) {
  if (value === null || value === undefined) return '–'
  const n = Number(value)
  if (!Number.isFinite(n)) return String(value)
  if (n < 1000) return `${Math.round(n)} ms`
  if (n < 120_000) return `${(n / 1000).toFixed(1)} s`
  if (n < 7_200_000) return `${(n / 60_000).toFixed(1)} min`
  return `${(n / 3_600_000).toFixed(1)} h`
}

export function num(value) {
  if (value === null || value === undefined) return '–'
  return Number(value).toLocaleString('en-IN')
}
