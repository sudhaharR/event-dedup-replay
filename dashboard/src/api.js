// Thin client for the Spring Boot consumer service. All calls go to /api/... (Vite proxy / nginx).
const BASE = import.meta.env.VITE_API_BASE ?? '/api'

export class ApiError extends Error {
  constructor(status, message) {
    super(message)
    this.status = status
  }
}

async function request(method, path, body, { raw = false } = {}) {
  const init = { method }
  if (body !== undefined) {
    init.headers = { 'Content-Type': 'application/json' }
    init.body = raw ? body : JSON.stringify(body)
  }
  let res
  try {
    res = await fetch(BASE + path, init)
  } catch {
    throw new ApiError(0, 'Cannot reach the consumer service - is it running on port 8080?')
  }
  const text = await res.text()
  let data = null
  try {
    data = text ? JSON.parse(text) : null
  } catch {
    data = text
  }
  if (!res.ok) {
    const message = (data && data.message) || res.statusText || `HTTP ${res.status}`
    throw new ApiError(res.status, message)
  }
  return data
}

export const api = {
  health: () => request('GET', '/actuator/health'),
  metrics: () => request('GET', '/metrics'),
  orders: () => request('GET', '/orders'),
  inbox: (limit = 25) => request('GET', `/inbox?limit=${limit}`),
  deadLetters: (limit = 20) => request('GET', `/dead-letters?limit=${limit}`),
  ledger: () => request('GET', '/ledger'),
  replayRuns: () => request('GET', '/replay-runs'),

  // a message is a JSON object, or a raw string (e.g. broken JSON) sent exactly as it is
  send: (message) =>
    typeof message === 'string'
      ? request('POST', '/events', message, { raw: true })
      : request('POST', '/events', message),

  reset: () => request('POST', '/admin/reset'),
  replay: () => request('POST', '/replay'),
  reconcile: () => request('POST', '/reconciliation/refresh'),
}
