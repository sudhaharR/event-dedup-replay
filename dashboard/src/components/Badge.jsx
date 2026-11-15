// Coloured label for a status (order status, inbox decision, reconciliation result ...)
const TONE = {
  APPLIED: 'good', MATCH: 'good', SHIPPED: 'good', COMPLETED: 'good', UP: 'good', PASS: 'good',
  PAID: 'info', CREATED: 'neutral', LIVE: 'neutral', REPLAY: 'info', PENDING: 'neutral',
  DUPLICATE: 'warn', STALE: 'warn', PARKED: 'warn', RUNNING: 'warn', NO_SNAPSHOT: 'warn',
  CANCELLED: 'muted',
  DEAD_LETTERED: 'bad', MISMATCH: 'bad', FAILED: 'bad', DOWN: 'bad', MISSING_IN_SNAPSHOT: 'bad', NOT_EXPECTED: 'bad',
}

export default function Badge({ value }) {
  if (value === null || value === undefined) return <span className="muted">–</span>
  const v = String(value)
  return <span className={`badge badge-${TONE[v] ?? 'neutral'}`}>{v}</span>
}
