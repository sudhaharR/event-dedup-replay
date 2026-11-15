// The event contract (docs/event-contract.md) - the same structural rules as the Java validator.
// A message is an object, or a string (sent exactly as typed - possibly broken JSON).

const REQUIRED = ['event_id', 'entity_id', 'version', 'event_type', 'event_time', 'payload']
export const EVENT_TYPES = ['OrderCreated', 'PaymentCaptured', 'OrderShipped', 'OrderCancelled']
const CURRENCY = /^[A-Z]{3}$/
const MAX_AMOUNT = 9999999999.99

const text = (v, max = 64) => typeof v === 'string' && v.trim() !== '' && v.trim().length <= max
const amount = (v) => typeof v === 'number' && Number.isFinite(v) && v > 0 && v <= MAX_AMOUNT
const isObject = (v) => v !== null && typeof v === 'object' && !Array.isArray(v)

/** Returns the event if it passes structural validation, otherwise null (it would be dead-lettered). */
export function parseValid(message) {
  let e = message
  if (typeof e === 'string') {
    try {
      e = JSON.parse(e)
    } catch {
      return null
    }
  }
  if (!isObject(e) || REQUIRED.some((f) => e[f] === undefined || e[f] === null)) return null
  if (!text(e.event_id) || !text(e.entity_id) || !text(e.event_type, 40)) return null
  if (!Number.isInteger(e.version)) return null
  if (typeof e.event_time !== 'string' || !/(Z|[+-]\d{2}:?\d{2})$/.test(e.event_time)
      || Number.isNaN(Date.parse(e.event_time))) return null
  const p = e.payload
  if (!isObject(p) || !EVENT_TYPES.includes(e.event_type)) return null

  const t = e.event_type
  if (e.version < 1 || (t === 'OrderCreated' && e.version !== 1) || (t !== 'OrderCreated' && e.version < 2)) return null
  if (t === 'OrderCreated' || t === 'PaymentCaptured') {
    const key = t === 'OrderCreated' ? 'customer_id' : 'payment_id'
    if (!text(p[key]) || !amount(p.amount) || typeof p.currency !== 'string' || !CURRENCY.test(p.currency)) return null
  }
  if (t === 'OrderShipped' && !text(p.tracking_id)) return null
  if (t === 'OrderCancelled' && !text(p.reason, 255)) return null
  return e
}

/** Money as whole paise/cents, so comparisons are exact. */
export const cents = (value) => Math.round(Number(value) * 100)
