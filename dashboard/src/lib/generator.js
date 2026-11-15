// Builds a messy event stream from settings: N order lifecycles, then duplicates, poison messages,
// arrival order, and late (held-back) events - like the Python simulator's shuffled_full scenario.

import { makeRandom, randomInt, runTag, shuffle } from './random.js'

export const POISON_KINDS = {
  broken: 'Broken JSON',
  missing: 'Missing entity_id',
  amount: 'Text amount',
  type: 'Unknown event type',
}

export const DEFAULT_SETTINGS = {
  orders: 5,
  duplicates: 2,
  poison: 2,
  poisonKinds: ['broken', 'missing', 'amount', 'type'],
  arrival: 'shuffled', // 'in-order' | 'shuffled' | 'reversed'
  late: 1,
  lateMs: 1500,
  shipped: 50,
  paid: 20,
  cancelled: 30,
  delayMs: 120,
  seed: '',
}

export const LIMITS = { orders: 200, duplicates: 200, poison: 50, late: 100, lateMs: 30000, delayMs: 2000 }

const clamp = (n, min, max) => Math.max(min, Math.min(max, Math.floor(Number(n) || 0)))

function pickEnding(s, random) {
  const total = Math.max(1, s.shipped + s.paid + s.cancelled)
  const r = random() * total
  if (r < s.shipped) return ['PaymentCaptured', 'OrderShipped']
  if (r < s.shipped + s.paid) return ['PaymentCaptured']
  return random() < 0.5 ? ['OrderCancelled'] : ['PaymentCaptured', 'OrderCancelled']
}

function poisonMessage(kind, tag, n, orderId, now) {
  const eventId = `E-POISON-${tag}-${n}`
  switch (kind) {
    case 'broken':
      return `{"event_id": "${eventId}", "entity_id": `
    case 'missing':
      return { event_id: eventId, version: 2, event_type: 'PaymentCaptured', event_time: now,
        payload: { payment_id: `PAY-X-${n}`, amount: 100, currency: 'INR' } }
    case 'amount':
      return { event_id: eventId, entity_id: orderId, version: 2, event_type: 'PaymentCaptured', event_time: now,
        payload: { payment_id: `PAY-Y-${n}`, amount: 'abc', currency: 'INR' } }
    default:
      return { event_id: eventId, entity_id: orderId, version: 2, event_type: 'PaymentRefunded', event_time: now,
        payload: {} }
  }
}

/**
 * @returns {{run: string, onTime: any[], late: any[], settings: object}}
 * The seed fixes the PATTERN (endings, amounts, which events are duplicated, arrival order);
 * order ids always get a fresh random tag so runs never collide.
 */
export function generate(input = {}) {
  const s = { ...DEFAULT_SETTINGS, ...input }
  s.orders = clamp(s.orders, 1, LIMITS.orders)
  s.duplicates = clamp(s.duplicates, 0, LIMITS.duplicates)
  s.poison = clamp(s.poison, 0, LIMITS.poison)
  s.late = clamp(s.late, 0, LIMITS.late)
  s.shipped = clamp(s.shipped, 0, 100)
  s.paid = clamp(s.paid, 0, 100)
  s.cancelled = clamp(s.cancelled, 0, 100)
  const kinds = (s.poisonKinds?.length ? s.poisonKinds : Object.keys(POISON_KINDS))

  const random = makeRandom(s.seed)
  const tag = runTag()
  const start = Date.now() - 10_000 // events "happened" ~10 s before they are sent
  const iso = (ms) => new Date(ms).toISOString()

  // 1. clean lifecycles, grouped per order
  const groups = []
  for (let n = 1; n <= s.orders; n++) {
    const orderId = `ORD-${tag}-${n}`
    const amount = Math.round((100 + random() * 4900) * 100) / 100
    const group = [{
      event_id: `E-${orderId}-v1`, entity_id: orderId, version: 1, event_type: 'OrderCreated',
      event_time: iso(start + n * 10), payload: { customer_id: `CUST-${n}`, amount, currency: 'INR' },
    }]
    pickEnding(s, random).forEach((type, i) => {
      const version = i + 2
      const payload = type === 'PaymentCaptured' ? { payment_id: `PAY-${orderId}`, amount, currency: 'INR' }
        : type === 'OrderShipped' ? { tracking_id: `TRK-${orderId}` } : { reason: 'customer request' }
      group.push({ event_id: `E-${orderId}-v${version}`, entity_id: orderId, version, event_type: type,
        event_time: iso(start + n * 10 + version), payload })
    })
    groups.push(group)
  }
  const events = groups.flat()

  // 2. arrival order of the real events
  let stream = s.arrival === 'in-order' ? [...events]
    : s.arrival === 'reversed' ? groups.flatMap((g) => [...g].reverse())
      : shuffle(events, random)

  // 3. duplicates: re-send random events at a later position than their first copy
  for (let i = 0; i < s.duplicates; i++) {
    const original = events[randomInt(random, 0, events.length - 1)]
    const first = stream.indexOf(original)
    stream.splice(randomInt(random, first + 1, stream.length), 0, structuredClone(original))
  }

  // 4. poison messages at random positions, rotating through the chosen kinds
  for (let i = 0; i < s.poison; i++) {
    const kind = kinds[i % kinds.length]
    const orderId = groups[randomInt(random, 0, groups.length - 1)][0].entity_id
    stream.splice(randomInt(random, 0, stream.length), 0, poisonMessage(kind, tag, i + 1, orderId, iso(Date.now())))
  }

  // 5. late events: held back and sent after a pause (only real events, never poison)
  const realIndexes = stream.map((m, i) => (typeof m === 'object' && m.entity_id ? i : -1)).filter((i) => i >= 0)
  const lateIndexes = new Set(shuffle(realIndexes, random).slice(0, s.late))
  const onTime = stream.filter((_, i) => !lateIndexes.has(i))
  const late = stream.filter((_, i) => lateIndexes.has(i))

  return { run: tag, onTime, late, settings: s }
}
