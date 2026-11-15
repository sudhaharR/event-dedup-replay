// After a stream is sent: wait until the consumer has finished, then compare the service's
// orders and payment ledger with the oracle's expected state.

import { cents, parseValid } from './contract.js'
import { expectedState } from './oracle.js'

const pause = (ms) => new Promise((r) => setTimeout(r, ms))

/** Waits until nothing is PENDING and the counters stop changing for ~1.2 s. */
export async function waitUntilIdle(api, { timeoutMs = 60_000, settleMs = 1200 } = {}) {
  const deadline = Date.now() + timeoutMs
  let last = null
  let stableSince = 0
  while (Date.now() < deadline) {
    const m = await api.metrics()
    const snapshot = `${m.pending}|${m.applied}|${m.currently_parked}|${m.dead_lettered}|${m.duplicates}`
    if (m.pending === 0 && snapshot === last) {
      if (Date.now() - stableSince >= settleMs) return m
    } else {
      last = snapshot
      stableSince = Date.now()
    }
    await pause(300)
  }
  throw new Error(`The service is still busy after ${timeoutMs / 1000} s`)
}

/**
 * Pure comparison (testable without a server).
 * Scope = orders mentioned by the valid messages that were sent.
 */
export function compare(arrivals, actualOrders, actualLedger) {
  const expected = expectedState(arrivals)
  const scope = new Set(arrivals.map(parseValid).filter(Boolean).map((e) => e.entity_id))

  const mismatches = []
  const actualById = new Map(actualOrders.filter((o) => scope.has(o.order_id)).map((o) => [o.order_id, o]))
  for (const id of [...new Set([...expected.orders.keys(), ...actualById.keys()])].sort()) {
    const want = expected.orders.get(id)
    const got = actualById.get(id)
    const w = want ? `${want.status} · paid ${(want.paidCents / 100).toFixed(2)} · v${want.lastVersion}` : 'should not exist'
    const g = got ? `${got.status} · paid ${(cents(got.amount_paid) / 100).toFixed(2)} · v${got.last_version}` : 'missing'
    const same = want && got && want.status === got.status && want.paidCents === cents(got.amount_paid)
      && want.lastVersion === Number(got.last_version)
    if (!same) mismatches.push({ orderId: id, expected: w, actual: g })
  }

  const expectedCharges = [...expected.charges.keys()]
  const actualCharges = actualLedger.filter((l) => scope.has(l.order_id)).map((l) => l.event_id)
  const missing = expectedCharges.filter((id) => !actualCharges.includes(id))
  const unexpected = actualCharges.filter((id) => !expected.charges.has(id))

  return {
    passed: mismatches.length === 0 && missing.length === 0 && unexpected.length === 0,
    ordersChecked: new Set([...expected.orders.keys(), ...actualById.keys()]).size,
    mismatches,
    chargesExpected: expectedCharges.length,
    chargesActual: actualCharges.length,
    missing,
    unexpected,
  }
}

/** Wait for the consumer, read the service's state, compare with the oracle. */
export async function checkResult(api, arrivals) {
  await waitUntilIdle(api)
  const [orders, ledger] = await Promise.all([api.orders(), api.ledger()])
  return compare(arrivals, orders, ledger)
}
