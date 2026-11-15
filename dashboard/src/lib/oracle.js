// Reference model ("oracle"): the CORRECT final state for messages arriving in a given order,
// computed independently of the Java service. Same rules as simulator/edr_sim/oracle.py:
//  - invalid messages are ignored (the service dead-letters them)
//  - the first arrival of an event_id wins; the first arrival of an (order, version) wins
//  - each order is applied version by version, only while there is no gap
//  - business rules (state machine, amount/currency match, payment id used once) stop an order

import { cents, parseValid } from './contract.js'

/** @returns {{orders: Map<string,{status,paidCents,lastVersion}>, charges: Map<string,{orderId,amountCents}>}} */
export function expectedState(arrivals) {
  const seenIds = new Set()
  const byOrder = new Map() // order -> Map(version -> event)

  for (const message of arrivals) {
    const e = parseValid(message)
    if (!e || seenIds.has(e.event_id)) continue
    seenIds.add(e.event_id)
    if (!byOrder.has(e.entity_id)) byOrder.set(e.entity_id, new Map())
    const versions = byOrder.get(e.entity_id)
    if (!versions.has(e.version)) versions.set(e.version, e)
  }

  const orders = new Map()
  const charges = new Map()
  const usedPayments = new Set()

  for (const [orderId, versions] of byOrder) {
    let state = null
    for (let v = 1; versions.has(v); v++) {
      const e = versions.get(v)
      const p = e.payload
      if (e.event_type === 'OrderCreated') {
        state = { status: 'CREATED', orderCents: cents(p.amount), currency: p.currency, paidCents: 0, lastVersion: 1 }
      } else if (e.event_type === 'PaymentCaptured') {
        if (!state || state.status !== 'CREATED' || cents(p.amount) !== state.orderCents
            || p.currency !== state.currency || usedPayments.has(p.payment_id)) break
        usedPayments.add(p.payment_id)
        state.status = 'PAID'
        state.paidCents = cents(p.amount)
        charges.set(e.event_id, { orderId, amountCents: cents(p.amount) })
      } else if (e.event_type === 'OrderShipped') {
        if (!state || state.status !== 'PAID') break
        state.status = 'SHIPPED'
      } else if (e.event_type === 'OrderCancelled') {
        if (!state || !['CREATED', 'PAID'].includes(state.status)) break
        state.status = 'CANCELLED'
      }
      state.lastVersion = v
    }
    if (state) orders.set(orderId, { status: state.status, paidCents: state.paidCents, lastVersion: state.lastVersion })
  }
  return { orders, charges }
}
