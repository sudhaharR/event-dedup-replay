// Unit tests for the dashboard's stream logic - run with: npm test  (Node's built-in test runner)
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'

import { parseValid } from './contract.js'
import { expectedState } from './oracle.js'
import { generate } from './generator.js'
import { arrange, parseCustom } from './custom.js'
import { describe, summarize } from './tags.js'
import { compare } from './check.js'
import { makeRandom, shuffle } from './random.js'

const sample = JSON.parse(readFileSync(new URL('../data/sample_events.json', import.meta.url), 'utf8'))

test('oracle: sample dataset ends SHIPPED/SHIPPED with two charges', () => {
  const e = expectedState(sample)
  assert.deepEqual(e.orders.get('ORD-1'), { status: 'SHIPPED', paidCents: 50000, lastVersion: 3 })
  assert.deepEqual(e.orders.get('ORD-2'), { status: 'SHIPPED', paidCents: 30000, lastVersion: 3 })
  assert.deepEqual([...e.charges.keys()].sort(), ['E102', 'E202'])
})

test('oracle: result does not depend on arrival order or duplicates', () => {
  const plan = generate({ orders: 20, duplicates: 10, poison: 4, late: 3, seed: 'x', arrival: 'in-order' })
  const all = [...plan.onTime, ...plan.late]
  const reference = expectedState(all)
  for (let s = 0; s < 25; s++) {
    const messy = shuffle(all, makeRandom(`s${s}`))
    assert.deepEqual(expectedState(messy), reference)
  }
})

test('contract: every poison kind is invalid, generated events are valid', () => {
  const plan = generate({ orders: 3, duplicates: 0, poison: 4, late: 0 })
  const poison = plan.onTime.filter((m) => parseValid(m) === null)
  assert.equal(poison.length, 4)
  assert.equal(plan.onTime.length - 4, plan.onTime.filter((m) => parseValid(m)).length)
})

test('generator: counts follow the settings', () => {
  const plan = generate({ orders: 10, duplicates: 6, poison: 3, late: 2 })
  const rows = describe(plan)
  const s = summarize(rows)
  assert.equal(s.repeats, 6)
  assert.equal(s.poison, 3)
  assert.equal(s.late, 2)
  assert.equal(plan.late.length, 2)
})

test('generator: the same seed gives the same pattern with new order ids', () => {
  const pattern = (p) => [...p.onTime, ...p.late]
    .map((m) => (typeof m === 'string' ? 'RAW' : `${m.event_type}:${m.version}:${String(m.entity_id ?? '').split('-').pop()}`))
  const a = generate({ orders: 8, duplicates: 4, poison: 2, late: 1, seed: '42' })
  const b = generate({ orders: 8, duplicates: 4, poison: 2, late: 1, seed: '42' })
  assert.deepEqual(pattern(a), pattern(b))
  assert.notEqual(a.run, b.run)
})

test('generator: in-order has no out-of-order events, reversed does', () => {
  const inOrder = summarize(describe(generate({ orders: 6, duplicates: 0, poison: 0, late: 0, arrival: 'in-order' })))
  const reversed = summarize(describe(generate({ orders: 6, duplicates: 0, poison: 0, late: 0, arrival: 'reversed',
    shipped: 100, paid: 0, cancelled: 0 })))
  assert.equal(inOrder.outOfOrder, 0)
  assert.ok(reversed.outOfOrder >= 6)
})

test('custom: JSON array, one-per-line with broken JSON, and errors', () => {
  assert.equal(parseCustom(JSON.stringify(sample), 'json').messages.length, 8)
  const lines = parseCustom(`${JSON.stringify(sample[0])}\n{"event_id": "BROKEN", \n\n${JSON.stringify(sample[1])}`, 'lines')
  assert.equal(lines.messages.length, 3)
  assert.equal(typeof lines.messages[1], 'string')
  assert.ok(parseCustom('{ not json', 'json').errors.length > 0)
  assert.ok(parseCustom('', 'json').errors.length > 0)
})

test('custom: repeat inserts a later copy, unknown ids are reported', () => {
  const plan = arrange(sample, { repeat: 'E101, NOPE', seed: '1' })
  assert.equal(plan.onTime.length, 9)
  assert.deepEqual(plan.notFound, ['NOPE'])
  const firstTwo = plan.onTime.map((m, i) => (m.event_id === 'E101' ? i : -1)).filter((i) => i >= 0)
  assert.equal(firstTwo.length, 2)
})

test('tags: sample shows the repeat, the out-of-order event and the poison event', () => {
  const rows = describe(arrange(sample))
  assert.deepEqual(rows[3].tags, ['REPEAT'])                         // second E102
  assert.ok(rows[4].tags[0].startsWith('OUT OF ORDER'))              // E203 before E202
  assert.deepEqual(rows[5].tags, ['POISON'])                         // E300 without entity_id
})

test('check: matching state passes, a missing charge fails', () => {
  const orders = [
    { order_id: 'ORD-1', status: 'SHIPPED', amount_paid: 500, last_version: 3 },
    { order_id: 'ORD-2', status: 'SHIPPED', amount_paid: '300.00', last_version: 3 },
    { order_id: 'OTHER', status: 'PAID', amount_paid: 1, last_version: 2 },     // outside the scope - ignored
  ]
  const ledger = [{ event_id: 'E102', order_id: 'ORD-1' }, { event_id: 'E202', order_id: 'ORD-2' }]
  assert.equal(compare(sample, orders, ledger).passed, true)

  const r = compare(sample, orders, ledger.slice(0, 1))
  assert.equal(r.passed, false)
  assert.deepEqual(r.missing, ['E202'])

  const wrong = compare(sample, [{ ...orders[0], status: 'PAID' }, orders[1]], ledger)
  assert.equal(wrong.passed, false)
  assert.equal(wrong.mismatches[0].orderId, 'ORD-1')
})
