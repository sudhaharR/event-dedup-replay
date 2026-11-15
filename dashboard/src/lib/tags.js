// Labels each message of a planned stream with what is special about it (like the simulator's output):
//   POISON        breaks the contract - the service will dead-letter it
//   REPEAT        this event_id was already sent
//   OUT OF ORDER  a later version arrives before the version it depends on
//   LATE          held back and sent after a pause

import { parseValid } from './contract.js'

/** @param plan {{onTime: any[], late: any[]}} @returns rows in arrival order */
export function describe(plan) {
  const rows = []
  const seen = new Set()
  const versions = new Map()
  const contiguous = new Map()
  const all = [...plan.onTime.map((m) => [m, false]), ...plan.late.map((m) => [m, true])]

  all.forEach(([message, late], index) => {
    const tags = []
    const e = parseValid(message)
    if (!e) {
      tags.push('POISON')
    } else if (seen.has(e.event_id)) {
      tags.push('REPEAT')
    } else {
      seen.add(e.event_id)
      let done = contiguous.get(e.entity_id) ?? 0
      if (e.version > done + 1) tags.push(`OUT OF ORDER (v${done + 1} not sent yet)`)
      if (!versions.has(e.entity_id)) versions.set(e.entity_id, new Set())
      versions.get(e.entity_id).add(e.version)
      while (versions.get(e.entity_id).has(done + 1)) done++
      contiguous.set(e.entity_id, done)
    }
    if (late) tags.push('LATE')

    const obj = typeof message === 'object' && message !== null ? message : null
    rows.push({
      index: index + 1,
      eventId: obj?.event_id ?? null,
      entityId: obj?.entity_id ?? null,
      version: obj?.version ?? null,
      eventType: obj?.event_type ?? null,
      raw: obj ? null : String(message),
      tags,
    })
  })
  return rows
}

export function summarize(rows) {
  const count = (prefix) => rows.filter((r) => r.tags.some((t) => t.startsWith(prefix))).length
  return {
    total: rows.length,
    poison: count('POISON'),
    repeats: count('REPEAT'),
    outOfOrder: count('OUT OF ORDER'),
    late: count('LATE'),
  }
}
