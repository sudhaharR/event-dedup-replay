// Parses the user's own input into messages, and arranges them (order, repeats).
//   mode 'json'  - a JSON array of events (or a single event object)
//   mode 'lines' - one message per line; a line that is not a JSON object is sent as raw text,
//                  so deliberately broken JSON (poison) can be included

import { makeRandom, randomInt, shuffle } from './random.js'

export const MAX_MESSAGES = 1000

/** @returns {{messages: any[], errors: string[]}} */
export function parseCustom(text, mode) {
  const errors = []
  const trimmed = (text ?? '').trim()
  if (!trimmed) return { messages: [], errors: ['Nothing to send - paste or upload some events.'] }

  let messages = []
  if (mode === 'lines') {
    trimmed.split(/\r?\n/).forEach((line) => {
      const l = line.trim()
      if (!l) return
      try {
        const v = JSON.parse(l)
        messages.push(v !== null && typeof v === 'object' && !Array.isArray(v) ? v : l)
      } catch {
        messages.push(l) // raw text - e.g. broken JSON on purpose
      }
    })
  } else {
    try {
      const v = JSON.parse(trimmed)
      if (Array.isArray(v)) messages = v
      else if (v !== null && typeof v === 'object') messages = [v]
      else errors.push('Expected a JSON array of events, or a single event object.')
    } catch (e) {
      errors.push(`Not valid JSON: ${e.message}. Tip: use "One message per line" to include broken messages on purpose.`)
    }
  }
  if (messages.length > MAX_MESSAGES) {
    errors.push(`Too many messages (${messages.length}); the limit is ${MAX_MESSAGES}.`)
  }
  return { messages, errors }
}

/**
 * @param options {{arrival: 'as-written'|'shuffled'|'reversed', repeat: string, seed: string}}
 *   repeat: comma-separated event ids to send again (each copy goes later than its first copy)
 * @returns {{onTime: any[], late: any[], notFound: string[]}}
 */
export function arrange(messages, { arrival = 'as-written', repeat = '', seed = '' } = {}) {
  const random = makeRandom(seed)
  let stream = arrival === 'shuffled' ? shuffle(messages, random)
    : arrival === 'reversed' ? [...messages].reverse() : [...messages]

  const notFound = []
  repeat.split(',').map((s) => s.trim()).filter(Boolean).forEach((eventId) => {
    const original = stream.find((m) => typeof m === 'object' && m?.event_id === eventId)
    if (!original) {
      notFound.push(eventId)
      return
    }
    const first = stream.indexOf(original)
    stream.splice(randomInt(random, first + 1, stream.length), 0, structuredClone(original))
  })
  return { onTime: stream, late: [], notFound }
}
