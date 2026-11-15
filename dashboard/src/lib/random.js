// Seeded random numbers, so the same seed always produces the same stream.

/** Turns any text into a 32-bit number. */
function hash(text) {
  let h = 2166136261
  for (let i = 0; i < text.length; i++) {
    h ^= text.charCodeAt(i)
    h = Math.imul(h, 16777619)
  }
  return h >>> 0
}

/** mulberry32: small, fast, good enough for test data. Empty seed = random. */
export function makeRandom(seed) {
  let a = seed === undefined || seed === null || String(seed).trim() === ''
    ? Math.floor(Math.random() * 2 ** 32)
    : hash(String(seed).trim())
  return function random() {
    a = (a + 0x6d2b79f5) >>> 0
    let t = a
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

export const randomInt = (random, min, max) => min + Math.floor(random() * (max - min + 1))

export function shuffle(list, random) {
  const a = [...list]
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(random() * (i + 1))
    ;[a[i], a[j]] = [a[j], a[i]]
  }
  return a
}

/** A short random tag that keeps order ids unique across runs (independent of the seed). */
export const runTag = () => Math.random().toString(16).slice(2, 8)
