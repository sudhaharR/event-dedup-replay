import { useState } from 'react'
import { api } from '../api.js'
import sample from '../data/sample_events.json'
import { DEFAULT_SETTINGS, LIMITS, POISON_KINDS, generate } from '../lib/generator.js'
import { arrange, parseCustom } from '../lib/custom.js'
import { describe, summarize } from '../lib/tags.js'
import { checkResult } from '../lib/check.js'

const pause = (ms) => new Promise((r) => setTimeout(r, ms))
const TABS = [['sample', 'Quick sample'], ['generate', 'Generate'], ['custom', 'Your JSON']]
const CUSTOM_EXAMPLE = JSON.stringify(sample.slice(0, 3), null, 2)

function NumberField({ label, value, onChange, min = 0, max, hint }) {
  return (
    <label className="field">
      <span>{label}</span>
      <input type="number" min={min} max={max} value={value}
             onChange={(e) => onChange(e.target.value === '' ? '' : Number(e.target.value))} />
      {hint && <small>{hint}</small>}
    </label>
  )
}

function Radio({ name, value, options, onChange }) {
  return (
    <div className="radios">
      {options.map(([v, label]) => (
        <label key={v}><input type="radio" name={name} checked={value === v} onChange={() => onChange(v)} />{label}</label>
      ))}
    </div>
  )
}

function Tag({ text }) {
  const tone = text.startsWith('POISON') ? 'bad' : text.startsWith('REPEAT') ? 'warn'
    : text.startsWith('OUT OF ORDER') ? 'info' : 'muted'
  return <span className={`badge badge-${tone}`}>{text}</span>
}

export default function SendPanel({ onChanged }) {
  const [tab, setTab] = useState('sample')
  const [settings, setSettings] = useState(DEFAULT_SETTINGS)
  const [customText, setCustomText] = useState(CUSTOM_EXAMPLE)
  const [customMode, setCustomMode] = useState('json')
  const [customArrival, setCustomArrival] = useState('as-written')
  const [repeat, setRepeat] = useState('')
  const [delayMs, setDelayMs] = useState(120)
  const [resetFirst, setResetFirst] = useState(true)
  const [preview, setPreview] = useState(null)   // { plan, rows, summary, warnings }
  const [errors, setErrors] = useState([])
  const [busy, setBusy] = useState(null)
  const [result, setResult] = useState(null)

  const set = (key) => (value) => { setSettings((s) => ({ ...s, [key]: value })); setPreview(null) }
  const switchTab = (t) => {
    setTab(t)
    setPreview(null)
    setErrors([])
    setResult(null)
    setResetFirst(t !== 'generate') // generated ids are always new; sample/custom ids may already exist
  }

  /** Builds the stream for the current tab. */
  function build() {
    if (tab === 'sample') return { plan: arrange(sample), warnings: [], delay: delayMs, lateMs: 0 }
    if (tab === 'generate') {
      const plan = generate(settings)
      return { plan, warnings: [], delay: plan.settings.delayMs, lateMs: plan.settings.lateMs }
    }
    const { messages, errors: parseErrors } = parseCustom(customText, customMode)
    if (parseErrors.length) return { errors: parseErrors }
    const plan = arrange(messages, { arrival: customArrival, repeat })
    const warnings = plan.notFound.map((id) => `--repeat ${id}: no event with this event_id in your input`)
    return { plan, warnings, delay: delayMs, lateMs: 0 }
  }

  function doPreview() {
    const built = build()
    if (built.errors) {
      setErrors(built.errors)
      setPreview(null)
      return null
    }
    const rows = describe(built.plan)
    const p = { ...built, rows, summary: summarize(rows) }
    setErrors(built.warnings)
    setPreview(p)
    return p
  }

  async function doSend() {
    const p = preview ?? doPreview()
    if (!p) return
    setResult(null)
    try {
      if (resetFirst) {
        setBusy('Resetting data')
        await api.reset()
      }
      const all = [...p.plan.onTime, ...p.plan.late]
      for (let i = 0; i < p.plan.onTime.length; i++) {
        setBusy(`Sending ${i + 1}/${all.length}`)
        await api.send(p.plan.onTime[i])
        onChanged()
        await pause(p.delay)
      }
      if (p.plan.late.length) {
        setBusy(`Holding ${p.plan.late.length} late event(s) for ${p.lateMs} ms`)
        await pause(p.lateMs)
        for (let i = 0; i < p.plan.late.length; i++) {
          setBusy(`Sending ${p.plan.onTime.length + i + 1}/${all.length} (late)`)
          await api.send(p.plan.late[i])
          await pause(p.delay)
        }
      }
      setBusy('Waiting for the consumer, then checking the result')
      setResult(await checkResult(api, all))
    } catch (e) {
      setResult({ error: e.message })
    } finally {
      setBusy(null)
      setPreview(null) // generated data is new next time
      onChanged()
    }
  }

  const loadFile = (e) => {
    const file = e.target.files?.[0]
    if (!file) return
    file.text().then((t) => { setCustomText(t); setPreview(null) })
    e.target.value = ''
  }

  const toggleKind = (kind) => set('poisonKinds')(
    settings.poisonKinds.includes(kind) ? settings.poisonKinds.filter((k) => k !== kind) : [...settings.poisonKinds, kind])

  return (
    <section className="panel send">
      <div className="panel-head"><h2>Send events</h2>{busy && <span className="busy">{busy}…</span>}</div>
      <div className="tabs" role="tablist">
        {TABS.map(([id, label]) => (
          <button key={id} role="tab" aria-selected={tab === id} className={tab === id ? 'tab active' : 'tab'}
                  onClick={() => switchTab(id)} disabled={!!busy}>{label}</button>
        ))}
      </div>

      {tab === 'sample' && (
        <p className="panel-note">The 8 events from <code>sample_events.json</code> in file order: a duplicate payment
          (E102 twice), an out-of-order shipment (E203 before E202) and a poison event (E300).</p>
      )}

      {tab === 'generate' && (
        <div className="form">
          <div className="grid">
            <NumberField label="Orders" value={settings.orders} onChange={set('orders')} min={1} max={LIMITS.orders} />
            <NumberField label="Duplicates" value={settings.duplicates} onChange={set('duplicates')} max={LIMITS.duplicates}
                         hint="random events re-sent" />
            <NumberField label="Poison messages" value={settings.poison} onChange={set('poison')} max={LIMITS.poison} />
            <NumberField label="Late events" value={settings.late} onChange={set('late')} max={LIMITS.late}
                         hint="held back, sent last" />
            <NumberField label="Late pause (ms)" value={settings.lateMs} onChange={set('lateMs')} max={LIMITS.lateMs} />
            <NumberField label="Delay between (ms)" value={settings.delayMs} onChange={set('delayMs')} max={LIMITS.delayMs} />
          </div>
          <div className="row">
            <span className="row-label">Poison kinds</span>
            <div className="checks">
              {Object.entries(POISON_KINDS).map(([k, label]) => (
                <label key={k}><input type="checkbox" checked={settings.poisonKinds.includes(k)}
                                      onChange={() => toggleKind(k)} />{label}</label>
              ))}
            </div>
          </div>
          <div className="row">
            <span className="row-label">Arrival order</span>
            <Radio name="arrival" value={settings.arrival} onChange={set('arrival')}
                   options={[['in-order', 'In order'], ['shuffled', 'Shuffled'], ['reversed', 'Reversed (newest first)']]} />
          </div>
          <div className="row">
            <span className="row-label">Order endings %</span>
            <div className="grid three">
              <NumberField label="Shipped" value={settings.shipped} onChange={set('shipped')} max={100} />
              <NumberField label="Paid only" value={settings.paid} onChange={set('paid')} max={100} />
              <NumberField label="Cancelled" value={settings.cancelled} onChange={set('cancelled')} max={100} />
            </div>
          </div>
          <label className="field seed">
            <span>Seed</span>
            <input type="text" placeholder="empty = random" value={settings.seed}
                   onChange={(e) => set('seed')(e.target.value)} />
            <small>same seed = same pattern (order ids are always new)</small>
          </label>
        </div>
      )}

      {tab === 'custom' && (
        <div className="form">
          <div className="row">
            <span className="row-label">Format</span>
            <Radio name="mode" value={customMode} onChange={(v) => { setCustomMode(v); setPreview(null) }}
                   options={[['json', 'JSON array'], ['lines', 'One message per line (allows broken JSON)']]} />
          </div>
          <textarea value={customText} spellCheck={false} rows={9}
                    onChange={(e) => { setCustomText(e.target.value); setPreview(null) }} />
          <div className="row">
            <label className="file"><input type="file" accept=".json,.txt,application/json" onChange={loadFile} />
              Upload a .json file</label>
            <button className="link" onClick={() => { setCustomText(JSON.stringify(sample, null, 2)); setCustomMode('json'); setPreview(null) }}>
              Load the 8 sample events</button>
          </div>
          <div className="row">
            <span className="row-label">Arrival order</span>
            <Radio name="carrival" value={customArrival} onChange={(v) => { setCustomArrival(v); setPreview(null) }}
                   options={[['as-written', 'As written'], ['shuffled', 'Shuffled'], ['reversed', 'Reversed']]} />
          </div>
          <div className="grid">
            <label className="field">
              <span>Repeat event_id(s)</span>
              <input type="text" placeholder="e.g. E102, E202" value={repeat}
                     onChange={(e) => { setRepeat(e.target.value); setPreview(null) }} />
            </label>
            <NumberField label="Delay between (ms)" value={delayMs} onChange={setDelayMs} max={LIMITS.delayMs} />
          </div>
        </div>
      )}

      {tab === 'sample' && (
        <div className="grid">
          <NumberField label="Delay between (ms)" value={delayMs} onChange={setDelayMs} max={LIMITS.delayMs} />
        </div>
      )}

      <label className="reset-first">
        <input type="checkbox" checked={resetFirst} onChange={(e) => setResetFirst(e.target.checked)} />
        Reset data before sending {tab === 'generate' ? '(optional: generated order ids are always new)'
          : '(recommended: the same ids may already exist)'}
      </label>

      <div className="buttons">
        <button onClick={doPreview} disabled={!!busy}>Preview</button>
        <button className="primary" onClick={doSend} disabled={!!busy}>Send &amp; check</button>
      </div>

      {errors.length > 0 && <ul className="errors">{errors.map((e) => <li key={e}>{e}</li>)}</ul>}

      {preview && (
        <div className="preview">
          <p className="summary">
            {preview.summary.total} messages · {preview.summary.outOfOrder} out of order · {preview.summary.repeats} repeats
            · {preview.summary.poison} poison · {preview.summary.late} late
          </p>
          <div className="table-wrap preview-table">
            <table>
              <thead><tr><th className="num">#</th><th>Event</th><th>Order</th><th className="num">Ver</th><th>Type</th><th>Note</th></tr></thead>
              <tbody>
                {preview.rows.map((r) => (
                  <tr key={r.index}>
                    <td className="num muted">{r.index}</td>
                    <td className="mono">{r.raw !== null ? <span className="muted">(raw) {r.raw.slice(0, 30)}</span> : (r.eventId ?? '–')}</td>
                    <td className="mono">{r.entityId ?? '–'}</td>
                    <td className="num">{r.version ?? '–'}</td>
                    <td>{r.eventType ?? '–'}</td>
                    <td>{r.tags.map((t) => <Tag key={t} text={t} />)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {result && (
        result.error ? <div className="report report-error"><h3>Result check</h3><p>{result.error}</p></div> : (
          <div className={`report ${result.passed ? 'report-pass' : 'report-fail'}`}>
            <h3>Result check <span className={`badge badge-${result.passed ? 'good' : 'bad'}`}>{result.passed ? 'PASS' : 'FAIL'}</span></h3>
            <div className="kv"><span>Orders as expected</span>
              <span>{result.ordersChecked - result.mismatches.length} / {result.ordersChecked}</span></div>
            <div className="kv"><span>Charges (expected / in ledger)</span>
              <span>{result.chargesExpected} / {result.chargesActual}</span></div>
            {result.mismatches.length > 0 && (
              <ul className="diffs">{result.mismatches.map((m) => (
                <li key={m.orderId}>{m.orderId}: expected {m.expected}, got {m.actual}</li>))}</ul>
            )}
            {result.missing.length > 0 && <p className="bad-text small">Missing charges: {result.missing.join(', ')}</p>}
            {result.unexpected.length > 0 && <p className="bad-text small">Unexpected charges: {result.unexpected.join(', ')}</p>}
            <p className="small muted">Expected state computed in the browser by an independent reference model (oracle).</p>
          </div>
        )
      )}
    </section>
  )
}
