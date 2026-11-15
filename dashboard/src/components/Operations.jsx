import { useState } from 'react'
import { api } from '../api.js'
import { ReconcileReport, ReplayReport } from './Reports.jsx'

export default function Operations({ onChanged }) {
  const [busy, setBusy] = useState(null)
  const [log, setLog] = useState([])
  const [replay, setReplay] = useState(null)
  const [recon, setRecon] = useState(null)

  const note = (text, kind = 'info') =>
    setLog((l) => [{ at: new Date().toLocaleTimeString(), text, kind }, ...l].slice(0, 6))

  async function run(name, fn) {
    setBusy(name)
    try {
      await fn()
    } catch (e) {
      note(`${name}: ${e.message}`, 'bad')
    } finally {
      setBusy(null)
      onChanged()
    }
  }

  const doReplay = () => run('Replay', async () => {
    try {
      const r = await api.replay()
      setReplay(r)
      note(`Replay #${r.run_id}: ${r.reconciliation}, new charges ${r.new_charges}`, r.new_charges === 0 ? 'good' : 'bad')
    } catch (e) {
      setReplay({ error: e.message })
      throw e
    }
  })
  const doReconcile = () => run('Reconcile', async () => {
    try {
      const r = await api.reconcile()
      setRecon(r)
      note(`Reconcile: state ${r.summary?.overall}, charges ${r.charges?.result}`,
        r.summary?.overall === 'MATCH' && r.charges?.result === 'MATCH' ? 'good' : 'bad')
    } catch (e) {
      setRecon({ error: e.message })
      throw e
    }
  })
  const reset = () => {
    if (!window.confirm('Empty all processing tables (inbox, orders, ledger, dead letters)? S3 is not touched.')) return
    run('Reset', async () => {
      await api.reset()
      setReplay(null)
      setRecon(null)
      note('Reset: all processing tables emptied', 'warn')
    })
  }

  return (
    <section className="panel">
      <div className="panel-head"><h2>Operations</h2>{busy && <span className="busy">{busy}…</span>}</div>
      <div className="buttons">
        <button className="primary" onClick={doReplay} disabled={!!busy}>Replay from S3</button>
        <button className="primary" onClick={doReconcile} disabled={!!busy}>Reconcile (Snowflake)</button>
        <button className="danger" onClick={reset} disabled={!!busy}>Reset data</button>
      </div>
      <p className="panel-note">Replay needs S3 enabled and a quiet system (wait for “S3 log: UP”). Reconcile checks
        the latest replay in Snowflake.</p>
      {log.length > 0 && (
        <ul className="log">{log.map((l, i) => <li key={i} className={`log-${l.kind}`}><span>{l.at}</span>{l.text}</li>)}</ul>
      )}
      <ReplayReport report={replay} />
      <ReconcileReport result={recon} />
    </section>
  )
}
