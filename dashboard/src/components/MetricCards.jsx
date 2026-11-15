import { ms, num } from '../format.js'

function Card({ label, value, hint, tone }) {
  return (
    <div className={`metric ${tone ? `metric-${tone}` : ''}`}>
      <div className="metric-value">{value}</div>
      <div className="metric-label">{label}</div>
      {hint && <div className="metric-hint">{hint}</div>}
    </div>
  )
}

export default function MetricCards({ m }) {
  if (!m) return <section className="metrics"><div className="empty">Waiting for metrics…</div></section>
  const warn = (n) => (n > 0 ? 'warn' : undefined)
  return (
    <section className="metrics" aria-label="Processing metrics">
      <Card label="Received" value={num(m.total_received)} hint={`${num(m.pending)} pending`} />
      <Card label="Applied" value={num(m.applied)} tone="good" />
      <Card label="Duplicates" value={num(m.duplicates)} tone={warn(m.duplicates)} hint="same event_id" />
      <Card label="Stale" value={num(m.stale)} tone={warn(m.stale)} hint="version already applied" />
      <Card label="Out of order" value={num(m.out_of_order)} tone={warn(m.out_of_order)}
            hint={`${num(m.currently_parked)} parked now`} />
      <Card label="Dead letters" value={num(m.dead_lettered)} tone={m.dead_lettered > 0 ? 'bad' : undefined} />
      <Card label="Charges" value={num(m.total_charges)} hint="payment ledger" />
      <Card label="Processing lag" value={ms(m.avg_processing_lag_ms)} hint="received → processed (avg)" />
      <Card label="End-to-end lag" value={ms(m.max_end_to_end_lag_ms)} hint="event time → processed (max)" />
      {m.s3_enabled && (
        <Card label="S3 archive" value={num(m.s3_uploaded ?? 0)}
              tone={m.s3_archive_backlog > 0 ? 'warn' : undefined}
              hint={`uploaded · backlog ${num(m.s3_archive_backlog)}`} />
      )}
      <Card label="Replayed" value={num(m.replayed_messages)} hint="messages re-fed by replay" />
    </section>
  )
}
