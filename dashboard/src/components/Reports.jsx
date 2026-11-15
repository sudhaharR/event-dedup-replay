import Badge from './Badge.jsx'

function Row({ label, children }) {
  return <div className="kv"><span>{label}</span><span>{children}</span></div>
}

export function ReplayReport({ report }) {
  if (!report) return null
  if (report.error) {
    return <div className="report report-error"><h3>Replay</h3><p>{report.error}</p></div>
  }
  return (
    <div className="report">
      <h3>Replay #{report.run_id} <Badge value={report.reconciliation} /></h3>
      <Row label="Events read from S3">{report.events_read}</Row>
      <Row label="Applied / duplicates / dead-lettered">
        {report.applied} / {report.duplicates} / {report.dead_lettered}
      </Row>
      <Row label="Orders before → after">{report.orders_before} → {report.orders_after}</Row>
      <Row label="Charges before → after">{report.ledger_before} → {report.ledger_after}</Row>
      <Row label="New charges">
        <span className={report.new_charges === 0 ? 'good-text' : 'bad-text'}>{report.new_charges}</span>
      </Row>
      <Row label="Side effects skipped">{report.side_effects_skipped}</Row>
      {report.differences?.length > 0 && (
        <ul className="diffs">{report.differences.map((d) => <li key={d}>{d}</li>)}</ul>
      )}
      <p className="small muted mono">{report.snapshot_s3_key}</p>
    </div>
  )
}

export function ReconcileReport({ result }) {
  if (!result) return null
  if (result.error) {
    return <div className="report report-error"><h3>Reconciliation (Snowflake)</h3><p>{result.error}</p></div>
  }
  const s = result.summary ?? {}
  const c = result.charges ?? {}
  return (
    <div className="report">
      <h3>Reconciliation (Snowflake) <Badge value={s.overall} /></h3>
      <Row label="Orders compared / matched">{s.orders_compared} / {s.matched}</Row>
      <Row label="Charges"><Badge value={c.result} /> expected {c.expected_charges}, in ledger {c.ledger_charges}</Row>
      {c.missing_charges?.length > 0 && <Row label="Missing charges">{c.missing_charges.join(', ')}</Row>}
      {c.unexpected_charges?.length > 0 && <Row label="Unexpected charges">{c.unexpected_charges.join(', ')}</Row>}
      {result.differences?.length > 0 && (
        <ul className="diffs">
          {result.differences.map((d) => (
            <li key={d.order_id}>{d.order_id}: {d.result} (expected {d.expected_status ?? '–'}, snapshot {d.snapshot_status ?? '–'})</li>
          ))}
        </ul>
      )}
      {result.daily_metrics?.length > 0 && (
        <table className="mini">
          <thead><tr><th>Day</th><th className="num">Messages</th><th className="num">Invalid</th><th className="num">Dup %</th></tr></thead>
          <tbody>
            {result.daily_metrics.map((d) => (
              <tr key={d.day}><td>{d.day}</td><td className="num">{d.messages}</td>
                <td className="num">{d.invalid_messages}</td><td className="num">{d.duplicate_rate_pct}</td></tr>
            ))}
          </tbody>
        </table>
      )}
      <p className="small muted mono">{s.snapshot_key}</p>
    </div>
  )
}
