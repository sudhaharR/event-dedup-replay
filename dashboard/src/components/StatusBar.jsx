import Badge from './Badge.jsx'

function Item({ label, children }) {
  return (
    <div className="status-item">
      <span className="status-label">{label}</span>
      {children}
    </div>
  )
}

export default function StatusBar({ health, metrics, lastUpdated, autoRefresh, onToggleRefresh, error }) {
  const s3 = !metrics ? null : !metrics.s3_enabled ? 'OFF'
    : metrics.s3_archive_backlog > 0 || metrics.s3_dlq_backlog > 0 ? 'BACKLOG' : 'UP'
  return (
    <header className="topbar">
      <div className="brand">
        <span className="brand-mark" aria-hidden="true" />
        <div>
          <h1>Event Dedup &amp; Replay</h1>
          <p className="subtitle">CAP-33 · live view of the consumer service</p>
        </div>
      </div>
      <div className="status">
        <Item label="Service"><Badge value={error ? 'DOWN' : health?.status ?? '…'} /></Item>
        <Item label="Database">
          <Badge value={health?.components?.db?.status ?? (error ? 'DOWN' : '…')} />
        </Item>
        <Item label="S3 log">
          {s3 === 'BACKLOG'
            ? <span className="badge badge-warn" title={metrics.s3_last_error ?? ''}>
                BACKLOG {metrics.s3_archive_backlog + metrics.s3_dlq_backlog}
              </span>
            : <Badge value={s3 ?? '…'} />}
        </Item>
        <Item label="Last replay"><Badge value={metrics?.last_replay_status ?? 'NONE'} /></Item>
        <label className="refresh">
          <input type="checkbox" checked={autoRefresh} onChange={onToggleRefresh} />
          Auto-refresh {lastUpdated ? `· ${lastUpdated}` : ''}
        </label>
      </div>
    </header>
  )
}
