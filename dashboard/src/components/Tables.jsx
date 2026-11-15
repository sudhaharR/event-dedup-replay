import Badge from './Badge.jsx'
import { money, time } from '../format.js'

function Panel({ title, count, children, note }) {
  return (
    <section className="panel">
      <div className="panel-head">
        <h2>{title}</h2>
        {count !== undefined && <span className="count">{count}</span>}
      </div>
      {note && <p className="panel-note">{note}</p>}
      <div className="table-wrap">{children}</div>
    </section>
  )
}

function Empty({ cols, text }) {
  return <tr><td colSpan={cols} className="empty">{text}</td></tr>
}

export function OrdersTable({ orders }) {
  return (
    <Panel title="Orders (projection)" count={orders?.length ?? 0}
           note="Current state of each order, built only from applied events.">
      <table>
        <thead>
          <tr><th>Order</th><th>Status</th><th className="num">Amount</th><th className="num">Paid</th>
            <th className="num">Ver</th><th>Last event</th><th>Updated</th></tr>
        </thead>
        <tbody>
          {!orders?.length && <Empty cols={7} text="No orders yet – send some events." />}
          {orders?.map((o) => (
            <tr key={o.order_id}>
              <td className="mono">{o.order_id}</td>
              <td><Badge value={o.status} /></td>
              <td className="num">{money(o.order_amount)}</td>
              <td className="num">{money(o.amount_paid)}</td>
              <td className="num">v{o.last_version}</td>
              <td className="mono">{o.last_event_id}</td>
              <td>{time(o.updated_at)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </Panel>
  )
}

export function InboxTable({ rows }) {
  return (
    <Panel title="Latest decisions (inbox)" count={rows?.length ?? 0}
           note="Newest first. Every received message and what the consumer decided.">
      <table>
        <thead>
          <tr><th className="num">#</th><th>Event</th><th>Order</th><th className="num">Ver</th>
            <th>Source</th><th>Decision</th><th>Reason</th></tr>
        </thead>
        <tbody>
          {!rows?.length && <Empty cols={7} text="Inbox is empty." />}
          {rows?.map((r) => (
            <tr key={r.id}>
              <td className="num muted">{r.id}</td>
              <td className="mono">{r.event_id ?? <span className="muted">(unreadable)</span>}</td>
              <td className="mono">{r.entity_id ?? '–'}</td>
              <td className="num">{r.version ?? '–'}</td>
              <td><Badge value={r.source} /></td>
              <td>
                <Badge value={r.status} />
                {r.was_out_of_order ? <span className="tag" title="was parked">out of order</span> : null}
              </td>
              <td className="reason" title={r.last_error ?? ''}>{r.last_error ?? ''}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </Panel>
  )
}

export function DeadLetterTable({ rows }) {
  return (
    <Panel title="Dead letters" count={rows?.length ?? 0} note="Rejected (poison) messages, isolated with the reason.">
      <table>
        <thead><tr><th>Event</th><th>Error code</th><th>Message</th></tr></thead>
        <tbody>
          {!rows?.length && <Empty cols={3} text="No dead letters." />}
          {rows?.map((d) => (
            <tr key={d.id}>
              <td className="mono">{d.event_id ?? <span className="muted">(unreadable)</span>}</td>
              <td><span className="badge badge-bad">{d.error_code}</span></td>
              <td className="reason" title={d.error_message}>{d.error_message}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </Panel>
  )
}

export function LedgerTable({ rows }) {
  return (
    <Panel title="Payment ledger (side effects)" count={rows?.length ?? 0}
           note="One row per real charge. Duplicates and replays never add a row.">
      <table>
        <thead><tr><th>Event</th><th>Order</th><th>Payment</th><th className="num">Amount</th></tr></thead>
        <tbody>
          {!rows?.length && <Empty cols={4} text="No charges yet." />}
          {rows?.map((l) => (
            <tr key={l.id}>
              <td className="mono">{l.event_id}</td>
              <td className="mono">{l.order_id}</td>
              <td className="mono">{l.payment_id}</td>
              <td className="num">{money(l.amount)} {l.currency}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </Panel>
  )
}
