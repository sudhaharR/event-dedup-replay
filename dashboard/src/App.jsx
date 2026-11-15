import { useCallback, useEffect, useState } from 'react'
import { api } from './api.js'
import StatusBar from './components/StatusBar.jsx'
import MetricCards from './components/MetricCards.jsx'
import SendPanel from './components/SendPanel.jsx'
import Operations from './components/Operations.jsx'
import { DeadLetterTable, InboxTable, LedgerTable, OrdersTable } from './components/Tables.jsx'

const REFRESH_MS = 2000

export default function App() {
  const [data, setData] = useState({})
  const [error, setError] = useState(null)
  const [lastUpdated, setLastUpdated] = useState(null)
  const [autoRefresh, setAutoRefresh] = useState(true)

  const load = useCallback(async () => {
    const [health, metrics, orders, inbox, deadLetters, ledger] = await Promise.allSettled([
      api.health(), api.metrics(), api.orders(), api.inbox(25), api.deadLetters(20), api.ledger(),
    ])
    const value = (r) => (r.status === 'fulfilled' ? r.value : undefined)
    setData({
      health: value(health), metrics: value(metrics), orders: value(orders),
      inbox: value(inbox), deadLetters: value(deadLetters), ledger: value(ledger),
    })
    setError(metrics.status === 'rejected' ? metrics.reason.message : null)
    setLastUpdated(new Date().toLocaleTimeString())
  }, [])

  useEffect(() => {
    load()
    if (!autoRefresh) return undefined
    const timer = setInterval(load, REFRESH_MS)
    return () => clearInterval(timer)
  }, [autoRefresh, load])

  return (
    <div className="app">
      <StatusBar health={data.health} metrics={data.metrics} lastUpdated={lastUpdated} error={error}
                 autoRefresh={autoRefresh} onToggleRefresh={() => setAutoRefresh((a) => !a)} />
      {error && <div className="banner">{error}</div>}
      <main>
        <MetricCards m={data.metrics} />
        <div className="columns">
          <div className="col-main">
            <OrdersTable orders={data.orders} />
            <InboxTable rows={data.inbox} />
          </div>
          <div className="col-side">
            <SendPanel onChanged={load} />
            <Operations onChanged={load} />
            <DeadLetterTable rows={data.deadLetters} />
            <LedgerTable rows={data.ledger} />
          </div>
        </div>
      </main>
      <footer>Spring Boot · MySQL · Amazon S3 · Snowflake · React</footer>
    </div>
  )
}
