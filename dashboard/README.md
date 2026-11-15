# Dashboard (React + Vite)

Live view of the consumer service: processing metrics, orders, the latest inbox decisions, dead letters,
the payment ledger, S3 archive health - plus buttons to send events, replay from S3 and reconcile in Snowflake.

## Run (development)

```powershell
cd dashboard
npm install        # once
npm run dev        # http://localhost:5173
```

The consumer service must be running on port 8080. The dashboard calls `/api/...`, and Vite forwards it to
`http://localhost:8080` (change with the `API_TARGET` environment variable).

## Build

```powershell
npm run build      # static files in dist/ (served by nginx in Docker, Stage 11)
npm run preview    # serve the build on http://localhost:4173 with the same /api proxy
```
