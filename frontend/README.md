# SentinelCore Frontend

SentinelCore's frontend is a React + Vite dashboard for monitoring the full
Collector -> Kafka -> Consumer -> PostgreSQL -> Feature Builder -> API pipeline.

## Current Hardening Status

- Phase 1: dashboard correctness improvements are implemented
- Phase 2: shared event indexing and coordinated polling are implemented
- Phase 3: env-driven config, backend-aligned auth headers, and reduced raw XML
  exposure are implemented
- Phase 4: lint, docs, and release-readiness cleanup are implemented in the
  frontend workspace

## Central Dashboard Topology

Recommended production model:

1. Run the SentinelCore backend API on the central monitoring system.
2. Build and serve this frontend from the same central monitoring system, or
   point it at that API using `VITE_SENTINEL_API_BASE_URL`.
3. Let all 100+ monitored systems send telemetry to the central backend.
4. Let operators access the dashboard from one primary dashboard machine by
   default.
5. Allow additional operator machines to open the same dashboard URL when
   needed.

This means one system can act as the main SOC dashboard host, while multiple
authenticated viewers can still monitor the environment from separate systems.

## Multi-Viewer Access

The frontend is not limited to a single browser or a single workstation.
Multiple viewers can access the same dashboard deployment as long as they can:

- reach the frontend URL
- reach the configured backend API
- satisfy the frontend login flow
- provide the backend bearer token when backend auth is enabled

The data model remains centralized. Extra viewers do not create extra telemetry
pipelines; they only consume the same shared dashboard/API layer.

## Environment Variables

Copy [`.env.example`](/C:/ProgramData/SentinelCore/frontend/.env.example) to a
local `.env` or production secret store.

Required for production:

- `VITE_SENTINEL_API_BASE_URL`
- `VITE_FIREBASE_API_KEY`
- `VITE_FIREBASE_AUTH_DOMAIN`
- `VITE_FIREBASE_PROJECT_ID`
- `VITE_FIREBASE_STORAGE_BUCKET`
- `VITE_FIREBASE_MESSAGING_SENDER_ID`
- `VITE_FIREBASE_APP_ID`

Required when backend bearer auth is enabled:

- `VITE_SENTINEL_API_BEARER_TOKEN`

Optional:

- `VITE_SENTINEL_RECENT_EVENTS_LIMIT`

## Local Development

```bash
npm install
npm run build
npm run lint
```

PowerShell users may need:

```powershell
cmd /c npm run build
cmd /c npm run lint
```

## Production Notes

- Set `VITE_SENTINEL_API_BASE_URL` to the central API host or reverse-proxy path.
- Use a single central backend/API deployment for all monitored systems.
- Serve the built frontend from the central dashboard host or reverse proxy.
- Keep `VITE_SENTINEL_API_BEARER_TOKEN` aligned with
  `SENTINEL_API_BEARER_TOKEN` on the backend when API auth is enabled.
- The dashboard fetch path now omits `raw_xml` by default for safer frontend
  handling of event data.

## Validation

Recommended pre-release checks:

```bash
python -m compileall src
.venv\Scripts\python.exe -m pytest -q
cmd /c npm run lint
cmd /c npm run build
```
