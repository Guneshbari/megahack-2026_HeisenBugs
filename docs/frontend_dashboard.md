# Frontend Dashboard Design

The frontend SentinelCore interface operates on **React + Vite** wrapped inside tailwind-compatible global stylesheets, engineered with an enterprise aesthetic focused on data density.

## Technology Stack
- **Framework:** React 18 / Vite
- **Styling:** Vanilla CSS design system via `index.css` global semantic tokens.
- **Charting Engine:** `recharts` for customizable interactive Area/Bar/Line UI visualizations.
- **Identity Provider:** `Firebase Web SDK`

## Component Render Cycle
```mermaid
flowchart TD
    U[User Browser] -->|Mounts| App[App.tsx]
    App --> Auth[Firebase Web SDK]
    Auth -->|JWT Token| Ctx[DashboardContext.tsx]
    
    Ctx -->|Props & Native Callbacks| Pages
    
    subgraph Page Modules
        A(AnalyticsPage\nAggregations)
        B(EventsPage\nStreaming Data)
        C(SystemsPage\nRemote Executions)
        D(AlertsPage\nNotification Rules)
        E(MLIntelligencePage\nML Clustering)
    end
    
    Pages --> Page Modules
    Page Modules -->|Axios Interceptor| API[FastAPI Backend]
```

## Structural Components
The App isolates heavy component tracking out of the index mapping. Major functional endpoints include:

- **`DashboardContext.tsx`:** An isolated functional React hook managing core synchronization mechanisms across all children (search bars, date intervals) allowing single-fetch UI refreshes across graphs, widgets, and API hooks simultaneously.
- **`AnalyticsPage.tsx`:** Modular graph views offering fully customizable `localStorage` widgets for advanced filtering timelines tracking `/metrics` behavior over time inputs.
- **`AlertsPage.tsx`:** Centralized logic combining `/alerts/rules` and recent notification fetching into interactive modal creations triggering backend mutations.
- **`EventsPage.tsx`:** Render-safe massive list mapping handling live pause arrays to securely limit infinite scrolling memory leaks during intense terminal streams.
- **`MLIntelligencePage.tsx`:** Real-time modular SOC interface using Zustand state management to track Isolation Forest clustering and anomaly tracking.
- **`SystemsPage.tsx`:** Administrative node-control wrapping simulated POST terminal hooks back to individual Windows IP instances.

## Authentication Cycle
The Dashboard natively leverages the Firebase Web Authentication identity overlay.
When a user launches `localhost:5173`, the local instance detects an empty user token routing automatically to `/login`. Upon successful sign-up / entry, FireBase issues a Google Bearer JWT injected asynchronously into the Axios parameters via `api.ts`, which unlocks the internal dashboard layer.
