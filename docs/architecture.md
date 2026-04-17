# Architectural Overview

SentinelCore operates as a highly specialized telemetry sink designed to scale across networks while heavily clamping down on unauthorized ingestion vectors.

## Top-Level Data Flow

```mermaid
flowchart TD
    subgraph Edge[Windows Endpoints]
        C[collector.py\n(Win32/Python)]
    end

    subgraph Streaming[Broker Layer]
        K[Kafka Cluster]
    end

    subgraph Core[Linux Backend Server]
        KP[kafka_to_postgres.py\n(Consumer)]
        FB[feature_builder.py\n(Aggregator)]
        ML[ml_engine.py\n(Predictive Analytics)]
        DB[(PostgreSQL)]
        API[api_server.py\n(FastAPI)]
    end

    subgraph UI[Web Client]
        DASH[React Dashboard]
        FB_AUTH[Firebase Auth]
    end

    %% Edge
    C -- "JSON Payload (PSK)" --> K
    
    %% Ingestion
    K -- "Consumes" --> KP
    KP -- "Idempotent Writes" --> DB
    
    %% Workers
    DB -- "Reads Events" --> FB
    FB -- "Writes Snapshots" --> DB
    DB -- "Reads Snapshots" --> ML
    ML -- "Writes Predictions" --> DB
    
    %% API
    DASH -- "Bearer Token auth" --> API
    API -- "Queries" --> DB
    
    %% Auth
    DASH -- "Logins" --> FB_AUTH
    FB_AUTH -. "Validates" .-> API
```

## 1. Edge Collectors
Client endpoints execute the Python-based `collector.py`, tracking WMI performance queries and Windows Event Viewer logs. They append local secret-based PSK headers and push `JSON` arrays upstream to Kafka. SentinelCore employs a **strict drop** policy natively at the Kafka ingest for unauthorized PSKs.

## 2. Ingestion & Persistence
`kafka_to_postgres.py` reads JSON topics asynchronously. All database commits are heavily wrapped in isolated connection pools and `CircuitBreaker` classes. Duplicate hashes hit database unicity constraints, ensuring exactly-once ingestion scaling.

## 3. Worker Architecture
SentinelCore avoids executing computational heavy-lifting while consuming HTTP requests. Instead, standard `events` are parsed by background loops (`feature_builder.py` and `ml_engine.py`) which produce statistical subsets named `feature_snapshots` and `ml_predictions`. These components rely on a modular configuration layer (`src/shared/`) using distinct domain constants (e.g. `ml_constants.py`, `kafka_constants.py`) to reduce tight coupling.

## 4. Analytical Dashboard
The FastAPI Backend sits behind Google Firebase Identity verification logic. Upon successful login, the React Dashboard renders statistical subsets in customizable `recharts` graphs using the read-optimized feature pools heavily mitigating database CPU stress.
