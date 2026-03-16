# SentinelCore

A production-grade **Windows Telemetry Agent** built for System Stability monitoring, Critical Fault Detection, and ML Training Data Generation. It uses the modern Windows Eventing API (`EvtQuery`) to collect high-value system events, classifies them, and streams them through a **Kafka → PostgreSQL → React Dashboard** pipeline with Prometheus + Grafana observability.

---

## Architecture & Data Flow

```
┌─────────────────────────────────────┐
│         Windows Host                │
│  Windows Event Logs                 │
│       ↓ (EvtQuery)                  │
│  collector.py                       │
│  (classify, hash, deduplicate)      │
└──────────────┬──────────────────────┘
               │ Kafka Producer (port 9092)
               ▼
┌─────────────────────────────────────┐
│         WSL / Linux Server          │
│                                     │
│  kafka_to_postgres.py               │
│       ↓ (psycopg2)                  │
│  PostgreSQL  (port 5432)            │
│       ↑                             │
│  api_server.py  ←─── (port 8080)   │──→  React Dashboard (port 5173)
│  /metrics-export ↓                  │
│  Prometheus      (port 9090)        │
│       ↓                             │
│  Grafana         (port 3000)        │
└─────────────────────────────────────┘
```

---

## Project Structure

```text
megahack-2026_HeisenBugs/
├── src/
│   ├── collector.py            # Windows agent — collects events, publishes to Kafka
│   ├── kafka_to_postgres.py    # Kafka consumer → PostgreSQL writer (runs in WSL)
│   ├── api_server.py           # FastAPI backend serving the dashboard (runs in WSL)
│   ├── analyzer.py             # Offline event analyzer / report generator
│   └── shared_constants.py     # Shared constants (LEVEL_NAMES, DB_CONFIG, thresholds)
├── frontend/                   # React + Vite + Tailwind dashboard
├── monitoring/
│   ├── prometheus.yml          # Prometheus scrape config
│   └── sentinel_dashboard.json # Grafana dashboard JSON (import this)
├── tests/
│   ├── test_e2e.py
│   ├── test_live_errors.py
│   └── validate_collector.py
├── config.json                 # Kafka + agent runtime config
├── requirements.txt            # Python dependencies
└── deploy_startup.ps1          # Registers collector as a Windows Scheduled Task
```

---

## ══════════════════════════════════════
## FIRST-TIME SETUP GUIDE
## ══════════════════════════════════════

> **Prerequisites:** Windows 10/11, WSL2 with Ubuntu, Python 3.9+, Node.js 18+, Java 17+ (for Kafka)

---

### PHASE 0 — Verify Prerequisites (Windows PowerShell)

Open a **PowerShell** window (no admin required for this step) and verify:

```powershell
python --version    # Must be 3.9+
node --version      # Must be 18+
java --version      # Must be 17+ (required by Kafka)
wsl --version       # Must show WSL 2
```

If any are missing:
- **Python**: https://python.org/downloads
- **Node.js**: https://nodejs.org
- **Java 17+**: https://adoptium.net
- **WSL2**: `wsl --install` in PowerShell as Administrator, then restart

---

### PHASE 1 — Install & Start Kafka 4.0.0 in WSL

**Open a WSL (Ubuntu) terminal.** This will be **WSL Terminal 1 — Kafka**.

```bash
# Download Kafka 4.0.0 with KRaft mode (no Zookeeper required)
cd ~
wget https://downloads.apache.org/kafka/4.0.0/kafka_2.13-4.0.0.tgz
tar -xzf kafka_2.13-4.0.0.tgz
mv kafka_2.13-4.0.0 kafka

# Move into the Kafka directory
cd ~/kafka

# Generate a cluster UUID (required for KRaft mode)
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"

# Format the storage directory with the KRaft config
bin/kafka-storage.sh format -t "$KAFKA_CLUSTER_ID" -c config/kraft/server.properties

# Start Kafka (keep this terminal open and running)
bin/kafka-server-start.sh config/kraft/server.properties
```

> ✅ Kafka is now listening on **port 9092**. Keep this terminal open.

---

### PHASE 2 — Create the Kafka Topic

**Open a NEW WSL terminal** (WSL Terminal 2). Keep Terminal 1 running.

```bash
cd ~/kafka

# Create the sentinel-events topic
bin/kafka-topics.sh --create \
  --topic sentinel-events \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

# Verify the topic was created
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
# Expected output: sentinel-events
```

---

### PHASE 3 — Install & Configure PostgreSQL in WSL

**In WSL Terminal 2** (or any free WSL terminal):

```bash
# Install PostgreSQL
sudo apt update
sudo apt install -y postgresql postgresql-contrib

# Start the PostgreSQL service
sudo service postgresql start

# Create the database and user for SentinelCore
sudo -u postgres psql <<EOF
CREATE DATABASE sentinel_logs;
CREATE USER sentinel_admin WITH PASSWORD 'changeme123';
GRANT ALL PRIVILEGES ON DATABASE sentinel_logs TO sentinel_admin;
ALTER DATABASE sentinel_logs OWNER TO sentinel_admin;
EOF

# Test the connection
psql -h localhost -U sentinel_admin -d sentinel_logs -c "SELECT version();"
```

> ✅ PostgreSQL is running on **port 5432**. Tables are auto-created on first consumer run.

---

### PHASE 4 — Get Your WSL IP and Update config.json

The WSL IP address changes on every Windows reboot. Get the current one:

**In any WSL terminal:**

```bash
ip addr show eth0 | grep 'inet ' | awk '{print $2}' | cut -d/ -f1
# Example output: 172.30.178.75
```

**On Windows**, open `C:\ProgramData\megahack-2026_HeisenBugs\config.json` and update `bootstrap_servers` with your WSL IP:

```json
{
  "kafka": {
    "bootstrap_servers": "172.30.178.75:9092",
    "topic": "sentinel-events",
    "client_id": "windows-test-agent",
    "acks": "all",
    "retries": 5,
    "retry_backoff_ms": 3000,
    "linger_ms": 50,
    "request_timeout_ms": 15000
  },
  "agent": {
    "system_id_mode": "AUTO",
    "batch_size": 100,
    "retry_attempts": 3,
    "retry_backoff_seconds": 3,
    "save_local_copy": true
  }
}
```

---

### PHASE 5 — Start the Kafka → PostgreSQL Consumer

**Open a NEW WSL terminal** (WSL Terminal 2 — Consumer):

```bash
# Navigate to the project
cd /mnt/c/ProgramData/megahack-2026_HeisenBugs

# Install dependencies
pip install kafka-python-ng psycopg2-binary

# Start the consumer (keep this terminal open)
python3 src/kafka_to_postgres.py
```

> ✅ You should see: `Listening to Kafka topic 'sentinel-events' acting as syncing bridge...`  
> Keep this terminal open. It inserts events + heartbeats into PostgreSQL automatically.

---

### PHASE 6 — Start the FastAPI Backend

**Open a NEW WSL terminal** (WSL Terminal 3 — API):

```bash
cd /mnt/c/ProgramData/megahack-2026_HeisenBugs

# Install FastAPI + Uvicorn
pip install fastapi uvicorn psycopg2-binary

# Start the API server (keep this terminal open)
uvicorn src.api_server:app --host 0.0.0.0 --port 8080 --reload
```

> ✅ API is live at **http://localhost:8080**  
> Verify it works: open http://localhost:8080/health in your browser — you should see `{"status":"healthy","database":"connected"}`

---

### PHASE 7 — Start the Collector Agent on Windows

**Open a new PowerShell window as Administrator:**  
(Right-click PowerShell → "Run as administrator")

```powershell
# Navigate to the project
cd C:\ProgramData\megahack-2026_HeisenBugs

# Install all Python dependencies
pip install -r requirements.txt

# Start the collector with Kafka mode enabled
$env:SENTINEL_KAFKA_MODE = "true"
python src\collector.py
```

> ✅ You should see `[Cycle 1]` start printing. Watch WSL Terminal 2 (consumer) to confirm events are being inserted into PostgreSQL.

---

### PHASE 8 — Start the React Frontend

**Open a new PowerShell window** (normal user, no admin needed):

```powershell
cd C:\ProgramData\megahack-2026_HeisenBugs\frontend

# Install Node.js dependencies (first time only — takes ~1 minute)
npm install

# Start the development server
npm run dev
```

> ✅ Dashboard is live at **http://localhost:5173**  
> The frontend talks to the FastAPI backend at port 8080.

---

### PHASE 9 — Set Up Prometheus

**Open a NEW WSL terminal** (WSL Terminal 4 — Prometheus):

```bash
# Download Prometheus
cd ~
wget https://github.com/prometheus/prometheus/releases/download/v2.52.0/prometheus-2.52.0.linux-amd64.tar.gz
tar -xzf prometheus-2.52.0.linux-amd64.tar.gz
mv prometheus-2.52.0.linux-amd64 prometheus

# Copy the project's pre-configured prometheus.yml
cp /mnt/c/ProgramData/megahack-2026_HeisenBugs/monitoring/prometheus.yml ~/prometheus/prometheus.yml

# Start Prometheus (keep this terminal open)
cd ~/prometheus
./prometheus --config.file=prometheus.yml
```

> ✅ Prometheus is running at **http://localhost:9090**  
> It scrapes `/metrics-export` from the API server every **5 seconds** automatically.  
> It also scrapes itself (port 9090) and node-exporter (port 9100) every 15 seconds.

---

### PHASE 10 — Set Up Grafana

**Open a NEW WSL terminal** (WSL Terminal 5 — Grafana):

```bash
# Install Grafana
sudo apt install -y apt-transport-https software-properties-common
wget -q -O - https://packages.grafana.com/gpg.key | sudo apt-key add -
echo "deb https://packages.grafana.com/oss/deb stable main" | sudo tee /etc/apt/sources.list.d/grafana.list
sudo apt update
sudo apt install -y grafana

# Start Grafana
sudo service grafana-server start
```

**Now import the SentinelCore dashboard:**

1. Open **http://localhost:3000** in your browser
2. Login with **admin / admin** (you'll be prompted to change it)
3. In the left sidebar, go to **Connections → Data Sources**
4. Click **Add data source** → choose **Prometheus**
5. Set URL to `http://localhost:9090` and click **Save & Test**
6. In the left sidebar, go to **Dashboards → Import**
7. Click **Upload JSON file** and select:  
   `C:\ProgramData\megahack-2026_HeisenBugs\monitoring\sentinel_dashboard.json`
8. Select your Prometheus data source and click **Import**

> ✅ Grafana is live at **http://localhost:3000** with the SentinelCore dashboard loaded.

---

### PHASE 11 — Register Collector as a Windows Service (Optional but Recommended)

Once you confirm the full pipeline is working, register the collector to start automatically on every Windows boot:

**PowerShell as Administrator:**

```powershell
cd C:\ProgramData\megahack-2026_HeisenBugs
.\deploy_startup.ps1
```

This script:
- Installs all Python dependencies
- Runs the validation suite
- Registers a Windows Scheduled Task named **`SentinelCore`** that:
  - Runs at system startup under the `SYSTEM` account (highest privileges)
  - Auto-restarts up to 3 times on failure (1-minute cooldown)
  - Runs indefinitely (no time limit)

Useful task management commands:

```powershell
# Check if it's running
Get-ScheduledTask -TaskName "SentinelCore" | Select-Object State

# Start manually
Start-ScheduledTask -TaskName "SentinelCore"

# Stop it
Stop-ScheduledTask -TaskName "SentinelCore"

# View live logs
Get-Content C:\ProgramData\megahack-2026_HeisenBugs\src\sentinel.log -Tail 50 -Wait

# Uninstall / remove the task
Unregister-ScheduledTask -TaskName "SentinelCore" -Confirm:$false
```

---

## ══════════════════════════════════════
## RESTART GUIDE (After First-Time Setup)
## ══════════════════════════════════════

After the first-time setup is complete, bring the full stack back up in this order after any reboot or shutdown. Each step needs its own terminal.

> ⚠️ **Important:** Your WSL IP changes on every reboot. Always check it and update `config.json` if needed (Step 0 below).

---

### Step 0 — Check WSL IP (if Kafka isn't connecting)

**WSL terminal:**

```bash
ip addr show eth0 | grep 'inet ' | awk '{print $2}' | cut -d/ -f1
```

If different from `config.json`, update it before starting the collector.

---

### Step 1 — Start PostgreSQL (WSL Terminal 1)

```bash
sudo service postgresql start

# Quick sanity check
psql -h localhost -U sentinel_admin -d sentinel_logs -c "SELECT COUNT(*) FROM events;"
```

---

### Step 2 — Start Kafka (WSL Terminal 2)

```bash
cd ~/kafka
bin/kafka-server-start.sh config/kraft/server.properties
```

> Keep this terminal open and running.

---

### Step 3 — Start the Kafka Consumer (WSL Terminal 3)

```bash
cd /mnt/c/ProgramData/megahack-2026_HeisenBugs
python3 src/kafka_to_postgres.py
```

> Keep this terminal open.

---

### Step 4 — Start the API Server (WSL Terminal 4)

```bash
cd /mnt/c/ProgramData/megahack-2026_HeisenBugs
uvicorn src.api_server:app --host 0.0.0.0 --port 8080 --reload
```

> Keep this terminal open.

---

### Step 5 — Start the Collector Agent (Windows — PowerShell as Administrator)

> **Skip if you registered the Windows Scheduled Task** in Phase 11 — it starts automatically on boot.

```powershell
cd C:\ProgramData\megahack-2026_HeisenBugs
$env:SENTINEL_KAFKA_MODE = "true"
python src\collector.py
```

---

### Step 6 — Start the Frontend (Windows — PowerShell)

```powershell
cd C:\ProgramData\megahack-2026_HeisenBugs\frontend
npm run dev
```

> Dashboard: **http://localhost:5173**

---

### Step 7 — Start Prometheus (WSL Terminal 5)

```bash
cd ~/prometheus
./prometheus --config.file=prometheus.yml
```

> Prometheus UI: **http://localhost:9090**

---

### Step 8 — Start Grafana (WSL Terminal 6)

```bash
sudo service grafana-server start
```

> Grafana: **http://localhost:3000** (admin / your-password)

---

## Quick Reference: All URLs & Ports

| Service | Port | URL |
|---------|------|-----|
| Kafka Broker | 9092 | — |
| PostgreSQL | 5432 | — |
| FastAPI Backend | 8080 | http://localhost:8080 |
| API Health Check | 8080 | http://localhost:8080/health |
| API Docs (Swagger) | 8080 | http://localhost:8080/docs |
| React Dashboard | 5173 | http://localhost:5173 |
| Prometheus | 9090 | http://localhost:9090 |
| Grafana | 3000 | http://localhost:3000 |

---

## API Endpoint Reference

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Database connectivity check |
| `GET /events?limit=100` | Recent telemetry events |
| `GET /systems` | Live system status + heartbeats |
| `GET /alerts` | CRITICAL/ERROR/WARNING alerts |
| `GET /metrics` | Time-bucketed event counts (for charts) |
| `GET /dashboard-metrics` | KPI summary card data |
| `GET /fault-distribution` | Fault type breakdown |
| `GET /severity-distribution` | Severity pie chart data |
| `GET /pipeline-health` | Live pipeline throughput + latency |
| `GET /system-metrics` | Avg CPU/memory/disk across all events |
| `GET /metrics-export` | Prometheus-compatible text metrics |

---

## Troubleshooting

### Kafka won't connect from Windows collector
```bash
# WSL IP changes on reboot — get the new one:
ip addr show eth0 | grep 'inet ' | awk '{print $2}' | cut -d/ -f1
# Update bootstrap_servers in config.json with the new IP
```

### collector.py exits immediately with "Another instance running"
```powershell
# Delete the stale PID lock file
Remove-Item C:\ProgramData\megahack-2026_HeisenBugs\src\sentinel.pid -Force
# Then retry
```

### API server returns 503 "Database unavailable"
```bash
# PostgreSQL is probably not running
sudo service postgresql status
sudo service postgresql start
```

### Frontend shows no data / blank charts
```bash
# Check the API server is running
curl http://localhost:8080/health
# Check browser DevTools console for CORS or network errors
```

### PostgreSQL tables don't exist error
```
# Tables (events, system_heartbeats) are auto-created when
# kafka_to_postgres.py starts and receives its first message.
# Make sure the consumer ran at least once successfully.
```

### Prometheus shows "Target down" for sentinelcore
```bash
# The API server must be running on port 8080
# Verify: curl http://localhost:8080/metrics-export
```

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SENTINEL_KAFKA_MODE` | `true` | Use Kafka pipeline (recommended) |
| `SENTINEL_LOCAL_MODE` | `false` | Write to local JSON file instead |
| `KAFKA_BOOTSTRAP` | from `config.json` | Override Kafka broker address |
| `SENTINEL_SERVER_URL` | `https://your-server.com/...` | HTTPS fallback endpoint |
| `SENTINEL_AUTH_TOKEN` | *(none)* | Bearer token for HTTPS mode |

---

## License

Production-grade software. Ensure compliance with your organization's telemetry policies before deployment.
