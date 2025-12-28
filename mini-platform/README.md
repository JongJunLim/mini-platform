# Mini Data Platform (Docker + ETL + DQ + API)

## Architecture
- MySQL : Service transaction DB
- MongoDB : Event log (JSON)
- PostgreSQL : Analytics warehouse
- ETL : Python (incremental, watermark, UPSERT)
- DQ : Row count / NULL ratio / PK / Freshness
- Alert : Day-over-day anomaly detection
- API : FastAPI (KPI / DQ / Alerts)

## How to Run (WSL/Linux)

### 1. Prepare env
```bash
cp .env.example .env
# edit .env
