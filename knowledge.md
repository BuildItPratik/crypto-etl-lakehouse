# knowledge.md — Project Knowledge Base

> **Project:** `crypto-etl-lakehouse` — an end-to-end, containerized streaming data engineering pipeline that ingests live cryptocurrency prices, lands them in a MinIO-based data lake, computes analytical metrics with Spark, serves them in PostgreSQL, and observes everything with Prometheus + Grafana.

---

## 1. Big Picture

```
                ┌─────────────┐
 CoinGecko API ─▶│  Producer   │──┐
 (20 coins,30s)  │ (Python/Kafka)│  │
                └─────────────┘  ▼
                            Kafka (3 brokers, KRaft)
                                 │ topic: crypto-prices
                                 ▼
                ┌──────────────────────────┐
                │ Spark Structured Streaming│  (kafka_streaming.py)
                │  parse → enrich → dedup    │
                └──────────┬───────────────┘
                           ▼  Parquet, partitioned by event_date
                ┌─────────────┐
                │   MinIO      │  (S3-compatible data lake, bucket: crypto-data)
                └──────┬──────┘
                       ▼  (Airflow DAG, every 2 min)
                ┌─────────────┐
                │ Spark Batch  │  (analytics.py)
                │  metrics via window fns
                └──────┬──────┘
                       ▼  staging → upsert
                ┌─────────────┐
                │  PostgreSQL  │  6 serving tables
                └──────┬──────┘
                       ▼
                  Grafana (2 dashboards)   ◀── Prometheus (metrics from all layers)
```

**Architecture pattern:** a classic **Kappa-ish / speed-layer-only lakehouse** —
streaming ingestion into an immutable raw zone (Parquet on MinIO) + periodic
micro-batch transformation into a serving layer (Postgres). No batch/speed
dual paths; the "batch" job is a recurring micro-batch over the lake.

---

## 2. Components in Detail

### 2.1 Producer — `jobs/producer.py` (Dockerfile.producer, python:3.10)
- Polls **CoinGecko** `/coins/markets` API every **30 s** for **20 coins** (BTC, ETH, SOL, …).
- Extracts only the needed fields (`id, symbol, current_price, market_cap, total_volume, high_24h, low_24h, last_updated`).
- Enriches each record with **streaming best practices**:
  - `event_id` (UUID) → downstream dedup key
  - `event_time` (source timestamp) → event-time semantics
  - `producer_time` (UTC ISO) → latency measurement
  - `producer_id` → lineage/traceability
- Kafka producer configured with `acks='all'` (durability), `retries=5`, `linger_ms=10` (throughput batching).

### 2.2 Kafka — 3-broker KRaft cluster (Confluent cp-kafka 7.5.0)
- **No ZooKeeper** — KRaft mode, combined broker+controller roles, 3-node quorum (`KAFKA_CONTROLLER_QUORUM_VOTERS`), `offsets.topic.replication.factor=3` (safe, survives a broker loss).
- Topic: `crypto-prices`.
- **Kafka UI** (port 8082) for topic/offset/lag inspection.
- **kafka-exporter** (port 9308) exposes consumer-group lag to Prometheus.

### 2.3 Ingestion / streaming — `jobs/kafka_streaming.py` (Spark 3.5.1 Structured Streaming)
- Reads `crypto-prices` from all 3 brokers, `startingOffsets=earliest`, dedicated `kafka.group.id=spark-crypto-consumer`.
- **Explicit schema** via `from_json` (no `inferSchema` in streaming — fails fast on malformed data).
- Binary → string → parse → flatten, keeping Kafka metadata (`kafka_timestamp`, `partition`, `offset`).
- Casts `event_time`/`producer_time` to timestamps, derives `event_date`.
- **Watermark + dedup**: 10-minute watermark on `event_time`, `dropDuplicates(["event_id"])` — bounded-state duplicate protection.
- **Write**: Parquet, `outputMode=append)`, `partitionBy("event_date")`, **30 s trigger**, to `s3a://crypto-data/raw/` with **checkpointing** at `s3a://crypto-data/checkpoints/...` (exactly-once-ish source offsets; the streaming job restarts from the checkpoint).
- **Self-instrumentation**: a monitoring loop reads `query.lastProgress` every 5 s and pushes custom Prometheus gauges on port 8000:
  - `spark_kafka_lag{topic,partition}` (latestOffset − endOffset)
  - `spark_input_rows_per_second`, `spark_processed_rows_per_second`, `spark_batch_duration_ms`
- Spark event log enabled → **Spark History Server** (port 18080) for post-hoc job inspection.

### 2.4 Orchestration — `dags/spark_dag.py` (Airflow 2.8.1, standalone)
- DAG `crypto_pipeline`, **every 2 minutes**, `catchup=False`, `max_active_runs=1` (prevents overlap), 1 retry with 1-min backoff.
- Linear task chain: `heartbeat → check_producer → check_kafka → run_spark_analytics`.
  - Health checks: `docker ps | grep " producer$"`; `kafka-topics --describe`.
  - Runs the batch job via `docker exec spark-master spark-submit ... jobs/analytics.py` with bounded resources (1G executor memory, 1 core).
- Airflow container has the **Docker socket mounted** so `docker exec` works — this is the "DockerOperator-by-Bash" pattern.

### 2.5 Transformation — `jobs/analytics.py` (Spark batch over the lake)
Reads Parquet from MinIO (last 20 min window) and applies a **medallion-style refinement in memory**:

1. **Read** raw zone → filter to last 20 min; empty → clean early exit.
2. **Clean**: select/cast columns, `dropDuplicates(["id","event_time"])`.
3. **Metadata/observability columns**: `processing_time`, `run_id`, `lateness_sec`, `ingestion_delay_sec`, and a **data-freshness SLA classification**:
   - `fresh` (≤ 420 s late), `late` (≤ 1020 s), `too_late` (filtered out).
4. **Metrics** via window functions (`Window.partitionBy("id").orderBy("event_time")`):
   - `price_1min_ago` (lag 2), `price_5min_ago` (lag 10) — assumes 30 s producer cadence
   - `change_1min`, `change_5min` (% change)
   - `sma` = 5-sample rolling mean, `volatility` = 5-sample rolling stddev.
5. **Dedup for idempotence**: row_number over `(id, event_time)` ordered by `processing_time desc` → keep the newest reprocessing.
6. **Latest snapshot**: rank_desc = 1 per coin → `crypto_latest`.
7. **Top-5 gainers/losers** by `change_5min` with rank, stamped with `processing_time`.

**Idempotent write pattern (the key trick):** Spark writes to `*_staging` tables (append), then a **Postgres transaction** runs `INSERT ... SELECT DISTINCT ... ON CONFLICT DO NOTHING` into the final table and truncates staging. Result: re-running the job never duplicates rows (idempotent upserts), and Spark never needs JDBC upsert support.

### 2.6 Serving layer — PostgreSQL 15 + `init.sql`
Schema designed by **data purpose** (a good contract between lake and serving):

| Table | Purpose | Write mode | Key |
|---|---|---|---|
| `crypto_events` | immutable raw event history | upsert-staging, `ON CONFLICT DO NOTHING` | (id, event_time) |
| `crypto_event_metadata` | pipeline health per event (lateness, run_id, data_status) | same | event_id |
| `crypto_metrics` | derived analytics (changes, SMA, volatility) | same | (id, event_time) |
| `crypto_latest` | current snapshot per coin (serving) | **overwrite** | id |
| `top_5_gainers` / `top_5_losers` | ranked aggregates, time-stamped | append | (processing_time, rank) |

Indexes match access patterns: `(id, event_time DESC)` for per-coin time series, `event_time` for range scans, `data_status` for health pie charts, `processing_time` for the "latest gainers" subquery.

- **pgAdmin** (port 5050) with pre-registered server (`servers.json`).

### 2.7 Storage — MinIO (S3-compatible lake)
- Bucket `crypto-data`, auto-created by a **one-shot `minio-init` service** (`minio/mc`) gated on the MinIO healthcheck — an init-container pattern.
- Holds both the **data** (`raw/`) and the **streaming checkpoints** (which is what makes the streaming job restartable).
- Spark talks to it with the **s3a connector** + path-style access ( jars: `hadoop-aws`, `aws-sdk-bundle`).

### 2.8 Observability stack
- **Prometheus** (port 9090, 5 s scrape): scrapes itself, kafka-exporter (lag), cAdvisor (container CPU/mem), node-exporter (host), and the Spark job's custom metrics endpoint (spark-master:8000).
- **Grafana** (port 3000, admin/admin) with **provisioned** datasources (Postgres + Prometheus) and dashboards (no manual click-ops):
  - **Crypto Streaming Pipeline Monitoring**: Kafka consumer lag (green→red at 80), total lag, input rate, microbatch duration, topic throughput, Spark CPU/memory gauges.
  - **Analytics** (Postgres-backed): BTC price tracking, `MAX(run_id)` streaming health stat, top-5 gainers/losers bar charts, SMA vs price, volatility, ingestion rate (events/min), pipeline latency (`AVG(ingestion_delay_sec)`), late/fresh/too-late event pie chart, event volume over time.
- **Spark History Server** (18080) for Spark-level debugging.

### 2.9 Developer tooling
- **Cross-platform operations via a `Makefile`** (`make up`, `make ps`, `make logs-follow`, `make jupyter`, `make psql`, `make reset`, …). All former `.bat` scripts (`producer.bat`, `consumer.bat`, `start-jupyter.bat`) were removed: the producer and the Spark streaming consumer are first-class compose services (`producer`, `spark-consumer`) with `restart: unless-stopped`, so a single `make up` runs the entire pipeline on Linux, macOS, or Windows (Git Bash/WSL).
- Notebooks: `delta_demo.ipynb` (Delta Lake experiments — delta jars are baked into the Spark image), `tinkering.ipynb`.

---

## 3. Port Map (host → service)

| Port | Service |
|---|---|
| 3000 | Grafana (admin/admin) |
| 5432 | PostgreSQL (admin/admin, db `crypto`) |
| 5050 | pgAdmin (admin@admin.com / admin) |
| 7077 | Spark master (spark://) |
| 8080 | Spark master Web UI |
| 8081 | Airflow web UI (admin/admin) |
| 8082 | Kafka UI |
| 8088 | cAdvisor |
| 8888 | Jupyter (inside spark-master) |
| 9000/9001 | MinIO API / console (admin/password) |
| 9090 | Prometheus |
| 9092/9094/9095 | Kafka brokers 1/2/3 |
| 9100 | node-exporter |
| 9308 | kafka-exporter |
| 18080 | Spark History Server |
| 8000 | Spark streaming job's Prometheus metrics endpoint (spark-consumer container) |

---

## 4. Data Engineering Best Practices Demonstrated Here

### 4.1 Data quality & contracts
- **Explicit schemas everywhere** (producer's `desired_keys`, Spark's `coin_schema`, SQL DDL) — the contract is code, and parsing fails loudly instead of silently.
- **Dedup at multiple layers** with a purpose-built key (`event_id` UUID): stream-level (watermark + dropDuplicates), batch-level (dropDuplicates + row_number tie-breaking).
- **Event-time vs processing-time vs ingestion-time kept as separate columns** — you can always answer "how late was this data?" (`lateness_sec`, `ingestion_delay_sec`, `data_status`).
- **SLA-based filtering** (`too_late` rows dropped) turns data quality into an explicit, tunable policy rather than an accident.

### 4.2 Idempotence & exactly-once effects
- Kafka `acks=all`; Spark **checkpoint location** for source offsets; **staging-table + `ON CONFLICT DO NOTHING`** upserts for the sink. Each layer is individually at-least-once, combined into an effectively-once pipeline.
- Re-running the Airflow task is safe by design — no duplicate rows.

### 4.3 Lakehouse / zone discipline
- **Immutable raw zone** (Parquet, `raw/`, partitioned by `event_date`) — replay/reprocess from source of truth at any time.
- **Refined/derived data lives separately** (`crypto_metrics`), **serving snapshots separate again** (`crypto_latest`, top-N tables).
- **Partitioning by date** keeps reads (the 20-min filter) cheap and makes retention/deletion a `rm` of a partition dir.

### 4.4 Separation of concerns
- Ingestion (streaming) and transformation (micro-batch) are **decoupled through the lake** — the streaming job never talks to Postgres; the batch job never talks to Kafka. Each can fail, restart, or be rewritten independently.
- Orchestration (Airflow) only *coordinates* — it holds no data logic.

### 4.5 Observability as a first-class feature
- Metrics at **every layer**: producer heartbeats (Airflow check), Kafka lag (exporter), Spark throughput/duration (custom gauges from `lastProgress`), container/host resource usage (cAdvisor + node-exporter), and **data-level health inside the warehouse itself** (metadata table + Grafana SQL panels on lateness and `data_status`).
- Alerting thresholds visualized (lag panel goes red at 80) — the operational "why is my pipeline slow" loop is covered end-to-end.
- **Spark event log + History Server** for job forensics.

### 4.6 Infrastructure practices
- Everything is **declarative & reproducible** (docker-compose, provisioned Grafana, `init.sql`, health-gated init container `minio-init`).
- **Healthchecks** on Postgres and MinIO with `depends_on` conditions — startup ordering without sleep hacks.
- **Resource bounds** on the worker (2 g / 2 cores) and per-executor limits in spark-submit — prevents a laptop-melting local cluster.
- **Timezone pinned to UTC** (`TZ` env) on every time-sensitive service (spark, airflow, producer, postgres via `TZ`+`PGTZ`), and `analytics.py` uses timezone-aware `datetime.now(timezone.utc)` — the naive `TIMESTAMP` / `TIMESTAMPTZ` column mix can no longer drift with a container's local TZ.
- Airflow's state (`airflow.db`, standalone config) lives on the named **`airflow_data` volume**, so DAG run history survives `docker compose down`/`up`.
- Names, credentials, and bucket setup are consistent across services (admin/admin everywhere — fine for local dev, a red flag for prod).
- Persistent volumes for stateful services (`pg_data`, `minio_data`, `grafana_data`); Prometheus keeps its TSDB on a bind mount.

---

## 5. Known Weaknesses / Improvement Opportunities (honest read)

1. **Secrets in plain text** — admin/password, MinIO keys, DB creds are hardcoded in compose, `analytics.py`, and Grafana provisioning. Should move to `.env` + Docker secrets.
2. **Docker socket mounted into Airflow** — convenient but root-equivalent; a compromised Airflow container owns the host. A `DockerOperator` with a dedicated proxy socket or SSH-based operator is safer.
3. **The lag gauge freezes (never alerts) when the stream dies** — the metrics loop in `kafka_streaming.py` exits with the query, so Prometheus keeps serving the last values; add a staleness/alert rule instead. (The `latestOffset − endOffset` math itself is correct in direction.)
4. **No `restart` policy on the producer container** — it dies silently on an unhandled exception (e.g. a network error that raises instead of returning a non-200), while every other service auto-restarts. The DAG detects it but nothing heals it.
5. **`analytics.py` filters on `current_timestamp()`** at scan time — with `partitionBy("event_date")` a partition-prunable filter on `event_date` would be cheaper.
6. ~~**Airflow `check_producer` greps `docker ps` for `test-producer`**~~ **(fixed)** — now greps for the explicit `container_name: producer`; the producer also has `restart: unless-stopped`.
7. **`exit()` mid-script** in analytics.py leaves `spark.stop()` in `finally` — actually handled, but `exit()` inside the `try` still runs `finally`, so it's OK; still, `sys.exit` with a distinct code would be cleaner for Airflow sensing.
8. **No tests** — schemas and dedup logic are prime candidates for pytest (schema evolution, malformed JSON, duplicate storms).
9. **No schema-registry / Avro/Protobuf** — JSON + `from_json` silently nulls malformed fields; a schema registry would enforce contracts at the Kafka boundary.
10. **Delta Lake is set up but unused in the pipeline** (only the notebook) — swapping Parquet for Delta would give ACID upserts, time travel, and `MERGE` into the lake, simplifying the Postgres staging dance.

---

## 6. Quick Reference

**Run order (one command, any OS):**
1. `make up` (or `docker compose up -d --build`) — the compose project is pinned to `crypto-etl-lakehouse` (top-level `name:`), so it works regardless of the clone's folder name. Everything starts in healthcheck-gated order: Postgres (init.sql) → MinIO + `minio-init` bucket creation → Spark master/worker → Kafka brokers (healthchecked via `kafka-broker-api-versions`) → `spark-consumer` (the streaming job, resuming from its MinIO checkpoint) → producer → Airflow (DAG runs every 2 min).
2. No manual steps: all `.bat` scripts are gone; the streaming job is the `spark-consumer` service and self-restarts.
3. Watch: Grafana (3000) → dashboards; Kafka UI (8082); Spark UI (8080); History Server (18080); pgAdmin (5050).

**Topic:** `crypto-prices` · **Bucket:** `crypto-data` (`raw/`, `checkpoints/`) · **DB:** `crypto` (Postgres 15)

**Key files:**
| File | Role |
|---|---|
| `docker-compose.yaml` | full topology, 16 services |
| `jobs/producer.py` | source ingestion |
| `jobs/kafka_streaming.py` | streaming ETL → lake |
| `jobs/analytics.py` | micro-batch metrics → Postgres |
| `dags/spark_dag.py` | orchestration + health gates |
| `init.sql` | serving-layer DDL + indexes |
| `Dockerfile.spark` | Spark 3.5.1 + kafka/s3/jdbc/delta jars |
| `grafana/*` | provisioned dashboards + datasources |
| `prometheus.yml` | scrape config (5 s interval) |
