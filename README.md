# crypto-etl-lakehouse

An end-to-end, fully containerized streaming data engineering pipeline: live cryptocurrency prices flow from the CoinGecko API through Kafka into a MinIO data lake, get enriched with Spark, and land in PostgreSQL for serving — with the whole stack observable in Grafana.

**One command brings up everything:** the pipeline, the orchestrator, and the dashboards.

## Architecture

```
CoinGecko API ──▶ Producer ──▶ Kafka (3 brokers, KRaft) ──▶ Spark Structured Streaming
                                                                    │
                                                    Parquet, partitioned by date
                                                                    ▼
                                                        MinIO (S3 data lake)
                                                                    │
                                            Airflow (every 2 min) ──▶ Spark batch job
                                                                    │
                                                        PostgreSQL serving layer
                                                                    │
                                                    Grafana ◀── Prometheus (all layers)
```

See [`knowledge.md`](knowledge.md) for the full deep-dive: every component, the port map, design decisions, and known trade-offs.

## Run the project, step by step

**0. Prerequisites.** Docker Desktop (or Docker Engine + Compose v2) running, ~6 GB RAM free, ~10 GB disk for images. First build downloads Spark and its connector jars — expect **10–15 minutes**; subsequent starts take under a minute.

**1. Clone and start.**

```bash
git clone https://github.com/BuildItPratik/crypto-etl-lakehouse.git
cd crypto-etl-lakehouse
make up          # or: docker compose up -d --build
```

> **Windows:** work from [Git Bash](https://git-scm.com/downloads) (or WSL). If Git Bash doesn't have `make` (it often doesn't), install it once via `choco install make`, `winget install GnuWin32.Make`, or `apt install make` inside WSL. Don't want to install it? Skip the Makefile entirely — every target is a plain `docker compose ...` command, listed in the table below.

**2. Wait for the build and boot.** The stack comes up in healthcheck-gated order — no manual steps:

1. **Postgres** boots and runs `init.sql` (serving-layer schema).
2. **MinIO** boots; the `minio-init` one-shot creates the `crypto-data` bucket.
3. **Spark** master + worker form the cluster.
4. **Kafka** (3-broker KRaft) becomes healthy.
5. **spark-consumer** starts the Structured Streaming job (Kafka → Parquet on MinIO); on any restart it resumes from its MinIO checkpoint.
6. **producer** starts polling CoinGecko every 30 s for 20 coins.
7. **Airflow** runs the `crypto_pipeline` DAG every 2 minutes: health-checks the producer and Kafka, then runs the analytics job that computes metrics and upserts into Postgres.

**3. Verify it's working.**

```bash
make ps          # all services should be "running" (or "completed 0" for minio-init, exit 0)
```

Then, in order of usefulness:

- `make producer-logs` — you should see `Pushed 20 records at ...` roughly every 30 s.
- `make spark-logs` — the streaming query should report batches with no errors.
- `make airflow-logs` — after the first 2-minute boundary, a `crypto_pipeline` DAG run should succeed.
- **Grafana** at http://localhost:3000 (admin/admin) → *Analytics* dashboard — BTC price and ingestion-rate panels populate within ~5 minutes.
- `make psql` — `SELECT COUNT(*) FROM crypto_events;` grows over time.

**4. Explore.** The "What runs where" table below lists every UI (Kafka UI, Spark UI, MinIO console, Airflow, …) and `make jupyter` starts a Spark-capable notebook.

**5. Stop / restart / reset.**

- `make down` — stops everything; **all data survives** in Docker volumes, so `make up` resumes where you left off (streaming continues from its checkpoint).
- `make restart` — restarts all services in place.
- `make reset` — ⚠️ stops the stack **and deletes all data** (lake, database, dashboards state) for a from-scratch start.

**If something looks wrong:**

- A service stuck in `restarting` → `docker compose logs <service>`; most commonly the CoinGecko free API rate-limiting your IP (429s in producer logs) — it retries on the next poll, so data resumes on its own.
- Grafana panels empty but `make ps` healthy → give it ~5 more minutes; the first DAG run only happens on a 2-minute cron boundary.
- Want a guaranteed clean slate → `make reset && make up`.

## Quick start (TL;DR)

```bash
git clone https://github.com/BuildItPratik/crypto-etl-lakehouse.git
cd crypto-etl-lakehouse
make up
```

Then verify with `make ps` and watch Grafana at http://localhost:3000 — data flows within ~5 minutes.

## Commands

All day-to-day operations go through the `Makefile` — run `make` (or `make help`) to list targets:

| Command | What it does | Plain docker equivalent |
|---|---|---|
| `make up` | Build and start the whole stack in the background | `docker compose up -d --build` |
| `make ps` | Status of all services | `docker compose ps` |
| `make logs-follow` | Tail all logs live | `docker compose logs -f` |
| `make spark-logs` | Tail the streaming consumer (Kafka → MinIO) | `docker compose logs -f spark-consumer` |
| `make producer-logs` | Tail the CoinGecko producer | `docker compose logs -f producer` |
| `make airflow-logs` | Tail Airflow / DAG runs | `docker compose logs -f airflow` |
| `make jupyter` | Start Jupyter (with Spark) at `http://localhost:8888` | see Makefile |
| `make psql` | Open a psql shell on the `crypto` database | see Makefile |
| `make down` | Stop the stack (all data survives in volumes) | `docker compose down` |
| `make restart` | Restart all services | `docker compose restart` |
| `make reset` | **Stop everything AND delete all data** (lake, DB, dashboards) | `docker compose down -v` |
| `make build` | Rebuild custom images after editing a Dockerfile | `docker compose build` |

If you don't have `make` handy, the **Plain docker equivalent** column above works everywhere as-is.

## What runs where

| URL | Service | Login |
|---|---|---|
| http://localhost:3000 | Grafana — dashboards & pipeline monitoring | admin / admin |
| http://localhost:8081 | Airflow — `crypto_pipeline` DAG | admin / admin |
| http://localhost:8082 | Kafka UI — topics, offsets, consumer lag | — |
| http://localhost:8080 | Spark master UI | — |
| http://localhost:18080 | Spark History Server | — |
| http://localhost:9001 | MinIO console — the data lake | admin / password |
| http://localhost:9090 | Prometheus | — |
| http://localhost:5050 | pgAdmin — Postgres | admin@admin.com / admin |

Postgres itself: `localhost:5432`, db `crypto`, admin / admin.

**Grafana dashboards** (auto-provisioned):
- **Crypto Streaming Pipeline Monitoring** — Kafka consumer lag, input rate, microbatch duration, Spark CPU/memory.
- **Analytics** — BTC price tracking, top-5 gainers/losers, SMA vs price, volatility, ingestion rate, pipeline latency, late-event breakdown.

## Repo layout

```
Makefile                  All operational commands (cross-platform)
docker-compose.yaml       The full stack, healthcheck-gated startup
dags/spark_dag.py         Airflow DAG: health checks + runs the analytics job
jobs/producer.py          CoinGecko → Kafka producer (30 s poll)
jobs/kafka_streaming.py   Kafka → Parquet on MinIO (Structured Streaming + custom metrics)
jobs/analytics.py         Lake → metrics → PostgreSQL (window functions, idempotent upserts)
init.sql                  Serving-layer DDL and indexes
grafana/                  Provisioned datasources + dashboards
prometheus.yml            Scrape config for Kafka, Spark, cAdvisor, node-exporter
knowledge.md              Detailed project knowledge base
```

## Notes & caveats

- **Dev credentials are hardcoded** (admin/admin, admin/password) for local convenience — do not deploy this as-is.
- The CoinGecko free API is rate-limited; occasional 429s are logged by the producer and retried on the next 30 s poll.
- **Airflow runs with the Docker socket mounted** so the DAG can `docker exec` into Spark — acceptable for a local demo, not for shared infrastructure.
- The whole stack is one Compose project pinned to the name `crypto-etl-lakehouse`, so it behaves the same regardless of what folder you clone it into.
