# crypto-etl-lakehouse — operational commands
#
# Works on Linux, macOS, and Windows (Git Bash or WSL).
# Requires: Docker with Compose v2 (`docker compose ...`).

.DEFAULT_GOAL := help

.PHONY: help up build down restart ps logs logs-follow \
        spark-logs producer-logs airflow-logs consumer-logs \
        jupyter psql reset clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# ---------- lifecycle ----------

build: ## Build all custom images (spark, airflow, producer)
	docker compose build

up: ## Start the entire stack (build if needed) in the background
	docker compose up -d --build

down: ## Stop the stack, keep all data volumes
	docker compose down

restart: ## Restart the whole stack
	docker compose restart

reset: ## STOP EVERYTHING AND DELETE ALL DATA (lake, db, dashboards state, airflow history)
	docker compose down -v

# ---------- status / debugging ----------

ps: ## Show status of all services
	docker compose ps

logs: ## Show the last logs of every service
	docker compose logs --tail=50

logs-follow: ## Tail all logs live (Ctrl+C to stop)
	docker compose logs -f --tail=20

spark-logs: ## Tail the streaming consumer (Kafka -> MinIO) logs
	docker compose logs -f --tail=50 spark-consumer

consumer-logs: spark-logs

producer-logs: ## Tail the CoinGecko producer logs
	docker compose logs -f --tail=50 producer

airflow-logs: ## Tail Airflow (DAG runs) logs
	docker compose logs -f --tail=50 airflow

# ---------- utilities ----------

jupyter: ## Start Jupyter (Spark-capable) at http://localhost:8888
	docker exec -it spark-master jupyter notebook \
		--ip=0.0.0.0 --no-browser --allow-root --port=8888

psql: ## Open a psql shell on the serving database (db: crypto)
	docker exec -it postgres psql -U admin -d crypto

clean: ## Remove stopped build cache (docker builder prune)
	docker builder prune -f
