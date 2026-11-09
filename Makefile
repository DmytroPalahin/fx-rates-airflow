# Shortcuts for everyday commands. Run `make` to list them.

-include .env
export

.DEFAULT_GOAL := help
COMPOSE := docker compose
AIRFLOW := $(COMPOSE) exec airflow-scheduler airflow
FX_DAGS := ingest_ecb_rates ingest_nbu_rates build_fx_marts

.PHONY: help venv env setup up down reset ps logs test lint fmt unpause trigger backfill psql airflow

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' Makefile | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

# --- Local Python environment (uv) -------------------------------------------

venv: ## Create/update the local uv environment in .venv
	uv sync

env: ## Create .env with fresh secrets (never overwrites an existing one)
	uv run python scripts/init_env.py

setup: venv env ## venv + .env: everything needed before `make up`

test: ## Unit tests + DAG integrity tests (no Docker needed)
	uv run pytest

lint: ## Lint and check formatting
	uv run ruff check .
	uv run ruff format --check .

fmt: ## Auto-format and auto-fix lint issues
	uv run ruff format .
	uv run ruff check --fix .

# --- Docker stack --------------------------------------------------------------

up: ## Build the image and start the stack
	$(COMPOSE) up -d --build
	@echo ""
	@echo "  Airflow UI      http://localhost:$(AIRFLOW_PORT)"
	@echo "  RustFS console  http://localhost:9001/rustfs/console/  ($(S3_ACCESS_KEY) / $(S3_SECRET_KEY))"
	@echo "  Warehouse       postgresql://$(FX_DB_USER):$(FX_DB_PASSWORD)@localhost:$(FX_DB_PORT)/$(FX_DB_NAME)"

down: ## Stop the stack (data is kept)
	$(COMPOSE) down

reset: ## Stop the stack and DELETE all data (metadata DB, warehouse, raw files, logs)
	$(COMPOSE) down --volumes

ps: ## Show containers and their health
	$(COMPOSE) ps

logs: ## Follow the logs of the Airflow services
	$(COMPOSE) logs -f airflow-scheduler airflow-dag-processor airflow-apiserver

# --- Working with the DAGs -----------------------------------------------------

unpause: ## Unpause the three FX DAGs (they start paused)
	@for dag in $(FX_DAGS); do $(AIRFLOW) dags unpause $$dag; done

trigger: ## Trigger a DAG now: make trigger DAG=ingest_nbu_rates
	$(AIRFLOW) dags trigger $(DAG)

backfill: ## Backfill both sources: make backfill FROM=2026-10-01 TO=2026-10-31
	@test -n "$(FROM)" -a -n "$(TO)" || { echo "usage: make backfill FROM=YYYY-MM-DD TO=YYYY-MM-DD"; exit 1; }
	$(AIRFLOW) backfill create --dag-id ingest_ecb_rates --from-date $(FROM) --to-date $(TO) --max-active-runs 4
	$(AIRFLOW) backfill create --dag-id ingest_nbu_rates --from-date $(FROM) --to-date $(TO) --max-active-runs 4

psql: ## Open psql in the warehouse
	$(COMPOSE) exec fx-postgres psql -U $(FX_DB_USER) -d $(FX_DB_NAME)

airflow: ## Any Airflow CLI command: make airflow CMD="dags list"
	$(AIRFLOW) $(CMD)
