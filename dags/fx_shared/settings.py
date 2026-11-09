"""Names and defaults shared by all FX DAGs.

Connections, pools and variables are referenced by *id* here. Their actual values
(hosts, passwords) live outside the code: in environment variables set by
docker-compose (see ``.env``) or in the Airflow UI.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

# --- Connections (Admin -> Connections, here provided as AIRFLOW_CONN_* env vars) ---
FX_POSTGRES_CONN_ID = "fx_postgres"  # data warehouse (NOT the Airflow metadata DB)
FX_S3_CONN_ID = "fx_s3"  # S3-compatible object storage (RustFS) for raw payloads

# --- Object storage ---
RAW_BUCKET = "fx-raw"

# --- Pools (Admin -> Pools, created by scripts/bootstrap.sh) ---
# Caps concurrent calls to the public APIs, e.g. during a big backfill.
FX_API_POOL = "fx_api"

# --- Variables (Admin -> Variables). Optional: defaults are used when absent. ---
VAR_CHANGE_THRESHOLD = "fx_alert_change_pct"
VAR_DEVIATION_THRESHOLD = "fx_alert_deviation_pct"
DEFAULT_CHANGE_THRESHOLD = 1.5
DEFAULT_DEVIATION_THRESHOLD = 1.0

# --- SQL files (used as Jinja templates by SQLExecuteQueryOperator) ---
# dags/fx_shared/settings.py -> <project root>/sql  (/opt/airflow/sql in Docker)
SQL_DIR = str(Path(__file__).resolve().parents[2] / "sql")


def notify_failure(context: dict[str, Any]) -> None:
    """Task failure callback.

    In a real team this would post to Slack / Teams / PagerDuty (see the
    ``airflow.sdk.BaseNotifier`` subclasses shipped by providers). Here we only log
    a structured line, which is enough to see *when* callbacks run.
    """
    ti = context["ti"]
    log.error(
        "ALERT task failed: dag=%s task=%s run=%s try=%s error=%r",
        ti.dag_id,
        ti.task_id,
        ti.run_id,
        ti.try_number,
        context.get("exception"),
    )


# Applied to every task of a DAG unless the task overrides it.
DEFAULT_ARGS: dict[str, Any] = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": 2.0,  # 2 min, 4 min, 8 min ...
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(minutes=10),
    "on_failure_callback": notify_failure,
}
