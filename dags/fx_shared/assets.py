"""Assets = data produced by one DAG and consumed by another.

Ingest DAGs declare them as task ``outlets``; ``build_fx_marts`` uses them as its
``schedule``. Airflow then starts the marts DAG when fresh data lands, instead of
us guessing a cron time ("data-aware scheduling").

The URI follows the Postgres asset convention ``postgres://host:port/db/schema/table``
so tools like OpenLineage understand it.
"""

from __future__ import annotations

from airflow.sdk import Asset

ECB_RATES = Asset(
    name="fx_core_ecb_rates",
    uri="postgres://fx-postgres:5432/fx/core/ecb_rates",
    group="fx",
)

NBU_RATES = Asset(
    name="fx_core_nbu_rates",
    uri="postgres://fx-postgres:5432/fx/core/nbu_rates",
    group="fx",
)
