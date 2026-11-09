"""
### build_fx_marts

Builds the analytical tables from the two core tables, checks data quality and
raises alerts on unusual moves.

```
create_tables ──────┐
resolve_date_range ─┴─> build_fx_daily_changes ─┐
                    └─> build_fx_cross_rates  ──┴─> quality_checks (mapped)
─> find_anomalies ─> route_alerts ─> report_anomalies | no_anomalies
```

* **Schedule:** data-aware. Runs when **both** assets `fx_core_ecb_rates` **and**
  `fx_core_nbu_rates` received new events since the last run (`ECB_RATES & NBU_RATES`).
* **Incremental:** `resolve_date_range` reads the `rate_date` of every triggering asset
  event and rebuilds only those days. Trigger manually with `full_refresh=true` to
  rebuild the whole history.
* **Dynamic task mapping:** one `run_quality_check` task instance per rule.
* **Branching:** alerts only when anomalies were found.
* **Variables (optional):** `fx_alert_change_pct` (default 1.5),
  `fx_alert_deviation_pct` (default 1.0).
"""

from __future__ import annotations

from datetime import date
import logging
from pathlib import Path

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Param, TaskGroup, Variable, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException
from fx_shared.assets import ECB_RATES, NBU_RATES
from fx_shared.settings import (
    DEFAULT_ARGS,
    DEFAULT_CHANGE_THRESHOLD,
    DEFAULT_DEVIATION_THRESHOLD,
    FX_POSTGRES_CONN_ID,
    SQL_DIR,
    VAR_CHANGE_THRESHOLD,
    VAR_DEVIATION_THRESHOLD,
)
import pendulum

from fx_pipeline import marts

log = logging.getLogger(__name__)


@dag(
    dag_id="build_fx_marts",
    schedule=(ECB_RATES & NBU_RATES),
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # marts are rebuilt in place: never two runs at the same time
    default_args=DEFAULT_ARGS,
    template_searchpath=SQL_DIR,
    params={
        "full_refresh": Param(
            False,
            type="boolean",
            description="Rebuild the marts for the whole history.",
        ),
        "lookback_days": Param(
            7,
            type="integer",
            minimum=1,
            maximum=10_000,
            description="Days to rebuild on a manual run that has no upstream asset events.",
        ),
    },
    tags=["fx", "marts", "quality"],
    doc_md=__doc__,
)
def build_fx_marts():
    create_tables = SQLExecuteQueryOperator(
        task_id="create_tables",
        conn_id=FX_POSTGRES_CONN_ID,
        sql="ddl/marts.sql",
    )

    @task
    def resolve_date_range(**context) -> dict[str, str]:
        """Which days to rebuild, based on the asset events that triggered this run."""
        changed: list[date] = []
        for asset_events in context["triggering_asset_events"].values():
            for event in asset_events:
                if event.extra and "rate_date" in event.extra:
                    changed.append(date.fromisoformat(event.extra["rate_date"]))

        params = context["params"]
        date_range = marts.resolve_date_range(
            changed,
            today=pendulum.today("UTC").date(),
            full_refresh=params["full_refresh"],
            lookback_days=params["lookback_days"],
        )
        log.info(
            "Rebuilding marts for %s .. %s (%d upstream events, full_refresh=%s)",
            date_range.start_date,
            date_range.end_date,
            len(changed),
            params["full_refresh"],
        )
        return date_range.as_params()

    date_range = resolve_date_range()

    # The XCom value of resolve_date_range becomes the SQL parameters at runtime.
    build_fx_daily_changes = SQLExecuteQueryOperator(
        task_id="build_fx_daily_changes",
        conn_id=FX_POSTGRES_CONN_ID,
        sql="marts/fx_daily_changes.sql",
        parameters=date_range,
        split_statements=True,  # DELETE and INSERT run one by one, in one transaction
    )
    build_fx_cross_rates = SQLExecuteQueryOperator(
        task_id="build_fx_cross_rates",
        conn_id=FX_POSTGRES_CONN_ID,
        sql="marts/fx_cross_rates.sql",
        parameters=date_range,
        split_statements=True,
    )

    @task(map_index_template="{{ check_name }}")
    def run_quality_check(check: dict[str, str]) -> int:
        """One mapped task instance per rule; the UI shows the rule name instead of 0, 1, 2..."""
        get_current_context()["check_name"] = check["name"]

        violations = PostgresHook(postgres_conn_id=FX_POSTGRES_CONN_ID).get_first(
            check["sql"]
        )[0]
        if violations:
            raise AirflowFailException(
                f"Quality check {check['name']!r} failed: {violations} bad rows. "
                f"{check['description']}"
            )
        log.info("Quality check %s passed", check["name"])
        return violations

    with TaskGroup(
        group_id="quality_checks", tooltip="One task per data quality rule"
    ) as checks:
        run_quality_check.expand(check=[c.as_dict() for c in marts.QUALITY_CHECKS])

    @task
    def find_anomalies(date_range: dict[str, str]) -> list[dict]:
        """Small result (a few rows at most), so returning it through XCom is fine."""
        # Read Variables inside tasks, never at the top of a DAG file:
        # top-level code runs on every DAG parse, i.e. every ~30 seconds.
        change_threshold = float(
            Variable.get(VAR_CHANGE_THRESHOLD, DEFAULT_CHANGE_THRESHOLD)
        )
        deviation_threshold = float(
            Variable.get(VAR_DEVIATION_THRESHOLD, DEFAULT_DEVIATION_THRESHOLD)
        )

        sql = Path(SQL_DIR, "marts", "find_anomalies.sql").read_text()
        rows = PostgresHook(postgres_conn_id=FX_POSTGRES_CONN_ID).get_records(
            sql,
            parameters={
                **date_range,
                "currencies": list(marts.WATCHLIST),
                "change_threshold": change_threshold,
                "deviation_threshold": deviation_threshold,
            },
        )
        return [
            {
                "kind": kind,
                "source": source,
                "rate_date": rate_date.isoformat(),
                "currency": currency.strip(),
                "value": float(value),
                "threshold": float(threshold),
            }
            for kind, source, rate_date, currency, value, threshold in rows
        ]

    @task.branch
    def route_alerts(anomalies: list[dict]) -> str:
        """Return the task_id to follow; the other branch is skipped."""
        return "report_anomalies" if anomalies else "no_anomalies"

    @task
    def report_anomalies(anomalies: list[dict]) -> None:
        # Swap this for a Slack/Teams/email notifier in a real setup.
        log.warning(marts.format_anomaly_report(anomalies))

    no_anomalies = EmptyOperator(task_id="no_anomalies")

    anomalies = find_anomalies(date_range)

    create_tables >> [build_fx_daily_changes, build_fx_cross_rates]
    [build_fx_daily_changes, build_fx_cross_rates] >> checks >> anomalies
    route_alerts(anomalies) >> [report_anomalies(anomalies), no_anomalies]


build_fx_marts()
