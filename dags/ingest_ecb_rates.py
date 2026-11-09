"""
### ingest_ecb_rates

Loads the ECB euro reference rates (via the Frankfurter API) for **one business day**.

```
create_table ───────────────────────────────┐
wait_for_publication ─> extract_to_raw ─> load_to_core ─> (asset fx_core_ecb_rates)
```

* **Schedule:** weekdays 16:00 Europe/Berlin (`CronTriggerTimetable`), when the ECB publishes.
* **Sensor:** `wait_for_publication` polls the API every 10 min in *reschedule* mode, so it
  does not hold a worker slot while waiting. On ECB holidays (and on weekends, which only
  manual runs can hit) it **skips** the rest of the run.
* **Raw layer:** the untouched JSON goes to `s3://fx-raw/ecb/rate_date=YYYY-MM-DD/rates.json`.
* **Idempotent:** re-running a day overwrites the raw file and replaces that day in
  `core.ecb_rates`, so retries, clears and backfills never create duplicates.
* **Produces** the asset `fx_core_ecb_rates`, which (together with the NBU asset)
  triggers `build_fx_marts`.
"""

from __future__ import annotations

from contextlib import closing
import json

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import CronTriggerTimetable, PokeReturnValue, dag, task
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException
from fx_shared.assets import ECB_RATES
from fx_shared.runtime import target_date
from fx_shared.settings import (
    DEFAULT_ARGS,
    FX_API_POOL,
    FX_POSTGRES_CONN_ID,
    FX_S3_CONN_ID,
    RAW_BUCKET,
    SQL_DIR,
)
import pendulum

from fx_pipeline import ecb, storage
from fx_pipeline.models import DataValidationError
from fx_pipeline.validation import validate_records


@dag(
    dag_id="ingest_ecb_rates",
    schedule=CronTriggerTimetable("0 16 * * 1-5", timezone="Europe/Berlin"),
    # First day of euro reference rates. With catchup=False this only matters for
    # backfills: Airflow will not create runs before start_date.
    start_date=pendulum.datetime(1999, 1, 4, tz="Europe/Berlin"),
    catchup=False,
    max_active_runs=4,
    default_args=DEFAULT_ARGS,
    template_searchpath=SQL_DIR,
    tags=["fx", "ingest", "ecb"],
    doc_md=__doc__,
)
def ingest_ecb_rates():
    # Classic operator: runs a templated .sql file found via template_searchpath.
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id=FX_POSTGRES_CONN_ID,
        sql="ddl/ecb_rates.sql",
    )

    @task.sensor(
        poke_interval=10 * 60,
        timeout=6 * 60 * 60,
        mode="reschedule",
        pool=FX_API_POOL,
    )
    def wait_for_publication(**context) -> PokeReturnValue:
        """Done when the API serves rates for our day; skip the run if it never will."""
        rate_date = target_date(context, ecb.ECB_TIMEZONE)
        payload = ecb.fetch_rates(rate_date)
        status = ecb.publication_status(
            rate_date, ecb.payload_date(payload), pendulum.now("UTC")
        )

        if status is ecb.PublicationStatus.NOT_PUBLISHED:
            # Skipped is not failed: the run ends green, downstream tasks are skipped.
            raise AirflowSkipException(
                f"The ECB did not publish rates for {rate_date} (weekend or holiday)"
            )
        return PokeReturnValue(is_done=status is ecb.PublicationStatus.PUBLISHED)

    @task(pool=FX_API_POOL)
    def extract_to_raw(**context) -> str:
        """Store the API response as-is. Returns the object key (a small reference) via XCom."""
        rate_date = target_date(context, ecb.ECB_TIMEZONE)
        payload = ecb.fetch_rates(rate_date)

        key = storage.raw_key("ecb", rate_date)
        S3Hook(aws_conn_id=FX_S3_CONN_ID).load_string(
            json.dumps(payload, indent=2),
            key=key,
            bucket_name=RAW_BUCKET,
            replace=True,
        )
        return key

    @task(outlets=[ECB_RATES])
    def load_to_core(raw_object_key: str, **context) -> int:
        """Parse + validate the raw file and replace the day in core.ecb_rates."""
        rate_date = target_date(context, ecb.ECB_TIMEZONE)
        raw = S3Hook(aws_conn_id=FX_S3_CONN_ID).read_key(
            raw_object_key, bucket_name=RAW_BUCKET
        )

        try:
            records = ecb.parse_rates(json.loads(raw), rate_date)
            validate_records(records, required_currencies=["USD"])
        except DataValidationError as err:
            # Bad data will not fix itself: fail immediately, without retries.
            raise AirflowFailException(str(err)) from err

        with closing(
            PostgresHook(postgres_conn_id=FX_POSTGRES_CONN_ID).get_conn()
        ) as conn:
            rows = storage.replace_day(conn, "ecb", rate_date, records)

        # Metadata attached to the asset event; build_fx_marts reads it to know
        # which day changed (incremental processing).
        context["outlet_events"][ECB_RATES].extra = {
            "rate_date": rate_date.isoformat(),
            "rows": rows,
        }
        return rows

    raw_object_key = extract_to_raw()
    wait_for_publication() >> raw_object_key
    loaded = load_to_core(raw_object_key)
    create_table >> loaded


ingest_ecb_rates()
