"""
### ingest_nbu_rates

Loads the National Bank of Ukraine official rates (UAH per unit of currency) for **one day**.

```
create_table ─────────────────────┐
extract_to_raw ─> load_to_core ─> (asset fx_core_nbu_rates)
```

* **Schedule:** every day 09:00 Europe/Kyiv. The NBU sets the rate for day D on D-1,
  and (unlike the ECB) has a rate for every calendar day, weekends included.
* **No sensor on purpose.** If the rate is not published yet, `extract_to_raw` raises
  and Airflow **retries** it (6 times, every 15 min). Compare with the sensor in
  `ingest_ecb_rates`: both are valid ways to wait for late data.
* **Raw layer:** `s3://fx-raw/nbu/rate_date=YYYY-MM-DD/rates.json`.
* **Idempotent:** the day is replaced in `core.nbu_rates` on every run.
* **Produces** the asset `fx_core_nbu_rates`.
"""

from __future__ import annotations

from contextlib import closing
from datetime import timedelta
import json

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import CronTriggerTimetable, dag, task
from airflow.sdk.exceptions import AirflowFailException
from fx_shared.assets import NBU_RATES
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

from fx_pipeline import nbu, storage
from fx_pipeline.models import DataValidationError
from fx_pipeline.validation import validate_records


@dag(
    dag_id="ingest_nbu_rates",
    schedule=CronTriggerTimetable(
        "0 9 * * *", timezone="Europe/Kyiv"
    ),  # every day 09h00 Kyiv time
    start_date=pendulum.datetime(2000, 1, 1, tz="Europe/Kyiv"),
    catchup=False,
    max_active_runs=4,
    default_args=DEFAULT_ARGS,
    template_searchpath=SQL_DIR,
    tags=["fx", "ingest", "nbu"],
    doc_md=__doc__,
)
def ingest_nbu_rates():
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id=FX_POSTGRES_CONN_ID,
        sql="ddl/nbu_rates.sql",
    )

    # task-level arguments override DEFAULT_ARGS: here we wait with a constant delay.
    @task(
        pool=FX_API_POOL,
        retries=6,
        retry_delay=timedelta(minutes=15),
        retry_exponential_backoff=0,
    )
    def extract_to_raw(**context) -> str:
        """Store the API response as-is; raises DataNotReadyError (-> retry) if it is empty."""
        rate_date = target_date(context, nbu.NBU_TIMEZONE)
        payload = nbu.fetch_rates(rate_date)

        key = storage.raw_key("nbu", rate_date)
        S3Hook(aws_conn_id=FX_S3_CONN_ID).load_string(
            json.dumps(payload, indent=2, ensure_ascii=False),
            key=key,
            bucket_name=RAW_BUCKET,
            replace=True,
        )
        return key

    @task(outlets=[NBU_RATES])
    def load_to_core(raw_object_key: str, **context) -> int:
        rate_date = target_date(context, nbu.NBU_TIMEZONE)
        raw = S3Hook(aws_conn_id=FX_S3_CONN_ID).read_key(
            raw_object_key, bucket_name=RAW_BUCKET
        )

        try:
            records = nbu.parse_rates(json.loads(raw), rate_date)
            validate_records(records, required_currencies=["USD", "EUR"])
        except DataValidationError as err:
            raise AirflowFailException(str(err)) from err

        with closing(
            PostgresHook(postgres_conn_id=FX_POSTGRES_CONN_ID).get_conn()
        ) as conn:
            rows = storage.replace_day(conn, "nbu", rate_date, records)

        context["outlet_events"][NBU_RATES].extra = {
            "rate_date": rate_date.isoformat(),
            "rows": rows,
        }
        return rows

    loaded = load_to_core(extract_to_raw())
    create_table >> loaded


ingest_nbu_rates()
