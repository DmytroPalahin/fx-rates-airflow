# Exercises

Small, self-contained changes to understand Airflow by breaking and extending this project.
Do them on a git branch. Each exercise says what to watch in the UI.

## Level 1: observe

1. **Trace one run end to end.** Trigger `ingest_nbu_rates`. Open the run in the **Grid**, then
   each task's **Logs** and **XCom** tabs. Find the S3 key in XCom and the same file in the RustFS console.
2. **Read the Gantt chart** of a backfill run: which tasks wait for the pool, which run in parallel?
3. **Find the asset events.** Assets page → `fx_core_ecb_rates`: which runs produced events,
   with which `extra`? Which `build_fx_marts` runs consumed them?

## Level 2: break things on purpose

1. **Watch a retry.** In `src/fx_pipeline/nbu.py` change `BASE_URL` to a wrong path, trigger the
   DAG, and watch `extract_to_raw` go *up_for_retry* with growing delays. Find the
   `ALERT task failed` line from `notify_failure` after the last try. Revert.
2. **Fail fast.** Make `validate_records` require a currency that does not exist (e.g. `"XYZ"`).
   The load fails immediately, **without** retries. Why? (`AirflowFailException`.)
3. **Idempotency proof.** Note `SELECT count(*) FROM core.nbu_rates`, Clear a successful
   `load_to_core` in the UI, compare the count and `loaded_at` afterwards.
4. **Sensor waiting.** Trigger `ingest_ecb_rates` manually on a weekday morning (before 16:00
   Berlin). The sensor stays *up_for_reschedule*; the pool slot is free meanwhile (Admin → Pools).

## Level 3: extend

1. **Alert on more currencies.** Add a currency to `WATCHLIST` (`src/fx_pipeline/marts.py`), then
   trigger `build_fx_marts` with `full_refresh=true`.
2. **New quality rule.** Add a `QualityCheck` (e.g. "no day in `core.nbu_rates` has fewer than 30
   currencies"). A new mapped task appears without touching the DAG file.
3. **Third source.** Add `ingest_nbp_rates` for the National Bank of Poland
    (`https://api.nbp.pl/api/exchangerates/tables/A/2024-01-02?format=json`, no key, PLN per unit,
    business days only). Copy the NBU DAG, write `src/fx_pipeline/nbp.py` + tests, add an
    asset, and decide: should `build_fx_marts` wait for it (`&`) or not?
4. **Real notifications.** Replace the log line in `report_anomalies` / `notify_failure` with a
    Telegram or Slack message. Store the token in a Connection, never in code.
5. **Deferrable waiting.** Before polling the API, wait until 16:05 Berlin with
    `DateTimeSensor(..., deferrable=True)` from the standard provider. Watch the task go to the
    *deferred* state and find its trigger in `docker compose logs airflow-triggerer`.

## Level 4: platform

 1. **CeleryExecutor.** Add Redis and an `airflow-worker` service (copy from the official
    docker-compose), switch `AIRFLOW__CORE__EXECUTOR`, and scale workers:
    `docker compose up -d --scale airflow-worker=2`.
 2. **CI.** Add a GitHub Actions workflow that runs `uv sync` + `make lint test` on every push.
 3. **dbt.** Move the two mart SQL files into a dbt project and run it from Airflow with
    `astronomer-cosmos`, keeping the asset trigger.
