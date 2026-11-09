# Project summary

One page: what was built, which Airflow features it uses, and the stories worth telling
in an interview. Details live in [architecture.md](architecture.md) and
[airflow-concepts.md](airflow-concepts.md).

## 30-second pitch

> I built an Airflow 3 pipeline that collects daily exchange rates from the European Central
> Bank and the National Bank of Ukraine. Raw API responses land in S3-compatible storage,
> validated rates go to Postgres, and a third DAG builds marts: day-over-day changes and a
> cross-check of the two banks, followed by data quality checks and anomaly alerts.
> Late data is handled per source: a reschedule-mode sensor for the ECB, which also skips
> holidays, and retries for the NBU. The marts DAG is triggered by assets, not by time.
> Every load is idempotent, so retries and backfills are safe. The business logic is a plain
> Python package with unit tests; the DAGs only orchestrate.

## Stack

Airflow 3.4 (LocalExecutor) · Python 3.12 · uv · Docker Compose · PostgreSQL 16 ·
RustFS (S3) · pytest · ruff

## Pipeline

```
ingest_ecb_rates  (weekdays 16:00 Berlin)  sensor -> raw (S3) -> core.ecb_rates ─┐ asset
ingest_nbu_rates  (daily 09:00 Kyiv)       raw (S3) -> core.nbu_rates (retries) ─┤ asset
                                                                                 ▼
build_fx_marts    (ECB & NBU assets)       date range -> 2 marts -> 5 quality checks
                                           -> find anomalies -> branch: report | no_anomalies
```

Layers: **raw** (API response as is) → **core** (validated, one row per day and currency)
→ **marts** (`fx_daily_changes`, `fx_cross_rates`). ELT, bronze / silver / gold.

## Airflow features used

| Feature | Where |
| --- | --- |
| TaskFlow `@task`, XCom through return values | all DAGs |
| `SQLExecuteQueryOperator` + `.sql` files via `template_searchpath` | table creation, mart builds |
| Hooks `S3Hook`, `PostgresHook`; connections from `AIRFLOW_CONN_*` env vars | load tasks |
| `CronTriggerTimetable` with a time zone | both ingest DAGs |
| Sensor: `@task.sensor`, `mode="reschedule"`, `PokeReturnValue`, `AirflowSkipException` | ECB `wait_for_publication` |
| Retries: 3 with exponential backoff by default; NBU 6 × 15 min | `DEFAULT_ARGS`, NBU extract |
| `AirflowFailException`: fail without retries on bad data | `load_to_core` |
| Assets: `outlets`, event `extra`, `schedule=(A & B)`, `triggering_asset_events` | ingest → marts |
| Dynamic task mapping: `.expand()` + `map_index_template` | 5 quality checks |
| TaskGroup | `quality_checks` |
| Branching: `@task.branch` + `EmptyOperator` | `route_alerts` |
| Params (`full_refresh`, `lookback_days`) | marts trigger form |
| Variables (alert thresholds), read inside the task | `find_anomalies` |
| Pool `fx_api` + `max_active_runs` | API calls, backfills |
| Backfill (`airflow backfill create`) | `make backfill` |
| DAG integrity tests (DagBag) + unit tests | `tests/` |

## Design decisions

- **Thin DAGs, logic in `src/`.** No Airflow imports there, so it is unit-tested and DAG files parse fast.
- **Idempotent loads.** Deterministic raw key + delete/insert of the whole day in one
  transaction. Retries, Clear and backfills never create duplicates.
- **The day comes from `logical_date`** (`run_after` for manual runs), never from `today()`.
- **XCom carries references** (the S3 key), not data.
- **Late data, two strategies.** ECB: a sensor, because it must tell "not yet" from
  "not today" (holiday → skip, run stays green). NBU: retries, because it publishes every day.
- **Fail fast on bad data, retry on flaky infrastructure.**
- **DAGs are chained by data, not by time.** Assets with `&`; each event carries its `rate_date`,
  so one marts run rebuilds exactly the days that changed, even after a backfill.
- **Secrets outside the code.** `.env` (git-ignored); DAGs only know connection ids.

## Problems hit and solved (interview stories)

1. **The API never says "no data".** For a holiday or a not-yet-published day, Frankfurter
   silently returns the previous business day. The sensor compares the requested date with
   the returned one and decides: published / pending / not published (after 21:00 Berlin).
2. **Fake % changes after a partial backfill.** `LAG()` took a "previous" day months away.
   Fix: run the window over the full history, filter by date afterwards, and ignore gaps
   longer than 5 days (ECB) or 1 day (NBU).
3. **A manual ECB run on a weekend would wait 6 h and then fail.** Found by triggering it on a
   Sunday. Fix: a weekend day is "not published" at once, so the run skips immediately.
4. **MinIO community images stopped being published.** Switched to RustFS: same S3 API,
   no code change thanks to `S3Hook` + a connection.

## In numbers

3 DAGs · 2 sources (~30 + ~45 currencies) · 3 data layers · 5 quality checks ·
37 tests · 7 containers

## Production next steps

Managed Airflow or Helm on Kubernetes, Celery/Kubernetes executor, secrets backend,
remote logging, Slack/Teams notifier, CI/CD for DAGs, dbt if the marts grow.
See [architecture.md](architecture.md#what-would-change-in-production).
