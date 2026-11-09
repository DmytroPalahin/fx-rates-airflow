# Runbook

Commands assume you are in the project root. `make help` lists all shortcuts.

## First start

```bash
make setup     # uv env in .venv + .env with generated secrets
make test      # unit + DAG integrity tests, no Docker needed
make up        # build image, start 7 containers, run airflow-init once
make ps        # everything should be "healthy" after ~1 min
make unpause   # DAGs are created paused
```

The local `.venv` is only for the IDE, tests and the linter. The containers have their own
Python environment inside the image. Logs and database files live in Docker volumes, not in
the project folder.

## Daily operations

| Task | How |
| --- | --- |
| Run a DAG now | UI: ▶ Trigger. CLI: `make trigger DAG=ingest_nbu_rates` |
| Rebuild all marts | UI: Trigger `build_fx_marts` with `full_refresh = true`. CLI: `make airflow CMD="dags trigger build_fx_marts --conf '{\"full_refresh\": true}'"` |
| Load history | `make backfill FROM=2026-01-01 TO=2026-02-01` (see below) |
| Re-run a failed task | UI: task → **Clear** (with downstream). Safe because every task is idempotent. |
| Re-run days already loaded | `make airflow CMD="backfill create --dag-id ingest_ecb_rates --from-date 2026-11-02 --to-date 2026-11-04 --reprocess-behavior completed"` |
| Change alert thresholds | UI: Admin → Variables → add `fx_alert_change_pct` / `fx_alert_deviation_pct` |
| Look at raw files | RustFS console <http://localhost:9001/rustfs/console/> → bucket `fx-raw` |
| Query the warehouse | `make psql`, or any SQL client on `localhost:5433` |
| Stop / start | `make down` / `make up` (data is kept) |
| Start from scratch | `make reset` (deletes all volumes), then `make up` |

### Backfills

```bash
make backfill FROM=2026-10-01 TO=2026-11-01
```

- Creates one run per **schedule tick** in the range: weekdays for the ECB, every day for the NBU.
- `TO` is a timestamp (midnight). To include the 31st, pass the 1st of the next month.
- Only one backfill per DAG can be active at a time. A second `backfill create` for the same DAG is refused until the first one finishes.
- Already successful days are skipped (`--reprocess-behavior none`, the default).
- `build_fx_marts` follows by itself: asset events from many runs are merged into a few marts runs.
- ECB holidays (e.g. 25-26 Dec) end as **success** with skipped tasks. That is expected.
- Watch it: UI → DAG → **Grid**; Admin → **Pools** → `fx_api` shows at most 2 running API calls.

## Useful SQL

```sql
-- How much history do we have?
SELECT 'ecb' AS src, count(DISTINCT rate_date) AS days, min(rate_date), max(rate_date) FROM core.ecb_rates
UNION ALL
SELECT 'nbu', count(DISTINCT rate_date), min(rate_date), max(rate_date) FROM core.nbu_rates;

-- EUR and USD in UAH over the last 2 weeks
SELECT rate_date, currency, rate FROM core.nbu_rates
WHERE currency IN ('EUR', 'USD') AND rate_date >= current_date - 14
ORDER BY rate_date, currency;

-- Biggest moves of watched currencies
SELECT source, rate_date, currency, pct_change
FROM marts.fx_daily_changes
WHERE currency IN ('USD','EUR','GBP','CHF','PLN') AND pct_change IS NOT NULL
ORDER BY abs(pct_change) DESC LIMIT 10;

-- How far apart are the two banks?
SELECT currency, round(avg(abs(deviation_pct)), 3) AS avg_abs_dev_pct, max(abs(deviation_pct)) AS max_abs_dev_pct
FROM marts.fx_cross_rates GROUP BY currency ORDER BY avg_abs_dev_pct DESC;

-- Idempotency check: must always return 0
SELECT count(*) - count(DISTINCT (rate_date, currency)) AS duplicates FROM core.ecb_rates;
```

## Where are the logs?

- UI: task instance → **Logs** (one tab per attempt).
- Files: Docker volume `airflow-logs`, path `dag_id=…/run_id=…/task_id=…/attempt=N.log`.
- Services: `make logs`, or `docker compose logs airflow-dag-processor`.

## Changing code

| You changed | What to do |
| --- | --- |
| a DAG file or `dags/fx_shared/*` | nothing: picked up within ~30 s |
| `src/fx_pipeline/*` | nothing for new task runs (`src/` is mounted, editable install) |
| SQL in `sql/` | nothing: read when the task runs |
| `pyproject.toml` dependencies | `uv lock`, then `make up` (rebuilds the image) |
| `docker-compose.yaml` / `.env` | `make up` |

Before committing: `make lint test`.

## Troubleshooting

| Symptom | Likely cause / fix |
| --- | --- |
| `Bind for 0.0.0.0:8080 failed: port is already allocated` | Another container uses the port. Set `AIRFLOW_PORT=8081` in `.env`, then `make up`. |
| DAG missing in the UI | Wait 30 s; then `make airflow CMD="dags list-import-errors"`. |
| DAG visible but nothing runs | It is paused (`make unpause`), or the scheduler is unhealthy (`make ps`, `make logs`). |
| Tasks stuck in **scheduled/queued** | Pool `fx_api` missing or full (Admin → Pools), or `max_active_runs` reached. |
| ECB sensor `up_for_reschedule` for hours | Normal before ~16:00 Berlin. After 21:00 on a holiday it skips. A manual run on a weekend skips at once. |
| NBU `extract_to_raw` up for retry | Rates for that day not published yet (normal for future dates). |
| `Quality check … failed` | Real data problem: read the message, fix the data/load, Clear the run. |
| `airflow-init` exited with an error | `docker compose logs airflow-init`. Usually the DB or RustFS was not healthy yet; `make up` again. |
| Everything is broken | `make reset && make up` (deletes all data, then reload with a backfill). |
