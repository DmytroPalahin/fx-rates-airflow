# Airflow concepts, as used in this project

Each section: **what it is → where it is in this repo → what to look at in the UI.**
Interview-style questions are collected at the end.

---

## 1. DAG, task, operator

- **DAG**: a set of tasks plus their order and a schedule. It describes the work, it is not the work itself.
- **Task**: one node of the DAG. A **task instance** is that task for one specific DAG run.
- **Operator**: a template for a task (`SQLExecuteQueryOperator`, `EmptyOperator`, …).
  `@task` turns a Python function into an operator (TaskFlow API).
- **Hook**: a client for an external system that reads credentials from a Connection
  (`S3Hook`, `PostgresHook`). Operators often use hooks inside.

Where: every file in `dags/`. `ingest_ecb_rates.py` mixes both styles: `create_table` is a
classic operator, the other tasks are TaskFlow functions.

UI: DAG list → a DAG → **Graph** tab.

## 2. TaskFlow API

```python
raw_object_key = extract_to_raw()  # returns an XComArg, not a string
loaded = load_to_core(raw_object_key)  # passing it creates the dependency AND the XCom pull
```

Return values are pushed to XCom automatically. Passing one task's output to another sets the
order and pulls the value at runtime. `**context` in the signature gives the task context
(`logical_date`, `params`, `ti`, …).

## 3. Scheduling: `logical_date`, `run_after`, timetables

- `schedule=CronTriggerTimetable("0 16 * * 1-5", timezone="Europe/Berlin")`
  → a run every weekday at 16:00 **Berlin time**, DST included (14:00 UTC in summer, 15:00 in winter).
- With a *trigger* timetable, `logical_date` = the moment the run is for. Our tasks turn it
  into "the day to load" (`fx_shared/runtime.py`).
- `CronDataIntervalTimetable` is the Airflow 2 behaviour: a run covers an *interval* and starts
  at its **end** (the run "for Monday" starts on Tuesday 00:00). Airflow 3 uses trigger semantics
  for cron strings by default.
- `run_after`: the earliest time the run may start. Manual runs in Airflow 3 can have
  `logical_date = None`, which is why `target_date()` falls back to `run_after`.
- `start_date`: no runs before it. It is **not** "when the DAG is deployed".

UI: DAG → **Details** shows the timetable and next run. Run IDs such as
`scheduled__2026-11-06T15:00:00+00:00` show the logical date in UTC.

## 4. `catchup` and backfill

- `catchup=True`: when a DAG is unpaused, Airflow creates a run for **every** missed
  interval since `start_date`. Our ECB `start_date` is 1999, so that would be ~7,000 runs:
  `catchup=False` (the Airflow 3 default).
- **Backfill** = run a DAG for a chosen past period, on purpose:

  ```bash
  airflow backfill create --dag-id ingest_ecb_rates --from-date 2026-10-01 --to-date 2026-11-01
  ```

  In Airflow 3 the scheduler executes backfills (you can also start them from the UI).
  Options: `--max-active-runs`, `--reprocess-behavior none|failed|completed`, `--run-backwards`, `--dry-run`.
- Gotcha: the dates are **timestamps**. `--to-date 2026-11-05` means 2026-11-05 00:00, so the
  16:00 run of that day is *not* included. Use the next day as `--to-date`.

## 5. Idempotency

The property that makes retries, clears and backfills safe: running a task twice for the
same logical date gives the same result as running it once.
Here: deterministic raw keys + "delete the day, insert the day" in one transaction
(`src/fx_pipeline/storage.py`). See `docs/architecture.md`, decision 2.

## 6. Sensors

A sensor is a task that waits for a condition.

| Mode | While waiting | Use when |
| --- | --- | --- |
| `poke` (default) | holds a worker slot and sleeps | the wait is seconds or a few minutes |
| `reschedule` | gives the slot back. State **up_for_reschedule**, retried after `poke_interval` | the wait is long (our ECB sensor) |
| deferrable (`deferrable=True` on supporting operators) | the task is suspended. An async **trigger** runs in the triggerer process | many long waits. The cheapest option. |

Where: `wait_for_publication` in `ingest_ecb_rates.py` (`@task.sensor`, returns
`PokeReturnValue(is_done=…)`). It raises `AirflowSkipException` on weekends (manual runs
only: the schedule has no weekend runs) and on ECB holidays.
Skipped tasks are not failures: the run ends **success**, downstream tasks are **skipped**.

UI: trigger the ECB DAG manually before 16:00 Berlin and watch the sensor go
`up_for_reschedule` in the **Grid**.

## 7. Retries, failures, callbacks

- `DEFAULT_ARGS` (`fx_shared/settings.py`): `retries=3`, `retry_delay=2 min`,
  `retry_exponential_backoff=2.0` (2, 4, 8 min…), `max_retry_delay`, `execution_timeout`.
- A task can override them: the NBU `extract_to_raw` uses `retries=6` every 15 min.
- `AirflowFailException`: fail **now**, skip remaining retries (bad data won't fix itself).
- `AirflowSkipException`: mark the task skipped.
- `on_failure_callback=notify_failure`: runs when a task finally fails. Same idea:
  `on_success_callback`, `on_retry_callback`. Providers ship ready-made notifiers (Slack, SMTP…).

UI: a failed try shows as **up_for_retry**. Each attempt has its own log tab.

## 8. XCom

Small values passed between tasks, stored in the metadata DB.

- ✅ object keys, dates, row counts, a short list of anomalies
- ❌ dataframes, files, API payloads: store them in S3 / a DB and pass the **reference**.

Where: `extract_to_raw` returns the S3 key; `resolve_date_range` returns
`{"start_date": …, "end_date": …}` which `SQLExecuteQueryOperator` uses as SQL `parameters`.

UI: task instance → **XCom** tab.

## 9. Connections and hooks

A Connection = how to reach a system (type, host, login, password, extra JSON), referenced by id.

- Here they come from env vars: `AIRFLOW_CONN_FX_POSTGRES`, `AIRFLOW_CONN_FX_S3` (`docker-compose.yaml`).
  Env-var connections work everywhere but are **not listed** in Admin → Connections,
  because they are not stored in the metadata DB. `airflow connections get fx_s3` resolves them.
- Connections created in the UI are stored encrypted with the **Fernet key**.
- In production: a secrets backend (Vault, AWS Secrets Manager, GCP Secret Manager).

## 10. Variables and Params

- **Variables**: global key/value settings (Admin → Variables). Here: `fx_alert_change_pct`,
  `fx_alert_deviation_pct`. Optional, with defaults in code.
  Read them **inside tasks**, never at the top level of a DAG file (it runs on every parse).
- **Params**: per-run input with a JSON-schema form in the UI "Trigger" dialog.
  `build_fx_marts` has `full_refresh` (boolean) and `lookback_days` (integer).
  CLI: `airflow dags trigger build_fx_marts --conf '{"full_refresh": true}'`.

## 11. Concurrency: pools, `max_active_runs`, `max_active_tasks`

| Setting | Limits |
| --- | --- |
| `max_active_runs` (DAG) | parallel runs of one DAG (4 for ingest, 1 for marts) |
| `max_active_tasks` (DAG) | parallel task instances inside one DAG |
| **pool** (`pool="fx_api"`, 2 slots) | parallel tasks using a shared resource, **across all DAGs** |
| `parallelism` (config) | total running tasks per scheduler |

The `fx_api` pool caps calls to the public APIs during a backfill: even with 8 runs
active, at most 2 API calls happen at once. UI: Admin → **Pools** shows used/queued slots.

## 12. Assets (data-aware scheduling)

- An `Asset` is a name/URI for a piece of data (`fx_shared/assets.py`).
- Producer: `@task(outlets=[ECB_RATES])`. When the task succeeds, Airflow records an **asset event**.
  The task can attach metadata: `context["outlet_events"][ECB_RATES].extra = {...}`.
- Consumer: `schedule=(ECB_RATES & NBU_RATES)`. The DAG runs when both got events since its last run.
  `|` would mean "either". `AssetOrTimeSchedule` mixes a cron with assets.
- The consumer reads `context["triggering_asset_events"]` to know what changed.

Compared with older ways to chain DAGs:

- `TriggerDagRunOperator` (the producer starts the consumer): the producer has to know its consumers.
- `ExternalTaskSensor` (the consumer waits for a task of another DAG): it is coupled to that DAG's schedule.
- Assets: producer and consumer only share the data's name.

UI: **Assets** page (producers, consumers and events of each asset). In the DAG list the
schedule of `build_fx_marts` shows its two assets and which ones already have new events.
API: `GET /api/v2/dags/build_fx_marts/assets/queuedEvents` lists the events still waiting for their pair.

## 13. Dynamic task mapping

```python
run_quality_check.expand(check=[c.as_dict() for c in marts.QUALITY_CHECKS])
```

One task instance per list element, created **at runtime** (the list could also come from
an upstream task's XCom). `map_index_template="{{ check_name }}"` shows the rule name in the UI
instead of `[0]`, `[1]`…
A Python `for` loop that creates tasks at parse time also works, but the list is then fixed
when the DAG file is parsed.

## 14. Branching, trigger rules, skips

- `@task.branch` returns the `task_id`(s) to follow. The other direct downstream tasks are skipped.
- **Trigger rules** decide when a task may run, based on its upstream tasks' states:
  default `all_success`. Others: `all_done`, `one_failed`, `none_failed`,
  `none_failed_min_one_success`, … A task *joining* two branches usually needs
  `none_failed_min_one_success`, otherwise the skip propagates into it.

Where: `route_alerts` → `report_anomalies` | `no_anomalies` in `build_fx_marts.py`.

## 15. TaskGroups

A visual and logical grouping of tasks (`quality_checks` in the marts DAG). Task ids become
`quality_checks.run_quality_check`. TaskGroups replace SubDAGs, which were removed in Airflow 3.

## 16. Templating

Operator fields listed in `template_fields` are rendered with Jinja before execution.
`SQLExecuteQueryOperator(sql="ddl/ecb_rates.sql")` loads the file from `template_searchpath`
(`SQL_DIR`) and renders it, so `{{ ds }}` etc. would work inside the SQL.
Our SQL uses **DB-API parameters** (`%(start_date)s`) instead: the database escapes them, which avoids SQL injection.

## 17. Architecture and executors

See `docs/architecture.md`. Short version:
dag-processor parses → metadata DB ← scheduler decides → executor runs → task talks to api-server.
Executors: **Local** (subprocesses, one machine), **Celery** (queue + worker fleet),
**Kubernetes** (one pod per task), **Edge** (remote workers over HTTP, new in Airflow 3).

## 18. DAG parsing and top-level code

The dag-processor imports every DAG file again and again. Anything at module level
(API calls, DB queries, `Variable.get`) runs on **every parse**. Keep top-level code to imports
and DAG/task definitions. Logic goes inside tasks.

## 19. Testing

`tests/test_dag_integrity.py` loads `dags/` with `DagBag`, like the dag-processor does:

- no import errors, all DAGs present;
- conventions (tags, docs, owner, retries);
- structure (task dependencies, sensor mode, branch targets, assets wiring).

Business logic is unit-tested separately (`tests/test_*.py`). `dag.test()` can also run a whole
DAG in one process for debugging, but it needs the connections to be reachable.

---

## What changed from Airflow 2 to Airflow 3 (frequent question)

| Airflow 2 | Airflow 3 |
| --- | --- |
| `webserver` (Flask) | `api-server` (FastAPI) + new React UI |
| tasks access the metadata DB directly | **Task Execution API / Task SDK**: tasks go through the API server (`from airflow.sdk import …`) |
| DAG parsing inside the scheduler (optional standalone processor) | `dag-processor` is a separate, required component |
| Datasets | **Assets** (+ metadata on events, asset watchers for external events) |
| `schedule_interval=` / `timetable=` | only `schedule=` |
| cron → data intervals, run starts at the interval end | cron → `CronTriggerTimetable` by default |
| `catchup=True` by default | `catchup=False` by default |
| `execution_date`, `next_ds`, `prev_ds`… | `logical_date` (may be `None` for manual runs), `run_after` |
| SubDAGs, SLAs | removed. Use TaskGroups and **Deadline Alerts** |
| `airflow dags backfill` runs in your terminal | `airflow backfill create`: the scheduler runs it, also from the UI |
| — | DAG versioning: each run remembers which version of the DAG it used |
| FAB auth built in | `SimpleAuthManager` by default, FAB is a provider |
| `PythonOperator`, `BashOperator` in core | moved to `apache-airflow-providers-standard` |

---

## Interview questions (answer them using this project)

1. **What does idempotent mean for a task, and how did you make your loads idempotent?**
   Same input logical date → same end state. Deterministic raw keys + delete/insert per day in one transaction.
2. **`logical_date` vs `data_interval` vs `run_after`?** See §3.
3. **`catchup` vs backfill?** Automatic on unpause vs explicit for a chosen range. §4.
4. **How do tasks exchange data? Limits of XCom?** §8. References, not payloads.
5. **Sensor modes? Why `reschedule` for the ECB?** §6. Waiting up to 6 h without holding a slot.
6. **How do you handle data that arrives late?** Sensor (ECB) vs retries (NBU). Trade-offs in §5 of the architecture doc.
7. **Retry or fail fast?** Transient errors → retry with backoff; validation errors → `AirflowFailException`.
8. **How do you chain DAGs?** Assets vs `TriggerDagRunOperator` vs `ExternalTaskSensor` (§12).
9. **How do you limit load on an external API during a backfill?** Pool + `max_active_runs` (§11).
10. **Dynamic task mapping vs a for-loop?** Runtime vs parse time (§13).
11. **Why must DAG files be light?** They are parsed every 30 s (§18).
12. **Executors?** §17. When would you move from Local to Celery/Kubernetes?
13. **How do you test DAGs and deploy them?** DagBag integrity tests + unit tests in CI; image/dags sync on merge.
14. **Where do secrets live?** Connections, env vars / secrets backend, Fernet key (§9).
15. **What is new in Airflow 3?** Table above.
16. **A task failed at 3 a.m. What do you do?** Logs of the failed try → fix → **Clear** the task
    (downstream included). It is safe because the task is idempotent.
