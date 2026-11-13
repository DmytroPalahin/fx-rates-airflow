# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

First version of the pipeline.

### Added

- **`ingest_ecb_rates`**: loads the ECB reference rates (via Frankfurter) on weekdays at 16:00
  Berlin. A sensor in reschedule mode waits for the publication and skips the run on ECB
  holidays and weekends.
- **`ingest_nbu_rates`**: loads the NBU rates every day at 09:00 Kyiv, retrying while the rates
  are not published yet.
- **Raw layer** in S3-compatible storage (RustFS): the API response as received, one file per
  source and day.
- **Core layer** in Postgres: validated rates, loaded idempotently (delete + insert of the day in
  one transaction).
- **`build_fx_marts`**: triggered by the assets of both sources; rebuilds the changed days of
  `marts.fx_daily_changes` and `marts.fx_cross_rates`, runs five mapped quality checks, and
  branches to an anomaly report when a watched currency moves beyond a threshold.
- Params for manual runs (`full_refresh`, `lookback_days`), Variables for the alert thresholds,
  a pool limiting concurrent API calls.
- Local stack with Docker Compose: Airflow 3.4 (LocalExecutor), Postgres warehouse, RustFS.
- `Makefile` shortcuts, including backfills.
- Unit tests and DAG integrity tests; CI with GitHub Actions (lint, tests, image build).
- Documentation: architecture, Airflow concepts, runbook, exercises and a one-page summary.

[Unreleased]: https://github.com/DmytroPalahin/fx-rates-airflow/commits/main
