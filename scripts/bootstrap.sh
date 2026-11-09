#!/usr/bin/env bash
# One-off initialisation, run by the `airflow-init` service before the other
# Airflow services start. Every step is idempotent: safe to run on every `up`.
set -euo pipefail

echo "==> Airflow $(airflow version)"

echo "==> Migrating the metadata database"
airflow db migrate

echo "==> Creating pool 'fx_api' (max 2 concurrent calls to the public FX APIs)"
airflow pools set fx_api 2 "Max concurrent calls to the public FX APIs"

echo "==> Creating bucket 'fx-raw' in object storage"
python - <<'PY'
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

hook = S3Hook(aws_conn_id="fx_s3")
if hook.check_for_bucket("fx-raw"):
    print("bucket fx-raw already exists")
else:
    hook.create_bucket("fx-raw")
    print("bucket fx-raw created")
PY

echo "==> Bootstrap done"
