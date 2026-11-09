# Airflow image for this project = official image + our `fx_pipeline` package.
#
# Why a Dockerfile instead of _PIP_ADDITIONAL_REQUIREMENTS: dependencies are
# installed once at build time (not on every container start), and the image is
# reproducible.

ARG AIRFLOW_VERSION=3.4.0
ARG PYTHON_VERSION=3.12
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

# ARGs declared before FROM are not visible after it, so re-declare.
ARG AIRFLOW_VERSION

WORKDIR /opt/airflow/project

# Only what is needed to install the package (see .dockerignore).
COPY --chown=airflow:root pyproject.toml README.md ./
COPY --chown=airflow:root src ./src

# The official image ships with uv and a virtualenv at /home/airflow/.local.
# Pinning apache-airflow guarantees our dependencies can never up/downgrade Airflow.
# Editable install: in docker-compose ./src is mounted over ./src, so code changes
# are picked up without rebuilding the image.
RUN uv pip install --no-cache "apache-airflow==${AIRFLOW_VERSION}" --editable .

WORKDIR /opt/airflow
