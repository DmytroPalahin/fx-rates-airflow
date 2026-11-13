# Contributing

Thank you for taking the time to contribute. This document explains how to set up the project,
what is expected of a change, and how a release is made.

## Getting started

You need [uv](https://docs.astral.sh/uv/) and `make`; Docker Desktop only to run the stack.

```bash
make setup       # .venv with Airflow, pytest and ruff + a .env with generated secrets
make lint test   # the definition of "it works"; CI runs the same checks
```

`make up` starts the whole stack (see the [runbook](docs/runbook.md)).

## Making a change

1. Open an issue first for anything larger than a fix, so the approach can be discussed.
2. Create a branch from `main`: `feat/nbp-source`, `fix/sensor-weekend`, ...
3. Make the change, with tests.
4. Update the documentation that the change makes inaccurate: `README.md` and `docs/`.
5. Add a line to the "Unreleased" section of [CHANGELOG.md](CHANGELOG.md).
6. Check that `make lint test` passes, then open a pull request that explains the *why*.

## Conventions

- **DAGs stay thin.** A DAG file describes when and in what order tasks run. Logic goes to
  `src/fx_pipeline`, which must not import Airflow, and gets unit tests.
- **No heavy top-level code in DAG files.** They are parsed every 30 seconds: no API calls,
  no database queries, no `Variable.get` outside tasks.
- **Every task is idempotent.** Running it twice for the same day gives the same result.
- **The day comes from the run** (`logical_date`, or `run_after` for manual runs), never from
  `date.today()`.
- **Retry what can heal, fail fast on bad data** (`AirflowFailException`).
- **Secrets never go in the code.** Connections come from `AIRFLOW_CONN_*` variables in `.env`.
- Formatting and imports are handled by ruff (`make fmt`).

## Commit messages

Commits follow [Conventional Commits](https://www.conventionalcommits.org/):

```text
<type>(<scope>): <summary in the imperative, lower case, no period>
```

Types: `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `build`, `ci`, `chore`. The scope is
usually a DAG or a module: `ecb`, `nbu`, `marts`, `storage`, `docker`.

## Releasing

1. Move the "Unreleased" entries of `CHANGELOG.md` under a new version heading with today's date.
2. Set the version in `pyproject.toml` and run `uv lock`.
3. Commit as `chore(release): x.y.z`, tag the commit `vx.y.z` and push the tag.
4. Create the GitHub release from the tag, with the changelog entry as notes.
