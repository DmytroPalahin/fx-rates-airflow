"""Test setup: isolate Airflow from any real installation before it is imported."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

import pytest

# Must happen before the first `import airflow` anywhere in the test session.
os.environ.setdefault("AIRFLOW_HOME", tempfile.mkdtemp(prefix="airflow-home-"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")

FIXTURES = Path(__file__).parent / "fixtures"
PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def ecb_payload() -> dict:
    return json.loads((FIXTURES / "ecb_2024-01-02.json").read_text())


@pytest.fixture
def nbu_payload() -> list[dict]:
    return json.loads((FIXTURES / "nbu_2024-01-02.json").read_text())
