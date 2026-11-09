"""Helpers that read the Airflow task context."""

from __future__ import annotations

from datetime import date, tzinfo
from typing import Any

import pendulum


def target_date(context: dict[str, Any], tz: tzinfo) -> date:
    """The business day a run is responsible for, in the source's own timezone.

    With a ``CronTriggerTimetable`` the ``logical_date`` is the moment the run was
    scheduled for (e.g. 2026-11-06 16:00 Europe/Berlin), so its local date *is* the
    day we load. Backfills create one run per cron tick, so they get past days.

    In Airflow 3 a manually triggered run may have no logical_date at all; then we
    fall back to ``run_after`` (when the run was requested).
    """
    moment = context.get("logical_date") or context["dag_run"].run_after
    return pendulum.instance(moment).in_timezone(tz).date()
