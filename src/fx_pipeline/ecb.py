"""European Central Bank reference rates, served by the free Frankfurter API.

* Docs: https://frankfurter.dev
* The ECB publishes rates once per TARGET business day, around 16:00 CET.
  No rates on weekends and on TARGET holidays (1 Jan, Good Friday, Easter Monday,
  1 May, 25-26 Dec).
* If you ask Frankfurter for a day without a publication, it silently returns the
  *previous* business day. We use that to detect "not published (yet)".
"""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from enum import StrEnum
from typing import Any
from zoneinfo import ZoneInfo

import requests

from fx_pipeline.models import DataValidationError, RateRecord

BASE_URL = "https://api.frankfurter.dev/v1"
BASE_CURRENCY = "EUR"
ECB_TIMEZONE = ZoneInfo("Europe/Berlin")

# After this local time we stop waiting: if the rate for today is still missing,
# the ECB did not publish today (holiday). Publication is ~16:00, so this is generous.
PUBLICATION_CUTOFF = time(21, 0)


class PublicationStatus(StrEnum):
    PUBLISHED = "published"  # rates for the requested day are available
    PENDING = "pending"  # not yet published, keep waiting
    NOT_PUBLISHED = "not_published"  # the ECB does not publish on this day


def fetch_rates(rate_date: date, *, timeout: float = 30.0) -> dict[str, Any]:
    """Return the raw JSON payload for ``rate_date`` (base EUR)."""
    response = requests.get(
        f"{BASE_URL}/{rate_date.isoformat()}",
        params={"base": BASE_CURRENCY},
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


def payload_date(payload: dict[str, Any]) -> date:
    """The day the payload actually belongs to (may differ from the requested day)."""
    return date.fromisoformat(payload["date"])


def publication_status(
    requested: date, returned: date, now: datetime
) -> PublicationStatus:
    """Decide whether the rates for ``requested`` exist, will exist, or never will.

    * The API serves the requested day -> published.
    * Saturday or Sunday -> never published, no need to wait.
    * A weekday without rates: before the cutoff the ECB may still publish (pending);
      after it, the day was a holiday (not published).

    ``now`` must be timezone-aware; it is converted to the ECB timezone.
    """
    if returned == requested:
        return PublicationStatus.PUBLISHED
    if returned > requested:
        raise DataValidationError(
            f"Asked for {requested}, but the API returned {returned}"
        )
    if requested.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        return PublicationStatus.NOT_PUBLISHED

    now_local = now.astimezone(ECB_TIMEZONE)
    cutoff = datetime.combine(requested, PUBLICATION_CUTOFF, tzinfo=ECB_TIMEZONE)
    if now_local >= cutoff:
        return PublicationStatus.NOT_PUBLISHED
    return PublicationStatus.PENDING


def parse_rates(payload: dict[str, Any], rate_date: date) -> list[RateRecord]:
    """Turn a Frankfurter payload into records. Fails if it is for another day."""
    if payload.get("base") != BASE_CURRENCY:
        raise DataValidationError(
            f"Expected base {BASE_CURRENCY}, got {payload.get('base')!r}"
        )

    returned = payload_date(payload)
    if returned != rate_date:
        raise DataValidationError(f"Payload is for {returned}, expected {rate_date}")

    return [
        RateRecord(rate_date=rate_date, currency=currency, rate=Decimal(str(rate)))
        for currency, rate in sorted(payload["rates"].items())
    ]
