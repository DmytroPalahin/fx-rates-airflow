"""National Bank of Ukraine official exchange rates.

* Docs: https://bank.gov.ua/ua/open-data/api-dev
* The NBU sets the official rate for day D in advance, on D-1 in the afternoon (Kyiv time).
* Unlike the ECB, the NBU has a rate for **every calendar day** (weekend rates repeat Friday's).
* For a day that is not set yet, the API returns an empty list.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

import requests

from fx_pipeline.models import DataNotReadyError, DataValidationError, RateRecord

BASE_URL = "https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange"
QUOTE_CURRENCY = "UAH"
NBU_TIMEZONE = ZoneInfo("Europe/Kyiv")


def fetch_rates(rate_date: date, *, timeout: float = 30.0) -> list[dict[str, Any]]:
    """Return the raw JSON payload for ``rate_date``.

    Raises :class:`DataNotReadyError` when the NBU has not set the rates yet,
    so the calling Airflow task fails and is retried later.
    """
    response = requests.get(
        BASE_URL,
        params={"date": rate_date.strftime("%Y%m%d"), "json": ""},
        timeout=timeout,
    )
    response.raise_for_status()
    payload = response.json()
    if not payload:
        raise DataNotReadyError(f"NBU has not published rates for {rate_date} yet")
    return payload


def parse_rates(payload: list[dict[str, Any]], rate_date: date) -> list[RateRecord]:
    """Turn an NBU payload into records. Fails if any row is for another day."""
    records = []
    for row in payload:
        row_date = datetime.strptime(row["exchangedate"], "%d.%m.%Y").date()
        if row_date != rate_date:
            raise DataValidationError(
                f"Row for {row['cc']} is dated {row_date}, expected {rate_date}"
            )
        records.append(
            RateRecord(rate_date=rate_date, currency=row["cc"], rate=Decimal(str(row["rate"])))
        )
    return sorted(records, key=lambda r: r.currency)
