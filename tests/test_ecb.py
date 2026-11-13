from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from fx_pipeline import ecb
from fx_pipeline.models import DataValidationError

BERLIN = ZoneInfo("Europe/Berlin")
TUESDAY = date(2024, 1, 2)


def test_parse_rates(ecb_payload):
    records = ecb.parse_rates(ecb_payload, TUESDAY)

    usd = next(r for r in records if r.currency == "USD")
    assert usd.rate == Decimal("1.0956")
    assert usd.rate_date == TUESDAY
    assert len(records) == len(ecb_payload["rates"])
    assert [r.currency for r in records] == sorted(r.currency for r in records)


def test_parse_rates_rejects_payload_for_another_day(ecb_payload):
    with pytest.raises(DataValidationError, match="expected 2024-01-03"):
        ecb.parse_rates(ecb_payload, date(2024, 1, 3))


def test_parse_rates_rejects_other_base(ecb_payload):
    with pytest.raises(DataValidationError, match="base"):
        ecb.parse_rates({**ecb_payload, "base": "USD"}, TUESDAY)


@pytest.mark.parametrize(
    ("requested", "returned", "now", "expected"),
    [
        # The API already serves today's rates.
        (TUESDAY, TUESDAY, datetime(2024, 1, 2, 16, 5, tzinfo=BERLIN), "published"),
        # 15:00, the ECB has not published yet -> keep waiting.
        (
            TUESDAY,
            date(2024, 1, 1),
            datetime(2024, 1, 2, 15, 0, tzinfo=BERLIN),
            "pending",
        ),
        # 1 January: no publication. Before the cutoff we still wait...
        (
            date(2024, 1, 1),
            date(2023, 12, 29),
            datetime(2024, 1, 1, 17, 0, tzinfo=BERLIN),
            "pending",
        ),
        # ...after the cutoff (or in a backfill, days later) it is final.
        (
            date(2024, 1, 1),
            date(2023, 12, 29),
            datetime(2024, 1, 1, 21, 0, tzinfo=BERLIN),
            "not_published",
        ),
        (
            date(2024, 1, 1),
            date(2023, 12, 29),
            datetime(2026, 11, 7, 9, 0, tzinfo=BERLIN),
            "not_published",
        ),
    ],
)
def test_publication_status(requested, returned, now, expected):
    assert ecb.publication_status(requested, returned, now) == expected


def test_publication_status_weekend_is_final_without_waiting():
    # A manual run on Sunday morning: the API falls back to Friday. No ECB rates
    # on weekends, so do not wait for the cutoff (the sensor would time out).
    sunday, friday = date(2026, 11, 8), date(2026, 11, 6)
    morning = datetime(2026, 11, 8, 10, 0, tzinfo=BERLIN)
    assert ecb.publication_status(sunday, friday, morning) == "not_published"


def test_publication_status_handles_utc_now():
    # 20:30 UTC in January = 21:30 in Berlin -> past the cutoff.
    now_utc = datetime(2024, 1, 1, 20, 30, tzinfo=ZoneInfo("UTC"))
    assert ecb.publication_status(date(2024, 1, 1), date(2023, 12, 29), now_utc) == "not_published"


def test_publication_status_rejects_future_payload():
    with pytest.raises(DataValidationError):
        ecb.publication_status(TUESDAY, date(2024, 1, 3), datetime(2024, 1, 3, tzinfo=BERLIN))
