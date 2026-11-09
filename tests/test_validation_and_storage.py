from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from fx_pipeline import storage
from fx_pipeline.models import DataValidationError, RateRecord
from fx_pipeline.validation import validate_records

DAY = date(2024, 1, 2)


def rec(currency: str, rate: str, day: date = DAY) -> RateRecord:
    return RateRecord(rate_date=day, currency=currency, rate=Decimal(rate))


def test_valid_records_pass():
    validate_records(
        [rec("USD", "1.09"), rec("GBP", "0.86")], required_currencies=["USD"]
    )


@pytest.mark.parametrize(
    ("records", "message"),
    [
        ([], "no records"),
        ([rec("USD", "1.0"), rec("USD", "1.1")], "duplicate"),
        ([rec("USD", "0")], "non-positive"),
        ([rec("US1", "1.0")], "invalid currency"),
        ([rec("GBP", "0.86")], "missing required currencies: ['USD']"),
        ([rec("USD", "1.0"), rec("GBP", "0.8", date(2024, 1, 3))], "more than one day"),
    ],
)
def test_invalid_records_fail(records, message):
    with pytest.raises(
        DataValidationError, match=message.replace("[", r"\[").replace("]", r"\]")
    ):
        validate_records(records, required_currencies=["USD"])


def test_raw_key_is_partitioned_by_day():
    assert storage.raw_key("ecb", DAY) == "ecb/rate_date=2024-01-02/rates.json"


def test_replace_day_deletes_then_inserts_in_one_transaction():
    conn = MagicMock()
    cursor = conn.cursor.return_value

    rows = storage.replace_day(
        conn, "nbu", DAY, [rec("USD", "37.98"), rec("EUR", "42.06")]
    )

    assert rows == 2
    delete_sql, delete_params = cursor.execute.call_args.args
    assert delete_sql.startswith("DELETE FROM core.nbu_rates")
    assert delete_params == (DAY,)
    insert_sql, insert_rows = cursor.executemany.call_args.args
    assert "INSERT INTO core.nbu_rates" in insert_sql
    assert insert_rows == [
        (DAY, "USD", Decimal("37.98")),
        (DAY, "EUR", Decimal("42.06")),
    ]
    conn.commit.assert_called_once()
    conn.rollback.assert_not_called()


def test_replace_day_rolls_back_on_error():
    conn = MagicMock()
    conn.cursor.return_value.executemany.side_effect = RuntimeError("boom")

    with pytest.raises(RuntimeError):
        storage.replace_day(conn, "ecb", DAY, [rec("USD", "1.09")])

    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()
