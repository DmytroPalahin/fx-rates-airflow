"""Naming of raw files and idempotent loading into the warehouse.

Everything here works with plain values and a DB-API connection, so it does not
depend on Airflow and can be reused or tested anywhere.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from typing import Any, Literal

from fx_pipeline.models import RateRecord

Source = Literal["ecb", "nbu"]

# Allow-list: table names cannot be passed as SQL parameters, so never format
# arbitrary strings into SQL.
CORE_TABLES: dict[Source, str] = {
    "ecb": "core.ecb_rates",
    "nbu": "core.nbu_rates",
}


def raw_key(source: Source, rate_date: date) -> str:
    """Object key of the raw payload, Hive-style partitioned by day.

    Re-running a day overwrites the same key, so the raw layer is idempotent too.
    """
    return f"{source}/rate_date={rate_date.isoformat()}/rates.json"


def replace_day(
    conn: Any, source: Source, rate_date: date, records: Sequence[RateRecord]
) -> int:
    """Replace all rows of ``rate_date`` in one transaction ("delete + insert").

    This is what makes the load idempotent: running it twice for the same day
    gives the same result as running it once, and currencies that disappeared
    from the source disappear from the table as well.
    """
    table = CORE_TABLES[source]
    cursor = conn.cursor()
    try:
        cursor.execute(f"DELETE FROM {table} WHERE rate_date = %s", (rate_date,))
        cursor.executemany(
            f"INSERT INTO {table} (rate_date, currency, rate) VALUES (%s, %s, %s)",
            [record.as_row() for record in records],
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
    return len(records)
