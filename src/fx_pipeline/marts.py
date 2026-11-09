"""Helpers for building marts: which days to rebuild, quality checks, anomaly report."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from typing import Any

# The first day the ECB published euro reference rates.
HISTORY_START = date(1999, 1, 4)

# Currencies we care about for alerts. Exotic currencies move a lot and would be noise.
WATCHLIST: tuple[str, ...] = ("USD", "GBP", "CHF", "JPY", "PLN", "CAD", "CNY", "EUR")


@dataclass(frozen=True, slots=True)
class DateRange:
    start_date: date
    end_date: date

    def as_params(self) -> dict[str, str]:
        """JSON-friendly form, usable as XCom value and as SQL parameters."""
        return {
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
        }


def resolve_date_range(
    changed_dates: Iterable[date],
    *,
    today: date,
    full_refresh: bool = False,
    lookback_days: int = 7,
) -> DateRange:
    """Pick the days a marts run has to rebuild.

    * ``full_refresh``      -> the whole history
    * dates from upstream    -> exactly the span that changed (incremental run)
    * nothing known (manual) -> the last ``lookback_days`` days
    """
    if full_refresh:
        return DateRange(HISTORY_START, today)

    dates = sorted(set(changed_dates))
    if dates:
        return DateRange(dates[0], dates[-1])

    return DateRange(today - timedelta(days=lookback_days), today)


@dataclass(frozen=True, slots=True)
class QualityCheck:
    """A data quality rule: ``sql`` returns the number of violating rows, 0 means OK."""

    name: str
    description: str
    sql: str

    def as_dict(self) -> dict[str, str]:
        return asdict(self)


QUALITY_CHECKS: tuple[QualityCheck, ...] = (
    QualityCheck(
        name="ecb_rates_positive",
        description="Every ECB rate is > 0",
        sql="SELECT COUNT(*) FROM core.ecb_rates WHERE rate <= 0",
    ),
    QualityCheck(
        name="nbu_rates_positive",
        description="Every NBU rate is > 0",
        sql="SELECT COUNT(*) FROM core.nbu_rates WHERE rate <= 0",
    ),
    QualityCheck(
        name="ecb_no_weekend_rows",
        description="The ECB never publishes on Saturday or Sunday",
        sql="SELECT COUNT(*) FROM core.ecb_rates WHERE EXTRACT(ISODOW FROM rate_date) IN (6, 7)",
    ),
    QualityCheck(
        name="ecb_usd_every_day",
        description="Every ECB day contains USD (needed for cross rates)",
        sql="""
            SELECT COUNT(*) FROM (
                SELECT rate_date FROM core.ecb_rates
                GROUP BY rate_date
                HAVING COUNT(*) FILTER (WHERE currency = 'USD') = 0
            ) AS days_without_usd
        """,
    ),
    QualityCheck(
        name="nbu_usd_eur_every_day",
        description="Every NBU day contains USD and EUR (needed for cross rates)",
        sql="""
            SELECT COUNT(*) FROM (
                SELECT rate_date FROM core.nbu_rates
                GROUP BY rate_date
                HAVING COUNT(*) FILTER (WHERE currency IN ('USD', 'EUR')) < 2
            ) AS incomplete_days
        """,
    ),
)


def format_anomaly_report(anomalies: Sequence[dict[str, Any]]) -> str:
    """Human-readable alert text (what you would send to Slack/Telegram/email)."""
    lines = [f"FX anomalies detected: {len(anomalies)}"]
    for a in anomalies:
        lines.append(
            f"  [{a['kind']}] {a['rate_date']} {a['source']}:{a['currency']} "
            f"value={a['value']}% (threshold {a['threshold']}%)"
        )
    return "\n".join(lines)
