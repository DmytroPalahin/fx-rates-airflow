from __future__ import annotations

from datetime import date

from fx_pipeline import marts

TODAY = date(2026, 11, 7)


def test_full_refresh_rebuilds_history():
    rng = marts.resolve_date_range([date(2026, 11, 6)], today=TODAY, full_refresh=True)
    assert rng == marts.DateRange(marts.HISTORY_START, TODAY)


def test_upstream_dates_define_the_range():
    changed = [date(2026, 11, 6), date(2026, 11, 2), date(2026, 11, 6)]
    rng = marts.resolve_date_range(changed, today=TODAY)
    assert rng == marts.DateRange(date(2026, 11, 2), date(2026, 11, 6))


def test_manual_run_without_events_uses_lookback():
    rng = marts.resolve_date_range([], today=TODAY, lookback_days=3)
    assert rng.as_params() == {"start_date": "2026-11-04", "end_date": "2026-11-07"}


def test_quality_checks_have_unique_names_and_are_serializable():
    names = [c.name for c in marts.QUALITY_CHECKS]
    assert len(names) == len(set(names))
    assert all(
        set(c.as_dict()) == {"name", "description", "sql"} for c in marts.QUALITY_CHECKS
    )


def test_anomaly_report_lists_every_row():
    text = marts.format_anomaly_report(
        [
            {
                "kind": "daily_change",
                "source": "nbu",
                "rate_date": "2026-11-06",
                "currency": "USD",
                "value": 2.1,
                "threshold": 1.5,
            }
        ]
    )
    assert "FX anomalies detected: 1" in text
    assert "nbu:USD value=2.1%" in text
