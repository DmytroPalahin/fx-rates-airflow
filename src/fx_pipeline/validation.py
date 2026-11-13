"""Validation of parsed records before they reach the warehouse."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Sequence

from fx_pipeline.models import DataValidationError, RateRecord


def validate_records(
    records: Sequence[RateRecord], required_currencies: Iterable[str] = ()
) -> None:
    """Raise :class:`DataValidationError` describing every problem found."""
    problems: list[str] = []

    if not records:
        problems.append("no records")

    duplicates = [cur for cur, n in Counter(r.currency for r in records).items() if n > 1]
    if duplicates:
        problems.append(f"duplicate currencies: {sorted(duplicates)}")

    non_positive = [r.currency for r in records if r.rate <= 0]
    if non_positive:
        problems.append(f"non-positive rates: {sorted(non_positive)}")

    bad_codes = [r.currency for r in records if len(r.currency) != 3 or not r.currency.isalpha()]
    if bad_codes:
        problems.append(f"invalid currency codes: {sorted(bad_codes)}")

    present = {r.currency for r in records}
    missing = sorted(set(required_currencies) - present)
    if missing:
        problems.append(f"missing required currencies: {missing}")

    if len({r.rate_date for r in records}) > 1:
        problems.append("records span more than one day")

    if problems:
        raise DataValidationError("; ".join(problems))
