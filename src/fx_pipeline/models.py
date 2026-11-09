"""Shared data structures."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class RateRecord:
    """One exchange rate for one currency on one day.

    The meaning of ``rate`` depends on the source:

    * ECB: units of ``currency`` per 1 EUR   (e.g. USD 1.0956 -> 1 EUR = 1.0956 USD)
    * NBU: UAH per 1 unit of ``currency``    (e.g. USD 38.04  -> 1 USD = 38.04 UAH)
    """

    rate_date: date
    currency: str
    rate: Decimal

    def as_row(self) -> tuple[date, str, Decimal]:
        return (self.rate_date, self.currency, self.rate)


class DataNotReadyError(Exception):
    """The source has not published data for the requested day yet (worth retrying later)."""


class DataValidationError(Exception):
    """The source returned data that must not be loaded (retrying will not help)."""
