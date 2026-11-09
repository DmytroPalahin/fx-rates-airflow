from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from fx_pipeline import nbu
from fx_pipeline.models import DataNotReadyError, DataValidationError

DAY = date(2024, 1, 2)


def test_parse_rates(nbu_payload):
    records = nbu.parse_rates(nbu_payload, DAY)

    by_currency = {r.currency: r.rate for r in records}
    assert by_currency["USD"] == Decimal("38.0144")
    assert by_currency["JPY"] == Decimal("0.26843")
    assert [r.currency for r in records] == sorted(by_currency)


def test_parse_rates_rejects_rows_for_another_day(nbu_payload):
    with pytest.raises(DataValidationError, match="expected 2024-01-03"):
        nbu.parse_rates(nbu_payload, date(2024, 1, 3))


def test_fetch_rates_raises_not_ready_on_empty_payload():
    response = MagicMock()
    response.json.return_value = []
    with (
        patch("fx_pipeline.nbu.requests.get", return_value=response) as get,
        pytest.raises(DataNotReadyError),
    ):
        nbu.fetch_rates(DAY)

    assert get.call_args.kwargs["params"]["date"] == "20240102"
