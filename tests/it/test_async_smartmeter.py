"""AsyncSmartmeter chunking and deduplication tests."""
import datetime as dt

import pytest

import it  # noqa: F401  # Ensures custom_components path is available for wnsmeter30 imports
from wnsmeter30.AsyncSmartmeter import AsyncSmartmeter
from wnsmeter30.const import HISTORICAL_API_CHUNK_DAYS


class _DummyHass:
    async def async_add_executor_job(self, func, *args):
        return func(*args)


class _DummySmartmeter:
    def __init__(self) -> None:
        self.daily_calls: list[tuple[str, dt.datetime, dt.datetime]] = []

    def historical_day_consumption(
        self, zaehlpunkt: str, start: dt.datetime, end: dt.datetime
    ) -> dict:
        self.daily_calls.append((zaehlpunkt, start, end))
        if len(self.daily_calls) == 1:
            return {
                "obisCode": "1-1:1.9.0",
                "einheit": "WH",
                "messwerte": [
                    {
                        "messwert": 1000,
                        "zeitVon": "2025-12-31T00:00:00Z",
                        "zeitBis": "2026-01-01T00:00:00Z",
                        "qualitaet": "VAL",
                    },
                    {
                        "messwert": 2000,
                        "zeitVon": "2026-01-01T00:00:00Z",
                        "zeitBis": "2026-01-02T00:00:00Z",
                        "qualitaet": "VAL",
                    },
                ],
            }
        return {
            "obisCode": "1-1:1.9.0",
            "einheit": "WH",
            "messwerte": [
                {
                    "messwert": 2500,
                    "zeitVon": "2026-01-01T00:00:00Z",
                    "zeitBis": "2026-01-02T00:00:00Z",
                    "qualitaet": "VAL",
                },
                {
                    "messwert": 3000,
                    "zeitVon": "2026-01-02T00:00:00Z",
                    "zeitBis": "2026-01-03T00:00:00Z",
                    "qualitaet": "VAL",
                },
            ],
        }


def test_build_chunk_ranges_splits_into_year_chunks():
    start = dt.datetime(2025, 1, 1, 12, 0, tzinfo=dt.timezone.utc)
    end = dt.datetime(2026, 1, 10, 6, 0, tzinfo=dt.timezone.utc)

    ranges = AsyncSmartmeter._build_chunk_ranges(start, end)

    assert len(ranges) == 2
    assert ranges[0] == (
        dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc),
        dt.datetime(2025, 12, 31, tzinfo=dt.timezone.utc),
    )
    assert ranges[1] == (
        dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc),
        dt.datetime(2026, 1, 10, tzinfo=dt.timezone.utc),
    )
    assert (ranges[0][1] - ranges[0][0]).days + 1 == HISTORICAL_API_CHUNK_DAYS


@pytest.mark.asyncio
async def test_get_historic_daily_consumption_deduplicates_chunk_overlap():
    dummy_smartmeter = _DummySmartmeter()
    async_smartmeter = AsyncSmartmeter(_DummyHass(), dummy_smartmeter)  # type: ignore[arg-type]
    start = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
    end = dt.datetime(2026, 1, 10, tzinfo=dt.timezone.utc)

    result = await async_smartmeter.get_historic_daily_consumption(
        "AT0010000000000000001000011111111", start, end
    )

    assert len(dummy_smartmeter.daily_calls) == 2
    assert dummy_smartmeter.daily_calls[0][1] == dt.datetime(
        2025, 1, 1, tzinfo=dt.timezone.utc
    )
    assert dummy_smartmeter.daily_calls[0][2] == dt.datetime(
        2025, 12, 31, tzinfo=dt.timezone.utc
    )
    assert dummy_smartmeter.daily_calls[1][1] == dt.datetime(
        2026, 1, 1, tzinfo=dt.timezone.utc
    )
    assert dummy_smartmeter.daily_calls[1][2] == dt.datetime(
        2026, 1, 10, tzinfo=dt.timezone.utc
    )

    values = result["values"]
    assert [value["zeitpunktVon"] for value in values] == [
        "2025-12-31T00:00:00Z",
        "2026-01-01T00:00:00Z",
        "2026-01-02T00:00:00Z",
    ]
    overlap_value = next(
        value for value in values if value["zeitpunktVon"] == "2026-01-01T00:00:00Z"
    )
    assert overlap_value["wert"] == 2500
