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


class _DummySmartmeterBewegungsdaten:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dt.datetime, dt.datetime, object]] = []

    def bewegungsdaten(
        self,
        zaehlpunkt: str,
        start: dt.datetime,
        end: dt.datetime,
        granularity: object,
    ) -> dict:
        self.calls.append((zaehlpunkt, start, end, granularity))
        if len(self.calls) == 1:
            return {
                "descriptor": {
                    "geschaeftspartnernummer": "123",
                    "zaehlpunktnummer": zaehlpunkt,
                    "rolle": "V002",
                    "aggregat": "NONE",
                    "granularitaet": "QH",
                    "einheit": None,
                },
                "values": [
                    {
                        "wert": 0.5,
                        "zeitpunktVon": "2025-12-31T23:45:00Z",
                        "zeitpunktBis": "2026-01-01T00:00:00Z",
                        "geschaetzt": False,
                    }
                ],
            }
        return {
            "descriptor": {
                "geschaeftspartnernummer": "123",
                "zaehlpunktnummer": zaehlpunkt,
                "rolle": "V002",
                "aggregat": "NONE",
                "granularitaet": "QH",
                "einheit": "KWH",
            },
            "values": [
                {
                    "wert": 0.7,
                    "zeitpunktVon": "2025-12-31T23:45:00Z",
                    "zeitpunktBis": "2026-01-01T00:00:00Z",
                    "geschaetzt": False,
                },
                {
                    "wert": 0.8,
                    "zeitpunktVon": "2026-01-01T00:00:00Z",
                    "zeitpunktBis": "2026-01-01T00:15:00Z",
                    "geschaetzt": False,
                },
            ],
        }


class _DummySmartmeterDaily400Split:
    def __init__(self, max_days: int = 90) -> None:
        self.max_days = max_days
        self.daily_calls: list[tuple[str, dt.datetime, dt.datetime]] = []

    def historical_day_consumption(
        self, zaehlpunkt: str, start: dt.datetime, end: dt.datetime
    ) -> dict:
        self.daily_calls.append((zaehlpunkt, start, end))
        day_span = (end.date() - start.date()).days + 1
        if day_span > self.max_days:
            raise RuntimeError(
                "API request failed for endpoint 'zaehlpunkte/123/AT000/messwerte' with status 400: {}"
            )

        next_day = start + dt.timedelta(days=1)
        return {
            "obisCode": "1-1:1.9.0",
            "einheit": "WH",
            "messwerte": [
                {
                    "messwert": 1000,
                    "zeitVon": start.strftime("%Y-%m-%dT00:00:00Z"),
                    "zeitBis": next_day.strftime("%Y-%m-%dT00:00:00Z"),
                    "qualitaet": "VAL",
                }
            ],
        }


class _DummySmartmeterDaily400BeforeCutoff:
    def __init__(self, cutoff: dt.date) -> None:
        self.cutoff = cutoff
        self.daily_calls: list[tuple[str, dt.datetime, dt.datetime]] = []

    def historical_day_consumption(
        self, zaehlpunkt: str, start: dt.datetime, end: dt.datetime
    ) -> dict:
        self.daily_calls.append((zaehlpunkt, start, end))
        if start.date() < self.cutoff:
            raise RuntimeError(
                "API request failed for endpoint 'zaehlpunkte/123/AT000/messwerte' with status 400: {}"
            )

        start_date = max(start.date(), self.cutoff)
        start_ts = dt.datetime.combine(start_date, dt.time.min, tzinfo=dt.timezone.utc)
        next_day = start_ts + dt.timedelta(days=1)
        return {
            "obisCode": "1-1:1.9.0",
            "einheit": "WH",
            "messwerte": [
                {
                    "messwert": 1000,
                    "zeitVon": start_ts.strftime("%Y-%m-%dT00:00:00Z"),
                    "zeitBis": next_day.strftime("%Y-%m-%dT00:00:00Z"),
                    "qualitaet": "VAL",
                }
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


@pytest.mark.asyncio
async def test_get_bewegungsdaten_merges_unit_from_later_chunk():
    dummy_smartmeter = _DummySmartmeterBewegungsdaten()
    async_smartmeter = AsyncSmartmeter(_DummyHass(), dummy_smartmeter)  # type: ignore[arg-type]
    zaehlpunkt = "AT0010000000000000001000011111111"
    start = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
    end = dt.datetime(2026, 1, 10, tzinfo=dt.timezone.utc)

    result = await async_smartmeter.get_bewegungsdaten(zaehlpunkt, start, end)

    assert len(dummy_smartmeter.calls) == 2
    assert result["unitOfMeasurement"] == "KWH"
    assert len(result["values"]) == 2
    overlap_value = next(
        value for value in result["values"] if value["zeitpunktVon"] == "2025-12-31T23:45:00Z"
    )
    assert overlap_value["wert"] == 0.7


@pytest.mark.asyncio
async def test_get_historic_daily_consumption_splits_ranges_after_400():
    dummy_smartmeter = _DummySmartmeterDaily400Split(max_days=90)
    async_smartmeter = AsyncSmartmeter(_DummyHass(), dummy_smartmeter)  # type: ignore[arg-type]
    start = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
    end = dt.datetime(2025, 12, 31, tzinfo=dt.timezone.utc)

    result = await async_smartmeter.get_historic_daily_consumption(
        "AT0010000000000000001000011111111", start, end
    )

    assert len(dummy_smartmeter.daily_calls) > 1
    assert any(
        ((range_end.date() - range_start.date()).days + 1) > dummy_smartmeter.max_days
        for _zaehlpunkt, range_start, range_end in dummy_smartmeter.daily_calls
    )
    assert result["unitOfMeasurement"] == "WH"
    assert len(result["values"]) > 0


@pytest.mark.asyncio
async def test_get_historic_daily_consumption_keeps_valid_ranges_when_older_ranges_400():
    dummy_smartmeter = _DummySmartmeterDaily400BeforeCutoff(cutoff=dt.date(2025, 7, 1))
    async_smartmeter = AsyncSmartmeter(_DummyHass(), dummy_smartmeter)  # type: ignore[arg-type]
    start = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
    end = dt.datetime(2025, 12, 31, tzinfo=dt.timezone.utc)

    result = await async_smartmeter.get_historic_daily_consumption(
        "AT0010000000000000001000011111111", start, end
    )

    assert len(dummy_smartmeter.daily_calls) > 1
    assert len(result["values"]) > 0
    first_value = result["values"][0]
    assert first_value["zeitpunktVon"] >= "2025-07-01T00:00:00Z"
