"""Importer regression tests."""
import datetime as dt
import sys
import types

import pytest

# Home Assistant recorder import requires this dependency in runtime environments.
if "psutil_home_assistant" not in sys.modules:
    stub = types.ModuleType("psutil_home_assistant")

    class _PsutilWrapper:  # pylint: disable=too-few-public-methods
        pass

    stub.PsutilWrapper = _PsutilWrapper
    sys.modules["psutil_home_assistant"] = stub

if "fnv_hash_fast" not in sys.modules:
    fnv_stub = types.ModuleType("fnv_hash_fast")

    def _fnv1a_32(_value):
        return 0

    fnv_stub.fnv1a_32 = _fnv1a_32
    sys.modules["fnv_hash_fast"] = fnv_stub

import it  # noqa: F401  # Ensures custom_components path is available for wnsm imports
from wnsm import importer as importer_module
from wnsm.importer import Importer


def _stat_value(statistic_row: object, field: str):
    if isinstance(statistic_row, dict):
        return statistic_row.get(field)
    return getattr(statistic_row, field)


class _DummyAsyncSmartmeter:
    def __init__(
        self,
        payload: dict,
        daily_payload: dict | None = None,
        meter_read_payload: dict | None = None,
    ) -> None:
        self._payload = payload
        self._daily_payload = daily_payload or {
            "unitOfMeasurement": "KWH",
            "values": [],
        }
        self._meter_read_payload = meter_read_payload or {
            "unitOfMeasurement": "KWH",
            "values": [],
        }

    async def get_bewegungsdaten(self, *_args, **_kwargs) -> dict:
        return self._payload

    async def get_historic_daily_consumption(self, *_args, **_kwargs) -> dict:
        return self._daily_payload

    async def get_meter_reading_history_from_historic_data(
        self, *_args, **_kwargs
    ) -> dict:
        return self._meter_read_payload


def _build_importer(
    payload: dict,
    daily_payload: dict | None = None,
    meter_read_payload: dict | None = None,
) -> Importer:
    return Importer(
        hass=object(),  # type: ignore[arg-type]
        async_smartmeter=_DummyAsyncSmartmeter(
            payload, daily_payload, meter_read_payload
        ),  # type: ignore[arg-type]
        zaehlpunkt="AT0010000000000000001000011111111",
        unit_of_measurement="kWh",
    )


@pytest.mark.asyncio
async def test_import_statistics_ignores_empty_values_without_unit(monkeypatch):
    calls: list[tuple[tuple, dict]] = []

    def _capture(*args, **kwargs):
        calls.append((args, kwargs))

    monkeypatch.setattr(importer_module, "async_add_external_statistics", _capture)

    importer = _build_importer(
        {
            "unitOfMeasurement": None,
            "values": [],
        }
    )
    start = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
    end = dt.datetime(2025, 1, 2, tzinfo=dt.timezone.utc)

    result = await importer._import_statistics(start=start, end=end)

    assert result is None
    assert calls == []


@pytest.mark.asyncio
async def test_import_statistics_raises_on_missing_unit_with_values(monkeypatch):
    calls: list[tuple[tuple, dict]] = []

    def _capture(*args, **kwargs):
        calls.append((args, kwargs))

    monkeypatch.setattr(importer_module, "async_add_external_statistics", _capture)

    importer = _build_importer(
        {
            "unitOfMeasurement": None,
            "values": [
                {
                    "wert": 1.234,
                    "zeitpunktVon": "2025-01-01T00:00:00Z",
                    "zeitpunktBis": "2025-01-01T00:15:00Z",
                    "geschaetzt": False,
                }
            ],
        }
    )
    start = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
    end = dt.datetime(2025, 1, 2, tzinfo=dt.timezone.utc)

    with pytest.raises(
        ValueError,
        match="non-empty bewegungsdaten without unitOfMeasurement",
    ):
        await importer._import_statistics(start=start, end=end)
    assert calls == []


@pytest.mark.asyncio
async def test_import_statistics_raises_on_unknown_unit_with_values(monkeypatch):
    calls: list[tuple[tuple, dict]] = []

    def _capture(*args, **kwargs):
        calls.append((args, kwargs))

    monkeypatch.setattr(importer_module, "async_add_external_statistics", _capture)

    importer = _build_importer(
        {
            "unitOfMeasurement": "MWH",
            "values": [
                {
                    "wert": 1.234,
                    "zeitpunktVon": "2025-01-01T00:00:00Z",
                    "zeitpunktBis": "2025-01-01T00:15:00Z",
                    "geschaetzt": False,
                }
            ],
        }
    )
    start = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
    end = dt.datetime(2025, 1, 2, tzinfo=dt.timezone.utc)

    with pytest.raises(NotImplementedError, match="Unit MWH"):
        await importer._import_statistics(start=start, end=end)
    assert calls == []


@pytest.mark.asyncio
async def test_import_statistics_emits_mean_for_cumulative_stream(monkeypatch):
    calls: list[tuple[tuple, dict]] = []

    def _capture(*args, **kwargs):
        calls.append((args, kwargs))

    monkeypatch.setattr(importer_module, "async_add_external_statistics", _capture)

    importer = _build_importer(
        {
            "unitOfMeasurement": "KWH",
            "values": [
                {
                    "wert": 1.0,
                    "zeitpunktVon": "2025-01-01T00:00:00Z",
                    "zeitpunktBis": "2025-01-01T00:15:00Z",
                    "geschaetzt": False,
                },
                {
                    "wert": 0.5,
                    "zeitpunktVon": "2025-01-01T00:15:00Z",
                    "zeitpunktBis": "2025-01-01T00:30:00Z",
                    "geschaetzt": False,
                },
            ],
        }
    )
    start = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
    end = dt.datetime(2025, 1, 2, tzinfo=dt.timezone.utc)

    result = await importer._import_statistics(start=start, end=end)

    assert result == pytest.approx(1.5)
    assert len(calls) == 2

    cumulative_call_args = calls[1][0]
    cumulative_metadata = cumulative_call_args[1]
    cumulative_statistics = cumulative_call_args[2]
    cumulative_statistic_id = (
        cumulative_metadata.get("statistic_id")
        if isinstance(cumulative_metadata, dict)
        else cumulative_metadata.statistic_id
    )
    assert cumulative_statistic_id.endswith("_cum_abs")
    if isinstance(cumulative_metadata, dict):
        if "has_mean" in cumulative_metadata:
            assert cumulative_metadata["has_mean"] is True
        if "has_sum" in cumulative_metadata:
            assert cumulative_metadata["has_sum"] is True
    else:
        if hasattr(cumulative_metadata, "has_mean"):
            assert cumulative_metadata.has_mean is True
        if hasattr(cumulative_metadata, "has_sum"):
            assert cumulative_metadata.has_sum is True
    assert len(cumulative_statistics) == 1
    assert _stat_value(cumulative_statistics[0], "state") == pytest.approx(1.5)
    assert _stat_value(cumulative_statistics[0], "mean") == pytest.approx(1.5)
    assert _stat_value(cumulative_statistics[0], "sum") == pytest.approx(1.5)


@pytest.mark.asyncio
async def test_import_daily_consumption_statistics_emits_daily_cons_stream(monkeypatch):
    calls: list[tuple[tuple, dict]] = []

    def _capture(*args, **kwargs):
        calls.append((args, kwargs))

    monkeypatch.setattr(importer_module, "async_add_external_statistics", _capture)

    importer = _build_importer(
        {
            "unitOfMeasurement": "KWH",
            "values": [],
        },
        {
            "unitOfMeasurement": "WH",
            "values": [
                {
                    "wert": 1000,
                    "zeitpunktVon": "2025-01-01T23:00:00Z",
                    "zeitpunktBis": "2025-01-02T23:00:00Z",
                    "geschaetzt": False,
                },
                {
                    "wert": 2500,
                    "zeitpunktVon": "2025-01-02T23:00:00Z",
                    "zeitpunktBis": "2025-01-03T23:00:00Z",
                    "geschaetzt": False,
                },
            ],
        },
    )
    start = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
    end = dt.datetime(2025, 1, 4, tzinfo=dt.timezone.utc)

    result = await importer._import_daily_consumption_statistics(start=start, end=end)

    assert result == pytest.approx(3.5)
    assert len(calls) == 1

    daily_call_args = calls[0][0]
    daily_metadata = daily_call_args[1]
    daily_statistics = daily_call_args[2]
    daily_statistic_id = (
        daily_metadata.get("statistic_id")
        if isinstance(daily_metadata, dict)
        else daily_metadata.statistic_id
    )
    assert daily_statistic_id.endswith("_daily_cons")
    if isinstance(daily_metadata, dict):
        if "has_mean" in daily_metadata:
            assert daily_metadata["has_mean"] is True
        if "has_sum" in daily_metadata:
            assert daily_metadata["has_sum"] is True
    else:
        if hasattr(daily_metadata, "has_mean"):
            assert daily_metadata.has_mean is True
        if hasattr(daily_metadata, "has_sum"):
            assert daily_metadata.has_sum is True
    assert len(daily_statistics) == 2
    assert _stat_value(daily_statistics[0], "state") == pytest.approx(1.0)
    assert _stat_value(daily_statistics[0], "mean") == pytest.approx(1.0)
    assert _stat_value(daily_statistics[0], "sum") == pytest.approx(1.0)
    assert _stat_value(daily_statistics[1], "state") == pytest.approx(3.5)
    assert _stat_value(daily_statistics[1], "mean") == pytest.approx(3.5)
    assert _stat_value(daily_statistics[1], "sum") == pytest.approx(3.5)


@pytest.mark.asyncio
async def test_import_daily_meter_read_statistics_emits_absolute_stream(monkeypatch):
    calls: list[tuple[tuple, dict]] = []

    def _capture(*args, **kwargs):
        calls.append((args, kwargs))

    monkeypatch.setattr(importer_module, "async_add_external_statistics", _capture)

    importer = _build_importer(
        {
            "unitOfMeasurement": "KWH",
            "values": [],
        },
        {
            "unitOfMeasurement": "KWH",
            "values": [],
        },
        {
            "unitOfMeasurement": "WH",
            "values": [
                {
                    "messwert": 4444000,
                    "zeitVon": "2025-01-01T23:00:11Z",
                    "zeitBis": "2025-01-02T23:00:00Z",
                    "qualitaet": "VAL",
                },
                {
                    "messwert": 4446500,
                    "zeitVon": "2025-01-02T23:00:00Z",
                    "zeitBis": "2025-01-03T23:00:00Z",
                    "qualitaet": "VAL",
                },
            ],
        },
    )
    start = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
    end = dt.datetime(2025, 1, 4, tzinfo=dt.timezone.utc)

    await importer._import_daily_meter_read_statistics(start=start, end=end)

    assert len(calls) == 1

    meter_read_call_args = calls[0][0]
    meter_read_metadata = meter_read_call_args[1]
    meter_read_statistics = meter_read_call_args[2]
    meter_read_statistic_id = (
        meter_read_metadata.get("statistic_id")
        if isinstance(meter_read_metadata, dict)
        else meter_read_metadata.statistic_id
    )
    assert meter_read_statistic_id.endswith("_daily_meter_read")
    if isinstance(meter_read_metadata, dict):
        if "has_mean" in meter_read_metadata:
            assert meter_read_metadata["has_mean"] is True
        if "has_sum" in meter_read_metadata:
            assert meter_read_metadata["has_sum"] is True
    else:
        if hasattr(meter_read_metadata, "has_mean"):
            assert meter_read_metadata.has_mean is True
        if hasattr(meter_read_metadata, "has_sum"):
            assert meter_read_metadata.has_sum is True
    assert len(meter_read_statistics) == 2
    assert _stat_value(meter_read_statistics[0], "start").second == 0
    assert _stat_value(meter_read_statistics[0], "state") == pytest.approx(4444.0)
    assert _stat_value(meter_read_statistics[0], "mean") == pytest.approx(4444.0)
    assert _stat_value(meter_read_statistics[0], "sum") == pytest.approx(4444.0)
    assert _stat_value(meter_read_statistics[1], "state") == pytest.approx(4446.5)
    assert _stat_value(meter_read_statistics[1], "mean") == pytest.approx(4446.5)
    assert _stat_value(meter_read_statistics[1], "sum") == pytest.approx(4446.5)


def test_daily_consumption_stat_validity_requires_mean_when_supported(monkeypatch):
    importer = _build_importer({"unitOfMeasurement": "KWH", "values": []})

    monkeypatch.setattr(
        importer,
        "_statistics_metadata_capabilities",
        lambda: {"has_mean": True, "has_sum": True},
    )
    last_inserted = {
        importer.daily_consumption_id: [
            {
                "state": 1.23,
                "sum": 4.56,
                "end": "2025-01-02T00:00:00+00:00",
            }
        ]
    }
    assert importer.is_last_inserted_daily_consumption_stat_valid(last_inserted) is False

    last_inserted[importer.daily_consumption_id][0]["mean"] = 1.23
    assert importer.is_last_inserted_daily_consumption_stat_valid(last_inserted) is False

    last_inserted[importer.daily_consumption_id][0]["sum"] = 1.23
    assert importer.is_last_inserted_daily_consumption_stat_valid(last_inserted) is True


def test_daily_meter_read_stat_validity_requires_mean_when_supported(monkeypatch):
    importer = _build_importer({"unitOfMeasurement": "KWH", "values": []})

    monkeypatch.setattr(
        importer,
        "_statistics_metadata_capabilities",
        lambda: {"has_mean": True, "has_sum": True},
    )
    last_inserted = {
        importer.daily_meter_read_id: [
            {
                "state": 4444.0,
                "end": "2025-01-02T00:00:00+00:00",
            }
        ]
    }
    assert importer.is_last_inserted_daily_meter_read_stat_valid(last_inserted) is False

    last_inserted[importer.daily_meter_read_id][0]["mean"] = 4444.0
    assert importer.is_last_inserted_daily_meter_read_stat_valid(last_inserted) is False

    last_inserted[importer.daily_meter_read_id][0]["sum"] = 4444.0
    assert importer.is_last_inserted_daily_meter_read_stat_valid(last_inserted) is True


def test_ensure_statistics_metadata_skips_daily_meter_read_when_disabled(monkeypatch):
    calls: list[str] = []

    def _capture(_hass, metadata, _statistics):
        statistic_id = (
            metadata.get("statistic_id")
            if isinstance(metadata, dict)
            else metadata.statistic_id
        )
        calls.append(statistic_id)

    monkeypatch.setattr(importer_module, "async_add_external_statistics", _capture)

    importer = _build_importer({"unitOfMeasurement": "KWH", "values": []})
    importer.enable_daily_meter_read_statistics = False
    importer._ensure_statistics_metadata()

    assert any(statistic_id.endswith("_daily_cons") for statistic_id in calls)
    assert not any(statistic_id.endswith("_daily_meter_read") for statistic_id in calls)


@pytest.mark.asyncio
async def test_safe_import_daily_consumption_statistics_ignores_errors(monkeypatch, caplog):
    importer = _build_importer(
        {
            "unitOfMeasurement": "KWH",
            "values": [],
        }
    )

    async def _raise_daily_import():
        raise RuntimeError("daily endpoint unavailable")

    monkeypatch.setattr(
        importer,
        "_import_daily_consumption_statistics",
        _raise_daily_import,
    )

    await importer._safe_import_daily_consumption_statistics()

    assert "Skipping daily consumption statistics import" in caplog.text


@pytest.mark.asyncio
async def test_get_latest_daily_consumption_day_value_uses_latest_when_single_row(
    monkeypatch,
):
    importer = _build_importer({"unitOfMeasurement": "KWH", "values": []})

    async def _fake_get_last_inserted_statistics(
        _statistic_id: str, _types: set[str], number_of_stats: int = 1
    ):
        assert number_of_stats == 2
        return {
            importer.daily_consumption_id: [
                {
                    "state": 3.5,
                    "sum": 3.5,
                }
            ]
        }

    monkeypatch.setattr(
        importer, "_get_last_inserted_statistics", _fake_get_last_inserted_statistics
    )

    assert await importer._get_latest_daily_consumption_day_value() == pytest.approx(3.5)


@pytest.mark.asyncio
async def test_get_latest_daily_consumption_day_value_uses_delta_between_rows(
    monkeypatch,
):
    importer = _build_importer({"unitOfMeasurement": "KWH", "values": []})

    async def _fake_get_last_inserted_statistics(
        _statistic_id: str, _types: set[str], number_of_stats: int = 1
    ):
        assert number_of_stats == 2
        return {
            importer.daily_consumption_id: [
                {
                    "state": 9.0,
                    "sum": 9.0,
                },
                {
                    "state": 6.5,
                    "sum": 6.5,
                },
            ]
        }

    monkeypatch.setattr(
        importer, "_get_last_inserted_statistics", _fake_get_last_inserted_statistics
    )

    assert await importer._get_latest_daily_consumption_day_value() == pytest.approx(2.5)


@pytest.mark.asyncio
async def test_get_latest_daily_consumption_day_value_falls_back_to_latest_on_negative_delta(
    monkeypatch,
):
    importer = _build_importer({"unitOfMeasurement": "KWH", "values": []})

    async def _fake_get_last_inserted_statistics(
        _statistic_id: str, _types: set[str], number_of_stats: int = 1
    ):
        assert number_of_stats == 2
        return {
            importer.daily_consumption_id: [
                {
                    "state": 1.0,
                    "sum": 1.0,
                },
                {
                    "state": 3.0,
                    "sum": 3.0,
                },
            ]
        }

    monkeypatch.setattr(
        importer, "_get_last_inserted_statistics", _fake_get_last_inserted_statistics
    )

    assert await importer._get_latest_daily_consumption_day_value() == pytest.approx(1.0)
