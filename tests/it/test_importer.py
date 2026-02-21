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


class _DummyAsyncSmartmeter:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    async def get_bewegungsdaten(self, *_args, **_kwargs) -> dict:
        return self._payload


def _build_importer(payload: dict) -> Importer:
    return Importer(
        hass=object(),  # type: ignore[arg-type]
        async_smartmeter=_DummyAsyncSmartmeter(payload),  # type: ignore[arg-type]
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
