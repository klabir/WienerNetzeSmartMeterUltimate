"""Coordinator attribute enrichment tests."""

import sys
import types

import it  # noqa: F401  # Ensure custom_components path is available
import pytest

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

from wnsmeter30.coordinator import WNSMDataUpdateCoordinator
from wnsmeter30.api.constants import ValueType


class _DummySmartmeter:
    def __init__(self, recent_calls: list[dict], logging_status: dict | None = None) -> None:
        self._recent_calls = list(recent_calls)
        self._logging_status = logging_status or {
            "prepared": True,
            "root": "/config/tmp/wnsm_api_calls",
            "directory": "/config/tmp/wnsm_api_calls/entry",
            "prepare_error": None,
            "last_write_error": None,
        }

    def get_recent_api_calls(self) -> list[dict]:
        return list(self._recent_calls)

    def get_raw_api_logging_status(self) -> dict:
        return dict(self._logging_status)


class _DummyAsyncSmartmeterQuarterHour:
    def __init__(self, response: dict) -> None:
        self.response = response
        self.calls: list[tuple] = []

    async def get_historic_data(self, zaehlpunkt, date_from, date_to, granularity):
        self.calls.append((zaehlpunkt, date_from, date_to, granularity))
        return self.response


def _build_coordinator(
    recent_calls: list[dict], enable_raw_api_response_write: bool
) -> WNSMDataUpdateCoordinator:
    coordinator = WNSMDataUpdateCoordinator.__new__(WNSMDataUpdateCoordinator)
    coordinator._smartmeter = _DummySmartmeter(recent_calls)
    coordinator._enable_raw_api_response_write = enable_raw_api_response_write
    return coordinator


def test_inject_api_log_attributes_sets_latest_api_call_without_raw_logging() -> None:
    coordinator = _build_coordinator(
        recent_calls=[
            {
                "timestamp": "2026-02-25T09:00:00",
                "endpoint": "/messdaten/customer/AT001/verbrauch",
                "file_path": None,
            },
            {
                "timestamp": "2026-02-25T10:00:00",
                "endpoint": "/messdaten/customer/AT001/verbrauch",
                "file_path": None,
            },
        ],
        enable_raw_api_response_write=False,
    )
    attributes: dict = {}

    coordinator._inject_api_log_attributes("AT001", attributes)

    assert attributes["latest_api_call"] == "2026-02-25T10:00:00"
    assert attributes["latest_api_call_date"] == "2026-02-25"
    assert attributes["latest_api_call_time"] == "10:00:00"
    assert "raw_api_logging_enabled" not in attributes


def test_inject_api_log_attributes_sets_latest_api_call_none_when_no_match() -> None:
    coordinator = _build_coordinator(
        recent_calls=[
            {
                "timestamp": "2026-02-25T09:00:00",
                "endpoint": "/messdaten/customer/AT999/verbrauch",
                "file_path": None,
            },
        ],
        enable_raw_api_response_write=False,
    )
    attributes: dict = {}

    coordinator._inject_api_log_attributes("AT001", attributes)

    assert attributes["latest_api_call"] is None
    assert attributes["latest_api_call_date"] is None
    assert attributes["latest_api_call_time"] is None


def test_inject_api_log_attributes_keeps_raw_api_logging_fields_when_enabled() -> None:
    coordinator = _build_coordinator(
        recent_calls=[
            {
                "timestamp": "2026-02-25T11:30:00",
                "endpoint": "/messdaten/customer/AT001/verbrauch",
                "file_path": "/tmp/wnsm_api_calls/AT001/call.json",
            }
        ],
        enable_raw_api_response_write=True,
    )
    attributes: dict = {}

    coordinator._inject_api_log_attributes("AT001", attributes)

    assert attributes["latest_api_call"] == "2026-02-25T11:30:00"
    assert attributes["latest_api_call_date"] == "2026-02-25"
    assert attributes["latest_api_call_time"] == "11:30:00"
    assert attributes["raw_api_logging_enabled"] is True
    assert attributes["api_call_count"] == 1
    assert attributes["last_api_call_file"] == "/tmp/wnsm_api_calls/AT001/call.json"


@pytest.mark.asyncio
async def test_fetch_live_quarter_hour_reading_returns_latest_slot_with_attributes() -> None:
    response = {
        "unitOfMeasurement": "WH",
        "values": [
            {
                "zeitVon": "2026-02-25T09:00:00.000Z",
                "zeitBis": "2026-02-25T09:15:00.000Z",
                "messwert": 125,
                "qualitaet": "VAL",
            },
            {
                "zeitVon": "2026-02-25T09:15:00.000Z",
                "zeitBis": "2026-02-25T09:30:00.000Z",
                "messwert": 250,
                "qualitaet": "EST",
            },
        ],
    }
    coordinator = WNSMDataUpdateCoordinator.__new__(WNSMDataUpdateCoordinator)
    coordinator._async_smartmeter = _DummyAsyncSmartmeterQuarterHour(response)  # type: ignore[attr-defined]

    value, attributes = await coordinator._fetch_live_quarter_hour_reading("AT001")

    assert value == pytest.approx(0.25)
    assert attributes["reading_time_from"] == "2026-02-25T09:15:00.000Z"
    assert attributes["reading_time_to"] == "2026-02-25T09:30:00.000Z"
    assert attributes["reading_quality"] == "EST"
    assert attributes["reading_raw_value"] == 250
    assert attributes["reading_unit"] == "WH"
    assert attributes["reading_kwh"] == pytest.approx(0.25)
    assert attributes["equivalent_power_w"] == pytest.approx(1000.0)
    assert attributes["source_granularity"] == ValueType.QUARTER_HOUR.value
    assert coordinator._async_smartmeter.calls[0][0] == "AT001"  # type: ignore[attr-defined]
    assert coordinator._async_smartmeter.calls[0][3] == ValueType.QUARTER_HOUR  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_fetch_live_quarter_hour_reading_returns_none_for_empty_values() -> None:
    coordinator = WNSMDataUpdateCoordinator.__new__(WNSMDataUpdateCoordinator)
    coordinator._async_smartmeter = _DummyAsyncSmartmeterQuarterHour(  # type: ignore[attr-defined]
        {"unitOfMeasurement": "KWH", "values": []}
    )

    value, attributes = await coordinator._fetch_live_quarter_hour_reading("AT001")

    assert value is None
    assert attributes == {}
