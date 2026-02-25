"""Coordinator attribute enrichment tests."""

import sys
import types

import it  # noqa: F401  # Ensure custom_components path is available

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
