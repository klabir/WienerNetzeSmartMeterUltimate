"""Coordinator for shared WNSM polling and imports."""
import logging
from datetime import datetime, timedelta
from typing import Any

from homeassistant.const import UnitOfEnergy
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.util import dt as dt_util

from .AsyncSmartmeter import AsyncSmartmeter
from .api import Smartmeter
from .api.constants import ValueType
from .importer import Importer
from .utils import before, today

_LOGGER = logging.getLogger(__name__)


class WNSMDataUpdateCoordinator(DataUpdateCoordinator[dict[str, dict[str, Any]]]):
    """Shared update coordinator for all WNSM sensors in one entry."""

    def __init__(
        self,
        hass: HomeAssistant,
        username: str,
        password: str,
        zaehlpunkte: list[str],
        scan_interval_minutes: int,
        enable_raw_api_response_write: bool,
        log_scope: str,
    ) -> None:
        self._zaehlpunkte = zaehlpunkte
        self._enable_raw_api_response_write = enable_raw_api_response_write
        self._smartmeter = Smartmeter(
            username=username,
            password=password,
            enable_raw_api_response_write=enable_raw_api_response_write,
            log_scope=log_scope,
        )
        self._async_smartmeter = AsyncSmartmeter(hass, self._smartmeter)
        super().__init__(
            hass,
            _LOGGER,
            name="wnsm",
            update_interval=timedelta(minutes=scan_interval_minutes),
        )

    def _inject_api_log_attributes(self, zaehlpunkt: str, attributes: dict[str, Any]) -> None:
        if not self._enable_raw_api_response_write:
            return

        recent_calls = self._smartmeter.get_recent_api_calls()
        logging_status = self._smartmeter.get_raw_api_logging_status()
        filtered_calls = [
            call
            for call in recent_calls
            if f"/{zaehlpunkt}/" in call.get("endpoint", "")
            or f"\\{zaehlpunkt}\\" in (call.get("file_path") or "")
            or f"/{zaehlpunkt}/" in (call.get("file_path") or "")
        ]
        attributes["raw_api_logging_enabled"] = True
        attributes["api_call_count"] = len(filtered_calls)
        attributes["recent_api_calls"] = filtered_calls[-5:]
        attributes["last_api_call_file"] = (
            filtered_calls[-1].get("file_path") if len(filtered_calls) > 0 else None
        )
        attributes["raw_api_logging_prepared"] = logging_status["prepared"]
        attributes["raw_api_logging_root"] = logging_status["root"]
        attributes["raw_api_logging_directory"] = logging_status["directory"]
        attributes["raw_api_logging_prepare_error"] = logging_status["prepare_error"]
        attributes["raw_api_last_write_error"] = logging_status["last_write_error"]

    async def _probe_quarter_hour_today(self, zaehlpunkt: str) -> dict[str, Any]:
        query_start = today()
        query_end = datetime.now()
        now_local = dt_util.now()
        bewegungsdaten = await self._async_smartmeter.get_bewegungsdaten(
            zaehlpunkt,
            query_start,
            query_end,
            ValueType.QUARTER_HOUR,
        )
        values = bewegungsdaten.get("values") or []
        latest_any: dict[str, Any] | None = None
        latest_today: dict[str, Any] | None = None
        today_values = 0

        for value in values:
            start_raw = value.get("zeitpunktVon")
            start_ts = dt_util.parse_datetime(start_raw) if isinstance(start_raw, str) else None
            if start_ts is None:
                continue

            if latest_any is None or start_ts > latest_any["start_ts"]:
                latest_any = {"start_ts": start_ts, "raw": value}

            if dt_util.as_local(start_ts).date() == now_local.date():
                today_values += 1
                if latest_today is None or start_ts > latest_today["start_ts"]:
                    latest_today = {"start_ts": start_ts, "raw": value}

        return {
            "status": "ok",
            "query_start": query_start.isoformat(),
            "query_end": query_end.isoformat(),
            "granularity": bewegungsdaten.get("granularity"),
            "unit_of_measurement": bewegungsdaten.get("unitOfMeasurement"),
            "returned_value_count": len(values),
            "today_value_count": today_values,
            "has_today_values": today_values > 0,
            "latest_any_zeitpunktVon": latest_any["raw"].get("zeitpunktVon")
            if latest_any
            else None,
            "latest_any_zeitpunktBis": latest_any["raw"].get("zeitpunktBis")
            if latest_any
            else None,
            "latest_today_zeitpunktVon": latest_today["raw"].get("zeitpunktVon")
            if latest_today
            else None,
            "latest_today_zeitpunktBis": latest_today["raw"].get("zeitpunktBis")
            if latest_today
            else None,
            "latest_today_wert": latest_today["raw"].get("wert") if latest_today else None,
            "latest_today_geschaetzt": latest_today["raw"].get("geschaetzt")
            if latest_today
            else None,
        }

    async def _async_update_data(self) -> dict[str, dict[str, Any]]:
        try:
            await self._async_smartmeter.login()
        except Exception as exception:  # pylint: disable=broad-except
            raise UpdateFailed(f"Login/update failed: {exception}") from exception

        data: dict[str, dict[str, Any]] = {}
        for zaehlpunkt in self._zaehlpunkte:
            native_value: float | int | None = 0
            attributes: dict[str, Any] = {}
            quarter_hour_probe: dict[str, Any] = {"status": "not_checked"}
            available = True
            try:
                zaehlpunkt_response = await self._async_smartmeter.get_zaehlpunkt(zaehlpunkt)
                attributes.update(zaehlpunkt_response)

                if self._async_smartmeter.is_active(zaehlpunkt_response):
                    reading_dates = [before(today(), 1), before(today(), 2)]
                    for reading_date in reading_dates:
                        meter_reading = await self._async_smartmeter.get_meter_reading_from_historic_data(
                            zaehlpunkt,
                            reading_date,
                            datetime.now(),
                        )
                        if meter_reading is not None:
                            native_value = meter_reading
                    importer = Importer(
                        self.hass,
                        self._async_smartmeter,
                        zaehlpunkt,
                        UnitOfEnergy.KILO_WATT_HOUR,
                        skip_login=True,
                        preloaded_zaehlpunkt=zaehlpunkt_response,
                    )
                    await importer.async_import()
                    try:
                        quarter_hour_probe = await self._probe_quarter_hour_today(zaehlpunkt)
                    except Exception as exception:  # pylint: disable=broad-except
                        quarter_hour_probe = {
                            "status": "error",
                            "error": str(exception),
                        }
                        _LOGGER.warning(
                            "Quarter-hour probe failed for zaehlpunkt %s: %s",
                            zaehlpunkt,
                            exception,
                        )
                else:
                    quarter_hour_probe = {"status": "inactive"}
            except Exception as exception:  # pylint: disable=broad-except
                available = False
                attributes["last_error"] = str(exception)
                _LOGGER.exception("Failed to update zaehlpunkt %s: %s", zaehlpunkt, exception)
                quarter_hour_probe = {
                    "status": "error",
                    "error": str(exception),
                }

            self._inject_api_log_attributes(zaehlpunkt, attributes)
            data[zaehlpunkt] = {
                "native_value": native_value,
                "attributes": attributes,
                "quarter_hour_probe": quarter_hour_probe,
                "available": available,
            }
        return data
