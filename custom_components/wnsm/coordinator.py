"""Coordinator for shared WNSM polling and imports."""
import logging
from datetime import datetime, timedelta
from typing import Any

from homeassistant.const import UnitOfEnergy
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .AsyncSmartmeter import AsyncSmartmeter
from .api import Smartmeter
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
        recent_calls = self._smartmeter.get_recent_api_calls()
        logging_status = self._smartmeter.get_raw_api_logging_status()
        filtered_calls = [
            call
            for call in recent_calls
            if f"/{zaehlpunkt}/" in call.get("endpoint", "")
            or f"\\{zaehlpunkt}\\" in (call.get("file_path") or "")
            or f"/{zaehlpunkt}/" in (call.get("file_path") or "")
        ]
        attributes["raw_api_logging_enabled"] = self._enable_raw_api_response_write
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

    async def _async_update_data(self) -> dict[str, dict[str, Any]]:
        try:
            await self._async_smartmeter.login()
        except Exception as exception:  # pylint: disable=broad-except
            raise UpdateFailed(f"Login/update failed: {exception}") from exception

        data: dict[str, dict[str, Any]] = {}
        for zaehlpunkt in self._zaehlpunkte:
            native_value: float | int | None = 0
            attributes: dict[str, Any] = {}
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
            except Exception as exception:  # pylint: disable=broad-except
                available = False
                attributes["last_error"] = str(exception)
                _LOGGER.exception("Failed to update zaehlpunkt %s: %s", zaehlpunkt, exception)

            self._inject_api_log_attributes(zaehlpunkt, attributes)
            data[zaehlpunkt] = {
                "native_value": native_value,
                "attributes": attributes,
                "available": available,
            }
        return data
