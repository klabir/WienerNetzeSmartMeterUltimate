"""Coordinator for shared WNSM polling and imports."""
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from homeassistant.const import UnitOfEnergy
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .AsyncSmartmeter import AsyncSmartmeter
from .api import Smartmeter
from .const import DOMAIN
from .importer import Importer

_LOGGER = logging.getLogger(__name__)


class WNSMDataUpdateCoordinator(DataUpdateCoordinator[dict[str, dict[str, Any]]]):
    """Shared update coordinator for all WNSM sensors in one entry."""
    _LIVE_READING_LOOKBACK_DAYS = 30

    def __init__(
        self,
        hass: HomeAssistant,
        username: str,
        password: str,
        zaehlpunkte: list[str],
        scan_interval_minutes: int,
        historical_days: int,
        enable_raw_api_response_write: bool,
        enable_daily_cons_statistics: bool,
        enable_daily_meter_read_statistics: bool,
        log_scope: str,
    ) -> None:
        self._zaehlpunkte = zaehlpunkte
        try:
            historical_days_int = int(historical_days)
        except (TypeError, ValueError):
            historical_days_int = 1
        self._historical_days = max(1, historical_days_int)
        self._enable_raw_api_response_write = enable_raw_api_response_write
        self._enable_daily_cons_statistics = enable_daily_cons_statistics
        self._enable_daily_meter_read_statistics = enable_daily_meter_read_statistics
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
            name=DOMAIN,
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

    def _historical_window(self) -> tuple[datetime, datetime]:
        end = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        start = end - timedelta(days=self._historical_days)
        return start, end

    def _live_meter_reading_windows(self) -> tuple[tuple[datetime, datetime], tuple[datetime, datetime] | None]:
        full_start, end = self._historical_window()
        short_days = max(1, min(self._LIVE_READING_LOOKBACK_DAYS, self._historical_days))
        short_start = end - timedelta(days=short_days)
        if short_start <= full_start:
            return (full_start, end), None
        return (short_start, end), (full_start, end)

    async def _async_update_data(self) -> dict[str, dict[str, Any]]:
        try:
            await self._async_smartmeter.login()
        except Exception as exception:  # pylint: disable=broad-except
            raise UpdateFailed(f"Login/update failed: {exception}") from exception

        data: dict[str, dict[str, Any]] = {}
        for zaehlpunkt in self._zaehlpunkte:
            native_value: float | int | None = 0
            daily_cons_value: float | int | None = None
            daily_cons_day_value: float | int | None = None
            attributes: dict[str, Any] = {}
            available = True
            try:
                zaehlpunkt_response = await self._async_smartmeter.get_zaehlpunkt(zaehlpunkt)
                attributes.update(zaehlpunkt_response)

                if self._async_smartmeter.is_active(zaehlpunkt_response):
                    short_window, fallback_window = self._live_meter_reading_windows()
                    meter_reading = await self._async_smartmeter.get_meter_reading_from_historic_data(
                        zaehlpunkt,
                        short_window[0],
                        short_window[1],
                    )
                    if meter_reading is None and fallback_window is not None:
                        _LOGGER.debug(
                            "No live meter reading found in %s-day window for %s. Retrying full historical window.",
                            (short_window[1] - short_window[0]).days,
                            zaehlpunkt,
                        )
                        meter_reading = await self._async_smartmeter.get_meter_reading_from_historic_data(
                            zaehlpunkt,
                            fallback_window[0],
                            fallback_window[1],
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
                        historical_days=self._historical_days,
                        enable_daily_consumption_statistics=self._enable_daily_cons_statistics,
                        enable_daily_meter_read_statistics=self._enable_daily_meter_read_statistics,
                    )
                    importer_result = await importer.async_import()
                    if isinstance(importer_result, dict):
                        daily_cons_value = importer_result.get("daily_consumption_value")
                        daily_cons_day_value = importer_result.get(
                            "daily_consumption_day_value"
                        )
            except Exception as exception:  # pylint: disable=broad-except
                available = False
                attributes["last_error"] = str(exception)
                _LOGGER.exception("Failed to update zaehlpunkt %s: %s", zaehlpunkt, exception)

            self._inject_api_log_attributes(zaehlpunkt, attributes)
            data[zaehlpunkt] = {
                "native_value": native_value,
                "daily_cons_value": daily_cons_value,
                "daily_cons_day_value": daily_cons_day_value,
                "attributes": attributes,
                "available": available,
            }
        return data
