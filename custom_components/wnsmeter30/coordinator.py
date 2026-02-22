"""Coordinator for shared WNSM polling and imports."""
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from homeassistant.const import UnitOfEnergy
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.util import slugify

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
        meter_aliases: dict[str, str] | None,
        scan_interval_minutes: int,
        historical_days: int,
        enable_raw_api_response_write: bool,
        enable_daily_cons_statistics: bool,
        enable_daily_meter_read_statistics: bool,
        use_alias_for_ids: bool,
        log_scope: str,
    ) -> None:
        self._zaehlpunkte = zaehlpunkte
        try:
            historical_days_int = int(historical_days)
        except (TypeError, ValueError):
            historical_days_int = 1
        self._historical_days = max(1, historical_days_int)
        self._meter_aliases = {
            str(meter_id): str(alias).strip()
            for meter_id, alias in (meter_aliases or {}).items()
            if str(alias).strip()
        }
        self._use_alias_for_ids = bool(use_alias_for_ids)
        self._alias_id_keys = self._build_alias_id_keys()
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

    def display_name(self, zaehlpunkt: str) -> str:
        alias = self._meter_aliases.get(zaehlpunkt)
        return alias if alias else zaehlpunkt

    def _build_alias_id_keys(self) -> dict[str, str]:
        if not self._use_alias_for_ids:
            return {}

        alias_slugs: dict[str, str] = {}
        for zaehlpunkt in self._zaehlpunkte:
            alias = self._meter_aliases.get(zaehlpunkt, "")
            alias_slug = slugify(alias).lower() if alias else ""
            if alias_slug:
                alias_slugs[zaehlpunkt] = alias_slug

        resolved: dict[str, str] = {}
        # Reserve fallback statistic keys from meters without alias to avoid
        # collisions when alias-based statistic IDs are enabled.
        used: set[str] = {
            zaehlpunkt.lower()
            for zaehlpunkt in self._zaehlpunkte
            if zaehlpunkt not in alias_slugs
        }
        for zaehlpunkt in self._zaehlpunkte:
            alias_slug = alias_slugs.get(zaehlpunkt)
            if not alias_slug:
                continue

            candidate = alias_slug
            if candidate in used:
                suffix = zaehlpunkt.lower()[-6:]
                candidate = f"{alias_slug}_{suffix}"
                if candidate in used:
                    candidate = f"{alias_slug}_{zaehlpunkt.lower()}"
                if candidate in used:
                    index = 2
                    while f"{candidate}_{index}" in used:
                        index += 1
                    candidate = f"{candidate}_{index}"
                _LOGGER.warning(
                    "Alias-based ID key '%s' conflicts with an existing key. Using '%s' for %s.",
                    alias_slug,
                    candidate,
                    zaehlpunkt,
                )

            used.add(candidate)
            resolved[zaehlpunkt] = candidate
        return resolved

    def entity_id_key(self, zaehlpunkt: str) -> str:
        return self._alias_id_keys.get(zaehlpunkt, zaehlpunkt)

    def statistic_id_key(self, zaehlpunkt: str) -> str:
        return self._alias_id_keys.get(zaehlpunkt, zaehlpunkt.lower())

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
                attributes["display_name"] = self.display_name(zaehlpunkt)

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
                        display_name=self.display_name(zaehlpunkt),
                        statistic_id_base=self.statistic_id_key(zaehlpunkt),
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
