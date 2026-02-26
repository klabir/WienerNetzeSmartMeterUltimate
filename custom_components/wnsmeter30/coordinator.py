"""Coordinator for shared WNSM polling and imports."""
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from homeassistant.const import UnitOfEnergy
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.util import dt as dt_util

from .AsyncSmartmeter import AsyncSmartmeter
from .api import Smartmeter
from .api.constants import ValueType
from .const import DOMAIN
from .importer import Importer
from .naming import (
    build_alias_id_keys,
    display_name as resolve_display_name,
    entity_id_key as resolve_entity_id_key,
    normalize_meter_aliases,
    statistic_id_key as resolve_statistic_id_key,
)

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
        enable_live_quarter_hour_sensor: bool,
        use_alias_for_ids: bool,
        log_scope: str,
    ) -> None:
        self._zaehlpunkte = zaehlpunkte
        try:
            historical_days_int = int(historical_days)
        except (TypeError, ValueError):
            historical_days_int = 1
        self._historical_days = max(1, historical_days_int)
        self._meter_aliases = normalize_meter_aliases(
            meter_aliases, set(self._zaehlpunkte)
        )
        self._use_alias_for_ids = bool(use_alias_for_ids)
        self._alias_id_keys = build_alias_id_keys(
            self._zaehlpunkte,
            self._meter_aliases,
            self._use_alias_for_ids,
            logger=_LOGGER,
        )
        self._enable_raw_api_response_write = enable_raw_api_response_write
        self._enable_daily_cons_statistics = enable_daily_cons_statistics
        self._enable_daily_meter_read_statistics = enable_daily_meter_read_statistics
        self._enable_live_quarter_hour_sensor = enable_live_quarter_hour_sensor
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
        recent_calls = self._smartmeter.get_recent_api_calls()
        filtered_calls = [
            call
            for call in recent_calls
            if f"/{zaehlpunkt}/" in call.get("endpoint", "")
            or f"\\{zaehlpunkt}\\" in (call.get("file_path") or "")
            or f"/{zaehlpunkt}/" in (call.get("file_path") or "")
        ]
        latest_api_call = filtered_calls[-1].get("timestamp") if len(filtered_calls) > 0 else None
        attributes["latest_api_call"] = latest_api_call
        attributes["latest_api_call_date"] = None
        attributes["latest_api_call_time"] = None
        if isinstance(latest_api_call, str):
            try:
                parsed = datetime.fromisoformat(latest_api_call.replace("Z", "+00:00"))
                attributes["latest_api_call_date"] = parsed.date().isoformat()
                attributes["latest_api_call_time"] = parsed.time().replace(
                    microsecond=0
                ).isoformat()
            except ValueError:
                if "T" in latest_api_call:
                    date_part, time_part = latest_api_call.split("T", maxsplit=1)
                    attributes["latest_api_call_date"] = date_part
                    attributes["latest_api_call_time"] = time_part
        if not self._enable_raw_api_response_write:
            return

        logging_status = self._smartmeter.get_raw_api_logging_status()
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
        return resolve_display_name(zaehlpunkt, self._meter_aliases)

    def entity_id_key(self, zaehlpunkt: str) -> str:
        return resolve_entity_id_key(zaehlpunkt, self._alias_id_keys)

    def statistic_id_key(self, zaehlpunkt: str) -> str:
        return resolve_statistic_id_key(zaehlpunkt, self._alias_id_keys)

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

    @staticmethod
    def _quarter_hour_factor(unit_of_measurement: str | None) -> float:
        unit_upper = str(unit_of_measurement or "WH").upper()
        if unit_upper == "WH":
            return 1e-3
        if unit_upper == "KWH":
            return 1.0
        _LOGGER.debug(
            "Unknown quarter-hour unit '%s'. Assuming KWH for live value conversion.",
            unit_of_measurement,
        )
        return 1.0

    @staticmethod
    def _extract_live_row_reading(row: dict[str, Any]) -> Any:
        return row.get("wert") if row.get("wert") is not None else row.get("messwert")

    @staticmethod
    def _extract_live_row_time_from(row: dict[str, Any]) -> Any:
        return row.get("zeitpunktVon") or row.get("zeitVon")

    @staticmethod
    def _extract_live_row_time_to(row: dict[str, Any]) -> Any:
        return row.get("zeitpunktBis") or row.get("zeitBis")

    @staticmethod
    def _extract_latest_quarter_hour_row(values: list[dict[str, Any]]) -> dict[str, Any] | None:
        latest_row = None
        latest_ts = None
        for value in values:
            raw_value = WNSMDataUpdateCoordinator._extract_live_row_reading(value)
            if raw_value is None:
                continue
            ts = dt_util.parse_datetime(
                WNSMDataUpdateCoordinator._extract_live_row_time_to(value)
                or WNSMDataUpdateCoordinator._extract_live_row_time_from(value)
            )
            if ts is None:
                if latest_row is None:
                    latest_row = value
                continue
            if latest_ts is None or ts > latest_ts:
                latest_ts = ts
                latest_row = value
        return latest_row

    async def _fetch_live_quarter_hour_reading(
        self, zaehlpunkt: str
    ) -> tuple[float | None, dict[str, Any]]:
        now = datetime.now(timezone.utc)
        lookup_start = now - timedelta(days=2)
        lookup_window_start_utc = lookup_start.isoformat()
        lookup_window_end_utc = now.isoformat()
        attempted_sources: list[str] = []
        source_errors: dict[str, str] = {}

        def _diagnostic_attributes(
            status: str, source_endpoint: str | None = None
        ) -> dict[str, Any]:
            attributes: dict[str, Any] = {
                "status": status,
                "source_endpoint": source_endpoint,
                "source_granularity": ValueType.QUARTER_HOUR.value,
                "source_attempt_order": list(attempted_sources),
                "lookup_window_start_utc": lookup_window_start_utc,
                "lookup_window_end_utc": lookup_window_end_utc,
            }
            if source_errors:
                attributes["source_errors"] = dict(source_errors)
            return attributes

        def _extract_from_payload(
            source_endpoint: str, payload: dict[str, Any]
        ) -> tuple[float | None, dict[str, Any]] | None:
            values = payload.get("values")
            if not isinstance(values, list) or len(values) == 0:
                _LOGGER.debug(
                    "Live quarter-hour source %s returned no values for %s (window %s to %s).",
                    source_endpoint,
                    zaehlpunkt,
                    lookup_window_start_utc,
                    lookup_window_end_utc,
                )
                return None

            latest_row = self._extract_latest_quarter_hour_row(values)
            if latest_row is None:
                _LOGGER.debug(
                    "Live quarter-hour source %s had values but no usable latest row for %s.",
                    source_endpoint,
                    zaehlpunkt,
                )
                return None

            raw_reading = self._extract_live_row_reading(latest_row)
            try:
                raw_reading_float = float(raw_reading)
            except (TypeError, ValueError):
                _LOGGER.debug(
                    "Ignoring live quarter-hour reading with non-numeric value for %s: %s",
                    zaehlpunkt,
                    raw_reading,
                )
                return None

            unit = payload.get("unitOfMeasurement")
            reading_kwh = raw_reading_float * self._quarter_hour_factor(unit)
            reading_quality = latest_row.get("qualitaet")
            if reading_quality is None and latest_row.get("geschaetzt") is not None:
                reading_quality = "EST" if bool(latest_row.get("geschaetzt")) else "VAL"

            attributes = _diagnostic_attributes("ok", source_endpoint)
            attributes.update(
                {
                    "reading_time_from": self._extract_live_row_time_from(latest_row),
                    "reading_time_to": self._extract_live_row_time_to(latest_row),
                    "reading_quality": reading_quality,
                    "reading_raw_value": raw_reading,
                    "reading_unit": unit,
                    "reading_kwh": reading_kwh,
                    "equivalent_power_w": reading_kwh * 4000,
                }
            )
            _LOGGER.debug(
                "Live quarter-hour value for %s from %s: raw=%s %s, kWh=%s, from=%s, to=%s, quality=%s",
                zaehlpunkt,
                source_endpoint,
                raw_reading,
                unit,
                reading_kwh,
                attributes.get("reading_time_from"),
                attributes.get("reading_time_to"),
                reading_quality,
            )
            return (
                reading_kwh,
                attributes,
            )

        attempted_sources.append("bewegungsdaten")
        try:
            bewegungsdaten = await self._async_smartmeter.get_bewegungsdaten(
                zaehlpunkt=zaehlpunkt,
                start=lookup_start,
                end=now,
                granularity=ValueType.QUARTER_HOUR,
            )
            extracted = _extract_from_payload("bewegungsdaten", bewegungsdaten)
            if extracted is not None:
                return extracted
        except Exception as exception:  # pylint: disable=broad-except
            source_errors["bewegungsdaten"] = str(exception)
            _LOGGER.debug(
                "Live quarter-hour bewegungsdaten request failed for %s: %s",
                zaehlpunkt,
                exception,
            )

        attempted_sources.append("historical_data")
        try:
            historic_data = await self._async_smartmeter.get_historic_data(
                zaehlpunkt=zaehlpunkt,
                date_from=lookup_start.date(),
                date_to=now.date(),
                granularity=ValueType.QUARTER_HOUR,
            )
            extracted = _extract_from_payload("historical_data", historic_data)
            if extracted is not None:
                return extracted
        except Exception as exception:  # pylint: disable=broad-except
            source_errors["historical_data"] = str(exception)
            _LOGGER.debug(
                "Live quarter-hour historical_data request failed for %s: %s",
                zaehlpunkt,
                exception,
            )

        _LOGGER.debug(
            "No live quarter-hour value for %s (attempted_sources=%s, window %s to %s).",
            zaehlpunkt,
            attempted_sources,
            lookup_window_start_utc,
            lookup_window_end_utc,
        )
        return None, _diagnostic_attributes("no_data")

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
            live_quarter_hour_value: float | int | None = None
            live_quarter_hour_attributes: dict[str, Any] = {}
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
                    if self._enable_live_quarter_hour_sensor:
                        try:
                            (
                                live_quarter_hour_value,
                                live_quarter_hour_attributes,
                            ) = await self._fetch_live_quarter_hour_reading(zaehlpunkt)
                        except Exception as exception:  # pylint: disable=broad-except
                            _LOGGER.warning(
                                "Failed to update live quarter-hour value for %s: %s",
                                zaehlpunkt,
                                exception,
                            )
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
                "live_quarter_hour_value": live_quarter_hour_value,
                "live_quarter_hour_attributes": live_quarter_hour_attributes,
                "attributes": attributes,
                "available": available,
            }
        return data
