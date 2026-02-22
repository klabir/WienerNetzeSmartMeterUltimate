import asyncio
import logging
from asyncio import Future
from datetime import datetime, timedelta, timezone

from homeassistant.core import HomeAssistant
from homeassistant.util import dt as dt_util

from .api import Smartmeter
from .api.constants import ValueType
from .const import (
    ATTRS_METERREADINGS_CALL,
    ATTRS_BASEINFORMATION_CALL,
    ATTRS_CONSUMPTIONS_CALL,
    ATTRS_BEWEGUNGSDATEN,
    ATTRS_ZAEHLPUNKTE_CALL,
    ATTRS_HISTORIC_DATA,
    ATTRS_VERBRAUCH_CALL,
    HISTORICAL_API_CHUNK_DAYS,
)
from .utils import translate_dict

_LOGGER = logging.getLogger(__name__)

class AsyncSmartmeter:

    def __init__(self, hass: HomeAssistant, smartmeter: Smartmeter = None):
        self.hass = hass
        self.smartmeter = smartmeter
        self.login_lock = asyncio.Lock()

    @staticmethod
    def _ensure_utc(value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
            return value.replace(tzinfo=timezone.utc)
        return dt_util.as_utc(value)

    @classmethod
    def _build_chunk_ranges(
        cls, start: datetime | None, end: datetime | None
    ) -> list[tuple[datetime | None, datetime | None]]:
        if start is None or end is None:
            return [(start, end)]
        start_utc = cls._ensure_utc(start)
        end_utc = cls._ensure_utc(end)
        if start_utc is None or end_utc is None:
            return [(start, end)]
        start_day = start_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        end_day = end_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        if start_day > end_day:
            return []

        ranges: list[tuple[datetime, datetime]] = []
        current_start = start_day
        span_days = max(1, HISTORICAL_API_CHUNK_DAYS)
        while current_start <= end_day:
            current_end = min(
                current_start + timedelta(days=span_days - 1),
                end_day,
            )
            ranges.append((current_start, current_end))
            current_start = current_end + timedelta(days=1)

        if len(ranges) > 1:
            _LOGGER.debug(
                "Chunking historical request into %d calls (chunk size %d days)",
                len(ranges),
                span_days,
            )
        return ranges

    @staticmethod
    def _extract_timestamp(value: dict[str, any]) -> datetime | None:
        return dt_util.parse_datetime(
            value.get("zeitpunktVon")
            or value.get("zeitVon")
            or value.get("zeitpunktBis")
            or value.get("zeitBis")
        )

    @classmethod
    def _sort_values(cls, values: list[dict[str, any]]) -> list[dict[str, any]]:
        def _sort_key(item: dict[str, any]) -> tuple[int, datetime]:
            ts = cls._extract_timestamp(item)
            if ts is None:
                return (1, datetime.max.replace(tzinfo=timezone.utc))
            return (0, cls._ensure_utc(ts) or datetime.max.replace(tzinfo=timezone.utc))

        return sorted(values, key=_sort_key)

    @classmethod
    def _deduplicate_values(cls, values: list[dict[str, any]]) -> list[dict[str, any]]:
        deduplicated: dict[str, dict[str, any]] = {}
        for index, value in enumerate(values):
            ts = cls._extract_timestamp(value)
            if ts is None:
                key = f"idx:{index}"
            else:
                key = (cls._ensure_utc(ts) or ts).isoformat()
            deduplicated[key] = value
        return cls._sort_values(list(deduplicated.values()))

    async def login(self) -> Future:
        async with self.login_lock:
            return await self.hass.async_add_executor_job(self.smartmeter.login)

    async def get_meter_readings(self) -> dict[str, any]:
        """
        asynchronously get and parse /meterReadings response
        Returns response already sanitized of the specified zaehlpunkt in ctor
        """
        response = await self.hass.async_add_executor_job(
            self.smartmeter.meter_readings,
        )
        if "Exception" in response:
            raise RuntimeError("Cannot access /meterReadings: ", response)
        return translate_dict(response, ATTRS_METERREADINGS_CALL)


    async def get_base_information(self) -> dict[str, str]:
        """
        asynchronously get and parse /baseInformation response
        Returns response already sanitized of the specified zaehlpunkt in ctor
        """
        response = await self.hass.async_add_executor_job(self.smartmeter.base_information)
        if "Exception" in response:
            raise RuntimeError("Cannot access /baseInformation: ", response)
        return translate_dict(response, ATTRS_BASEINFORMATION_CALL)

    def contracts2zaehlpunkte(self, contracts: dict, zaehlpunkt: str) -> list[dict]:
        zaehlpunkte = []
        if contracts is not None and isinstance(contracts, list) and len(contracts) > 0:
            for contract in contracts:
                if "zaehlpunkte" in contract:
                    geschaeftspartner = contract["geschaeftspartner"] if "geschaeftspartner" in contract else None
                    zaehlpunkte += [
                        {**z, "geschaeftspartner": geschaeftspartner} for z in contract["zaehlpunkte"] if z["zaehlpunktnummer"] == zaehlpunkt
                    ]
        else:
            raise RuntimeError(f"Cannot access Zaehlpunkt {zaehlpunkt}")
        return zaehlpunkte

    async def get_zaehlpunkt(self, zaehlpunkt: str) -> dict[str, str]:
        """
        asynchronously get and parse /zaehlpunkt response
        Returns response already sanitized of the specified zaehlpunkt in ctor
        """
        contracts = await self.hass.async_add_executor_job(self.smartmeter.zaehlpunkte)
        zaehlpunkte = self.contracts2zaehlpunkte(contracts, zaehlpunkt)
        zp = [z for z in zaehlpunkte if z["zaehlpunktnummer"] == zaehlpunkt]
        if len(zp) == 0:
            raise RuntimeError(f"Zaehlpunkt {zaehlpunkt} not found")

        return (
            translate_dict(zp[0], ATTRS_ZAEHLPUNKTE_CALL)
            if len(zp) > 0
            else None
        )

    async def get_consumption(self, customer_id: str, zaehlpunkt: str, start_date: datetime):
        """Return 24h of hourly consumption starting from a date"""
        response = await self.hass.async_add_executor_job(
            self.smartmeter.verbrauch, customer_id, zaehlpunkt, start_date
        )
        if "Exception" in response:
            raise RuntimeError(f"Cannot access daily consumption: {response}")

        return translate_dict(response, ATTRS_VERBRAUCH_CALL)

    async def get_consumption_raw(self, customer_id: str, zaehlpunkt: str, start_date: datetime):
        """Return daily consumptions from the given start date until today"""
        response = await self.hass.async_add_executor_job(
            self.smartmeter.verbrauchRaw, customer_id, zaehlpunkt, start_date
        )
        if "Exception" in response:
            raise RuntimeError(f"Cannot access daily consumption: {response}")

        return translate_dict(response, ATTRS_VERBRAUCH_CALL)

    async def get_historic_data(self, zaehlpunkt: str, date_from: datetime = None, date_to: datetime = None, granularity: ValueType = ValueType.QUARTER_HOUR):
        """Return three years of historic quarter-hourly data"""
        response = await self.hass.async_add_executor_job(
            self.smartmeter.historical_data,
            zaehlpunkt,
            date_from,
            date_to,
            granularity
        )
        if "Exception" in response:
            raise RuntimeError(f"Cannot access historic data: {response}")
        _LOGGER.debug(f"Raw historical data: {response}")
        return translate_dict(response, ATTRS_HISTORIC_DATA)

    async def get_historic_daily_consumption(
        self,
        zaehlpunkt: str,
        date_from: datetime = None,
        date_to: datetime = None,
    ) -> dict[str, any]:
        """Return daily consumption history from historical data (`wertetyp=DAY`)."""
        ranges = self._build_chunk_ranges(date_from, date_to)
        responses: list[dict[str, any]] = []
        if len(ranges) == 0:
            return {"obisCode": None, "unitOfMeasurement": None, "values": []}

        for range_start, range_end in ranges:
            response = await self.hass.async_add_executor_job(
                self.smartmeter.historical_day_consumption,
                zaehlpunkt,
                range_start,
                range_end,
            )
            if "Exception" in response:
                raise RuntimeError(f"Cannot access daily historic data: {response}")
            _LOGGER.debug(f"Raw daily historic data: {response}")
            responses.append(response)

        values: list[dict[str, any]] = []
        for response in responses:
            for value in response.get("messwerte", []):
                quality = str(value.get("qualitaet", "")).upper()
                values.append(
                    {
                        "wert": value.get("messwert"),
                        "zeitpunktVon": value.get("zeitVon"),
                        "zeitpunktBis": value.get("zeitBis"),
                        "geschaetzt": quality not in {"", "VAL"},
                    }
                )

        values = self._deduplicate_values(values)

        obis_code = None
        unit = None
        for response in responses:
            obis_code = response.get("obisCode") or obis_code
            unit = response.get("einheit") or unit

        return {
            "obisCode": obis_code,
            "unitOfMeasurement": unit,
            "values": values,
        }

    async def get_meter_reading_from_historic_data(
        self,
        zaehlpunkt: str,
        start_date: datetime = None,
        end_date: datetime = None,
    ) -> float | None:
        """Return latest meter reading from historical METER_READ values."""
        meter_readings = await self.get_meter_reading_history_from_historic_data(
            zaehlpunkt,
            start_date,
            end_date,
        )
        values = meter_readings.get("values")
        if not isinstance(values, list) or len(values) == 0:
            return None

        latest_row = None
        latest_ts = None
        for messwert in values:
            if "messwert" not in messwert or messwert["messwert"] is None:
                continue
            ts = dt_util.parse_datetime(
                messwert.get("zeitBis") or messwert.get("zeitVon")
            )
            if ts is None:
                if latest_row is None:
                    latest_row = messwert
                continue
            if latest_ts is None or ts > latest_ts:
                latest_ts = ts
                latest_row = messwert

        if latest_row is None:
            return None
        return latest_row["messwert"] / 1000

    async def get_meter_reading_history_from_historic_data(
        self,
        zaehlpunkt: str,
        start_date: datetime = None,
        end_date: datetime = None,
    ) -> dict[str, any]:
        """Return historical meter reading values (`wertetyp=METER_READ`)."""
        ranges = self._build_chunk_ranges(start_date, end_date)
        translated_chunks: list[dict[str, any]] = []
        if len(ranges) == 0:
            return {"obisCode": None, "unitOfMeasurement": None, "values": []}

        for range_start, range_end in ranges:
            response = await self.hass.async_add_executor_job(
                self.smartmeter.historical_meter_reading,
                zaehlpunkt,
                range_start,
                range_end,
            )
            if "Exception" in response:
                raise RuntimeError(f"Cannot access historic data: {response}")
            _LOGGER.debug(f"Raw historical data: {response}")
            translated_chunks.append(translate_dict(response, ATTRS_HISTORIC_DATA))

        values: list[dict[str, any]] = []
        obis_code = None
        unit = None
        for translated in translated_chunks:
            values.extend(translated.get("values", []) or [])
            obis_code = translated.get("obisCode") or obis_code
            unit = translated.get("unitOfMeasurement") or unit

        return {
            "obisCode": obis_code,
            "unitOfMeasurement": unit,
            "values": self._deduplicate_values(values),
        }

    @staticmethod
    def is_active(zaehlpunkt_response: dict) -> bool:
        """
        returns active status of smartmeter, according to zaehlpunkt response
        """
        return (
                "active" not in zaehlpunkt_response or zaehlpunkt_response["active"]
        ) or (
                "smartMeterReady" not in zaehlpunkt_response
                or zaehlpunkt_response["smartMeterReady"]
        )

    async def get_bewegungsdaten(self, zaehlpunkt: str, start: datetime = None, end: datetime = None, granularity: ValueType = ValueType.QUARTER_HOUR):
        """Return three years of historic quarter-hourly data"""
        ranges = self._build_chunk_ranges(start, end)
        translated_chunks: list[dict[str, any]] = []
        if len(ranges) == 0:
            return {"values": []}

        for range_start, range_end in ranges:
            response = await self.hass.async_add_executor_job(
                self.smartmeter.bewegungsdaten,
                zaehlpunkt,
                range_start,
                range_end,
                granularity
            )
            if "Exception" in response:
                raise RuntimeError(f"Cannot access bewegungsdaten: {response}")
            _LOGGER.debug(f"Raw bewegungsdaten: {response}")
            translated_chunks.append(translate_dict(response, ATTRS_BEWEGUNGSDATEN))

        values: list[dict[str, any]] = []
        merged: dict[str, any] = {}
        for index, translated in enumerate(translated_chunks):
            if index == 0:
                merged = dict(translated)
            values.extend(translated.get("values", []) or [])

        merged["values"] = self._deduplicate_values(values)
        return merged

    async def get_consumptions(self) -> dict[str, str]:
        """
        asynchronously get and parse /consumptions response
        Returns response already sanitized of the specified zaehlpunkt in ctor
        """
        response = await self.hass.async_add_executor_job(self.smartmeter.consumptions)
        if "Exception" in response:
            raise RuntimeError("Cannot access /consumptions: ", response)
        return translate_dict(response, ATTRS_CONSUMPTIONS_CALL)
