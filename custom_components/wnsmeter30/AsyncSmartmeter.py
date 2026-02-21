import asyncio
import logging
from asyncio import Future
from datetime import datetime

from homeassistant.core import HomeAssistant
from homeassistant.util import dt as dt_util

from .api import Smartmeter
from .api.constants import ValueType
from .const import ATTRS_METERREADINGS_CALL, ATTRS_BASEINFORMATION_CALL, ATTRS_CONSUMPTIONS_CALL, ATTRS_BEWEGUNGSDATEN, ATTRS_ZAEHLPUNKTE_CALL, ATTRS_HISTORIC_DATA, ATTRS_VERBRAUCH_CALL
from .utils import translate_dict

_LOGGER = logging.getLogger(__name__)

class AsyncSmartmeter:

    def __init__(self, hass: HomeAssistant, smartmeter: Smartmeter = None):
        self.hass = hass
        self.smartmeter = smartmeter
        self.login_lock = asyncio.Lock()

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
        response = await self.hass.async_add_executor_job(
            self.smartmeter.historical_day_consumption,
            zaehlpunkt,
            date_from,
            date_to,
        )
        if "Exception" in response:
            raise RuntimeError(f"Cannot access daily historic data: {response}")
        _LOGGER.debug(f"Raw daily historic data: {response}")

        values = []
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

        return {
            "obisCode": response.get("obisCode"),
            "unitOfMeasurement": response.get("einheit"),
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
        response = await self.hass.async_add_executor_job(
            self.smartmeter.historical_meter_reading,
            zaehlpunkt,
            start_date,
            end_date,
        )
        if "Exception" in response:
            raise RuntimeError(f"Cannot access historic data: {response}")
        _LOGGER.debug(f"Raw historical data: {response}")
        return translate_dict(response, ATTRS_HISTORIC_DATA)

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
        response = await self.hass.async_add_executor_job(
            self.smartmeter.bewegungsdaten,
            zaehlpunkt,
            start,
            end,
            granularity
        )
        if "Exception" in response:
            raise RuntimeError(f"Cannot access bewegungsdaten: {response}")
        _LOGGER.debug(f"Raw bewegungsdaten: {response}")
        return translate_dict(response, ATTRS_BEWEGUNGSDATEN)

    async def get_consumptions(self) -> dict[str, str]:
        """
        asynchronously get and parse /consumptions response
        Returns response already sanitized of the specified zaehlpunkt in ctor
        """
        response = await self.hass.async_add_executor_job(self.smartmeter.consumptions)
        if "Exception" in response:
            raise RuntimeError("Cannot access /consumptions: ", response)
        return translate_dict(response, ATTRS_CONSUMPTIONS_CALL)
