import logging
from collections import defaultdict
from datetime import timedelta, timezone, datetime
from decimal import Decimal
from functools import lru_cache
from operator import itemgetter
from typing import Any

from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.models import (
    StatisticData,
    StatisticMetaData,
)
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics,
    get_last_statistics,
    statistics_during_period,
)
from homeassistant.core import HomeAssistant
from homeassistant.util import dt as dt_util
from homeassistant.util.unit_conversion import EnergyConverter

from .AsyncSmartmeter import AsyncSmartmeter
from .api.constants import ValueType
from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)


class Importer:

    def __init__(
        self,
        hass: HomeAssistant,
        async_smartmeter: AsyncSmartmeter,
        zaehlpunkt: str,
        unit_of_measurement: str,
        granularity: ValueType = ValueType.QUARTER_HOUR,
        skip_login: bool = False,
        preloaded_zaehlpunkt: dict | None = None,
    ):
        self.id = f'{DOMAIN}:{zaehlpunkt.lower()}'
        self.cumulative_id = f"{self.id}_cum_abs"
        self.zaehlpunkt = zaehlpunkt
        self.granularity = granularity
        self.unit_of_measurement = unit_of_measurement
        self.hass = hass
        self.async_smartmeter = async_smartmeter
        self.skip_login = skip_login
        self.preloaded_zaehlpunkt = preloaded_zaehlpunkt

    def is_last_inserted_stat_valid(self, last_inserted_stat):
        return (
            self.id in last_inserted_stat
            and len(last_inserted_stat[self.id]) == 1
            and "sum" in last_inserted_stat[self.id][0]
            and "end" in last_inserted_stat[self.id][0]
        )

    def is_last_inserted_cumulative_stat_valid(self, last_inserted_stat):
        return (
            self.cumulative_id in last_inserted_stat
            and len(last_inserted_stat[self.cumulative_id]) == 1
            and "state" in last_inserted_stat[self.cumulative_id][0]
            and "end" in last_inserted_stat[self.cumulative_id][0]
        )

    @staticmethod
    @lru_cache(maxsize=1)
    def _statistics_metadata_capabilities() -> dict[str, Any]:
        """Detect recorder metadata fields supported by the current HA core."""
        capabilities: dict[str, Any] = {
            "has_mean": True,
            "has_sum": True,
            "unit_class": False,
            "mean_type": False,
            "mean_type_none": 0,
        }
        try:
            from homeassistant.components.recorder.db_schema import StatisticsMeta

            columns = set(StatisticsMeta.__table__.columns.keys())
            capabilities["has_mean"] = "has_mean" in columns
            capabilities["has_sum"] = "has_sum" in columns
            capabilities["unit_class"] = "unit_class" in columns
            capabilities["mean_type"] = "mean_type" in columns
            if capabilities["mean_type"]:
                try:
                    from homeassistant.components.recorder.models import StatisticMeanType

                    capabilities["mean_type_none"] = StatisticMeanType.NONE
                except Exception:  # pylint: disable=broad-except
                    capabilities["mean_type_none"] = 0
        except Exception:  # pylint: disable=broad-except
            # Keep compatibility with older HA cores where these fields do not exist.
            return capabilities
        return capabilities

    @staticmethod
    def _to_datetime(value: Any) -> datetime | None:
        if isinstance(value, datetime):
            if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
                return None
            return dt_util.as_utc(value)
        if isinstance(value, (int, float)):
            return dt_util.utc_from_timestamp(value)
        if isinstance(value, str):
            parsed = dt_util.parse_datetime(value)
            if parsed is None:
                return None
            return dt_util.as_utc(parsed)
        return None

    async def _get_last_inserted_statistics(
        self,
        statistic_id: str,
        types: set[str],
    ) -> dict[str, list[dict[str, Any]]]:
        return await get_instance(self.hass).async_add_executor_job(
            get_last_statistics,
            self.hass,
            1,  # Get at most one entry
            statistic_id,
            True,  # convert the units
            types,
        )

    async def _backfill_cumulative_from_existing_sum(self) -> None:
        """Populate cumulative state stream from existing hourly sum stream once."""
        existing_cumulative = await self._get_last_inserted_statistics(
            self.cumulative_id, {"state"}
        )
        if self.is_last_inserted_cumulative_stat_valid(existing_cumulative):
            return

        rows_by_id = await get_instance(self.hass).async_add_executor_job(
            statistics_during_period,
            self.hass,
            datetime(1970, 1, 1, tzinfo=timezone.utc),
            None,
            {self.id},
            "hour",
            None,
            {"sum"},
        )
        rows = rows_by_id.get(self.id, [])
        if not rows:
            return

        cumulative_metadata = self.get_cumulative_statistics_metadata()
        cumulative_statistics: list[StatisticData] = []
        for row in rows:
            row_sum = row.get("sum")
            row_start = self._to_datetime(row.get("start"))
            if row_sum is None or row_start is None:
                continue
            cumulative_statistics.append(
                StatisticData(
                    start=row_start,
                    state=float(row_sum),
                )
            )

        if not cumulative_statistics:
            return

        _LOGGER.info(
            "Backfilling cumulative statistics for %s with %d rows",
            self.zaehlpunkt,
            len(cumulative_statistics),
        )
        async_add_external_statistics(self.hass, cumulative_metadata, cumulative_statistics)

    def _ensure_statistics_metadata(self) -> None:
        """Ensure metadata is updated for existing statistic IDs across core versions."""
        async_add_external_statistics(self.hass, self.get_statistics_metadata(), [])
        async_add_external_statistics(
            self.hass, self.get_cumulative_statistics_metadata(), []
        )

    def prepare_start_off_point(self, last_inserted_stat):
        # Previous data found in the statistics table
        _sum = Decimal(last_inserted_stat[self.id][0]["sum"])
        # The next start is the previous end
        # XXX: since HA core 2022.12, we get a datetime and not a str...
        # XXX: since HA core 2023.03, we get a float and not a datetime...
        start = last_inserted_stat[self.id][0]["end"]
        if isinstance(start, (int, float)):
            start = dt_util.utc_from_timestamp(start)
        if isinstance(start, str):
            start = dt_util.parse_datetime(start)

        if not isinstance(start, datetime):
            _LOGGER.error("HA core decided to change the return type AGAIN! "
                          "Please open a bug report. "
                          "Additional Information: %s Type: %s",
                          last_inserted_stat,
                          type(last_inserted_stat[self.id][0]["end"]))
            return None
        _LOGGER.debug("New starting datetime: %s", start)

        # Extra check to not strain the API too much:
        # If the last insert date is less than 24h away, simply exit here,
        # because we will not get any data from the API
        min_wait = timedelta(hours=24)
        delta_t = datetime.now(timezone.utc).replace(microsecond=0) - start.replace(microsecond=0)
        if delta_t <= min_wait:
            _LOGGER.debug(
                "Not querying the API, because last update is not older than 24 hours. Earliest update in %s" % (
                        min_wait - delta_t))
            return None
        return start, _sum

    async def async_import(self):
        # Query the statistics database for the last value
        # It is crucial to use get_instance here!
        last_inserted_stat = await self._get_last_inserted_statistics(
            self.id,
            # XXX: since HA core 2022.12 need to specify this:
            {"sum", "state"},  # the fields we want to query (state might be used in the future)
        )
        _LOGGER.debug("Last inserted stat: %s" % last_inserted_stat)
        try:
            if not self.skip_login:
                await self.async_smartmeter.login()
            zaehlpunkt = self.preloaded_zaehlpunkt
            if zaehlpunkt is None:
                zaehlpunkt = await self.async_smartmeter.get_zaehlpunkt(self.zaehlpunkt)

            if not self.async_smartmeter.is_active(zaehlpunkt):
                _LOGGER.debug("Smartmeter %s is not active" % zaehlpunkt)
                return

            self._ensure_statistics_metadata()

            if not self.is_last_inserted_stat_valid(last_inserted_stat):
                # No previous data - start from scratch
                _LOGGER.warning("Starting import of historical data. This might take some time.")
                _sum = await self._initial_import_statistics()
            else:
                start_off_point = self.prepare_start_off_point(last_inserted_stat)
                if start_off_point is None:
                    await self._backfill_cumulative_from_existing_sum()
                    return
                start, _sum = start_off_point
                _sum = await self._incremental_import_statistics(start, _sum)

            await self._backfill_cumulative_from_existing_sum()

            # XXX: Note that the state of this sensor must never be an integer value, such as 0!
            # If it is set to any number, home assistant will assume that a negative consumption
            # compensated the last statistics entry and add a negative consumption in the energy
            # dashboard.
            # This is a technical debt of HA, as we cannot import statistics and have states at the
            # same time.
            # Due to None, the sensor will always show "unkown" - but that is currently the only way
            # how historical data can be imported without rewriting the database on our own...
            last_inserted_stat = await self._get_last_inserted_statistics(
                self.id,
                {"sum"},  # the fields we want to query
            )
            _LOGGER.debug("Last inserted stat: %s", last_inserted_stat)
        except TimeoutError as e:
            _LOGGER.warning("Error retrieving data from smart meter api - Timeout: %s" % e)
        except RuntimeError as e:
            _LOGGER.exception("Error retrieving data from smart meter api - Error: %s" % e)

    def get_statistics_metadata(self):
        capabilities = self._statistics_metadata_capabilities()
        metadata: dict[str, Any] = {
            "source": DOMAIN,
            "statistic_id": self.id,
            "name": self.zaehlpunkt,
            "unit_of_measurement": self.unit_of_measurement,
        }
        if capabilities["has_mean"]:
            metadata["has_mean"] = False
        if capabilities["has_sum"]:
            metadata["has_sum"] = True
        if capabilities["unit_class"]:
            metadata["unit_class"] = EnergyConverter.UNIT_CLASS
        if capabilities["mean_type"]:
            metadata["mean_type"] = capabilities["mean_type_none"]
        return StatisticMetaData(**metadata)

    def get_cumulative_statistics_metadata(self):
        capabilities = self._statistics_metadata_capabilities()
        metadata: dict[str, Any] = {
            "source": DOMAIN,
            "statistic_id": self.cumulative_id,
            "name": f"{self.zaehlpunkt} cumulative",
            "unit_of_measurement": self.unit_of_measurement,
        }
        if capabilities["has_mean"]:
            metadata["has_mean"] = False
        if capabilities["has_sum"]:
            metadata["has_sum"] = False
        if capabilities["unit_class"]:
            metadata["unit_class"] = EnergyConverter.UNIT_CLASS
        if capabilities["mean_type"]:
            metadata["mean_type"] = capabilities["mean_type_none"]
        return StatisticMetaData(**metadata)

    async def _initial_import_statistics(self):
        return await self._import_statistics()

    async def _incremental_import_statistics(self, start: datetime, total_usage: Decimal):
        return await self._import_statistics(start=start, total_usage=total_usage)

    async def _import_statistics(self, start: datetime = None, end: datetime = None, total_usage: Decimal = Decimal(0)):
        """Import statistics"""

        start = start if start is not None else datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=365 * 3)
        end = end if end is not None else datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

        if start.tzinfo is None:
            raise ValueError("start datetime must be timezone-aware!")

        _LOGGER.debug("Selecting data up to %s" % end)
        if start > end:
            _LOGGER.warning(f"Ignoring async update since last import happened in the future (should not happen) {start} > {end}")
            return

        bewegungsdaten = await self.async_smartmeter.get_bewegungsdaten(self.zaehlpunkt, start, end, self.granularity)
        _LOGGER.debug(f"Mapped historical data: {bewegungsdaten}")

        values = bewegungsdaten.get("values") or []
        if len(values) == 0:
            _LOGGER.debug(
                "Batch of data starting at %s does not contain bewegungsdaten. "
                "Seems there is nothing to import, yet.",
                start,
            )
            return

        unit_of_measurement = bewegungsdaten.get("unitOfMeasurement")
        if unit_of_measurement is None:
            raise ValueError(
                "WienerNetze returned non-empty bewegungsdaten without unitOfMeasurement"
            )
        unit_of_measurement = str(unit_of_measurement).upper()
        if unit_of_measurement == 'WH':
            factor = 1e-3
        elif unit_of_measurement == 'KWH':
            factor = 1.0
        else:
            raise NotImplementedError(
                f'Unit {unit_of_measurement}" is not yet implemented. Please report!'
            )

        dates = defaultdict(Decimal)
        total_consumption = sum([v.get("wert", 0) for v in values])
        # Can actually check, if the whole batch can be skipped.
        if total_consumption == 0:
            _LOGGER.debug(
                "Batch of data starting at %s does not contain any bewegungsdaten. "
                "Seems there is nothing to import, yet.",
                start,
            )
            return

        last_ts = start
        for value in values:
            ts = dt_util.parse_datetime(value['zeitpunktVon'])
            if ts < last_ts:
                # This should prevent any issues with ambiguous values though...
                _LOGGER.warning(f"Timestamp from API ({ts}) is less than previously collected timestamp ({last_ts}), ignoring value!")
                continue
            last_ts = ts
            if value['wert'] is None:
                # Usually this means that the measurement is not yet in the WSTW database.
                continue
            reading = Decimal(value['wert'] * factor)
            if ts.minute % 15 != 0 or ts.second != 0 or ts.microsecond != 0:
                _LOGGER.warning(f"Unexpected time detected in historic data: {value}")
            dates[ts.replace(minute=0)] += reading
            if value['geschaetzt']:
                _LOGGER.debug(f"Not seen that before: Estimated Value found for {ts}: {reading}")

        statistics = []
        cumulative_statistics = []
        metadata = self.get_statistics_metadata()
        cumulative_metadata = self.get_cumulative_statistics_metadata()

        for ts, usage in sorted(dates.items(), key=itemgetter(0)):
            total_usage += usage
            total_usage_float = float(total_usage)
            statistics.append(
                StatisticData(start=ts, sum=total_usage_float, state=float(usage))
            )
            cumulative_statistics.append(
                StatisticData(start=ts, state=total_usage_float)
            )
        if len(statistics) > 0:
            _LOGGER.debug(f"Importing statistics from {statistics[0]} to {statistics[-1]}")
        async_add_external_statistics(self.hass, metadata, statistics)
        async_add_external_statistics(self.hass, cumulative_metadata, cumulative_statistics)
        return total_usage
