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
        enable_daily_consumption_statistics: bool = True,
    ):
        self.id = f'{DOMAIN}:{zaehlpunkt.lower()}'
        self.cumulative_id = f"{self.id}_cum_abs"
        self.daily_consumption_id = f"{self.id}_daily_cons"
        self.daily_meter_read_id = f"{self.id}_daily_meter_read"
        self.zaehlpunkt = zaehlpunkt
        self.granularity = granularity
        self.unit_of_measurement = unit_of_measurement
        self.hass = hass
        self.async_smartmeter = async_smartmeter
        self.skip_login = skip_login
        self.preloaded_zaehlpunkt = preloaded_zaehlpunkt
        self.enable_daily_consumption_statistics = enable_daily_consumption_statistics
        self._latest_daily_consumption_day_value: float | None = None

    def is_last_inserted_stat_valid(self, last_inserted_stat):
        return (
            self.id in last_inserted_stat
            and len(last_inserted_stat[self.id]) == 1
            and "sum" in last_inserted_stat[self.id][0]
            and "end" in last_inserted_stat[self.id][0]
        )

    def is_last_inserted_cumulative_stat_valid(self, last_inserted_stat):
        if (
            self.cumulative_id not in last_inserted_stat
            or len(last_inserted_stat[self.cumulative_id]) != 1
        ):
            return False

        row = last_inserted_stat[self.cumulative_id][0]
        if "state" not in row or "end" not in row:
            return False

        # Daily statistics-graph rendering is most reliable when this stream
        # also carries mean and sum values, so old rows should be upgraded.
        capabilities = self._statistics_metadata_capabilities()
        has_required_mean = (
            row.get("mean") is not None if capabilities["has_mean"] else True
        )
        has_required_sum = (
            row.get("sum") is not None if capabilities["has_sum"] else True
        )
        return has_required_mean and has_required_sum

    def is_last_inserted_daily_consumption_stat_valid(self, last_inserted_stat):
        if (
            self.daily_consumption_id not in last_inserted_stat
            or len(last_inserted_stat[self.daily_consumption_id]) != 1
        ):
            return False

        row = last_inserted_stat[self.daily_consumption_id][0]
        if "state" not in row or "sum" not in row or "end" not in row:
            return False

        capabilities = self._statistics_metadata_capabilities()
        has_required_mean = (
            row.get("mean") is not None if capabilities["has_mean"] else True
        )
        has_required_sum = (
            row.get("sum") is not None if capabilities["has_sum"] else True
        )
        state_value = row.get("state")
        sum_value = row.get("sum")
        if state_value is None:
            return False
        try:
            # _daily_cons is now cumulative (state==sum). This also marks
            # previously imported daily-delta rows as invalid so backfill upgrades them.
            state_equals_sum = abs(float(state_value) - float(sum_value)) < 1e-9
        except (TypeError, ValueError):
            state_equals_sum = False
        return has_required_mean and has_required_sum and state_equals_sum

    def is_last_inserted_daily_meter_read_stat_valid(self, last_inserted_stat):
        if (
            self.daily_meter_read_id not in last_inserted_stat
            or len(last_inserted_stat[self.daily_meter_read_id]) != 1
        ):
            return False

        row = last_inserted_stat[self.daily_meter_read_id][0]
        if "state" not in row or "end" not in row:
            return False

        capabilities = self._statistics_metadata_capabilities()
        has_required_mean = (
            row.get("mean") is not None if capabilities["has_mean"] else True
        )
        has_required_sum = (
            row.get("sum") is not None if capabilities["has_sum"] else True
        )
        return has_required_mean and has_required_sum

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
            "mean_type_arithmetic": 1,
        }
        try:
            from homeassistant.components.recorder.db_schema import StatisticsMeta

            columns = set(StatisticsMeta.__table__.columns.keys())
            capabilities["has_mean"] = "has_mean" in columns
            capabilities["has_sum"] = "has_sum" in columns
            capabilities["unit_class"] = "unit_class" in columns
            capabilities["mean_type"] = "mean_type" in columns
            if capabilities["mean_type"]:
                statistic_mean_type = None
                try:
                    from homeassistant.components.recorder.models.statistics import (
                        StatisticMeanType,
                    )

                    statistic_mean_type = StatisticMeanType
                except Exception:  # pylint: disable=broad-except
                    try:
                        from homeassistant.components.recorder.models import StatisticMeanType

                        statistic_mean_type = StatisticMeanType
                    except Exception:  # pylint: disable=broad-except
                        statistic_mean_type = None

                if statistic_mean_type is not None:
                    capabilities["mean_type_none"] = getattr(
                        statistic_mean_type, "NONE", 0
                    )
                    capabilities["mean_type_arithmetic"] = getattr(
                        statistic_mean_type, "ARITHMETIC", 1
                    )
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
        number_of_stats: int = 1,
    ) -> dict[str, list[dict[str, Any]]]:
        return await get_instance(self.hass).async_add_executor_job(
            get_last_statistics,
            self.hass,
            number_of_stats,
            statistic_id,
            True,  # convert the units
            types,
        )

    @staticmethod
    def _stat_row_value(row: dict[str, Any]) -> float | None:
        value = row.get("state")
        if value is None:
            value = row.get("sum")
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    async def _get_latest_daily_consumption_value(self) -> float | None:
        last_inserted_stat = await self._get_last_inserted_statistics(
            self.daily_consumption_id,
            {"state", "sum"},
        )
        rows = last_inserted_stat.get(self.daily_consumption_id)
        if not rows:
            return None
        return self._stat_row_value(rows[0])

    async def _get_latest_daily_consumption_day_value(self) -> float | None:
        last_inserted_stat = await self._get_last_inserted_statistics(
            self.daily_consumption_id,
            {"state", "sum"},
            number_of_stats=2,
        )
        rows = last_inserted_stat.get(self.daily_consumption_id)
        if not rows:
            return None

        latest_total = self._stat_row_value(rows[0])
        if latest_total is None:
            return None
        if len(rows) < 2:
            return latest_total

        previous_total = self._stat_row_value(rows[1])
        if previous_total is None:
            return latest_total

        day_value = latest_total - previous_total
        if day_value < 0:
            return latest_total
        return day_value

    async def _backfill_cumulative_from_existing_sum(self) -> None:
        """Populate cumulative state stream from existing hourly sum stream once."""
        existing_cumulative = await self._get_last_inserted_statistics(
            self.cumulative_id, {"state", "mean", "sum"}
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
                    mean=float(row_sum),
                    sum=float(row_sum),
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

    async def _backfill_daily_consumption_from_existing_rows(self) -> None:
        """Upgrade daily consumption stream so rows always carry state/mean/sum."""
        existing_daily = await self._get_last_inserted_statistics(
            self.daily_consumption_id, {"state", "mean", "sum"}
        )
        if self.is_last_inserted_daily_consumption_stat_valid(existing_daily):
            return

        rows_by_id = await get_instance(self.hass).async_add_executor_job(
            statistics_during_period,
            self.hass,
            datetime(1970, 1, 1, tzinfo=timezone.utc),
            None,
            {self.daily_consumption_id},
            "hour",
            None,
            {"state", "sum"},
        )
        rows = rows_by_id.get(self.daily_consumption_id, [])
        if not rows:
            return

        daily_metadata = self.get_daily_consumption_statistics_metadata()
        daily_statistics: list[StatisticData] = []
        for row in rows:
            row_sum = row.get("sum")
            row_start = self._to_datetime(row.get("start"))
            if row_sum is None or row_start is None:
                continue
            daily_statistics.append(
                StatisticData(
                    start=row_start,
                    state=float(row_sum),
                    mean=float(row_sum),
                    sum=float(row_sum),
                )
            )

        if not daily_statistics:
            return

        _LOGGER.info(
            "Backfilling daily consumption statistics for %s with %d rows",
            self.zaehlpunkt,
            len(daily_statistics),
        )
        async_add_external_statistics(self.hass, daily_metadata, daily_statistics)

    async def _backfill_daily_meter_read_from_existing_rows(self) -> None:
        """Upgrade daily meter-read stream so rows always carry state/mean/sum."""
        existing_daily_meter_read = await self._get_last_inserted_statistics(
            self.daily_meter_read_id, {"state", "mean", "sum"}
        )
        if self.is_last_inserted_daily_meter_read_stat_valid(existing_daily_meter_read):
            return

        rows_by_id = await get_instance(self.hass).async_add_executor_job(
            statistics_during_period,
            self.hass,
            datetime(1970, 1, 1, tzinfo=timezone.utc),
            None,
            {self.daily_meter_read_id},
            "hour",
            None,
            {"state", "sum"},
        )
        rows = rows_by_id.get(self.daily_meter_read_id, [])
        if not rows:
            return

        daily_meter_read_metadata = self.get_daily_meter_read_statistics_metadata()
        daily_meter_read_statistics: list[StatisticData] = []
        for row in rows:
            row_value = row.get("state")
            if row_value is None:
                row_value = row.get("sum")
            row_start = self._to_datetime(row.get("start"))
            if row_value is None or row_start is None:
                continue
            daily_meter_read_statistics.append(
                StatisticData(
                    start=row_start,
                    state=float(row_value),
                    mean=float(row_value),
                    sum=float(row_value),
                )
            )

        if not daily_meter_read_statistics:
            return

        _LOGGER.info(
            "Backfilling daily meter-read statistics for %s with %d rows",
            self.zaehlpunkt,
            len(daily_meter_read_statistics),
        )
        async_add_external_statistics(
            self.hass,
            daily_meter_read_metadata,
            daily_meter_read_statistics,
        )

    def _ensure_statistics_metadata(self) -> None:
        """Ensure metadata is updated for existing statistic IDs across core versions."""
        async_add_external_statistics(self.hass, self.get_statistics_metadata(), [])
        async_add_external_statistics(
            self.hass, self.get_cumulative_statistics_metadata(), []
        )
        if self.enable_daily_consumption_statistics:
            async_add_external_statistics(
                self.hass, self.get_daily_consumption_statistics_metadata(), []
            )
        async_add_external_statistics(
            self.hass, self.get_daily_meter_read_statistics_metadata(), []
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

    async def async_import(self) -> dict[str, float | None]:
        daily_consumption_value: float | None = None
        daily_consumption_day_value: float | None = None
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
                return {
                    "daily_consumption_value": None,
                    "daily_consumption_day_value": None,
                }

            self._ensure_statistics_metadata()

            if not self.is_last_inserted_stat_valid(last_inserted_stat):
                # No previous data - start from scratch
                _LOGGER.warning("Starting import of historical data. This might take some time.")
                _sum = await self._initial_import_statistics()
            else:
                start_off_point = self.prepare_start_off_point(last_inserted_stat)
                if start_off_point is None:
                    await self._backfill_cumulative_from_existing_sum()
                    if self.enable_daily_consumption_statistics:
                        await self._backfill_daily_consumption_from_existing_rows()
                        daily_consumption_value = (
                            await self._safe_import_daily_consumption_statistics()
                        )
                    await self._backfill_daily_meter_read_from_existing_rows()
                    await self._safe_import_daily_meter_read_statistics()
                    if (
                        self.enable_daily_consumption_statistics
                        and daily_consumption_value is None
                    ):
                        daily_consumption_value = (
                            await self._get_latest_daily_consumption_value()
                        )
                    if self.enable_daily_consumption_statistics:
                        daily_consumption_day_value = (
                            self._latest_daily_consumption_day_value
                            if self._latest_daily_consumption_day_value is not None
                            else await self._get_latest_daily_consumption_day_value()
                        )
                    return {
                        "daily_consumption_value": daily_consumption_value,
                        "daily_consumption_day_value": daily_consumption_day_value,
                    }
                start, _sum = start_off_point
                _sum = await self._incremental_import_statistics(start, _sum)

            await self._backfill_cumulative_from_existing_sum()
            if self.enable_daily_consumption_statistics:
                await self._backfill_daily_consumption_from_existing_rows()
                daily_consumption_value = (
                    await self._safe_import_daily_consumption_statistics()
                )
            await self._backfill_daily_meter_read_from_existing_rows()
            await self._safe_import_daily_meter_read_statistics()

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
            if self.enable_daily_consumption_statistics and daily_consumption_value is None:
                daily_consumption_value = await self._get_latest_daily_consumption_value()
            if self.enable_daily_consumption_statistics:
                daily_consumption_day_value = (
                    self._latest_daily_consumption_day_value
                    if self._latest_daily_consumption_day_value is not None
                    else await self._get_latest_daily_consumption_day_value()
                )
        except TimeoutError as e:
            _LOGGER.warning("Error retrieving data from smart meter api - Timeout: %s" % e)
        except RuntimeError as e:
            _LOGGER.exception("Error retrieving data from smart meter api - Error: %s" % e)
        return {
            "daily_consumption_value": daily_consumption_value,
            "daily_consumption_day_value": daily_consumption_day_value,
        }

    def _build_statistics_metadata(
        self,
        statistic_id: str,
        name: str,
        has_mean: bool,
        has_sum: bool,
    ) -> StatisticMetaData:
        capabilities = self._statistics_metadata_capabilities()
        metadata: dict[str, Any] = {
            "source": DOMAIN,
            "statistic_id": statistic_id,
            "name": name,
            "unit_of_measurement": self.unit_of_measurement,
        }
        if capabilities["has_mean"]:
            metadata["has_mean"] = has_mean
        if capabilities["has_sum"]:
            metadata["has_sum"] = has_sum
        if capabilities["unit_class"]:
            metadata["unit_class"] = EnergyConverter.UNIT_CLASS
        if capabilities["mean_type"]:
            metadata["mean_type"] = (
                capabilities["mean_type_arithmetic"]
                if has_mean
                else capabilities["mean_type_none"]
            )
        return StatisticMetaData(**metadata)

    def get_statistics_metadata(self):
        return self._build_statistics_metadata(
            statistic_id=self.id,
            name=self.zaehlpunkt,
            has_mean=False,
            has_sum=True,
        )

    def get_cumulative_statistics_metadata(self):
        return self._build_statistics_metadata(
            statistic_id=self.cumulative_id,
            name=f"{self.zaehlpunkt} cumulative",
            has_mean=True,
            has_sum=True,
        )

    def get_daily_consumption_statistics_metadata(self):
        return self._build_statistics_metadata(
            statistic_id=self.daily_consumption_id,
            name=f"{self.zaehlpunkt} daily consumption",
            has_mean=True,
            has_sum=True,
        )

    def get_daily_meter_read_statistics_metadata(self):
        return self._build_statistics_metadata(
            statistic_id=self.daily_meter_read_id,
            name=f"{self.zaehlpunkt} daily meter read",
            has_mean=True,
            has_sum=True,
        )

    async def _initial_import_statistics(self):
        return await self._import_statistics()

    async def _incremental_import_statistics(self, start: datetime, total_usage: Decimal):
        return await self._import_statistics(start=start, total_usage=total_usage)

    async def _safe_import_daily_consumption_statistics(self) -> float | None:
        try:
            value = await self._import_daily_consumption_statistics()
            if value is None:
                return None
            return float(value)
        except Exception as err:  # pylint: disable=broad-except
            _LOGGER.warning(
                "Skipping daily consumption statistics import for %s: %s",
                self.zaehlpunkt,
                err,
            )
            return None

    async def _safe_import_daily_meter_read_statistics(self) -> None:
        try:
            await self._import_daily_meter_read_statistics()
        except Exception as err:  # pylint: disable=broad-except
            _LOGGER.warning(
                "Skipping daily meter-read statistics import for %s: %s",
                self.zaehlpunkt,
                err,
            )

    @staticmethod
    def _unit_factor(unit_of_measurement: str) -> float:
        unit_upper = str(unit_of_measurement).upper()
        if unit_upper == "WH":
            return 1e-3
        if unit_upper == "KWH":
            return 1.0
        raise NotImplementedError(
            f'Unit {unit_upper}" is not yet implemented. Please report!'
        )

    async def _import_daily_consumption_statistics(
        self,
        start: datetime = None,
        end: datetime = None,
        total_usage: Decimal = Decimal(0),
    ) -> Decimal | None:
        self._latest_daily_consumption_day_value = None
        if start is None:
            last_inserted_stat = await self._get_last_inserted_statistics(
                self.daily_consumption_id,
                {"state", "mean", "sum"},
            )
            if self.is_last_inserted_daily_consumption_stat_valid(last_inserted_stat):
                row = last_inserted_stat[self.daily_consumption_id][0]
                start = self._to_datetime(row.get("end"))
                if start is None:
                    _LOGGER.warning(
                        "Skipping incremental import for %s daily consumption due to invalid end timestamp: %s",
                        self.zaehlpunkt,
                        row.get("end"),
                    )
                    return total_usage
                total_usage = Decimal(row["sum"])
            else:
                start = (
                    datetime.now(timezone.utc)
                    .replace(hour=0, minute=0, second=0, microsecond=0)
                    - timedelta(days=365 * 3)
                )

        if end is None:
            end = datetime.now(timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        if start.tzinfo is None:
            raise ValueError("start datetime must be timezone-aware!")
        if start > end:
            _LOGGER.warning(
                "Ignoring daily consumption import since last import happened in the future %s > %s",
                start,
                end,
            )
            return total_usage

        daily_data = await self.async_smartmeter.get_historic_daily_consumption(
            self.zaehlpunkt, start, end
        )
        values = daily_data.get("values") or []
        if len(values) == 0:
            _LOGGER.debug(
                "Batch of data starting at %s does not contain daily consumption values.",
                start,
            )
            return total_usage

        unit_of_measurement = daily_data.get("unitOfMeasurement")
        if unit_of_measurement is None:
            raise ValueError(
                "WienerNetze returned non-empty daily history without unitOfMeasurement"
            )
        factor = self._unit_factor(unit_of_measurement)

        metadata = self.get_daily_consumption_statistics_metadata()
        statistics: list[StatisticData] = []
        last_ts = start
        latest_daily_usage: float | None = None
        for value in values:
            ts = dt_util.parse_datetime(value.get("zeitpunktVon"))
            if ts is None:
                continue
            if ts < last_ts:
                _LOGGER.warning(
                    "Timestamp from API (%s) is less than previously collected timestamp (%s), ignoring value!",
                    ts,
                    last_ts,
                )
                continue
            last_ts = ts
            if value.get("wert") is None:
                continue
            reading = Decimal(str(value["wert"])) * Decimal(str(factor))
            latest_daily_usage = float(reading)
            total_usage += reading
            total_usage_float = float(total_usage)
            statistics.append(
                StatisticData(
                    start=ts,
                    state=total_usage_float,
                    mean=total_usage_float,
                    sum=total_usage_float,
                )
            )

        if len(statistics) == 0:
            return total_usage

        _LOGGER.debug(
            "Importing daily consumption statistics from %s to %s",
            statistics[0],
            statistics[-1],
        )
        async_add_external_statistics(self.hass, metadata, statistics)
        self._latest_daily_consumption_day_value = latest_daily_usage
        return total_usage

    async def _import_daily_meter_read_statistics(
        self,
        start: datetime = None,
        end: datetime = None,
    ) -> None:
        if start is None:
            last_inserted_stat = await self._get_last_inserted_statistics(
                self.daily_meter_read_id,
                {"state", "mean", "sum"},
            )
            if self.is_last_inserted_daily_meter_read_stat_valid(last_inserted_stat):
                row = last_inserted_stat[self.daily_meter_read_id][0]
                start = self._to_datetime(row.get("end"))
                if start is None:
                    _LOGGER.warning(
                        "Skipping incremental import for %s daily meter read due to invalid end timestamp: %s",
                        self.zaehlpunkt,
                        row.get("end"),
                    )
                    return
            else:
                start = (
                    datetime.now(timezone.utc)
                    .replace(hour=0, minute=0, second=0, microsecond=0)
                    - timedelta(days=365 * 3)
                )

        if end is None:
            end = datetime.now(timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        if start.tzinfo is None:
            raise ValueError("start datetime must be timezone-aware!")
        if start > end:
            _LOGGER.warning(
                "Ignoring daily meter-read import since last import happened in the future %s > %s",
                start,
                end,
            )
            return

        meter_read_data = (
            await self.async_smartmeter.get_meter_reading_history_from_historic_data(
                self.zaehlpunkt, start, end
            )
        )
        values = meter_read_data.get("values") or []
        if len(values) == 0:
            _LOGGER.debug(
                "Batch of data starting at %s does not contain daily meter-read values.",
                start,
            )
            return

        unit_of_measurement = meter_read_data.get("unitOfMeasurement")
        if unit_of_measurement is None:
            raise ValueError(
                "WienerNetze returned non-empty meter-read history without unitOfMeasurement"
            )
        factor = Decimal(str(self._unit_factor(unit_of_measurement)))

        daily_points: dict[Any, tuple[datetime, Decimal]] = {}
        for value in values:
            raw_ts = dt_util.parse_datetime(
                value.get("zeitpunktVon")
                or value.get("zeitVon")
                or value.get("zeitpunktBis")
                or value.get("zeitBis")
            )
            if raw_ts is None:
                continue
            ts = dt_util.as_utc(raw_ts)
            if ts < start:
                continue
            # Recorder statistics expect normalized boundaries.
            ts = ts.replace(minute=0, second=0, microsecond=0)
            raw_value = value.get("wert")
            if raw_value is None:
                raw_value = value.get("messwert")
            if raw_value is None:
                continue
            reading = Decimal(str(raw_value)) * factor
            day_key = dt_util.as_local(raw_ts).date()
            previous = daily_points.get(day_key)
            if previous is None or ts > previous[0]:
                daily_points[day_key] = (ts, reading)

        if not daily_points:
            return

        metadata = self.get_daily_meter_read_statistics_metadata()
        statistics: list[StatisticData] = []
        previous_reading: float | None = None
        running_sum: float | None = None
        for day_key in sorted(daily_points.keys()):
            ts, reading_value = daily_points[day_key]
            reading = float(reading_value)
            if running_sum is None:
                running_sum = reading
            else:
                delta = 0.0 if previous_reading is None else reading - previous_reading
                if delta < 0:
                    # Meter replacements/resets can produce lower absolute reads.
                    # Keep sum monotonic so recorder accepts the statistics batch.
                    delta = 0.0
                running_sum += delta
            previous_reading = reading
            statistics.append(
                StatisticData(
                    start=ts,
                    state=reading,
                    mean=reading,
                    sum=running_sum,
                )
            )

        if len(statistics) == 0:
            return

        _LOGGER.debug(
            "Importing daily meter-read statistics from %s to %s",
            statistics[0],
            statistics[-1],
        )
        async_add_external_statistics(self.hass, metadata, statistics)

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
        factor = self._unit_factor(unit_of_measurement)

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
                StatisticData(
                    start=ts,
                    state=total_usage_float,
                    mean=total_usage_float,
                    sum=total_usage_float,
                )
            )
        if len(statistics) > 0:
            _LOGGER.debug(f"Importing statistics from {statistics[0]} to {statistics[-1]}")
        async_add_external_statistics(self.hass, metadata, statistics)
        async_add_external_statistics(self.hass, cumulative_metadata, cumulative_statistics)
        return total_usage
