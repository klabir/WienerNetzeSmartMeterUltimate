"""
WienerNetze Smartmeter sensor platform
"""
import collections.abc
import logging
from datetime import timedelta
from typing import Optional

import homeassistant.helpers.config_validation as cv
import voluptuous as vol
from homeassistant import core, config_entries
from homeassistant.components.sensor import (
    PLATFORM_SCHEMA
)
from homeassistant.const import (
    CONF_USERNAME,
    CONF_PASSWORD,
    CONF_DEVICE_ID,
)
from homeassistant.helpers.typing import (
    ConfigType,
    DiscoveryInfoType,
)
from .const import (
    CONF_ENABLE_DAILY_CONS,
    CONF_ENABLE_DAILY_METER_READ,
    CONF_ENABLE_RAW_API_RESPONSE_WRITE,
    CONF_SCAN_INTERVAL,
    CONF_SELECTED_ZAEHLPUNKTE,
    CONF_ZAEHLPUNKTE,
    DEFAULT_ENABLE_DAILY_CONS,
    DEFAULT_ENABLE_DAILY_METER_READ,
    DEFAULT_SCAN_INTERVAL_MINUTES,
    DOMAIN,
)
from .daily_cons_day_sensor import WNSMDailyConsDaySensor
from .coordinator import WNSMDataUpdateCoordinator
from .daily_cons_sensor import WNSMDailyConsSensor
from .wnsm_sensor import WNSMSensor

_LOGGER = logging.getLogger(__name__)
# Time between updating data from Wiener Netze
SCAN_INTERVAL = timedelta(minutes=60 * 6)
PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend(
    {
        vol.Required(CONF_USERNAME): cv.string,
        vol.Required(CONF_PASSWORD): cv.string,
        vol.Required(CONF_DEVICE_ID): cv.string,
        vol.Optional(
            CONF_SCAN_INTERVAL, default=DEFAULT_SCAN_INTERVAL_MINUTES
        ): vol.All(vol.Coerce(int), vol.Range(min=5, max=720)),
        vol.Optional(CONF_ENABLE_RAW_API_RESPONSE_WRITE, default=False): cv.boolean,
        vol.Optional(CONF_ENABLE_DAILY_CONS, default=DEFAULT_ENABLE_DAILY_CONS): cv.boolean,
        vol.Optional(
            CONF_ENABLE_DAILY_METER_READ, default=DEFAULT_ENABLE_DAILY_METER_READ
        ): cv.boolean,
    }
)


def _resolve_selected_zaehlpunkte(config_entry: config_entries.ConfigEntry) -> list[str]:
    """Return selected meter IDs with backward-compatible defaults."""
    config = config_entry.data
    available = [zp["zaehlpunktnummer"] for zp in config.get(CONF_ZAEHLPUNKTE, [])]
    active_default = [
        zp["zaehlpunktnummer"]
        for zp in config.get(CONF_ZAEHLPUNKTE, [])
        if zp.get("active", True) and zp.get("smartMeterReady", True)
    ]
    default_selected = active_default if active_default else available

    selected = config_entry.options.get(
        CONF_SELECTED_ZAEHLPUNKTE,
        config.get(CONF_SELECTED_ZAEHLPUNKTE, default_selected),
    )
    if isinstance(selected, str):
        selected = [selected]
    if not isinstance(selected, list):
        selected = default_selected

    selected_filtered = [value for value in selected if value in available]
    if selected_filtered:
        return selected_filtered

    _LOGGER.warning(
        "No selected WNSM meters matched available meters for entry %s. Falling back to defaults.",
        config_entry.entry_id,
    )
    return default_selected


async def async_setup_entry(
    hass: core.HomeAssistant,
    config_entry: config_entries.ConfigEntry,
    async_add_entities,
):
    """Setup sensors from a config entry created in the integrations UI."""
    config = config_entry.data
    enable_raw_api_response_write = config_entry.options.get(
        CONF_ENABLE_RAW_API_RESPONSE_WRITE,
        config.get(CONF_ENABLE_RAW_API_RESPONSE_WRITE, False),
    )
    scan_interval = int(
        config_entry.options.get(
            CONF_SCAN_INTERVAL,
            config.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL_MINUTES),
        )
    )
    enable_daily_cons = bool(
        config_entry.options.get(
            CONF_ENABLE_DAILY_CONS,
            config.get(CONF_ENABLE_DAILY_CONS, DEFAULT_ENABLE_DAILY_CONS),
        )
    )
    enable_daily_meter_read = bool(
        config_entry.options.get(
            CONF_ENABLE_DAILY_METER_READ,
            config.get(CONF_ENABLE_DAILY_METER_READ, DEFAULT_ENABLE_DAILY_METER_READ),
        )
    )
    zaehlpunkte = _resolve_selected_zaehlpunkte(config_entry)
    coordinator = WNSMDataUpdateCoordinator(
        hass=hass,
        username=config[CONF_USERNAME],
        password=config[CONF_PASSWORD],
        zaehlpunkte=zaehlpunkte,
        scan_interval_minutes=scan_interval,
        enable_raw_api_response_write=enable_raw_api_response_write,
        enable_daily_cons_statistics=enable_daily_cons,
        enable_daily_meter_read_statistics=enable_daily_meter_read,
        log_scope=config_entry.entry_id,
    )
    await coordinator.async_config_entry_first_refresh()
    entities = [
        WNSMSensor(coordinator, zaehlpunkt)
        for zaehlpunkt in zaehlpunkte
    ]
    if enable_daily_cons:
        entities.extend(
            WNSMDailyConsSensor(coordinator, zaehlpunkt)
            for zaehlpunkt in zaehlpunkte
        )
        entities.extend(
            WNSMDailyConsDaySensor(coordinator, zaehlpunkt)
            for zaehlpunkt in zaehlpunkte
        )
    async_add_entities(entities)


async def async_setup_platform(
    hass: core.HomeAssistant,  # pylint: disable=unused-argument
    config: ConfigType,
    async_add_entities: collections.abc.Callable,
    discovery_info: Optional[
        DiscoveryInfoType
    ] = None,  # pylint: disable=unused-argument
) -> None:
    """Set up the sensor platform by adding it into configuration.yaml"""
    coordinator = WNSMDataUpdateCoordinator(
        hass=hass,
        username=config[CONF_USERNAME],
        password=config[CONF_PASSWORD],
        zaehlpunkte=[config[CONF_DEVICE_ID]],
        scan_interval_minutes=int(
            config.get(CONF_SCAN_INTERVAL, SCAN_INTERVAL.total_seconds() // 60)
        ),
        enable_raw_api_response_write=bool(
            config.get(CONF_ENABLE_RAW_API_RESPONSE_WRITE, False)
        ),
        enable_daily_cons_statistics=bool(
            config.get(CONF_ENABLE_DAILY_CONS, DEFAULT_ENABLE_DAILY_CONS)
        ),
        enable_daily_meter_read_statistics=bool(
            config.get(
                CONF_ENABLE_DAILY_METER_READ, DEFAULT_ENABLE_DAILY_METER_READ
            )
        ),
        log_scope="yaml",
    )
    await coordinator.async_config_entry_first_refresh()
    wnsm_sensor = WNSMSensor(coordinator, config[CONF_DEVICE_ID])
    entities = [wnsm_sensor]
    if bool(config.get(CONF_ENABLE_DAILY_CONS, DEFAULT_ENABLE_DAILY_CONS)):
        entities.append(WNSMDailyConsSensor(coordinator, config[CONF_DEVICE_ID]))
        entities.append(WNSMDailyConsDaySensor(coordinator, config[CONF_DEVICE_ID]))
    async_add_entities(entities, update_before_add=True)
