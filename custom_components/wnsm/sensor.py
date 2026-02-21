"""
WienerNetze Smartmeter sensor platform
"""
import collections.abc
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
from .const import CONF_ZAEHLPUNKTE, DOMAIN
from .coordinator import WNSMDataUpdateCoordinator
from .wnsm_sensor import WNSMSensor
# Time between updating data from Wiener Netze
SCAN_INTERVAL = timedelta(minutes=60 * 6)
CONF_ENABLE_RAW_API_RESPONSE_WRITE = "enable_raw_api_response_write"
CONF_SCAN_INTERVAL = "scan_interval"
DEFAULT_SCAN_INTERVAL_MINUTES = 360
PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend(
    {
        vol.Required(CONF_USERNAME): cv.string,
        vol.Required(CONF_PASSWORD): cv.string,
        vol.Required(CONF_DEVICE_ID): cv.string,
    }
)


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
    zaehlpunkte = [zp["zaehlpunktnummer"] for zp in config[CONF_ZAEHLPUNKTE]]
    coordinator = WNSMDataUpdateCoordinator(
        hass=hass,
        username=config[CONF_USERNAME],
        password=config[CONF_PASSWORD],
        zaehlpunkte=zaehlpunkte,
        scan_interval_minutes=scan_interval,
        enable_raw_api_response_write=enable_raw_api_response_write,
        log_scope=config_entry.entry_id,
    )
    await coordinator.async_config_entry_first_refresh()
    wnsm_sensors = [
        WNSMSensor(coordinator, zaehlpunkt)
        for zaehlpunkt in zaehlpunkte
    ]
    async_add_entities(wnsm_sensors)


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
        scan_interval_minutes=int(SCAN_INTERVAL.total_seconds() // 60),
        enable_raw_api_response_write=False,
        log_scope="yaml",
    )
    await coordinator.async_config_entry_first_refresh()
    wnsm_sensor = WNSMSensor(coordinator, config[CONF_DEVICE_ID])
    async_add_entities([wnsm_sensor], update_before_add=True)
