"""
WienerNetze Smartmeter sensor platform
"""
import logging

from homeassistant import core, config_entries
from homeassistant.const import (
    CONF_USERNAME,
    CONF_PASSWORD,
)
from .const import (
    CONF_ENABLE_DAILY_CONS,
    CONF_ENABLE_DAILY_METER_READ,
    CONF_HISTORICAL_DAYS,
    CONF_ENABLE_RAW_API_RESPONSE_WRITE,
    CONF_SCAN_INTERVAL,
    CONF_USE_ALIAS_FOR_IDS,
    CONF_ZAEHLPUNKT_ALIASES,
    CONF_SELECTED_ZAEHLPUNKTE,
    CONF_ZAEHLPUNKTE,
    DEFAULT_ENABLE_DAILY_CONS,
    DEFAULT_ENABLE_DAILY_METER_READ,
    DEFAULT_USE_ALIAS_FOR_IDS,
    DEFAULT_HISTORICAL_DAYS,
    MAX_HISTORICAL_DAYS,
    DEFAULT_SCAN_INTERVAL_MINUTES,
    DOMAIN,
)
from .daily_cons_day_sensor import WNSMDailyConsDaySensor
from .coordinator import WNSMDataUpdateCoordinator
from .daily_cons_sensor import WNSMDailyConsSensor
from .wnsm_sensor import WNSMSensor

_LOGGER = logging.getLogger(__name__)


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


def _resolve_zaehlpunkt_aliases(
    config_entry: config_entries.ConfigEntry, selected_meters: list[str]
) -> dict[str, str]:
    config = config_entry.data
    raw_aliases = config_entry.options.get(
        CONF_ZAEHLPUNKT_ALIASES,
        config.get(CONF_ZAEHLPUNKT_ALIASES, {}),
    )
    if not isinstance(raw_aliases, dict):
        return {}

    selected = set(selected_meters)
    aliases: dict[str, str] = {}
    for meter_id, alias in raw_aliases.items():
        meter_id_str = str(meter_id)
        if meter_id_str not in selected:
            continue
        alias_str = str(alias).strip()
        if alias_str:
            aliases[meter_id_str] = alias_str
    return aliases


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
    use_alias_for_ids = bool(
        config_entry.options.get(
            CONF_USE_ALIAS_FOR_IDS,
            config.get(CONF_USE_ALIAS_FOR_IDS, DEFAULT_USE_ALIAS_FOR_IDS),
        )
    )
    try:
        historical_days = int(
            config_entry.options.get(
                CONF_HISTORICAL_DAYS,
                config.get(CONF_HISTORICAL_DAYS, DEFAULT_HISTORICAL_DAYS),
            )
        )
    except (TypeError, ValueError):
        historical_days = DEFAULT_HISTORICAL_DAYS
    historical_days = max(1, min(MAX_HISTORICAL_DAYS, historical_days))
    zaehlpunkte = _resolve_selected_zaehlpunkte(config_entry)
    meter_aliases = _resolve_zaehlpunkt_aliases(config_entry, zaehlpunkte)
    coordinator = WNSMDataUpdateCoordinator(
        hass=hass,
        username=config[CONF_USERNAME],
        password=config[CONF_PASSWORD],
        zaehlpunkte=zaehlpunkte,
        meter_aliases=meter_aliases,
        scan_interval_minutes=scan_interval,
        historical_days=historical_days,
        enable_raw_api_response_write=enable_raw_api_response_write,
        enable_daily_cons_statistics=enable_daily_cons,
        enable_daily_meter_read_statistics=enable_daily_meter_read,
        use_alias_for_ids=use_alias_for_ids,
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
