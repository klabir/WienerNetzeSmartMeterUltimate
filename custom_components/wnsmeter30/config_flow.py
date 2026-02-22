"""Setting up config flow for homeassistant."""
import logging
from typing import Any, Optional

import homeassistant.helpers.config_validation as cv
import voluptuous as vol
from homeassistant import config_entries
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.helpers import selector

from .api import Smartmeter
from .const import (
    CONF_ENABLE_DAILY_METER_READ,
    ATTRS_ZAEHLPUNKTE_CALL,
    CONF_ENABLE_DAILY_CONS,
    CONF_HISTORICAL_DAYS,
    CONF_ENABLE_RAW_API_RESPONSE_WRITE,
    CONF_SCAN_INTERVAL,
    CONF_SELECTED_ZAEHLPUNKTE,
    CONF_ZAEHLPUNKTE,
    DEFAULT_ENABLE_DAILY_CONS,
    DEFAULT_ENABLE_DAILY_METER_READ,
    DEFAULT_HISTORICAL_DAYS,
    HISTORICAL_API_CHUNK_DAYS,
    MAX_HISTORICAL_DAYS,
    DEFAULT_SCAN_INTERVAL_MINUTES,
    DOMAIN,
)
from .utils import translate_dict

_LOGGER = logging.getLogger(__name__)

def _scan_interval_field(default_scan_interval: int):
    """Return a version-safe scan interval field."""
    try:
        selector_config: dict[str, Any] = {
            "min": 5,
            "max": 720,
            "step": 5,
            "unit_of_measurement": "min",
        }
        if hasattr(selector, "NumberSelectorMode"):
            selector_config["mode"] = selector.NumberSelectorMode.BOX
        return selector.NumberSelector(selector.NumberSelectorConfig(**selector_config))
    except Exception:  # pylint: disable=broad-except
        return vol.All(vol.Coerce(int), vol.Range(min=5, max=720))


def _historical_days_field(default_historical_days: int):
    """Return a version-safe historical-days field."""
    try:
        selector_config: dict[str, Any] = {
            "min": 1,
            "max": MAX_HISTORICAL_DAYS,
            "step": 1,
            "unit_of_measurement": "days",
        }
        if hasattr(selector, "NumberSelectorMode"):
            selector_config["mode"] = selector.NumberSelectorMode.BOX
        return selector.NumberSelector(selector.NumberSelectorConfig(**selector_config))
    except Exception:  # pylint: disable=broad-except
        return vol.All(vol.Coerce(int), vol.Range(min=1, max=MAX_HISTORICAL_DAYS))


def _normalize_historical_days(value: Any) -> int:
    try:
        days = int(value)
    except (TypeError, ValueError):
        days = DEFAULT_HISTORICAL_DAYS
    return max(1, min(MAX_HISTORICAL_DAYS, days))


def _historical_days_description_placeholders() -> dict[str, str]:
    return {
        "historical_days_default": str(DEFAULT_HISTORICAL_DAYS),
        "historical_days_limit": str(MAX_HISTORICAL_DAYS),
        "historical_days_chunk": str(HISTORICAL_API_CHUNK_DAYS),
    }


def _meter_id(zp: dict[str, Any]) -> str | None:
    meter_id = zp.get("zaehlpunktnummer")
    if meter_id is None:
        return None
    meter_id_str = str(meter_id).strip()
    return meter_id_str if meter_id_str else None


def _is_active_meter(zp: dict[str, Any]) -> bool:
    is_active = zp.get("isActive", zp.get("active", True))
    is_ready = zp.get("isSmartMeterMarketReady", zp.get("smartMeterReady", True))
    return bool(is_active) and bool(is_ready)


def _meter_label(zp: dict[str, Any]) -> str:
    meter_id = _meter_id(zp) or "unknown"
    custom_label = zp.get("customLabel") or zp.get("label")
    city = zp.get("city")
    if city is None:
        city = (zp.get("verbrauchsstelle") or {}).get("ort")
    status = "active" if _is_active_meter(zp) else "inactive"
    if custom_label and city:
        return f"{meter_id} ({custom_label}, {city}, {status})"
    if custom_label:
        return f"{meter_id} ({custom_label}, {status})"
    if city:
        return f"{meter_id} ({city}, {status})"
    return f"{meter_id} ({status})"


def _build_meter_options(zps: list[dict[str, Any]]) -> tuple[list[dict[str, str]], list[str]]:
    options: list[dict[str, str]] = []
    default_selected: list[str] = []
    seen: set[str] = set()

    for zp in zps:
        meter_id = _meter_id(zp)
        if meter_id is None or meter_id in seen:
            continue
        seen.add(meter_id)
        options.append({"value": meter_id, "label": _meter_label(zp)})
        if _is_active_meter(zp):
            default_selected.append(meter_id)

    if not default_selected:
        default_selected = [option["value"] for option in options]

    return options, default_selected


def _normalize_selected_meters(selected: Any) -> list[str]:
    if selected is None:
        return []
    if isinstance(selected, str):
        return [selected]
    if isinstance(selected, list):
        return [str(item) for item in selected]
    return []


def _meter_select_field(options: list[dict[str, str]]):
    try:
        selector_config: dict[str, Any] = {
            "options": options,
            "multiple": True,
        }
        if hasattr(selector, "SelectSelectorMode"):
            selector_config["mode"] = selector.SelectSelectorMode.DROPDOWN
        return selector.SelectSelector(selector.SelectSelectorConfig(**selector_config))
    except Exception:  # pylint: disable=broad-except
        option_values = [option["value"] for option in options]
        return vol.All(cv.ensure_list, [vol.In(option_values)])


def user_schema(default_scan_interval: int, default_historical_days: int):
    """Build user step schema."""
    return vol.Schema(
        {
            vol.Required(CONF_USERNAME): cv.string,
            vol.Required(CONF_PASSWORD): cv.string,
            vol.Optional(CONF_SCAN_INTERVAL, default=default_scan_interval): _scan_interval_field(
                default_scan_interval
            ),
            vol.Optional(CONF_ENABLE_RAW_API_RESPONSE_WRITE, default=False): cv.boolean,
            vol.Optional(
                CONF_HISTORICAL_DAYS,
                default=default_historical_days,
            ): _historical_days_field(default_historical_days),
            vol.Optional(
                CONF_ENABLE_DAILY_CONS, default=DEFAULT_ENABLE_DAILY_CONS
            ): cv.boolean,
            vol.Optional(
                CONF_ENABLE_DAILY_METER_READ, default=DEFAULT_ENABLE_DAILY_METER_READ
            ): cv.boolean,
        }
    )


def _options_schema(
    *,
    scan_interval: int,
    enable_raw_api_response_write: bool,
    historical_days: int,
    enable_daily_cons: bool,
    enable_daily_meter_read: bool,
    selected_meters: list[str],
    meter_options: list[dict[str, str]],
) -> vol.Schema:
    return vol.Schema(
        {
            vol.Optional(
                CONF_SCAN_INTERVAL,
                default=scan_interval,
            ): _scan_interval_field(scan_interval),
            vol.Optional(
                CONF_ENABLE_RAW_API_RESPONSE_WRITE,
                default=enable_raw_api_response_write,
            ): cv.boolean,
            vol.Optional(
                CONF_HISTORICAL_DAYS,
                default=historical_days,
            ): _historical_days_field(historical_days),
            vol.Optional(
                CONF_ENABLE_DAILY_CONS,
                default=enable_daily_cons,
            ): cv.boolean,
            vol.Optional(
                CONF_ENABLE_DAILY_METER_READ,
                default=enable_daily_meter_read,
            ): cv.boolean,
            vol.Required(
                CONF_SELECTED_ZAEHLPUNKTE,
                default=selected_meters,
            ): _meter_select_field(meter_options),
        }
    )


class WienerNetzeSmartMeterCustomConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Wiener Netze Smartmeter config flow."""

    data: Optional[dict[str, Any]]
    _discovered_zaehlpunkte: list[dict[str, Any]]

    @staticmethod
    def async_get_options_flow(
        config_entry: config_entries.ConfigEntry,
    ) -> config_entries.OptionsFlow:
        """Get options flow for this handler."""
        return WienerNetzeSmartMeterOptionsFlow()

    async def validate_auth(self, username: str, password: str) -> list[dict]:
        """
        Validates credentials for smartmeter.
        Raises a ValueError if the auth credentials are invalid.
        """
        smartmeter = Smartmeter(username, password)
        await self.hass.async_add_executor_job(smartmeter.login)
        contracts = await self.hass.async_add_executor_job(smartmeter.zaehlpunkte)
        zaehlpunkte = []
        if contracts is not None and isinstance(contracts, list) and len(contracts) > 0:
            for contract in contracts:
                if "zaehlpunkte" in contract:
                    zaehlpunkte.extend(contract["zaehlpunkte"])
        return zaehlpunkte

    async def async_step_user(self, user_input: Optional[dict[str, Any]] = None):
        """Invoked when a user initiates a flow via the user interface."""
        errors: dict[str, str] = {}
        zps: list[dict[str, Any]] = []
        if user_input is not None:
            try:
                zps = await self.validate_auth(
                    user_input[CONF_USERNAME], user_input[CONF_PASSWORD]
                )
            except Exception as exception:  # pylint: disable=broad-except
                _LOGGER.error("Error validating Wiener Netze auth")
                _LOGGER.exception(exception)
                errors["base"] = "auth"
            if not errors:
                meter_options, default_selected = _build_meter_options(zps)
                if not meter_options:
                    errors["base"] = "no_meter_selected"
                else:
                    self._discovered_zaehlpunkte = zps
                    # Input is valid, set data
                    self.data = dict(user_input)
                    self.data[CONF_HISTORICAL_DAYS] = _normalize_historical_days(
                        user_input.get(CONF_HISTORICAL_DAYS, DEFAULT_HISTORICAL_DAYS)
                    )
                    self.data[CONF_ZAEHLPUNKTE] = [
                        translate_dict(zp, ATTRS_ZAEHLPUNKTE_CALL)
                        for zp in zps
                    ]
                    self.data[CONF_SELECTED_ZAEHLPUNKTE] = default_selected
                    return await self.async_step_select_meters()

        return self.async_show_form(
            step_id="user",
            data_schema=user_schema(
                DEFAULT_SCAN_INTERVAL_MINUTES,
                DEFAULT_HISTORICAL_DAYS,
            ),
            description_placeholders=_historical_days_description_placeholders(),
            errors=errors,
        )

    async def async_step_select_meters(self, user_input: Optional[dict[str, Any]] = None):
        """Allow users to select which discovered meters should be set up."""
        errors: dict[str, str] = {}
        if getattr(self, "data", None) is None:
            return await self.async_step_user()

        meter_options, default_selected = _build_meter_options(
            getattr(self, "_discovered_zaehlpunkte", [])
        )
        option_values = {option["value"] for option in meter_options}
        current_selected = _normalize_selected_meters(
            self.data.get(CONF_SELECTED_ZAEHLPUNKTE, default_selected)
        )
        current_selected = [value for value in current_selected if value in option_values]
        if not current_selected:
            current_selected = default_selected

        if user_input is not None:
            selected_meters = _normalize_selected_meters(
                user_input.get(CONF_SELECTED_ZAEHLPUNKTE)
            )
            selected_meters = [
                value for value in selected_meters if value in option_values
            ]
            if not selected_meters:
                errors["base"] = "no_meter_selected"
            else:
                self.data[CONF_SELECTED_ZAEHLPUNKTE] = selected_meters
                return self.async_create_entry(
                    title="WienerNetzeSmartmeter 3.0", data=self.data
                )

        return self.async_show_form(
            step_id="select_meters",
            data_schema=vol.Schema(
                {
                    vol.Required(
                        CONF_SELECTED_ZAEHLPUNKTE,
                        default=current_selected,
                    ): _meter_select_field(meter_options)
                }
            ),
            errors=errors,
        )


class WienerNetzeSmartMeterOptionsFlow(config_entries.OptionsFlow):
    """Options flow for Wiener Netze Smartmeter."""

    def _get_config_entry(self):
        """Get config entry with legacy fallback."""
        if getattr(self, "config_entry", None) is not None:
            return self.config_entry
        return self.hass.config_entries.async_get_entry(self.handler)

    async def async_step_init(self, user_input: Optional[dict[str, Any]] = None):
        """Manage options."""
        config_entry = self._get_config_entry()
        if config_entry is None:
            return self.async_abort(reason="unknown_error")

        available_meters = config_entry.data.get(CONF_ZAEHLPUNKTE, [])
        meter_options, default_selected = _build_meter_options(available_meters)
        option_values = {option["value"] for option in meter_options}
        current_selected_meters = _normalize_selected_meters(
            config_entry.options.get(
                CONF_SELECTED_ZAEHLPUNKTE,
                config_entry.data.get(CONF_SELECTED_ZAEHLPUNKTE, default_selected),
            )
        )
        current_selected_meters = [
            value for value in current_selected_meters if value in option_values
        ]
        if not current_selected_meters:
            current_selected_meters = default_selected
        current_historical_days = _normalize_historical_days(
            config_entry.options.get(
                CONF_HISTORICAL_DAYS,
                config_entry.data.get(
                    CONF_HISTORICAL_DAYS, DEFAULT_HISTORICAL_DAYS
                ),
            )
        )
        current_scan_interval = config_entry.options.get(
            CONF_SCAN_INTERVAL,
            config_entry.data.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL_MINUTES),
        )
        current_enable_raw_api_response_write = config_entry.options.get(
            CONF_ENABLE_RAW_API_RESPONSE_WRITE,
            config_entry.data.get(CONF_ENABLE_RAW_API_RESPONSE_WRITE, False),
        )
        current_enable_daily_cons = config_entry.options.get(
            CONF_ENABLE_DAILY_CONS,
            config_entry.data.get(CONF_ENABLE_DAILY_CONS, DEFAULT_ENABLE_DAILY_CONS),
        )
        current_enable_daily_meter_read = config_entry.options.get(
            CONF_ENABLE_DAILY_METER_READ,
            config_entry.data.get(
                CONF_ENABLE_DAILY_METER_READ, DEFAULT_ENABLE_DAILY_METER_READ
            ),
        )

        if user_input is not None:
            selected_meters = _normalize_selected_meters(
                user_input.get(CONF_SELECTED_ZAEHLPUNKTE)
            )
            selected_meters = [
                value for value in selected_meters if value in option_values
            ]
            if not selected_meters:
                return self.async_show_form(
                    step_id="init",
                    data_schema=_options_schema(
                        scan_interval=current_scan_interval,
                        enable_raw_api_response_write=current_enable_raw_api_response_write,
                        historical_days=current_historical_days,
                        enable_daily_cons=current_enable_daily_cons,
                        enable_daily_meter_read=current_enable_daily_meter_read,
                        selected_meters=current_selected_meters,
                        meter_options=meter_options,
                    ),
                    description_placeholders=_historical_days_description_placeholders(),
                    errors={"base": "no_meter_selected"},
                )
            user_input = dict(user_input)
            user_input[CONF_SELECTED_ZAEHLPUNKTE] = selected_meters
            user_input[CONF_HISTORICAL_DAYS] = _normalize_historical_days(
                user_input.get(CONF_HISTORICAL_DAYS, current_historical_days)
            )
            self.hass.async_create_task(
                self.hass.config_entries.async_reload(config_entry.entry_id)
            )
            return self.async_create_entry(title="", data=user_input)

        return self.async_show_form(
            step_id="init",
            data_schema=_options_schema(
                scan_interval=current_scan_interval,
                enable_raw_api_response_write=current_enable_raw_api_response_write,
                historical_days=current_historical_days,
                enable_daily_cons=current_enable_daily_cons,
                enable_daily_meter_read=current_enable_daily_meter_read,
                selected_meters=current_selected_meters,
                meter_options=meter_options,
            ),
            description_placeholders=_historical_days_description_placeholders(),
        )
