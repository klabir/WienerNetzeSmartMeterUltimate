"""Setting up config flow for homeassistant."""
import logging
from typing import Any, Optional

import homeassistant.helpers.config_validation as cv
import voluptuous as vol
from homeassistant import config_entries
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.helpers import selector

from .api import Smartmeter
from .const import ATTRS_ZAEHLPUNKTE_CALL, CONF_ZAEHLPUNKTE, DOMAIN
from .utils import translate_dict

_LOGGER = logging.getLogger(__name__)

CONF_SCAN_INTERVAL = "scan_interval"
CONF_ENABLE_RAW_API_RESPONSE_WRITE = "enable_raw_api_response_write"
DEFAULT_SCAN_INTERVAL_MINUTES = 360


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


def user_schema(default_scan_interval: int):
    """Build user step schema."""
    return vol.Schema(
        {
            vol.Required(CONF_USERNAME): cv.string,
            vol.Required(CONF_PASSWORD): cv.string,
            vol.Optional(CONF_SCAN_INTERVAL, default=default_scan_interval): _scan_interval_field(
                default_scan_interval
            ),
            vol.Optional(CONF_ENABLE_RAW_API_RESPONSE_WRITE, default=False): cv.boolean,
        }
    )


class WienerNetzeSmartMeterCustomConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Wiener Netze Smartmeter config flow."""

    data: Optional[dict[str, Any]]

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
        zps = []
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
                # Input is valid, set data
                self.data = dict(user_input)
                self.data[CONF_ZAEHLPUNKTE] = [
                    translate_dict(zp, ATTRS_ZAEHLPUNKTE_CALL)
                    for zp in zps
                    if zp["isActive"]  # only create active zaehlpunkte
                ]
                # User is done authenticating, create entry
                return self.async_create_entry(
                    title="Wiener Netze Smartmeter", data=self.data
                )

        return self.async_show_form(
            step_id="user",
            data_schema=user_schema(DEFAULT_SCAN_INTERVAL_MINUTES),
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

        if user_input is not None:
            self.hass.async_create_task(
                self.hass.config_entries.async_reload(config_entry.entry_id)
            )
            return self.async_create_entry(title="", data=user_input)

        current_scan_interval = config_entry.options.get(
            CONF_SCAN_INTERVAL,
            config_entry.data.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL_MINUTES),
        )
        current_enable_raw_api_response_write = config_entry.options.get(
            CONF_ENABLE_RAW_API_RESPONSE_WRITE,
            config_entry.data.get(CONF_ENABLE_RAW_API_RESPONSE_WRITE, False),
        )

        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema(
                {
                    vol.Optional(
                        CONF_SCAN_INTERVAL,
                        default=current_scan_interval,
                    ): _scan_interval_field(current_scan_interval),
                    vol.Optional(
                        CONF_ENABLE_RAW_API_RESPONSE_WRITE,
                        default=current_enable_raw_api_response_write,
                    ): cv.boolean,
                }
            ),
        )
