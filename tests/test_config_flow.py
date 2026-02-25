"""Config flow auth-error mapping tests."""
import pytest

import it  # noqa: F401  # Ensure custom_components path is available
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from wnsmeter30.api.errors import (
    SmartmeterConnectionError,
    SmartmeterLoginError,
    SmartmeterQueryError,
)
from wnsmeter30.config_flow import (
    WienerNetzeSmartMeterCustomConfigFlow,
    _map_auth_exception,
)


def test_map_auth_exception_login_error() -> None:
    assert _map_auth_exception(SmartmeterLoginError("invalid credentials")) == "auth"


def test_map_auth_exception_connection_error() -> None:
    assert (
        _map_auth_exception(SmartmeterConnectionError("timeout")) == "connection_error"
    )


def test_map_auth_exception_query_error() -> None:
    assert _map_auth_exception(SmartmeterQueryError("bad response")) == "connection_error"


def test_map_auth_exception_unknown_error_falls_back_to_auth() -> None:
    assert _map_auth_exception(RuntimeError("unexpected")) == "auth"


@pytest.mark.asyncio
async def test_async_step_user_maps_connection_error(monkeypatch) -> None:
    flow = WienerNetzeSmartMeterCustomConfigFlow()

    async def _raise_validate_auth(_username: str, _password: str):
        raise SmartmeterConnectionError("network down")

    monkeypatch.setattr(flow, "validate_auth", _raise_validate_auth)
    monkeypatch.setattr(flow, "async_show_form", lambda **kwargs: kwargs)

    result = await flow.async_step_user(
        {CONF_USERNAME: "user@example.com", CONF_PASSWORD: "secret"}
    )

    assert result["errors"]["base"] == "connection_error"
    assert result["step_id"] == "user"


@pytest.mark.asyncio
async def test_async_step_user_maps_login_error(monkeypatch) -> None:
    flow = WienerNetzeSmartMeterCustomConfigFlow()

    async def _raise_validate_auth(_username: str, _password: str):
        raise SmartmeterLoginError("bad credentials")

    monkeypatch.setattr(flow, "validate_auth", _raise_validate_auth)
    monkeypatch.setattr(flow, "async_show_form", lambda **kwargs: kwargs)

    result = await flow.async_step_user(
        {CONF_USERNAME: "user@example.com", CONF_PASSWORD: "secret"}
    )

    assert result["errors"]["base"] == "auth"
    assert result["step_id"] == "user"
