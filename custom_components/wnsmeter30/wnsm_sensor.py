import logging
from typing import Any, Optional

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorStateClass,
)
from homeassistant.components.sensor import SensorEntity
from homeassistant.const import UnitOfEnergy
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .coordinator import WNSMDataUpdateCoordinator
from .naming import build_main_entity_unique_id

_LOGGER = logging.getLogger(__name__)


class WNSMSensor(CoordinatorEntity[WNSMDataUpdateCoordinator], SensorEntity):
    """
    Representation of a Wiener Smartmeter sensor
    for measuring total increasing energy consumption for a specific zaehlpunkt
    """

    def _icon(self) -> str:
        return "mdi:flash"

    def __init__(
        self,
        coordinator: WNSMDataUpdateCoordinator,
        zaehlpunkt: str,
    ) -> None:
        super().__init__(coordinator)
        self.zaehlpunkt = zaehlpunkt
        display_name = coordinator.display_name(zaehlpunkt)
        entity_id_key = coordinator.entity_id_key(zaehlpunkt)

        self._attr_native_value: int | float | None = 0
        self._attr_extra_state_attributes = {}
        self._attr_name = display_name
        self._attr_unique_id = build_main_entity_unique_id(entity_id_key)
        self._attr_suggested_object_id = build_main_entity_unique_id(entity_id_key)
        self._attr_icon = self._icon()
        self._attr_state_class = SensorStateClass.TOTAL_INCREASING
        self._attr_device_class = SensorDeviceClass.ENERGY
        self._attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR

        self.attrs: dict[str, Any] = {}
        self._name: str = display_name
        self._available: bool = True
        self._updatets: str | None = None

    @property
    def get_state(self) -> Optional[str]:
        return f"{self._attr_native_value:.3f}"

    @property
    def icon(self) -> str:
        return self._attr_icon

    @property
    def name(self) -> str:
        """Return the name of the entity."""
        return self._name

    @property
    def unique_id(self) -> str:
        """Return the unique ID of the sensor."""
        return self._attr_unique_id

    @property
    def available(self) -> bool:
        """Return True if entity is available."""
        item = (self.coordinator.data or {}).get(self.zaehlpunkt, {})
        return bool(item.get("available", False))

    @property
    def native_value(self) -> int | float | None:
        item = (self.coordinator.data or {}).get(self.zaehlpunkt, {})
        return item.get("native_value")

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        item = (self.coordinator.data or {}).get(self.zaehlpunkt, {})
        return item.get("attributes", {})
