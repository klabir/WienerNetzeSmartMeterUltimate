"""Runtime sensor exposing latest 15-minute consumption value."""
from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.const import UnitOfEnergy
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .coordinator import WNSMDataUpdateCoordinator
from .naming import build_live_quarter_hour_unique_id


class WNSMLiveQuarterHourSensor(
    CoordinatorEntity[WNSMDataUpdateCoordinator], SensorEntity
):
    """Expose the latest quarter-hour consumption value per meter."""

    def __init__(self, coordinator: WNSMDataUpdateCoordinator, zaehlpunkt: str) -> None:
        super().__init__(coordinator)
        self.zaehlpunkt = zaehlpunkt
        display_name = coordinator.display_name(zaehlpunkt)
        entity_id_key = coordinator.entity_id_key(zaehlpunkt)
        unique_id = build_live_quarter_hour_unique_id(entity_id_key)
        self._attr_name = f"{display_name} quarter hour live"
        self._attr_unique_id = unique_id
        self._attr_suggested_object_id = unique_id
        self._attr_icon = "mdi:flash-clock"
        self._attr_device_class = SensorDeviceClass.ENERGY
        self._attr_state_class = SensorStateClass.MEASUREMENT
        self._attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
        self._attr_suggested_display_precision = 3

    @property
    def available(self) -> bool:
        item = (self.coordinator.data or {}).get(self.zaehlpunkt, {})
        return bool(item.get("available", False))

    @property
    def native_value(self) -> int | float | None:
        item = (self.coordinator.data or {}).get(self.zaehlpunkt, {})
        return item.get("live_quarter_hour_value")

    @property
    def extra_state_attributes(self) -> dict:
        item = (self.coordinator.data or {}).get(self.zaehlpunkt, {})
        attributes = item.get("live_quarter_hour_attributes")
        return attributes if isinstance(attributes, dict) else {}
