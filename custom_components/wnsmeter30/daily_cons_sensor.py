"""Runtime sensor exposing latest _daily_cons source value."""
from homeassistant.components.sensor import SensorDeviceClass, SensorEntity
from homeassistant.const import UnitOfEnergy
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .coordinator import WNSMDataUpdateCoordinator


class WNSMDailyConsSensor(CoordinatorEntity[WNSMDataUpdateCoordinator], SensorEntity):
    """Expose latest cumulative daily-consumption value per meter."""

    def __init__(self, coordinator: WNSMDataUpdateCoordinator, zaehlpunkt: str) -> None:
        super().__init__(coordinator)
        self.zaehlpunkt = zaehlpunkt
        display_name = coordinator.display_name(zaehlpunkt)
        entity_id_key = coordinator.entity_id_key(zaehlpunkt)
        self._attr_name = f"{display_name} daily cons"
        self._attr_unique_id = f"{entity_id_key}_daily_cons"
        self._attr_suggested_object_id = f"{entity_id_key}_daily_cons"
        self._attr_icon = "mdi:calendar-month"
        self._attr_device_class = SensorDeviceClass.ENERGY
        self._attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
        self._attr_suggested_display_precision = 3

    @property
    def available(self) -> bool:
        item = (self.coordinator.data or {}).get(self.zaehlpunkt, {})
        return bool(item.get("available", False))

    @property
    def native_value(self) -> int | float | None:
        item = (self.coordinator.data or {}).get(self.zaehlpunkt, {})
        return item.get("daily_cons_value")
