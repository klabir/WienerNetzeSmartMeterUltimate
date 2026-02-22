"""Runtime sensor exposing latest day value from the _daily_cons stream."""
from homeassistant.components.sensor import SensorDeviceClass, SensorEntity
from homeassistant.const import UnitOfEnergy
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .coordinator import WNSMDataUpdateCoordinator


class WNSMDailyConsDaySensor(
    CoordinatorEntity[WNSMDataUpdateCoordinator], SensorEntity
):
    """Expose latest daily consumption delta value per meter."""

    def __init__(self, coordinator: WNSMDataUpdateCoordinator, zaehlpunkt: str) -> None:
        super().__init__(coordinator)
        self.zaehlpunkt = zaehlpunkt
        display_name = coordinator.display_name(zaehlpunkt)
        self._attr_name = f"{display_name} daily cons day"
        self._attr_unique_id = f"{zaehlpunkt}_daily_cons_day"
        self._attr_icon = "mdi:calendar-today"
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
        return item.get("daily_cons_day_value")
