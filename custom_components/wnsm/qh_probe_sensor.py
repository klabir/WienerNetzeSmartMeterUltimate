"""Diagnostic sensor for quarter-hour availability in current day."""
from typing import Any

from homeassistant.components.sensor import SensorEntity
from homeassistant.helpers.entity import EntityCategory
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .coordinator import WNSMDataUpdateCoordinator


class WNSMQuarterHourProbeSensor(
    CoordinatorEntity[WNSMDataUpdateCoordinator], SensorEntity
):
    """Expose quarter-hour API availability diagnostics per zaehlpunkt."""

    def __init__(self, coordinator: WNSMDataUpdateCoordinator, zaehlpunkt: str) -> None:
        super().__init__(coordinator)
        self.zaehlpunkt = zaehlpunkt
        self._attr_name = f"{zaehlpunkt} quarter-hour probe"
        self._attr_unique_id = f"{zaehlpunkt}_quarter_hour_probe"
        self._attr_icon = "mdi:chart-timeline-variant"
        self._attr_entity_category = EntityCategory.DIAGNOSTIC
        self._attr_native_unit_of_measurement = "slots"
        self._attr_suggested_display_precision = 0

    @property
    def available(self) -> bool:
        item = (self.coordinator.data or {}).get(self.zaehlpunkt, {})
        return bool(item.get("available", False))

    @property
    def native_value(self) -> int | float | None:
        item = (self.coordinator.data or {}).get(self.zaehlpunkt, {})
        probe = item.get("quarter_hour_probe", {})
        return probe.get("today_value_count")

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        item = (self.coordinator.data or {}).get(self.zaehlpunkt, {})
        return item.get("quarter_hour_probe", {})
