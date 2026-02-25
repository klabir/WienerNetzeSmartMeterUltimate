"""Shared naming and ID generation helpers."""
from __future__ import annotations

import logging
from collections.abc import Mapping

from homeassistant.util import slugify

from .const import DOMAIN

CUMULATIVE_SUFFIX = "_cum_abs"
DAILY_CONS_SUFFIX = "_daily_cons"
DAILY_CONS_DAY_SUFFIX = "_daily_cons_day"
METER_READ_SUFFIX = "_meter_read"
LIVE_QUARTER_HOUR_SUFFIX = "_quarter_hour_live"


def normalize_meter_aliases(
    aliases: object, allowed_meter_ids: set[str] | None = None
) -> dict[str, str]:
    """Normalize alias mapping and remove blank values."""
    if not isinstance(aliases, dict):
        return {}

    normalized: dict[str, str] = {}
    for meter_id, alias in aliases.items():
        meter_id_str = str(meter_id)
        if allowed_meter_ids is not None and meter_id_str not in allowed_meter_ids:
            continue
        alias_str = str(alias).strip()
        if alias_str:
            normalized[meter_id_str] = alias_str
    return normalized


def display_name(
    zaehlpunkt: str, meter_aliases: Mapping[str, str] | None = None
) -> str:
    """Return the preferred display name for a meter."""
    alias = (meter_aliases or {}).get(zaehlpunkt)
    return alias if alias else zaehlpunkt


def build_alias_id_keys(
    meter_ids: list[str],
    meter_aliases: Mapping[str, str],
    use_alias_for_ids: bool,
    logger: logging.Logger | None = None,
) -> dict[str, str]:
    """Build optional alias-based ID keys with collision-safe fallbacks."""
    if not use_alias_for_ids:
        return {}

    alias_slugs: dict[str, str] = {}
    for meter_id in meter_ids:
        alias = meter_aliases.get(meter_id, "")
        alias_slug = slugify(alias).lower() if alias else ""
        if alias_slug:
            alias_slugs[meter_id] = alias_slug

    resolved: dict[str, str] = {}
    # Reserve fallback statistic keys from meters without alias to avoid
    # collisions when alias-based statistic IDs are enabled.
    used: set[str] = {
        meter_id.lower() for meter_id in meter_ids if meter_id not in alias_slugs
    }
    for meter_id in meter_ids:
        alias_slug = alias_slugs.get(meter_id)
        if not alias_slug:
            continue

        candidate = alias_slug
        if candidate in used:
            suffix = meter_id.lower()[-6:]
            candidate = f"{alias_slug}_{suffix}"
            if candidate in used:
                candidate = f"{alias_slug}_{meter_id.lower()}"
            if candidate in used:
                index = 2
                while f"{candidate}_{index}" in used:
                    index += 1
                candidate = f"{candidate}_{index}"
            if logger is not None:
                logger.warning(
                    "Alias-based ID key '%s' conflicts with an existing key. "
                    "Using '%s' for %s.",
                    alias_slug,
                    candidate,
                    meter_id,
                )

        used.add(candidate)
        resolved[meter_id] = candidate

    return resolved


def entity_id_key(
    zaehlpunkt: str, alias_id_keys: Mapping[str, str] | None = None
) -> str:
    """Return the entity ID key (alias-based if configured)."""
    return (alias_id_keys or {}).get(zaehlpunkt, zaehlpunkt)


def statistic_id_key(
    zaehlpunkt: str, alias_id_keys: Mapping[str, str] | None = None
) -> str:
    """Return the statistic ID key (alias-based if configured)."""
    return (alias_id_keys or {}).get(zaehlpunkt, zaehlpunkt.lower())


def build_statistics_base_id(
    zaehlpunkt: str, statistic_id_base: str | None = None
) -> str:
    """Build canonical statistic base ID (`domain:key`)."""
    base_id_source = statistic_id_base or zaehlpunkt.lower()
    base_id = slugify(base_id_source).lower()
    if not base_id:
        base_id = zaehlpunkt.lower()
    return f"{DOMAIN}:{base_id}"


def build_statistics_ids(
    zaehlpunkt: str, statistic_id_base: str | None = None
) -> dict[str, str]:
    """Build all statistic IDs from one base."""
    base = build_statistics_base_id(zaehlpunkt, statistic_id_base)
    return {
        "base": base,
        "cumulative": f"{base}{CUMULATIVE_SUFFIX}",
        "daily_cons": f"{base}{DAILY_CONS_SUFFIX}",
        "meter_read": f"{base}{METER_READ_SUFFIX}",
    }


def build_main_entity_unique_id(entity_key: str) -> str:
    """Build unique ID for the primary meter sensor."""
    return entity_key


def build_daily_cons_unique_id(entity_key: str) -> str:
    """Build unique ID for daily consumption sensor."""
    return f"{entity_key}{DAILY_CONS_SUFFIX}"


def build_daily_cons_day_unique_id(entity_key: str) -> str:
    """Build unique ID for daily consumption day sensor."""
    return f"{entity_key}{DAILY_CONS_DAY_SUFFIX}"


def build_live_quarter_hour_unique_id(entity_key: str) -> str:
    """Build unique ID for the live quarter-hour sensor."""
    return f"{entity_key}{LIVE_QUARTER_HOUR_SUFFIX}"
