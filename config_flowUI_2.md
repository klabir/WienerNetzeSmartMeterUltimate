# WNSM Config-Flow UI Patch (No Runtime Logging Wiring)

This document captures the exact **config-flow UI changes** that were implemented, so another LLM can rebuild the same result.

## Scope

- Changed files:
  - `custom_components/wnsm/config_flow.py`
  - `custom_components/wnsm/translations/en.json`
- Explicitly **not** implemented:
  - No runtime/local-folder API logging toggle wiring
  - No sensor/statistics/importer behavior changes

## Goal

Implement all config-flow UI changes from `auth_confflow.md`:

1. Add user-step fields for scan interval and raw API write toggle.
2. Add options flow with reload-on-save.
3. Add translation strings for both user and options forms.

## `config_flow.py` changes

### Constants added

- `CONF_SCAN_INTERVAL = "scan_interval"`
- `CONF_ENABLE_RAW_API_RESPONSE_WRITE = "enable_raw_api_response_write"`
- `DEFAULT_SCAN_INTERVAL_MINUTES = 360`

### Number field helper added

- Function: `_scan_interval_field(default_scan_interval)`
- Behavior:
  1. Try `selector.NumberSelector` with config:
     - `min=5`
     - `max=720`
     - `step=5`
     - `unit_of_measurement="min"`
     - `mode=selector.NumberSelectorMode.BOX` when available
  2. Fallback to:
     - `vol.All(vol.Coerce(int), vol.Range(min=5, max=720))`

### User schema changed

- Added `user_schema(default_scan_interval)` returning:
  - `vol.Required(CONF_USERNAME): cv.string`
  - `vol.Required(CONF_PASSWORD): cv.string`
  - `vol.Optional("scan_interval", default=360): _scan_interval_field(...)`
  - `vol.Optional("enable_raw_api_response_write", default=False): cv.boolean`

### ConfigFlow class updates

- Added `async_get_options_flow(config_entry)` returning `WienerNetzeSmartMeterOptionsFlow(config_entry)`.
- `async_step_user(...)` now uses `data_schema=user_schema(DEFAULT_SCAN_INTERVAL_MINUTES)`.
- On successful auth:
  - `self.data = dict(user_input)` (copy, not direct reference)
  - build `self.data[CONF_ZAEHLPUNKTE]` via:
    - `translate_dict(zp, ATTRS_ZAEHLPUNKTE_CALL)`
    - filtered with `if zp["isActive"]`
  - create entry titled `"Wiener Netze Smartmeter"`

### Options flow added

- Class: `WienerNetzeSmartMeterOptionsFlow(config_entries.OptionsFlow)`
- Includes legacy-compatible config entry resolution:
  - use `self.config_entry` when present
  - fallback `self.hass.config_entries.async_get_entry(self.handler)`
- `async_step_init(user_input)` behavior:
  1. If config entry missing: `abort("unknown_error")`
  2. If submitted:
     - schedule reload task:
       - `self.hass.config_entries.async_reload(config_entry.entry_id)`
     - create options entry with submitted data
  3. If no input:
     - show form with defaults resolved from:
       - options -> entry data -> hard default
     - fields shown:
       - `scan_interval`
       - `enable_raw_api_response_write`

## `translations/en.json` changes

### Config user step labels added

Under `config.step.user.data`:

- `"scan_interval": "Scan interval (minutes)"`
- `"enable_raw_api_response_write": "Enable raw Api Response written to /config/tmp/wnsm_api_calls"`

### Options section added

Added top-level `options.step.init`:

- `title`: `"Wiener Netze Smartmeter Options"`
- `description`: `"Configure polling behavior. Saving automatically reloads the integration."`
- `data` labels:
  - `"scan_interval": "Scan interval (minutes)"`
  - `"enable_raw_api_response_write": "Enable raw Api Response written to /config/tmp/wnsm_api_calls"`

## Verification performed

1. Syntax compile:
   - `python -m py_compile custom_components/wnsm/config_flow.py`
2. Translation JSON parse check:
   - parsed `custom_components/wnsm/translations/en.json` successfully

## Rebuild checklist for another LLM

1. Edit only `custom_components/wnsm/config_flow.py` and `custom_components/wnsm/translations/en.json`.
2. Add scan interval constants + toggle key constants.
3. Add selector-first/fallback helper for scan interval (5..720, step 5).
4. Replace old username/password-only schema with new `user_schema(...)`.
5. Keep auth validation flow and active-meter filtering.
6. Add options flow class with reload-on-save and legacy entry fallback.
7. Add all translation keys for new user + options fields.
8. Do **not** implement any runtime local-file logging logic yet.
