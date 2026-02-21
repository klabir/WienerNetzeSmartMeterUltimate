# WNSM Logging Patch (File + Sensor Attributes)

This document specifies the exact logging changes that were implemented so another LLM can rebuild them.

## Scope

- Modified files:
  - `custom_components/wnsm/api/client.py`
  - `custom_components/wnsm/sensor.py`
  - `custom_components/wnsm/wnsm_sensor.py`
- Uses existing config-flow option:
  - `enable_raw_api_response_write`
- Purpose:
  1. Write raw API call payloads to files when toggle is enabled.
  2. Expose recent API call logging metadata in each sensor’s attributes.

## Behavior summary

1. Toggle resolution:
   - Read in sensor setup from:
     - `config_entry.options["enable_raw_api_response_write"]`
     - fallback `config_entry.data["enable_raw_api_response_write"]`
     - fallback `False`
2. If enabled:
   - Each API call made via `Smartmeter._call_api()` is written to:
     - `/config/tmp/wnsm_api_calls/*.json`
3. Always:
   - A recent API call summary list is kept in-memory on `Smartmeter`.
   - Sensor attributes are enriched with logging metadata after update.

## File-by-file changes

## 1) `custom_components/wnsm/sensor.py`

### Added constant

- `CONF_ENABLE_RAW_API_RESPONSE_WRITE = "enable_raw_api_response_write"`

### `async_setup_entry(...)`

Added resolution logic:

- `enable_raw_api_response_write = config_entry.options.get(CONF_ENABLE_RAW_API_RESPONSE_WRITE, config.get(CONF_ENABLE_RAW_API_RESPONSE_WRITE, False))`

Passed toggle into each sensor:

- `WNSMSensor(..., enable_raw_api_response_write=enable_raw_api_response_write)`

### `async_setup_platform(...)`

For YAML setup path, explicitly passes:

- `enable_raw_api_response_write=False`

## 2) `custom_components/wnsm/wnsm_sensor.py`

### Constructor signature changed

From:

- `__init__(self, username, password, zaehlpunkt)`

To:

- `__init__(self, username, password, zaehlpunkt, enable_raw_api_response_write: bool = False)`

Stores:

- `self.enable_raw_api_response_write`

### New helper method

- `_inject_api_log_attributes(self, smartmeter: Smartmeter)`

It reads:

- `recent_calls = smartmeter.get_recent_api_calls()`

And sets/overlays attributes:

- `raw_api_logging_enabled` (bool)
- `api_call_count` (int)
- `recent_api_calls` (last 5 summaries)
- `last_api_call_file` (path of latest file or `None`)

### `async_update(...)` wiring

Smartmeter creation changed to:

- `Smartmeter(..., enable_raw_api_response_write=self.enable_raw_api_response_write)`

After update flow (and importer call), inject logging attrs:

- `self._inject_api_log_attributes(smartmeter)`

## 3) `custom_components/wnsm/api/client.py`

### `Smartmeter.__init__(...)` extended

Added arg:

- `enable_raw_api_response_write: bool = False`

New fields:

- `self._enable_raw_api_response_write`
- `self._raw_api_response_dir = "/config/tmp/wnsm_api_calls"`
- `self._recent_api_calls = []`
- `self._max_recent_api_calls = 20`

### `reset()` update

- Resets `self._recent_api_calls = []`

### New helper methods

1. `_sanitize_filename(value: str) -> str`
   - Replaces non `[A-Za-z0-9_.-]` chars with `_`
2. `_redact_headers(headers: dict) -> dict`
   - Redacts:
     - `Authorization` -> `"Bearer ***"`
     - `X-Gateway-APIKey` -> `"***"`
3. `_write_raw_api_response(payload, endpoint, method) -> str | None`
   - No-op if toggle disabled.
   - Ensures folder exists: `/config/tmp/wnsm_api_calls`
   - Writes pretty JSON file:
     - filename format: `{timestamp}_{method}_{sanitized_endpoint}.json`
   - Returns file path or `None`
4. `_record_api_call(...)`
   - Builds full payload with:
     - timestamp, method, endpoint, url, query
     - redacted request headers
     - request body
     - response status
     - response body
   - Calls `_write_raw_api_response(...)`
   - Pushes summary to `_recent_api_calls`
   - Keeps only newest 20 entries
5. `get_recent_api_calls() -> list[dict]`
   - Returns shallow copy of in-memory summaries

### `_call_api(...)` logging integration

Updated flow:

1. Perform request as before.
2. Parse response:
   - try `response.json()`
   - fallback to `response.text` when non-JSON
3. Keep debug logging.
4. Call `_record_api_call(...)` with request/response details.
5. Return behavior:
   - if `return_response=True`, return raw `response`
   - else return parsed JSON when available
   - else raise:
     - `SmartmeterConnectionError("Could not parse JSON response for endpoint '...'" )`

## Attribute schema exposed on sensor

After each successful update, sensor attributes include:

- `raw_api_logging_enabled`: bool
- `api_call_count`: int
- `recent_api_calls`: list of summaries (max 5 shown)
- `last_api_call_file`: string path or `None`

Each `recent_api_calls` item has:

- `timestamp`
- `method`
- `endpoint`
- `url`
- `response_status`
- `file_path`

## File output schema

When enabled, each JSON file contains:

- `timestamp`
- `method`
- `endpoint`
- `url`
- `query`
- `request_headers` (redacted)
- `request_body`
- `response_status`
- `response_body`

Target directory:

- `/config/tmp/wnsm_api_calls`

## Rebuild checklist

1. Add toggle propagation in `sensor.py` from options/data/default.
2. Extend `WNSMSensor` to accept toggle and expose logging attributes.
3. Extend `Smartmeter` with:
   - in-memory recent call store
   - optional file writer
   - `_call_api` record hook
4. Ensure sensitive headers are redacted in persisted payload.
5. Keep existing API auth and sensor/statistics logic unchanged except wiring/logging additions.

## Verification performed

- Syntax compile passed:
  - `python -m py_compile custom_components/wnsm/api/client.py custom_components/wnsm/sensor.py custom_components/wnsm/wnsm_sensor.py`
