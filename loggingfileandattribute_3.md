# WNSM Logging Patch (File + Sensor Attributes)

This document specifies the exact logging changes that were implemented so another LLM can rebuild them.

## Scope

- Modified files:
  - `custom_components/wnsm/api/client.py`
  - `custom_components/wnsm/sensor.py`
  - `custom_components/wnsm/coordinator.py`
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
      - `<selected_root>/<entry_id>/<zaehlpunkt>/*.json`
      - where `<selected_root>` is the first writable candidate:
        - `/homeassistant/tmp/wnsm_api_calls`
        - `/config/tmp/wnsm_api_calls`
        - `/tmp/wnsm_api_calls`
    - Before first write for a client session, the logging root contents are fully cleaned:
      - all files/subfolders under the selected root are removed
      - cleanup is strict: if any item cannot be deleted, that root candidate is rejected
        and the next fallback root is tried
    - On Smartmeter initialization (if toggle is enabled), directory preparation is attempted
      immediately and failures are captured for visibility.
    - Failed API responses (HTTP 4xx/5xx) are logged as well before exceptions are raised
    - Request-level failures (e.g. timeout/socket `RequestException`) are also logged
      as entries with `response_status = null` and exception text in `response_body`
3. Always:
   - A recent API call summary list is kept in-memory on `Smartmeter`.
   - Sensor attributes are enriched with logging metadata after update.
4. GET API calls use bounded retry/backoff for transient errors; each failed attempt is still captured in raw logs before retry/raise.

## File-by-file changes

## 1) `custom_components/wnsm/sensor.py`

### Added constant

- `CONF_ENABLE_RAW_API_RESPONSE_WRITE = "enable_raw_api_response_write"`

### `async_setup_entry(...)`

Added resolution logic:

- `enable_raw_api_response_write = options->data->False`
- `scan_interval = options->data->360`

Creates one shared coordinator:

- `WNSMDataUpdateCoordinator(...)`
  - `enable_raw_api_response_write=...`
  - `scan_interval_minutes=...`
  - `log_scope=config_entry.entry_id`

Sensors now receive coordinator + zaehlpunkt, not username/password.

### `async_setup_platform(...)`

For YAML setup path, creates coordinator with:

- `enable_raw_api_response_write=False`
- `log_scope="yaml"`

## 2) `custom_components/wnsm/coordinator.py`

### New file: shared polling and logging source

- Class: `WNSMDataUpdateCoordinator(DataUpdateCoordinator)`
- Holds one shared:
  - `Smartmeter`
  - `AsyncSmartmeter`
- Poll interval is driven by config-flow `scan_interval`.
- Login happens once per cycle for the shared client.
- Loops all configured zaehlpunkte and stores per-zp state in `coordinator.data`.
- Imports statistics via existing `Importer` per zaehlpunkt.
- Passes importer dedup hints:
  - `skip_login=True`
  - `preloaded_zaehlpunkt=<coordinator-fetched zp response>`

### Logging attributes assembled in coordinator

- `_inject_api_log_attributes(zaehlpunkt, attributes)` filters recent API calls for that zaehlpunkt and writes:
  - `raw_api_logging_enabled`
  - `api_call_count`
  - `recent_api_calls` (max 5)
  - `last_api_call_file`
  - `raw_api_logging_prepared`
  - `raw_api_logging_root`
  - `raw_api_logging_directory`
  - `raw_api_logging_prepare_error`
  - `raw_api_last_write_error`

## 3) `custom_components/wnsm/wnsm_sensor.py`

### Constructor signature changed

From constructor-per-sensor auth model to coordinator model:

- `__init__(self, coordinator, zaehlpunkt)`
- Subclasses `CoordinatorEntity`.
- No per-entity Smartmeter creation.

### `async_update(...)` wiring

State now comes from `coordinator.data[zaehlpunkt]`:

- `native_value`
- `extra_state_attributes`
- `available`

## 4) `custom_components/wnsm/api/client.py`

### `Smartmeter.__init__(...)` extended

Added arg:

- `enable_raw_api_response_write: bool = False`
- `log_scope: str = "default"`

New fields:

- `self._enable_raw_api_response_write`
- `self._raw_api_scope = <sanitized_log_scope or "default">`
- `self._raw_api_response_root = None` (set after successful prepare)
- `self._raw_api_response_dir = None` (set after successful prepare)
- `self._raw_api_response_root_candidates = ["/homeassistant/tmp/wnsm_api_calls", "/config/tmp/wnsm_api_calls", "/tmp/wnsm_api_calls"]`
- `self._raw_api_log_prepared = False`
- `self._raw_api_log_prepare_error = None`
- `self._raw_api_last_write_error = None`
- `self._recent_api_calls = []`
- `self._max_recent_api_calls = 20`
- if `enable_raw_api_response_write=True`, call `_prepare_raw_api_response_dir()` immediately

### `reset()` update

- Resets `self._raw_api_log_prepared = False`
- Resets `self._raw_api_log_prepare_error = None`
- Resets `self._raw_api_last_write_error = None`
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
   - Calls directory preparation once per session via `_prepare_raw_api_response_dir()`.
   - Resolves target meter via `_extract_zaehlpunkt_for_log(endpoint, query, request_body)`.
   - Writes into per-meter subfolder:
      - `<selected_root>/<sanitized_log_scope>/<sanitized_zaehlpunkt>/`
   - Writes pretty JSON file:
      - filename format: `{timestamp}_{method}_{sanitized_endpoint}.json`
   - Returns file path or `None`
4. `_prepare_raw_api_response_dir()`
    - If already prepared in current session: no-op
    - Else:
       - iterate fallback roots in this exact order:
         - `/homeassistant/tmp/wnsm_api_calls`
         - `/config/tmp/wnsm_api_calls`
         - `/tmp/wnsm_api_calls`
        - for each candidate:
          - ensure directory exists
          - write/delete a temporary probe file to verify write access
          - delete all files/subfolders inside that candidate root
          - verify root is empty after cleanup; otherwise fail this candidate
          - create scope dir `<candidate>/<sanitized_log_scope>`
          - set `self._raw_api_response_root` and `self._raw_api_response_dir`
          - mark prepared `True` and return
       - if all candidates fail:
         - keep prepared `False`
         - keep root/dir `None`
         - store `self._raw_api_log_prepare_error` (last error)
         - log error
5. `_extract_zaehlpunkt_for_log(endpoint, query, request_body) -> str`
   - Priority:
     1. `query["zaehlpunkt"]` if present
     2. `request_body["zaehlpunkt"]` if present
     3. regex from endpoint:
        - `messdaten/[^/]+/([^/]+)/`
        - `zaehlpunkte/[^/]+/([^/]+)/`
     4. fallback `"general"`
6. `_record_api_call(...)`
   - Builds full payload with:
      - timestamp, method, endpoint, url, query
      - redacted request headers
      - request body
      - response status
      - response body
   - Calls `_write_raw_api_response(...)`
   - Pushes summary to `_recent_api_calls`
   - Keeps only newest 20 entries
7. `get_recent_api_calls() -> list[dict]`
    - Returns shallow copy of in-memory summaries
8. `get_raw_api_logging_status() -> dict`
   - Returns:
      - `enabled`
      - `prepared`
      - `root`
      - `directory`
      - `prepare_error`
      - `last_write_error`

### `_call_api(...)` logging integration

Updated flow:

1. Perform request as before.
2. Parse response:
   - try `response.json()`
   - fallback to `response.text` when non-JSON
3. Keep debug logging.
4. Call `_record_api_call(...)` with request/response details.
5. For `GET`, retry up to 3 attempts with jitter on:
   - request exceptions
   - `429/500/502/503/504`
   - first `401/403` can trigger gateway key refresh + retry
   - each request exception attempt is recorded via `_record_api_call(...)`
     before retry/raise
6. Apply centralized status handling:
   - 401/403 -> `SmartmeterLoginError`
   - other 4xx/5xx -> `SmartmeterConnectionError`
   - includes endpoint + status + response payload in exception message
7. Return behavior:
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
- `raw_api_logging_prepared`: bool
- `raw_api_logging_root`: string path or `None`
- `raw_api_logging_directory`: string path or `None`
- `raw_api_logging_prepare_error`: string or `None`
- `raw_api_last_write_error`: string or `None`

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
  - for request exceptions:
    - `response_status` is `null`
    - `response_body` starts with `RequestException: ...`

Target directory:

- root is first writable of:
  - `/homeassistant/tmp/wnsm_api_calls`
  - `/config/tmp/wnsm_api_calls`
  - `/tmp/wnsm_api_calls`
- subfolder per entry scope:
  - `<root>/<entry_id>/`
- subfolder per meter:
  - `<root>/<entry_id>/<zaehlpunkt>/`
- fallback subfolder when no meter is detectable:
  - `<root>/<entry_id>/general/`

## Rebuild checklist

1. Add toggle propagation in `sensor.py` from options/data/default.
2. Build one shared coordinator per entry and pass `log_scope=entry_id`.
3. Move per-zp logging attribute assembly into coordinator update data.
4. Keep sensors as coordinator-backed readers.
5. Extend `Smartmeter` with:
   - in-memory recent call store
   - optional file writer
   - scope-aware base log directory
   - writable-root fallback + startup probe
   - strict root cleanup validation (no silent delete failures)
   - explicit logging status/error surface (`get_raw_api_logging_status`)
   - per-zaehlpunkt log subfolders
   - one-time cleanup of the entire logging root contents before first write
   - `_call_api` record hook including request-exception attempts
6. Ensure sensitive headers are redacted in persisted payload.
7. Add GET-only retry/backoff in `_call_api` while keeping exception mapping intact.
8. Keep existing API auth and statistics logic unchanged except wiring/logging additions.

## Verification performed

- Syntax compile passed:
  - `python -m py_compile custom_components/wnsm/api/client.py custom_components/wnsm/coordinator.py`
