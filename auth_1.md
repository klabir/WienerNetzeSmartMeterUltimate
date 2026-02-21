# WNSM Auth Patch Notes (Auth + Runtime Wiring Delta)

This file documents the authentication changes and the later runtime wiring update that impacts how often/auth where login is performed.

## Scope

- File changed: `custom_components/wnsm/api/client.py`
- No config-flow UI logic changes in this file
- Existing auth flow structure remains:
  - `load_login_page()` -> `credentials_login()` -> `load_tokens()` -> `_get_api_key()`

## Goals of the patch

1. Keep the existing OIDC/PKCE login chain behavior.
2. Improve robustness around HTML form extraction and token validation.
3. Preserve error semantics with `SmartmeterConnectionError` / `SmartmeterLoginError`.
4. Keep auth protocol unchanged while allowing shared runtime session usage.

## Exact changes

### 1) Centralized first form action extraction

Added helper method:

- `Smartmeter._extract_first_form_action(content, no_form_error)`

Behavior:

1. Parse HTML via `lxml.html.fromstring(content)`.
2. Extract form actions with XPath `(//form/@action)`.
3. If no form exists, raise:
   - `SmartmeterConnectionError(no_form_error)`
4. Return first action URL (`forms[0]`).

### 2) `load_login_page()` now uses helper

Before:

- Parsed HTML inline and checked for missing form.

Now:

- Calls `_extract_first_form_action(result.content, "No form found on the login page.")`.

Net effect:

- Same external behavior, cleaner shared logic.

### 3) `credentials_login(url)` now uses helper for first POST response

Before:

- Parsed first POST response HTML inline via `tree.xpath(...)[0]` (could raise index errors).

Now:

- Calls `_extract_first_form_action(result.content, "Could not login with credentials")`.

Net effect:

- Missing form is converted into consistent `SmartmeterConnectionError` in auth path.

### 4) `load_tokens(code)` bearer check hardened

Before:

- Used `tokens["token_type"]` directly.

Now:

- Uses `token_type = tokens.get("token_type")`.
- Raises:
  - `SmartmeterLoginError(f"Bearer token required, but got {token_type!r}")`
  when token type is not `"Bearer"` (including missing key).

Net effect:

- Avoids unhandled `KeyError`; keeps auth failure in expected login-error class.

### 5) `_access_valid_or_raise()` handles uninitialized token state

Before:

- Compared `datetime.now() >= self._access_token_expiration` directly.
- Could fail if expiration was `None`.

Now:

1. If `_access_token is None` or `_access_token_expiration is None`, raise:
   - `SmartmeterConnectionError("Access Token is not valid anymore, please re-log!")`
2. Keep existing expiry-time check and same error message.

Net effect:

- Consistent failure semantics when token state is missing/uninitialized.

### 6) `Smartmeter.__init__` supports scoped runtime logging path (non-auth argument)

Added ctor arg:

- `log_scope: str = "default"`

Behavior:

- Does not change login algorithm.
- Sets raw logging base for this client to:
  - `/config/tmp/wnsm_api_calls/<sanitized_log_scope>/`

### 7) Runtime auth call pattern changed to shared coordinator session

Auth internals are unchanged, but runtime usage is now:

1. One `Smartmeter` + one `AsyncSmartmeter` is created per config entry in coordinator.
2. Coordinator calls `await async_smartmeter.login()` once per update cycle.
3. Per-zaehlpunkt requests reuse that authenticated session/token.

Net effect:

- Lower login churn and fewer repeated auth handshakes.
- Same auth protocol and same auth error semantics.

### 8) Refresh token flow implemented

Added:

- `is_refresh_expired()`
- `refresh_tokens()`

`refresh_tokens()` behavior:

1. Validate refresh token exists and is not expired.
2. POST to `AUTH_URL + "token"` with:
   - `grant_type=refresh_token`
   - `client_id=wn-smartmeter`
   - `redirect_uri=<REDIRECT_URI>`
   - `refresh_token=<stored_refresh_token>`
3. Require HTTP 200 and `token_type == "Bearer"`.
4. Update:
   - `_access_token`
   - optional `_refresh_token` (if response includes new one)
   - `_access_token_expiration`
   - optional `_refresh_token_expiration`

Usage:

- `login()` now attempts refresh first when access token is expired.
- `_access_valid_or_raise()` now refreshes access token instead of immediately failing.

### 9) Auth/API HTTP status handling hardened

Added helper:

- `_raise_for_response(endpoint, status_code, error_data)`

Behavior:

- status `< 400`: no-op
- status `401/403`: raise `SmartmeterLoginError(...)`
- status `>= 400` otherwise: raise `SmartmeterConnectionError(...)`

This is called in `_call_api()` after response parsing and logging capture.

### 10) Auth network calls now use explicit timeouts

Added `timeout=60.0` in:

- `load_login_page()` GET auth page
- `credentials_login()` first POST + second POST
- `load_tokens()` token exchange
- `refresh_tokens()` refresh exchange
- `_get_api_key()` app-config request

Net effect:

- Prevents hangs during login/refresh/api-key fetch.

### 11) Related runtime API call fix (from improvement item #3)

File:

- `custom_components/wnsm/AsyncSmartmeter.py`

Change:

- `get_meter_readings()` now calls `self.smartmeter.meter_readings` (correct endpoint wrapper)
- previously it called `self.smartmeter.historical_data`

Net effect:

- endpoint intent matches method name and avoids wrong payload mapping.

## Rebuild checklist for another LLM

1. Edit only `custom_components/wnsm/api/client.py`.
2. Add `_extract_first_form_action(content, no_form_error)` static method in `Smartmeter`.
3. Replace inline form parsing in:
   - `load_login_page()`
   - `credentials_login()`
   with helper usage.
4. In `load_tokens()`, switch bearer validation to `tokens.get("token_type")`.
5. In `_access_valid_or_raise()`, add early guard for missing token or expiration.
6. Keep all existing auth messages unchanged except the bearer message now prints `None` if missing.
7. Extend ctor with `log_scope` but keep auth sequence and token handling unchanged.
8. Implement runtime usage with a shared coordinator-backed client per config entry.
9. Implement refresh token flow and use it in `login()` + `_access_valid_or_raise()`.
10. Add centralized HTTP status->exception mapping in `_call_api()`.
11. Add explicit timeouts for all auth-related HTTP calls.
12. In `AsyncSmartmeter.get_meter_readings()`, call `meter_readings` instead of `historical_data`.

## Post-change verification done

- Syntax checks passed:
  - `python -m py_compile custom_components/wnsm/api/client.py`
  - `python -m py_compile custom_components/wnsm/coordinator.py custom_components/wnsm/sensor.py custom_components/wnsm/wnsm_sensor.py`

## Notes about test environment issue (not code behavior)

- Auth tests could not be executed in this environment because local `pytest_socket` policy blocks sockets needed by `pytest-asyncio` event loop creation on Windows.
