# Wiener Netze Smartmeter (WNSM) - User Documentation

This integration imports Wiener Netze smart meter data into Home Assistant and exposes:
- sensor entities
- recorder statistics streams for Energy Dashboard and history views

## Configuration Mode

This custom component is configurable through the Home Assistant UI only.
YAML configuration is not supported.

## Initial Setup (Login Screen)

Use the Home Assistant integration dialog to enter your Wiener Netze username and password.

![Wiener Netze Smartmeter Authentication - Initial setup login screen](doc/wnsm5.png)

## What You Get

For each selected meter (`zaehlpunkt`), the integration can create up to 3 sensor entities.

## Created Sensors

`<zaehlpunkt>` below means your meter ID, for example `at0010000000000000001000009104483`.

| Entity ID pattern | Created by default | Type | Description |
| --- | --- | --- | --- |
| `sensor.<zaehlpunkt>` | Yes | Sensor entity | Main total energy sensor (kWh), `total_increasing`. |
| `sensor.<zaehlpunkt>_daily_cons` | Yes (if `_daily_cons` toggle is enabled) | Sensor entity | Latest cumulative value from daily historical consumption stream (kWh). |
| `sensor.<zaehlpunkt>_daily_cons_day` | Yes (if `_daily_cons` toggle is enabled) | Sensor entity | Latest day value (daily delta) derived from the daily consumption stream (kWh). |

Important:
- There is currently no dedicated `sensor.<zaehlpunkt>_daily_meter_read` entity.
- `_daily_meter_read` is implemented as recorder statistics (see below).

## Recorder Statistics Streams

For each selected meter, these statistic IDs are used:

| Statistic ID pattern | Default | Controlled by |
| --- | --- | --- |
| `wnsm:<zaehlpunkt_lowercase>` | Enabled | Always on |
| `wnsm:<zaehlpunkt_lowercase>_cum_abs` | Enabled | Always on |
| `wnsm:<zaehlpunkt_lowercase>_daily_cons` | Enabled | Toggle: `Enable daily historical values, sensor, and statistics (Suffix _daily_cons).` |
| `wnsm:<zaehlpunkt_lowercase>_daily_meter_read` | Enabled | Toggle: `Enable daily total consumption historical values, statistics (Suffix  _daily_meter_read).` |

## Configuration Defaults

Default values in the UI:
- `Scan interval (minutes)`: `360` (6 hours)
- `Enable raw Api Response written to /config/tmp/wnsm_api_calls`: `False`
- `Enable daily historical values, sensor, and statistics (Suffix _daily_cons).`: `True`
- `Enable daily total consumption historical values, statistics (Suffix  _daily_meter_read).`: `True`
- `Meters`: active/ready meters are pre-selected by default

## Toggle Behavior

### `_daily_cons` toggle

When enabled (default):
- Creates sensor entities:
  - `sensor.<zaehlpunkt>_daily_cons`
  - `sensor.<zaehlpunkt>_daily_cons_day`
- Imports/maintains statistics stream:
  - `wnsm:<zaehlpunkt_lowercase>_daily_cons`

When disabled:
- The two `_daily_cons*` sensors are not created.
- No new `_daily_cons` statistics are imported.

### `_daily_meter_read` toggle

When enabled (default):
- Imports/maintains statistics stream:
  - `wnsm:<zaehlpunkt_lowercase>_daily_meter_read`

When disabled:
- No new `_daily_meter_read` statistics are imported.
- Existing other sensors/entities are unaffected.

## Typical Tile Card Example

Show daily value (not cumulative):

```yaml
type: tile
entity: sensor.<zaehlpunkt>_daily_cons_day
vertical: false
features_position: bottom
```

## After Changing Options

After changing options in the integration:
- Home Assistant reloads the integration automatically.
- If needed, run a full Home Assistant restart to force immediate entity/statistics refresh.

## Credits

This integration is based on the original work by DarwinsBuddy:
- https://github.com/DarwinsBuddy/WienerNetzeSmartmeter
