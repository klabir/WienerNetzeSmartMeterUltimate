# Daily Cumulative Graph (`_cum_abs`) - LLM Runbook

## Goal
Make Home Assistant `statistics-graph` render a **daily cumulative line** for:

- `wnsm:<zaehlpunkt>_cum_abs`

and avoid the "data exists in SQLite but graph is empty" issue.

## Why This Fails Without the Fix
`statistics-graph` with `period: day` can ignore series that are not exposed as full long-term statistics capabilities.

For reliable rendering, `_cum_abs` should provide:

- metadata: `has_mean = true`, `has_sum = true`
- rows: `state`, `mean`, and `sum` populated

Older `_cum_abs` rows often had only `state`, which caused daily graphs to stay empty.

## Files To Change
- `custom_components/wnsm/importer.py`
- `tests/it/test_importer.py`

## Required Code Changes

### 1. Upgrade validity check for cumulative stream
In `Importer.is_last_inserted_cumulative_stat_valid(...)`:

- keep existing checks for `state` and `end`
- additionally require `mean` and `sum` (when supported by HA schema)
- return `False` if old rows are missing those fields so backfill runs

### 2. Backfill cumulative stream with full fields
In `Importer._backfill_cumulative_from_existing_sum(...)`:

- query last cumulative stats with `{"state", "mean", "sum"}`
- when writing backfilled rows, set:
  - `state = row_sum`
  - `mean = row_sum`
  - `sum = row_sum`

### 3. Publish cumulative metadata as graph-friendly
In `Importer.get_cumulative_statistics_metadata(...)`:

- set `has_mean = True` (if available)
- set `has_sum = True` (if available)

### 4. Write new cumulative imports with full fields
In `Importer._import_statistics(...)`, for each cumulative row:

- write `StatisticData(start=..., state=..., mean=..., sum=...)`

### 5. Add/adjust regression test
In `tests/it/test_importer.py`:

- assert cumulative stream emits `mean` and `sum`
- assert metadata has `has_mean` and `has_sum` (when attributes exist on current HA version)

## Reference Patch Intent (Human Summary)
- `_cum_abs` is now a complete long-term stats stream, not state-only.
- older state-only data gets auto-upgraded by forced backfill logic.

## Verification After Deployment

### A. Restart + Reload
1. Full Home Assistant restart.
2. Reload WNSM integration once.

This ensures metadata update and backfill execution.

### B. Validate metadata
```sql
SELECT id, statistic_id, has_mean, has_sum, unit_of_measurement
FROM statistics_meta
WHERE statistic_id='wnsm:at0010000000000000001000009104483_cum_abs';
```
Expected:
- `has_mean = 1`
- `has_sum = 1`

### C. Validate row content
```sql
SELECT datetime(start_ts,'unixepoch','localtime') AS ts, state, mean, sum
FROM statistics
WHERE metadata_id = (
  SELECT id
  FROM statistics_meta
  WHERE statistic_id='wnsm:at0010000000000000001000009104483_cum_abs'
)
ORDER BY start_ts DESC
LIMIT 20;
```
Expected:
- `state`, `mean`, `sum` all populated and numerically equal for each row.

## Dashboard YAML (daily cumulative)
Use this card config:

```yaml
type: statistics-graph
title: Einspeisung kumuliert (30 Tage)
chart_type: line
period: day
days_to_show: 30
entities:
  - wnsm:at0010000000000000001000009104483_cum_abs
stat_types:
  - state
hide_legend: true
```

Notes:
- `entities` must be a string list item for this card, not `- statistic: ...`.
- If data still does not appear after restart/reload, force metadata repair once:

```sql
UPDATE statistics_meta
SET has_mean = 1, has_sum = 1
WHERE statistic_id='wnsm:at0010000000000000001000009104483_cum_abs';
```

then restart HA again.

## LLM Prompt Template (Copy/Paste)
Use this prompt with another coding LLM if you need to recreate the fix:

```text
Patch the WNSM integration so Home Assistant statistics-graph can reliably render daily cumulative data for wnsm:<zaehlpunkt>_cum_abs.

Constraints:
1) In custom_components/wnsm/importer.py:
   - Ensure cumulative metadata sets has_mean=True and has_sum=True (when supported).
   - Ensure cumulative rows always write state, mean, and sum with the same cumulative numeric value.
   - Ensure backfill writes state/mean/sum and upgrades old state-only rows.
   - Ensure cumulative validity check returns false if mean/sum are missing so backfill runs.
2) Add/update regression test in tests/it/test_importer.py:
   - assert cumulative metadata has_mean/has_sum true (if present)
   - assert cumulative row contains state/mean/sum and values match.
3) Keep compatibility with older HA cores where some metadata fields may not exist.
4) Do not break existing statistics stream behavior for wnsm:<zaehlpunkt>.

After patching:
- run syntax checks,
- provide final SQL verification queries for statistics_meta and statistics tables,
- provide final Lovelace statistics-graph YAML for period: day.
```

## Operational Checklist
- [ ] Patch applied
- [ ] Syntax check passed
- [ ] HA restarted
- [ ] Integration reloaded
- [ ] `statistics_meta` has `has_mean=1`, `has_sum=1`
- [ ] `_cum_abs` rows show `state`, `mean`, `sum`
- [ ] Lovelace daily graph renders

