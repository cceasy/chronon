# GroupBy Output Feature Naming

This note describes how Chronon's Python API auto-generates output column names for `GroupBy` aggregations.

The rules below match `get_output_col_names()` and `_get_op_suffix()` in `python/src/ai/chronon/group_by.py`.

## Naming Pattern

Chronon builds output names in three steps:

1. Base name: `<input_column>_<operation_suffix>`
2. Optional window suffix: `_<length><unit>`
3. Optional bucket suffix: `_by_<bucket>`

That yields these final shapes:

- Unwindowed, unbucketed: `<input>_<op>`
- Windowed: `<input>_<op>_<window>`
- Bucketed only: `<input>_<op>_by_<bucket>`
- Windowed and bucketed: `<input>_<op>_<window>_by_<bucket>`

## Operation Suffix

For most operations, the suffix is just the lowercase thrift operation name.

Examples:

- `SUM` -> `sum`
- `AVERAGE` -> `average`
- `LAST` -> `last`
- `APPROX_PERCENTILE` -> `approx_percentile`

There is one special case group:

- `LAST_K(k)` -> `last{k}`
- `FIRST_K(k)` -> `first{k}`
- `TOP_K(k)` -> `top{k}`
- `BOTTOM_K(k)` -> `bottom{k}`

So Chronon drops the trailing `_k` and appends the `k` value directly.

Examples:

- `LAST_K(10)` on `item_id` -> `item_id_last10`
- `TOP_K(5)` on `score` -> `score_top5`

## Window Suffix

If `aggregation.windows` is set, Chronon emits one output name per window and appends:

- `h` for hours
- `d` for days

Examples:

- `1h` -> `_1h`
- `7d` -> `_7d`
- `30d` -> `_30d`

## Bucket Suffix

If `aggregation.buckets` is set, Chronon emits one output name per bucket and appends:

- `_by_<bucket>`

If multiple buckets are configured, Chronon creates separate output columns for each bucket name. It does not combine bucket names into a single suffix.

## Examples

| Aggregation | Output column(s) |
| --- | --- |
| `Aggregation(input_column="price", operation=Operation.SUM)` | `price_sum` |
| `Aggregation(input_column="price", operation=Operation.SUM, windows=["7d", "30d"])` | `price_sum_7d`, `price_sum_30d` |
| `Aggregation(input_column="item_id", operation=Operation.LAST_K(3), windows=["7d"])` | `item_id_last3_7d` |
| `Aggregation(input_column="score", operation=Operation.TOP_K(5), buckets=["country"])` | `score_top5_by_country` |
| `Aggregation(input_column="price", operation=Operation.AVERAGE, windows=["1h"], buckets=["device", "country"])` | `price_average_1h_by_device`, `price_average_1h_by_country` |

## Notes

- The output order is window-first, then bucket. In code terms, Chronon first expands windows, then expands buckets over those windowed names.
- Only `LAST_K`, `FIRST_K`, `TOP_K`, and `BOTTOM_K` fold `k` into the generated suffix.
- `FREQUENT_K(...)` currently uses the `histogram` suffix in this module, because the helper maps it to the `HISTOGRAM` thrift operation before naming.
- Aggregation tags are attached to each generated output column name, so these names are also the keys used in `MetaData.columnTags`.
