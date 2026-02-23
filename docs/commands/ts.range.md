# TS.RANGE

Query a time series for raw samples or aggregated data over a specified time range.

### Syntax

```bash
TS.RANGE key fromTimestamp toTimestamp
  [LATEST]
  [FILTER_BY_TS timestamp ...]
  [FILTER_BY_VALUE min max]
  [COUNT count]
  [[ALIGN align] AGGREGATION aggregator bucketDuration [CONDITION op value] [BUCKETTIMESTAMP bt] [EMPTY]]
```

---

## Required Arguments

<details open><summary><code>key</code></summary>
the time series key to query.
</details>
<details open><summary><code>fromTimestamp</code></summary>
the start of the time range to query, inclusive. Accepts:
- Numeric timestamp in milliseconds
- `-` for the earliest timestamp in the series
- Duration spec (e.g., `2h` for 2 hours ago)
</details>
<details open><summary><code>toTimestamp</code></summary>
the end of the time range to query, inclusive. Accepts:
  - Numeric timestamp in milliseconds
  - `+` for the latest timestamp in the series
  - `*` for the current time
  - Duration spec (e.g., `30m` for 30 minutes ago)
</details>

## Optional Arguments

<details open><summary><code>LATEST</code></summary>
When querying a compaction, return the latest bucket value even if the bucket is not yet closed. This is in addition to the regular range results. 
</details>
<details open><summary><code>FILTER_BY_TS timestamp ...</code></summary>
Include only samples at the specified timestamp(s). Multiple timestamps can be provided. Applied before aggregation.
</details>
<details open><summary><code>FILTER_BY_VALUE min max</code></summary>
Include only samples with values in `[min, max]`. Both bounds are inclusive. Applied before aggregation.
</details>
<details open><summary><code>COUNT count</code></summary>
Limit output to the first `count` samples or buckets. When used with aggregation, limits bucket
count (not samples per bucket).
</details>
<details open><summary><code>AGGREGATION aggregator bucketDuration</code></summary>
Aggregate raw samples into fixed-size time buckets. See [Aggregators](#aggregators) for supported aggregation functions.
</details>
<details open><summary><code>ALIGN align</code></summary> 
Control bucket alignment:
  - `start` — Align buckets to range start
  - `end` — Align buckets to range end
  - Numeric timestamp — Align all buckets to a specific timestamp
  - If omitted, uses module default alignment.
</details>
<details open><summary><code>BUCKETTIMESTAMP bt</code></summary>
(Optional) Which timestamp to return for each bucket:

- `start` (default) — Bucket start time
- `end` — Bucket end time
- `mid` — Bucket midpoint

</details>
<details open><summary><code>CONDITION op value</code></summary>
Comparison filter for conditional aggregators (e.g., `countif`, `sumif`, `share`, `all/any/none`):
- `op` is a comparison operator: `>`, `<`, `>=`, `<=`, `==`, or `!=`
- `value` is the value to compare against
Only samples satisfying the condition are included in the aggregation.
</details>

### Aggregation

- **`AGGREGATION aggregator bucketDuration`** — Aggregate raw samples into fixed-size time buckets
  - **`aggregator`** — Aggregation function to apply (see [Aggregators](#aggregators))
  - **`bucketDuration`** — Bucket size in milliseconds (must be positive)
---

## Supported Aggregators

### Simple Aggregators

| Aggregator | Description                    | Empty Bucket Value |
|------------|--------------------------------|--------------------|
| `avg`      | Arithmetic mean                | `NaN`              |
| `sum`      | Sum of all values              | `0`                |
| `count`    | Number of samples              | `0`                |
| `min`      | Minimum value                  | `NaN`              |
| `max`      | Maximum value                  | `NaN`              |
| `range`    | Difference between max and min | `NaN`              |
| `first`    | Earliest sample value          | —                  |
| `last`     | Latest sample value            | —                  |

### Statistical Aggregators

| Aggregator | Description                   | Empty Bucket Value     |
|------------|-------------------------------|------------------------|
| `std.p`    | Population standard deviation | `NaN`                  |
| `std.s`    | Sample standard deviation     | `NaN` (if < 2 samples) |
| `var.p`    | Population variance           | `NaN`                  |
| `var.s`    | Sample variance               | `NaN` (if < 2 samples) |

### Counter/Rate Aggregators

| Aggregator | Description                                      | Notes                                                                 |
|------------|--------------------------------------------------|-----------------------------------------------------------------------|
| `increase` | Total increase for monotonic counters            | Handles resets                                                        |
| `rate`     | Rate of change per second over the bucket window | —                                                                     |
| `irate`    | Instantaneous rate from the last two samples     | Requires ≥ 2 samples and positive time delta; returns `NaN` otherwise |

### Filtered Aggregators

> These operate only on samples matching a comparison condition.

| Aggregator | Description                                           | Empty Bucket Value |
|------------|-------------------------------------------------------|--------------------|
| `countif`  | Count of samples matching condition                   | `0`                |
| `sumif`    | Sum of samples matching condition                     | `0`                |
| `share`    | Fraction of samples matching condition (`[0.0, 1.0]`) | `NaN`              |
| `all`      | `1.0` if all samples match, `0.0` otherwise           | `NaN`              |
| `any`      | `1.0` if any sample matches, `0.0` otherwise          | `NaN`              |
| `none`     | `1.0` if no samples match, `0.0` otherwise            | `NaN`              |

---

## Return Value

**Without aggregation:**  
Array of `[timestamp, value]` pairs

**With aggregation:**  
Array of `[bucketTimestamp, aggregatedValue]` pairs

---

## Examples

### Basic Query

Get all samples in a time range:

```
TS.RANGE temperature 1609459200000 1609545600000
```

### Latest Sample

Get the most recent sample:

```
TS.RANGE temperature - + LATEST
```

### Value Filtering

Get samples where value is between 20 and 30:

```
TS.RANGE temperature 1609459200000 1609545600000 FILTER_BY_VALUE 20 30
```

### Specific Timestamps

Get samples at exact timestamps:

```
TS.RANGE sensor:001 - + FILTER_BY_TS -2h 1609459260000 1609459320000
```

### Hourly Average

Compute average per hour:

```
TS.RANGE requests 1609459200000 1609545600000 AGGREGATION avg 3600000
```

### 5-Minute Sums with Empty Buckets

```
TS.RANGE bytes 1609459200000 1609470000000 
  ALIGN start 
  AGGREGATION sum 300000 
  BUCKETTIMESTAMP mid 
  EMPTY
```

### Limited Results

Get first 100 aggregated buckets:

```
TS.RANGE metrics 1609459200000 1609545600000 
  AGGREGATION avg 60000 
  COUNT 100
```

---

## Behavior Notes

- **Timestamp Inclusivity:** Both `fromTimestamp` and `toTimestamp` are inclusive
- **Empty Buckets:** Omitted by default; use `EMPTY` to include them
- **Filtered Aggregators:** Condition filters are applied within each bucket after timestamp/value filters
- **Reverse Queries:** `TS.REVRANGE` adjusts semantics of `FIRST`/`LAST` appropriately
- **Bucket Boundaries:** Computed based on alignment and `bucketDuration`
- **Special Values:** Use `-inf`/`+inf` for unbounded value ranges

---

## Common Errors

- **Wrong arity** — Missing required arguments
- **invalid AGGREGATION value** — Unrecognized aggregator name
- **invalid BUCKETTIMESTAMP** — Invalid bucket timestamp option
- **invalid ALIGN** — Invalid alignment parameter
- **invalid bucketDuration** — Bucket duration must be a positive integer

---

## See Also

- `TS.REVRANGE` — Same query in reverse order
- `TS.MRANGE` — Query multiple time series at once
- `TS.GET` — Get the latest sample only
- `TS.ADD` — Add samples to a time series


