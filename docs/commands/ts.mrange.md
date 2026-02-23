# TS.MRANGE

Query multiple time series over a time range, optionally filtering, aggregating, and grouping results.

## Syntax

```
TS.MRANGE fromTimestamp toTimestamp
    [LATEST]
    [FILTER_BY_TS ts...]
    [FILTER_BY_VALUE min max]
    [WITHLABELS | SELECTED_LABELS label...]
    [COUNT count]
    [[ALIGN align] AGGREGATION aggregator bucketDuration [CONDITION op value] [BUCKETTIMESTAMP bt] [EMPTY]]
    FILTER selector...
    [GROUPBY label REDUCE reducer]
```

## Required Arguments

### fromTimestamp toTimestamp

Start and end timestamps for the query range (inclusive).

**Supported formats:**

- Unix timestamp in milliseconds (e.g., `1609459200000`)
- `-` for earliest available timestamp
- `+` for latest available timestamp
- Relative time with units (e.g., `1h`, `30m`, `7d`)

**Example:**

```
TS.MRANGE 1609459200000 1609545600000 FILTER sensor_id=12
TS.MRANGE - + FILTER region=us-west
TS.MRANGE -1h + FILTER metric_type=temperature
```

### FILTER selector...

One or more [series selectors](../topics/series-selectors.md) to match time series. At least one selector is required.

**Example:**

```
FILTER sensor_id=12 region=us-west
FILTER metric_type=temperature
```

## Optional Arguments

### LATEST

When specified and a series has compaction rules, returns samples from the latest compacted series instead of raw
samples.

**Default:** Returns raw samples.

### FILTER_BY_TS ts...

Return only samples at the specified timestamps. Timestamps outside the query range are automatically excluded.

**Example:**

```
FILTER_BY_TS 1609459200000 1609459260000 1609459320000
```

**Note:** Maximum 128 timestamps per query.

### FILTER_BY_VALUE min max

Return only samples where the value falls within the specified range (inclusive).

**Example:**

```
FILTER_BY_VALUE 20.0 25.0
```

### WITHLABELS

Return all label name-value pairs for each matched series.

**Note:** Cannot be used with `SELECTED_LABELS`.

### SELECTED_LABELS label...

Return only the specified label name-value pairs for each matched series.

**Example:**
```
SELECTED_LABELS sensor_id region
```

**Note:** Cannot be used with `WITHLABELS`.

### COUNT count

Maximum number of samples to return per series.

**Example:**

```
COUNT 100
```

### ALIGN align

Specify the alignment strategy for aggregation buckets. Must be specified before `AGGREGATION`.

**Valid values:**

- `start` - Align buckets to start timestamp (default)
- `end` - Align buckets to end timestamp
- `-` - Align to start of time range
- `+` - Align to end of time range
- Unix timestamp in milliseconds
- Time value with units (e.g., `1h`)

**Example:**

```
ALIGN start AGGREGATION avg 1h
ALIGN 1609459200000 AGGREGATION sum 5m
```

**Restrictions:**

- Cannot use `start` align with `-` range start timestamp
- Cannot use `end` align with `+` range end timestamp

### AGGREGATION aggregator bucketDuration

Aggregate samples into time buckets using the specified aggregator and bucket size.

**Supported aggregators:**

- `all` - 1 if all samples satisfy a condition, else 0
- `any` - 1 if any sample satisfies a condition, else 0
- `avg` - Average value
- `count` - Count of samples
- `countif` - Count of samples satisfying a condition
- `first` - First sample value
- `last` - Last sample value
- `max` - Maximum value
- `min` - Minimum value
- `none` - 1 if no samples satisfy a condition, else 0
- `range` - Difference between max and min
- `rate` - Per-second rate of change (only for numeric values)
- `share` - Ratio of samples satisfying a condition to total samples
- `std.p` - Population standard deviation
- `std.s` - Sample standard deviation
- `sum` - Sum of values
- `sumif` - Sum of values satisfying a condition
- `var.p` - Population variance
- `var.s` - Sample variance

**Example:**

```
AGGREGATION avg 1h
AGGREGATION sum 5m
```

#### CONDITION op value

Filter samples before aggregation based on a comparison condition.

**Supported operators:** `<`, `<=`, `>`, `>=`, `=`, `!=`

**Example:**

```
AGGREGATION share 1h CONDITION > 20.0
```

#### BUCKETTIMESTAMP bt

Specify which timestamp to use for aggregated buckets.

**Valid values:**

- `start` - Bucket start time (default)
- `end` - Bucket end time
- `mid` - Bucket midpoint time

**Example:**

```
AGGREGATION avg 1h BUCKETTIMESTAMP mid
```

#### EMPTY

Include empty buckets (buckets with no samples) in results with no value.

**Example:**

```
AGGREGATION avg 1h EMPTY
```

### GROUPBY label REDUCE reducer

Group matching series by label value and apply a reducer across each group.

**Example:**

```
GROUPBY region REDUCE sum
```

**Supported reducers:**

Supports all aggregators except `rate` (e.g., `avg`, `sum`, `count`, `max`, `min`, etc.)

#### CONDITION op value

Filter samples before reduction based on a comparison condition (same as aggregation `CONDITION`).

**Example:**

```
GROUPBY region REDUCE countif CONDITION > 20.0
```

## Return Value

Returns an array where each element represents a matched series (or group when using `GROUPBY`):

```
1) 1) "series:key"              # Series key (or group label value)
   2) 1) "label1"               # Labels (if WITHLABELS or SELECTED_LABELS)
      2) "value1"
      3) "label2"
      4) "value2"
   3) 1) 1) (integer) 1609459200000  # Timestamp
         2) "23.5"                     # Value
      2) 1) (integer) 1609459260000
         2) "24.1"
      ...
```

- If no labels are requested, element 2 is empty
- Element 3 contains timestamp-value pairs
- When using `GROUPBY`, element 1 contains the group label value instead of series key
- Series/groups are returned in no guaranteed order

## Complexity

O(n×m×k) where:

- n = number of matched series
- m = number of samples per series in the range
- k = aggregation/grouping cost

## Examples

### Query multiple series with all labels

```bash
127.0.0.1:6379> TS.MRANGE 1609459200000 1609545600000 WITHLABELS FILTER sensor_id=12
1) 1) "temperature:sensor:12"
   2) 1) "sensor_id"
      2) "12"
      3) "metric_type"
      4) "temperature"
      5) "location"
      6) "warehouse"
   3) 1) 1) (integer) 1609459200000
         2) "23.5"
      2) 1) (integer) 1609459260000
         2) "23.8"
      3) 1) (integer) 1609459320000
         2) "24.1"
```

### Query with selected labels and value filter

```bash
127.0.0.1:6379> TS.MRANGE - + SELECTED_LABELS region FILTER_BY_VALUE 20 25 FILTER metric_type=temperature
1) 1) "temperature:sensor:12"
   2) 1) "region"
      2) "us-west"
   3) 1) 1) (integer) 1609459200000
         2) "23.5"
      2) 1) (integer) 1609459260000
         2) "23.8"
```

### Query with aggregation

```bash
127.0.0.1:6379> TS.MRANGE - + AGGREGATION avg 1h FILTER sensor_id=12
1) 1) "temperature:sensor:12"
   2) (empty array)
   3) 1) 1) (integer) 1609459200000
         2) "23.8"
      2) 1) (integer) 1609462800000
         2) "24.2"
```

### Query with grouping

```bash
127.0.0.1:6379> TS.MRANGE - + FILTER metric_type=temperature GROUPBY region REDUCE avg
1) 1) "us-west"
   2) (empty array)
   3) 1) 1) (integer) 1609459200000
         2) "23.5"
      2) 1) (integer) 1609459260000
         2) "23.8"
2) 1) "us-east"
   2) (empty array)
   3) 1) 1) (integer) 1609459200000
         2) "21.2"
      2) 1) (integer) 1609459260000
         2) "21.5"
```

### Query with timestamp filter and count limit

```bash
127.0.0.1:6379> TS.MRANGE - + FILTER_BY_TS 1609459200000 1609459320000 COUNT 10 FILTER sensor_id=12
1) 1) "temperature:sensor:12"
   2) (empty array)
   3) 1) 1) (integer) 1609459200000
         2) "23.5"
      2) 1) (integer) 1609459320000
         2) "24.1"
```

### Query with aggregation condition and empty buckets

```bash
127.0.0.1:6379> TS.MRANGE - + AGGREGATION avg 1h CONDITION > 23.0 EMPTY FILTER sensor_id=12
1) 1) "temperature:sensor:12"
   2) (empty array)
   3) 1) 1) (integer) 1609459200000
         2) "23.8"
      2) 1) (integer) 1609462800000
         2) (empty)
```

## Notes

- All filter selectors must match for a series to be included (logical AND)
- For clustered deployments, the command fans out to all shards automatically
- When using `GROUPBY`, series are grouped by the specified label value
- Aggregation is applied before grouping when both are specified
- `LATEST` is useful when you have compaction rules and want aggregated data without querying compacted series directly

## See Also

- [TS.RANGE](range.md) - Query a single series over a time range
- [TS.MREVRANGE](mrevrange.md) - Query multiple series in reverse order
- [TS.MGET](mget.md) - Get the last sample from multiple series
- [Series Selectors](../topics/series-selectors.md) - Label filter syntax