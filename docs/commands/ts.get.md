# TS.GET

Get the last sample (most recent data point) from a time series.

## Syntax

```
TS.GET key [LATEST]
```

### Required Arguments

**key**: the time series key.

## Optional Arguments

**LATEST**: retrieves the latest compacted sample of the series. If the series is a compaction,
a possibly partial bucket may be returned.

## Return Value

Returns an array containing:

- **Timestamp** (integer): Unix timestamp in milliseconds of the sample.
- **Value** (float): The numeric value of the sample.

Returns an empty array if the time series is empty or does not exist.

## Examples

Get the last sample from a time series:

```
> TS.GET temperature:room1
1) (integer) 1609459200000
2) "22.5"
```

Get the latest compacted sample:

```
> TS.GET temperature:room1 LATEST
1) (integer) 1609459200000
2) "22.5"
```

Empty time series:

```
> TS.GET nonexistent:key
(empty array)
```

## Complexity

O(1) – Constant time access to the last sample.

## See Also

- [TS.RANGE](./range.md) – Query a range of samples
- [TS.MGET](./mget.md) – Get the latest sample from multiple series
- [TS.ADD](./add.md) – Add a sample to a time series
```