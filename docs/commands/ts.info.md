# TS.INFO

Returns information and statistics about a time series.

```
TS.INFO key [DEBUG]
```


## Arguments

- **key**: The time series key to retrieve information for.
- **DEBUG** (optional): When provided, includes additional debugging information such as chunk details.

## Return Value

Returns a map containing the following fields:

| Field                 | Type           | Description                                                  |
|-----------------------|----------------|--------------------------------------------------------------|
| `metric`              | string         | Prometheus metric name derived from the key and labels       |
| `totalSamples`        | integer        | Total number of samples in the time series                   |
| `memoryUsage`         | integer        | Approximate memory usage in bytes                            |
| `firstTimestamp`      | integer        | Timestamp of the first sample                                |
| `lastTimestamp`       | integer        | Timestamp of the most recent sample                          |
| `retentionTime`       | integer        | Retention period in milliseconds                             |
| `chunkCount`          | integer        | Number of chunks storing the time series data                |
| `chunkSize`           | integer        | Size of each chunk in bytes                                  |
| `chunkType`           | string         | Either `"compressed"` or `"uncompressed"`                    |
| `duplicatePolicy`     | string \| null | Policy for handling duplicate timestamps, or null if not set |
| `labels`              | array \| null  | Array of label key-value pairs, or null if no labels         |
| `sourceKey`           | string         | Key of the source time series (for downsampled series)       |
| `rules`               | array          | Compaction rules applied to this series                      |
| `ignoreMaxTimeDiff`   | integer        | Maximum time difference for duplicate handling               |
| `ignoreMaxValDiff`    | float          | Maximum value difference for duplicate handling              |
| `rounding`            | array          | Rounding strategy and precision, if configured               |
| `Chunks` (debug only) | array          | Detailed information about each chunk                        |

## Examples

```
TS.INFO myts
TS.INFO myts DEBUG
```

## Permissions

Requires `ACCESS` permission on the time series key.

## See Also

- `TS.CREATE` — Create a new time series
- `TS.ADD` — Add samples to a time series
- `TS.QUERY` — Query time series data