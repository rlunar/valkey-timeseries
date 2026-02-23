# TS.MREVRANGE

Reverse range query for multiple time series.

```
TS.MREVRANGE fromTimestamp toTimestamp
    [LATEST]
    [FILTER_BY_TS ts...]
    [FILTER_BY_VALUE min max]
    [WITHLABELS | SELECTED_LABELS label...]
    [COUNT count]
    [[ALIGN align] AGGREGATION aggregator bucketDuration [CONDITION op value] [BUCKETTIMESTAMP bt] [EMPTY]]
    [GROUPBY label REDUCE reducer]
    FILTER selector...
```