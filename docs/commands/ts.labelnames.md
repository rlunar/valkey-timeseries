## TS.LABELNAMES

returns a list of label names for select series.

```
TS.LABELNAMES [START fromTimestamp] [END toTimestamp] FILTER selector... 
```

### Required Arguments
<details open><summary><code>fromTimestamp</code></summary>
Repeated series selector argument that selects the series to return. At least one selector argument must be provided..
</details>

### Optional Arguments
<details open><summary><code>fromTimestamp</code></summary>
If specified along with `toTimestamp`, this limits the result to only labels from series which
have data in the date range [`fromTimestamp` .. `toTimestamp`]
</details>

<details open><summary><code>toTimestamp</code></summary>
If specified along with `fromTimestamp`, this limits the result to only labels from series which
have data in the date range [`fromTimestamp` .. `toTimestamp`]
</details>

#### Return

The data section of the JSON response is a list of string label names.

#### Error

Return an error reply in the following cases:

- Invalid options.
- TODO

#### Examples

```
TS.LABELS FILTER up process_start_time_seconds{job="prometheus"}
```
