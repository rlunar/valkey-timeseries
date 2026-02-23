### TS.DEL

#### Syntax

```
TS.DEL key fromTimestamp toTimestamp
```

**TS.DEL** deletes data for a selection of series in a time range.

### Required Arguments

- **key**: the key being deleted from.
- **fromTimestamp**: Start timestamp, inclusive. Optional.
- **toTimestamp**: End timestamp, inclusive. Optional.

#### Return

- the number of samples deleted.

#### Error

Return an error reply in the following cases:

TODO

#### Examples

```
TS.DEL requests:status:200 587396550 1587396550
```