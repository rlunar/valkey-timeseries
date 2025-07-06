use crate::common::binary_search::*;
use crate::common::{Sample, Timestamp};
use crate::error::{TsdbError, TsdbResult};
use crate::series::types::ValueFilter;
use smallvec::SmallVec;

#[inline]
pub(crate) fn filter_samples_by_value(samples: &mut Vec<Sample>, value_filter: &ValueFilter) {
    samples.retain(|s| s.value >= value_filter.min && s.value <= value_filter.max)
}

/// Finds the start and end indices of timestamps within a specified range.
///
/// This function searches for the indices of timestamps that fall within the given
/// start and end timestamps (inclusive).
///
/// ## Parameters
///
/// * `timestamps`: A slice of i64 values representing timestamps, expected to be sorted.
/// * `start_ts`: The lower bound of the timestamp range to search for (inclusive).
/// * `end_ts`: The upper bound of the timestamp range to search for (inclusive).
///
/// ## Returns
///
/// Returns `Option<(usize, usize)>`:
/// * `Some((start_index, end_index))` if valid indices are found within the range.
/// * `None` if the input `timestamps` slice is empty.
///
/// The returned indices can be used to slice the original `timestamps` array
/// to get the subset of timestamps within the specified range.
pub(crate) fn get_timestamp_index_bounds(
    timestamps: &[i64],
    start_ts: Timestamp,
    end_ts: Timestamp,
) -> Option<(usize, usize)> {
    get_index_bounds(timestamps, &start_ts, &end_ts)
}

pub(crate) fn filter_timestamp_slice(
    ts_filter: &[Timestamp],
    start: Timestamp,
    end: Timestamp,
) -> SmallVec<Timestamp, 32> {
    let mut filtered: SmallVec<Timestamp, 32> = ts_filter
        .iter()
        .filter_map(|ts| {
            let ts = *ts;
            if ts >= start && ts <= end {
                Some(ts)
            } else {
                None
            }
        })
        .collect();

    filtered.sort();
    filtered.dedup();
    filtered
}

pub(crate) fn write_usize(slice: &mut Vec<u8>, size: usize) {
    slice.extend_from_slice(&size.to_le_bytes());
}

pub(crate) fn read_usize(input: &mut &[u8], field: &str) -> TsdbResult<usize> {
    let (int_bytes, rest) = input.split_at(size_of::<usize>());
    let buf = int_bytes.try_into().map_err(|_| {
        TsdbError::CannotDeserialize(format!("invalid usize reading {field}").to_string())
    })?;

    *input = rest;
    Ok(usize::from_le_bytes(buf))
}
