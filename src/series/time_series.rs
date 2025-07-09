use super::chunks::utils::{filter_samples_by_value, filter_timestamp_slice};
use super::{SampleAddResult, SampleDuplicatePolicy, TimeSeriesOptions, ValueFilter};
use crate::common::hash::IntMap;
use crate::common::parallel::join;
use crate::common::rounding::RoundingStrategy;
use crate::common::time::current_time_millis;
use crate::common::{Sample, Timestamp};
use crate::config::DEFAULT_CHUNK_SIZE_BYTES;
use crate::error::{TsdbError, TsdbResult};
use crate::labels::{InternedLabel, InternedMetricName};
use crate::series::chunks::{validate_chunk_size, Chunk, ChunkEncoding, TimeSeriesChunk};
use crate::series::index::next_timeseries_id;
use crate::series::DuplicatePolicy;
use crate::{config, error_consts};
use get_size::GetSize;
use smallvec::SmallVec;
use std::hash::Hash;
use std::mem::size_of;
use std::time::Duration;
use std::vec;
use valkey_module::logging;
use valkey_module::{ValkeyError, ValkeyResult};

pub type TimeseriesId = u64;
pub type SeriesRef = u64;

/// Represents a time series consisting of chunks of samples, each with a timestamp and value.
#[derive(Clone, Debug, PartialEq, GetSize)]
pub struct TimeSeries {
    /// fixed internal id used in indexing
    pub id: SeriesRef,
    /// The label/value pairs
    pub labels: InternedMetricName,
    /// How long data is kept before being removed
    pub retention: Duration,
    /// Policy for handling duplicate samples
    pub sample_duplicates: SampleDuplicatePolicy,
    /// The chunk compression algorithm used (Uncompressed, Gorilla, or Pco)
    pub chunk_compression: ChunkEncoding,
    /// Optional strategy for rounding values (either by significant or decimal digits)
    pub rounding: Option<RoundingStrategy>,
    /// Target size for chunks in bytes
    pub chunk_size_bytes: usize,
    /// The time series chunks
    pub chunks: Vec<TimeSeriesChunk>,
    // meta
    /// Total number of samples in the time series
    pub total_samples: usize,
    /// The first timestamp in the time series
    pub first_timestamp: Timestamp,
    /// The last timestamp in the time series
    pub last_sample: Option<Sample>,
    /// Internal bookkeeping for current db. Simplifies event handling related to indexing.
    /// This is not part of the time series data itself, not is it stored to rdb.
    pub(crate) _db: i32,
}

/// Hash based on the metric name, which should be unique in the db
impl Hash for TimeSeries {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.labels.hash(state);
    }
}

impl TimeSeries {
    /// Create a new empty time series.
    pub fn new() -> Self {
        TimeSeries::default()
    }

    pub fn with_options(options: TimeSeriesOptions) -> TsdbResult<Self> {
        let mut res = Self::new();
        if let Some(chunk_size) = options.chunk_size {
            validate_chunk_size(chunk_size)?;
            res.chunk_size_bytes = chunk_size;
        }

        res.chunk_compression = options.chunk_compression;
        res.retention = options.retention.unwrap_or_else(|| {
            let retention = config::RETENTION_PERIOD
                .lock()
                .expect("failed to lock RETENTION_PERIOD mutex");
            *retention
        });
        res.rounding = options.rounding;
        res.sample_duplicates = options.sample_duplicate_policy;

        // todo: make sure labels are sorted and dont contain __name__
        // if !options.labels.iter().any(|x| x.name == METRIC_NAME_LABEL) {
        //     return Err(TsdbError::InvalidMetric(
        //         "ERR missing metric name".to_string(),
        //     ));
        // }

        res.labels = InternedMetricName::new(&options.labels);
        res.id = next_timeseries_id();

        Ok(res)
    }

    pub fn len(&self) -> usize {
        self.total_samples
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the full metric name of the time series, including labels in Prometheus format.
    /// For example,
    ///
    /// `http_requests_total{method="POST", status="500"}`
    ///
    /// Note that for our internal purposes, we store the metric name and labels separately and
    /// assume that the labels are sorted by name.
    pub fn prometheus_metric_name(&self) -> String {
        self.labels.to_string()
    }

    pub fn label_value(&self, name: &str) -> Option<&str> {
        self.labels.get_value(name)
    }

    pub fn get_label(&self, name: &str) -> Option<InternedLabel<'_>> {
        self.labels.get_tag(name)
    }

    #[inline]
    fn adjust_value(&self, value: f64) -> f64 {
        self.rounding.as_ref().map_or(value, |r| r.round(value))
    }

    pub fn add(
        &mut self,
        ts: Timestamp,
        value: f64,
        dp_override: Option<DuplicatePolicy>,
    ) -> SampleAddResult {
        let sample = Sample {
            value: self.adjust_value(value),
            timestamp: ts,
        };

        if let Some(last) = self.last_sample {
            let last_ts = last.timestamp;
            if ts >= last_ts && !self.validate_sample(&sample, &last, dp_override).is_ok() {
                return SampleAddResult::Ignored(last_ts);
            }
            if ts <= last_ts {
                return self.upsert_sample(sample, dp_override);
            }
        }

        self.add_sample_internal(sample)
    }

    pub(crate) fn validate_sample(
        &self,
        sample: &Sample,
        last_sample: &Sample,
        on_duplicate: Option<DuplicatePolicy>,
    ) -> SampleAddResult {
        if self
            .sample_duplicates
            .is_duplicate(sample, last_sample, on_duplicate)
        {
            SampleAddResult::Ignored(last_sample.timestamp)
        } else {
            SampleAddResult::Ok(sample.timestamp)
        }
    }

    pub(super) fn add_sample_internal(&mut self, sample: Sample) -> SampleAddResult {
        let chunk = self.get_last_chunk();
        match chunk.add_sample(&sample) {
            Ok(_) => {
                self.update_after_sample_add(sample);
                SampleAddResult::Ok(sample.timestamp)
            }
            Err(TsdbError::CapacityFull(_)) => self.handle_full_chunk(sample),
            Err(_) => SampleAddResult::Error(error_consts::CANNOT_ADD_SAMPLE),
        }
    }

    /// (Possibly) add a new chunk and append the given sample.
    fn add_chunk_with_sample(&mut self, sample: Sample) -> TsdbResult<()> {
        let mut chunk = self.create_chunk();
        chunk.add_sample(&sample)?;
        self.chunks.push(chunk);
        // trim chunks to keep memory usage in check
        self.chunks.shrink_to_fit();
        self.update_after_sample_add(sample);

        Ok(())
    }

    fn append_chunk(&mut self) {
        let new_chunk = self.create_chunk();
        self.chunks.push(new_chunk);
    }

    fn create_chunk(&mut self) -> TimeSeriesChunk {
        TimeSeriesChunk::new(self.chunk_compression, self.chunk_size_bytes)
    }

    fn handle_full_chunk(&mut self, sample: Sample) -> SampleAddResult {
        match self.add_chunk_with_sample(sample) {
            Ok(_) => SampleAddResult::Ok(sample.timestamp),
            Err(TsdbError::DuplicateSample(_)) => SampleAddResult::Duplicate,
            Err(_) => SampleAddResult::Error(error_consts::CANNOT_ADD_SAMPLE),
        }
    }

    fn update_after_sample_add(&mut self, sample: Sample) {
        if self.is_empty() {
            self.first_timestamp = sample.timestamp;
        }
        self.last_sample = Some(sample);
        self.total_samples += 1;
    }

    #[inline]
    fn get_last_chunk(&mut self) -> &mut TimeSeriesChunk {
        if self.chunks.is_empty() {
            self.append_chunk();
        }
        self.chunks.last_mut().unwrap()
    }

    fn upsert_sample(
        &mut self,
        sample: Sample,
        duplicate_policy_override: Option<DuplicatePolicy>,
    ) -> SampleAddResult {
        let dp_policy = self
            .sample_duplicates
            .resolve_policy(duplicate_policy_override);
        let chunks_len = self.chunks.len();
        let (chunk, is_last) = if sample.timestamp <= self.first_timestamp {
            if self.is_older_than_retention(sample.timestamp) {
                return SampleAddResult::TooOld;
            }
            let chunk = self.chunks.get_mut(0).expect("chunks.is_empty() in upsert");
            (chunk, chunks_len == 1)
        } else {
            let (pos, _found) = get_chunk_index(&self.chunks, sample.timestamp);
            let chunk = self
                .chunks
                .get_mut(pos)
                .expect("index out of range in upsert");
            (chunk, pos + 1 == chunks_len)
        };

        // Try to upsert in the existing chunk if it doesn't need splitting
        if !chunk.should_split() {
            let old_size = chunk.len();
            let (size, res) = chunk.upsert(sample, dp_policy);
            if res.is_ok() {
                self.total_samples += size - old_size;
                if is_last {
                    self.update_last_sample();
                }
                self.first_timestamp = sample.timestamp.min(self.first_timestamp);
            }
            return res;
        }

        // Handle the case where we need to split the chunk
        match chunk.split() {
            Ok(mut new_chunk) => {
                let (size, res) = new_chunk.upsert(sample, dp_policy);
                if !res.is_ok() {
                    return res;
                }

                // Try to trim time series and log any errors
                // TODO: do this in a separate thread to avoid blocking ingestion
                if let Err(e) = self.trim() {
                    logging::log_warning(format!("Error trimming time series: {e:?}"));
                }

                // Insert the new chunk at the correct position
                let insert_at = self
                    .chunks
                    .partition_point(|c| c.first_timestamp() <= new_chunk.first_timestamp());
                self.chunks.insert(insert_at, new_chunk);

                self.total_samples += size;
                if is_last {
                    self.update_last_sample();
                }
                self.first_timestamp = sample.timestamp.min(self.first_timestamp);

                SampleAddResult::Ok(sample.timestamp)
            }
            Err(_) => SampleAddResult::Error(error_consts::CHUNK_SPLIT),
        }
    }

    /// Merges a collection of samples into the time series.
    ///
    /// This function efficiently groups samples by the chunks they would belong to
    /// and applies the appropriate duplicate policy when merging. If samples are split across
    /// multiple chunks, they are processed (mostly) in parallel to optimize performance.
    ///
    /// ## Arguments
    ///
    /// * `samples` - A slice of samples to merge into the time series
    /// * `policy_override` - Optional override for the duplicate policy to use when merging
    ///
    /// ## Returns
    ///
    /// A result containing a vector of `SampleAddResult` with the outcome for each sample.
    ///
    pub fn merge_samples(
        &mut self,
        samples: &[Sample],
        policy_override: Option<DuplicatePolicy>,
    ) -> TsdbResult<Vec<SampleAddResult>> {
        if samples.is_empty() {
            return Ok(Vec::new());
        }

        let policy = self.sample_duplicates.resolve_policy(policy_override);
        let earliest_allowed_timestamp = if self.retention.is_zero() {
            0
        } else {
            self.get_min_timestamp()
        };

        let mut results = vec![SampleAddResult::Error("Unknown error"); samples.len()];

        // Ensure there's at least one chunk to work with
        if self.chunks.is_empty() {
            self.append_chunk();
        }

        // Group samples by chunk. Map is chunk_idx -> Vec<(original_index, sample)>
        let chunk_groups =
            self.group_samples_by_chunk(samples, &mut results, earliest_allowed_timestamp);

        // The following is done this way because Vec::get_disjoint_mut() ony takes slices (not even slice refs),
        // so we need to process the groups in pairs, and then handle the last group separately if needed.
        let mut cursor = &chunk_groups[0..];
        let mut chunk_results: SmallVec<_, 8> = SmallVec::new();
        while !cursor.is_empty() {
            match cursor {
                [] => break, // No more groups to process
                [first] => {
                    // If there's only one group left, process it directly
                    let chunk = self
                        .chunks
                        .get_mut(first.chunk_index)
                        .expect("TimeSeries chunks should not be empty");
                    chunk_results.extend(first.handle_merge(chunk, first, policy));
                    break;
                }
                [first, second, rest @ ..] => {
                    // Process the first two groups in parallel
                    let [l, r] = self
                        .chunks
                        .get_disjoint_mut([first.chunk_index, second.chunk_index])
                        .expect("TimeSeries chunks should be disjoint");
                    let (left_results, right_results) = join(
                        || first.handle_merge(l, first, policy),
                        || second.handle_merge(r, second, policy),
                    );
                    chunk_results.extend(left_results);
                    chunk_results.extend(right_results);
                    cursor = rest;
                }
            }
        }

        // Map results back to original indices
        for (orig_idx, result) in chunk_results.into_iter() {
            results[orig_idx] = result;

            // Update metadata for successful additions
            if let SampleAddResult::Ok(ts) = result {
                // The first timestamp might need updating
                if ts < self.first_timestamp || self.is_empty() {
                    self.first_timestamp = ts;
                }

                // Update sample count
                self.total_samples += 1;
            }
        }

        // Update last_sample
        self.update_last_sample();

        Ok(results)
    }

    /// Groups samples by the chunk they belong to, while filtering out old samples.
    fn group_samples_by_chunk(
        &mut self,
        samples: &[Sample],
        results: &mut [SampleAddResult],
        earliest_allowed_timestamp: Timestamp,
    ) -> SmallVec<GroupedSamples, 8> {
        let mut chunk_groups: IntMap<usize, GroupedSamples> = IntMap::default();

        for (index, &sample) in samples.iter().enumerate() {
            if sample.timestamp < earliest_allowed_timestamp {
                results[index] = SampleAddResult::TooOld;
                continue;
            }

            let adjusted_sample = Sample {
                value: self.adjust_value(sample.value),
                timestamp: sample.timestamp,
            };

            let chunk_index = if self.is_empty() || sample.timestamp < self.first_timestamp {
                0
            } else {
                let index = find_last_ge_index(&self.chunks, sample.timestamp).0;
                // see if we need to split the chunk
                let chunk = &mut self.chunks[index];
                if chunk.should_split() {
                    // split the chunk and return the new index
                    let new_chunk = chunk.split().unwrap();
                    let in_new_chunk = new_chunk.is_timestamp_in_range(sample.timestamp);
                    self.chunks.insert(index + 1, new_chunk);
                    if in_new_chunk {
                        index + 1
                    } else {
                        index
                    }
                } else {
                    index
                }
            };
            //let chunk = &mut self.chunks[chunk_index];
            chunk_groups
                .entry(chunk_index)
                .or_insert_with(|| GroupedSamples::new(chunk_index))
                .add_sample(adjusted_sample, index);
        }

        for group in chunk_groups.values_mut() {
            group.sort_by_timestamp();
        }

        chunk_groups.into_values().collect::<SmallVec<_, 8>>()
    }

    /// Get the time series between given start and end time (both inclusive).
    pub fn get_range(&self, start_time: Timestamp, end_time: Timestamp) -> Vec<Sample> {
        if !self.overlaps(start_time, end_time) {
            return Vec::new();
        }
        let Some(range) = self.get_chunk_index_bounds(start_time, end_time) else {
            return Vec::new();
        };
        let (start_index, end_index) = range;
        let chunks = &self.chunks[start_index..=end_index];
        get_range_parallel(chunks, start_time, end_time).unwrap_or_default()
    }

    pub fn get_range_filtered(
        &self,
        start_timestamp: Timestamp,
        end_timestamp: Timestamp,
        timestamp_filter: Option<&[Timestamp]>,
        value_filter: Option<ValueFilter>,
    ) -> Vec<Sample> {
        debug_assert!(start_timestamp <= end_timestamp);

        // TODO: propagate errors
        let mut samples = if let Some(ts_filter) = timestamp_filter {
            let timestamps = filter_timestamp_slice(ts_filter, start_timestamp, end_timestamp);
            self.samples_by_timestamps(&timestamps)
                .unwrap_or_default()
                .into_iter()
                .collect()
        } else {
            self.get_range(start_timestamp, end_timestamp)
        };

        if let Some(value_filter) = value_filter {
            filter_samples_by_value(&mut samples, &value_filter);
        }

        samples
    }

    pub fn get_sample(&self, start_time: Timestamp) -> ValkeyResult<Option<Sample>> {
        let (index, found) = get_chunk_index(&self.chunks, start_time);
        if found {
            let chunk = &self.chunks[index];
            // todo: better error handling
            let mut samples = chunk
                .get_range(start_time, start_time)
                .map_err(|_e| ValkeyError::Str(error_consts::ERROR_FETCHING_SAMPLE))?;
            Ok(samples.pop())
        } else {
            Ok(None)
        }
    }

    pub fn samples_by_timestamps(&self, timestamps: &[Timestamp]) -> TsdbResult<Vec<Sample>> {
        if self.is_empty() || timestamps.is_empty() {
            return Ok(vec![]);
        }

        struct ChunkMeta<'a> {
            chunk: &'a TimeSeriesChunk,
            timestamps: SmallVec<Timestamp, 6>,
        }

        let mut meta_map: IntMap<usize, ChunkMeta> = Default::default();

        for &ts in timestamps {
            let (index, found) = get_chunk_index(&self.chunks, ts);
            if found && index < self.chunks.len() {
                meta_map
                    .entry(index)
                    .or_insert_with(|| ChunkMeta {
                        chunk: &self.chunks[index],
                        timestamps: SmallVec::new(),
                    })
                    .timestamps
                    .push(ts);
            }
        }

        fn meta_fetch(meta: &ChunkMeta) -> TsdbResult<Vec<Sample>> {
            meta.chunk.samples_by_timestamps(&meta.timestamps)
        }

        fn fetch_parallel(slice: &[ChunkMeta]) -> TsdbResult<Vec<Sample>> {
            match slice {
                [] => Ok(vec![]),
                [meta] => meta_fetch(meta),
                [first, second] => {
                    let (left_samples, right_samples) =
                        join(|| meta_fetch(first), || meta_fetch(second));
                    let mut samples = left_samples?;
                    samples.extend(right_samples?);
                    Ok(samples)
                }
                _ => {
                    let mid = slice.len() / 2;
                    let (left, right) = slice.split_at(mid);
                    let (left_samples, right_samples) =
                        join(|| fetch_parallel(left), || fetch_parallel(right));
                    let mut samples = left_samples?;
                    samples.extend(right_samples?);
                    Ok(samples)
                }
            }
        }

        let len = meta_map.len();
        if len == 0 {
            Ok(vec![])
        } else {
            let metas = meta_map.into_values().collect::<Vec<_>>();
            fetch_parallel(&metas)
        }
    }

    pub fn iter(&self) -> SeriesSampleIterator<'_> {
        SeriesSampleIterator::new(
            self,
            self.first_timestamp,
            self.last_timestamp(),
            &None,
            &None,
        )
    }

    pub fn range_iter(&self, start: Timestamp, end: Timestamp) -> SeriesSampleIterator<'_> {
        SeriesSampleIterator::new(self, start, end, &None, &None)
    }

    pub fn overlaps(&self, start_ts: Timestamp, end_ts: Timestamp) -> bool {
        self.last_timestamp() >= start_ts && self.first_timestamp <= end_ts
    }

    pub fn is_older_than_retention(&self, timestamp: Timestamp) -> bool {
        if self.retention.is_zero() {
            return false;
        }
        let min_ts = self.get_min_timestamp();
        timestamp < min_ts
    }

    pub(super) fn trim(&mut self) -> TsdbResult<usize> {
        let min_timestamp = self.get_min_timestamp();
        if self.first_timestamp == min_timestamp {
            return Ok(0);
        }

        let mut deleted_count = 0;

        // Remove entire chunks that are before min_timestamp
        self.chunks.retain(|chunk| {
            let last_ts = chunk.last_timestamp();
            if last_ts <= min_timestamp {
                deleted_count += chunk.len();
                false
            } else {
                true
            }
        });

        // Handle partial chunk
        if let Some(chunk) = self.chunks.first_mut() {
            if chunk.first_timestamp() < min_timestamp {
                if let Ok(count) = chunk.remove_range(0, min_timestamp) {
                    deleted_count += count;
                } else {
                    return Err(TsdbError::RemoveRangeError);
                }
            }
        }

        self.total_samples -= deleted_count;

        if deleted_count > 0 {
            // Update first_timestamp and last_timestamp
            self.update_first_last_timestamps();
        }

        self.chunks.shrink_to_fit();

        Ok(deleted_count)
    }

    pub fn remove_range(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<usize> {
        debug_assert!(start_ts <= end_ts);

        let mut deleted_samples = 0;
        if let Some((start_index, end_index)) = self.get_chunk_index_bounds(start_ts, end_ts) {
            let mut deleted_chunks = 0;

            for index in start_index..=end_index {
                // todo: get_unchecked
                let chunk = self
                    .chunks
                    .get_mut(index)
                    .expect("Range out of bounds iterating ts chunk");

                // Should we delete the entire chunk?
                if chunk.is_contained_by_range(start_ts, end_ts) {
                    deleted_samples += chunk.len();
                    chunk.clear();
                    deleted_chunks += 1;
                } else {
                    // handle partial deletion
                    deleted_samples += chunk.remove_range(start_ts, end_ts)?;
                    if chunk.is_empty() {
                        deleted_chunks += 1;
                    }
                }
            }

            // Remove empty chunks
            if deleted_chunks > 0 {
                // if we're deleting all chunks, save one so we can spare an allocation
                let mut saved_chunk: Option<TimeSeriesChunk> = None;
                if deleted_chunks == self.chunks.len() {
                    saved_chunk = self.chunks.pop();
                }
                self.chunks.retain(|chunk| !chunk.is_empty());

                if let Some(saved_chunk) = saved_chunk {
                    self.chunks.push(saved_chunk);
                }
            }

            // Update metadata
            self.total_samples -= deleted_samples;
            self.update_first_last_timestamps();
        }

        Ok(deleted_samples)
    }

    /// Checks if the time series has at least one sample in the given time range.
    ///
    /// # Arguments
    ///
    /// * `start_time` - Start timestamp (inclusive)
    /// * `end_time` - End timestamp (inclusive)
    ///
    /// # Returns
    ///
    /// `true` if at least one sample exists in the given range, `false` otherwise
    pub fn has_samples_in_range(&self, start_time: Timestamp, end_time: Timestamp) -> bool {
        // Check if the time series could possibly have samples in the range
        if !self.overlaps(start_time, end_time) || self.is_empty() {
            return false;
        }

        // Find the actual min timestamp accounting for retention
        let min_timestamp = self.get_min_timestamp().max(start_time);

        // Get chunk index bounds for the range
        if let Some((start_index, end_index)) = self.get_chunk_index_bounds(min_timestamp, end_time)
        {
            // Check if any chunk in the range has samples within the time range
            for index in start_index..=end_index {
                let chunk = &self.chunks[index];
                if chunk.has_samples_in_range(min_timestamp, end_time) {
                    return true;
                }
            }
        }

        false
    }

    pub fn increment_sample_value(
        &mut self,
        timestamp: Option<Timestamp>,
        delta: f64,
    ) -> ValkeyResult<SampleAddResult> {
        // if we have at least one sample, increment the last one
        let (timestamp, last_ts, value) = if let Some(sample) = self.last_sample {
            let last_ts = sample.timestamp;
            let ts = timestamp.unwrap_or(last_ts);
            let value = sample.value + delta;
            (ts, last_ts, value)
        } else {
            let ts = timestamp.unwrap_or_else(current_time_millis);
            (ts, ts, delta)
        };

        if timestamp < last_ts {
            return Err(ValkeyError::Str(
                "TSDB: timestamp must be equal to or higher than the maximum existing timestamp",
            ));
        }

        // todo: should we add a flag to skip adjust_value()?
        Ok(self.add(timestamp, value, Some(DuplicatePolicy::KeepLast)))
    }

    fn update_first_last_timestamps(&mut self) {
        if let Some(first_chunk) = self.chunks.first() {
            self.first_timestamp = first_chunk.first_timestamp();
        } else {
            self.first_timestamp = 0;
        }

        if let Some(last_chunk) = self.chunks.last() {
            self.last_sample = last_chunk.last_sample();
        } else {
            self.last_sample = None;
        }
    }

    pub fn data_size(&self) -> usize {
        self.chunks.iter().map(|x| x.size()).sum()
    }

    pub fn memory_usage(&self) -> usize {
        size_of::<Self>() + self.get_heap_size()
    }

    /// Returns the minimum timestamp of the time series, considering the retention period.
    pub(crate) fn get_min_timestamp(&self) -> Timestamp {
        if self.retention.is_zero() {
            self.first_timestamp
        } else {
            self.last_sample.map_or(0, |last| {
                last.timestamp
                    .saturating_sub(self.retention.as_millis() as i64)
                    .max(0)
            })
        }
    }

    fn update_last_sample(&mut self) {
        if let Some(last_chunk) = self.chunks.last() {
            self.last_sample = last_chunk.last_sample();
        } else {
            self.last_sample = None;
        }
    }

    pub(crate) fn last_timestamp(&self) -> Timestamp {
        if let Some(last_sample) = self.last_sample {
            last_sample.timestamp
        } else {
            0
        }
    }

    /// Finds the start and end chunk indices (inclusive) for a date range.
    ///
    /// # Parameters
    ///
    /// * `start`: The lower bound of the range to search for.
    /// * `end`: The upper bound of the range to search for.
    ///
    /// # Returns
    ///
    /// Returns `Option<(usize, usize)>`:
    /// * `Some((start_idx, end_idx))` if valid indices are found within the range.
    /// * `None` if the series is empty, if all samples are less than `start`,
    ///   or if `start` and `end` are equal and greater than the sample at the found index.
    ///
    /// Used to get an inclusive bound for series chunks (all chunks containing samples in the range [start_index...=end_index])
    pub(crate) fn get_chunk_index_bounds(
        &self,
        start: Timestamp,
        end: Timestamp,
    ) -> Option<(usize, usize)> {
        if self.is_empty() {
            return None;
        }

        let len = self.chunks.len();

        let start_idx = find_start_chunk_index(&self.chunks, start);
        if start_idx >= len {
            return None;
        }

        let right = &self.chunks[start_idx..];
        let (idx, _found) = find_last_ge_index(right, end);
        let end_idx = start_idx + idx;

        // imagine this scenario:
        // chunk start timestamps = [10, 20, 30, 40]
        // start = 25, end = 25
        // we have a situation where start_index == end_index (2), yet samples[2] is greater than end,
        if start_idx == end_idx {
            // todo: get_unchecked
            if self.chunks[start_idx].first_timestamp() > end {
                return None;
            }
        }

        Some((start_idx, end_idx))
    }

    pub fn optimize(&mut self) {
        fn optimize_internal(chunks: &mut [TimeSeriesChunk]) {
            match chunks {
                [] => {}
                [chunk] => {
                    let _ = chunk.optimize();
                }
                [first, second] => {
                    let _ = join(|| first.optimize(), || second.optimize());
                }
                _ => {
                    let mid = chunks.len() / 2;
                    let (left, right) = chunks.split_at_mut(mid);
                    let _ = join(|| optimize_internal(left), || optimize_internal(right));
                }
            }
        }

        // todo: merge chunks if possible
        // trim
        optimize_internal(&mut self.chunks)
    }

    #[cfg(test)]
    pub(super) fn update_state_from_chunks(&mut self) {
        self.update_first_last_timestamps();
        self.total_samples = self.chunks.iter().map(|x| x.len()).sum();
    }
}

impl Default for TimeSeries {
    fn default() -> Self {
        Self {
            id: 0,
            labels: Default::default(),
            retention: Default::default(),
            sample_duplicates: Default::default(),
            chunk_compression: Default::default(),
            chunk_size_bytes: DEFAULT_CHUNK_SIZE_BYTES,
            chunks: vec![],
            total_samples: 0,
            first_timestamp: 0,
            rounding: None,
            last_sample: None,
            _db: 0,
        }
    }
}

/// Represents a collection of samples grouped by chunk index.
struct GroupedSamples {
    chunk_index: usize,
    samples: SmallVec<Sample, 8>,
    indices: SmallVec<usize, 8>,
}

impl GroupedSamples {
    fn new(chunk_index: usize) -> Self {
        Self {
            chunk_index,
            samples: SmallVec::new(),
            indices: SmallVec::new(),
        }
    }

    /// Sorts samples by timestamp in ascending order, maintaining the relationship with their original indices.
    fn sort_by_timestamp(&mut self) {
        // Create a vector of (index, sample) pairs
        let mut pairs: SmallVec<(usize, Sample), 8> = self
            .indices
            .iter()
            .copied()
            .zip(self.samples.iter().copied())
            .collect();

        // Sort the pairs by sample timestamp
        pairs.sort_by_key(|(_idx, sample)| sample.timestamp);

        // Clear the existing SmallVecs
        self.indices.clear();
        self.samples.clear();

        // Push sorted elements back into the SmallVecs
        for (idx, sample) in pairs {
            self.indices.push(idx);
            self.samples.push(sample);
        }
    }

    fn add_sample(&mut self, sample: Sample, index: usize) {
        self.samples.push(sample);
        self.indices.push(index);
    }

    fn handle_merge(
        &self,
        chunk: &mut TimeSeriesChunk,
        group: &GroupedSamples,
        policy: DuplicatePolicy,
    ) -> SmallVec<(usize, SampleAddResult), 8> {
        // Merge samples into this chunk
        match chunk.merge_samples(&group.samples, Some(policy)) {
            Ok(chunk_results) => chunk_results
                .iter()
                .zip(group.indices.iter().cloned())
                .map(|(res, index)| (index, *res))
                .collect::<SmallVec<_, 8>>(),
            Err(_e) => {
                let err = SampleAddResult::Error(error_consts::CANNOT_ADD_SAMPLE);
                group
                    .indices
                    .iter()
                    .cloned()
                    .map(|index| (index, err))
                    .collect::<SmallVec<_, 8>>()
            }
        }
    }
}

fn binary_search_chunks_by_timestamp(chunks: &[TimeSeriesChunk], ts: Timestamp) -> (usize, bool) {
    match chunks.binary_search_by(|probe| {
        if ts < probe.first_timestamp() {
            std::cmp::Ordering::Greater
        } else if ts > probe.last_timestamp() {
            std::cmp::Ordering::Less
        } else {
            std::cmp::Ordering::Equal
        }
    }) {
        Ok(pos) => (pos, true),
        Err(pos) => (pos, false),
    }
}

/// Find the index of the first chunk in which the timestamp belongs. Assumes !chunks.is_empty()
pub(super) fn find_start_chunk_index(arr: &[TimeSeriesChunk], ts: Timestamp) -> usize {
    if arr.is_empty() {
        // If the vector is empty, return the first index.
        return 0;
    }
    if ts <= arr[0].first_timestamp() {
        // If the timestamp is less than the first chunk's start timestamp, return the first index.
        return 0;
    }
    if arr.len() <= 16 {
        // If the vectors are small, perform a linear search.
        return arr
            .iter()
            .position(|x| ts >= x.first_timestamp())
            .unwrap_or(arr.len());
    }
    let (pos, _) = binary_search_chunks_by_timestamp(arr, ts);
    pos
}

/// Return the index of the chunk in which the timestamp belongs. Assumes !chunks.is_empty()
fn get_chunk_index(chunks: &[TimeSeriesChunk], timestamp: Timestamp) -> (usize, bool) {
    if chunks.len() <= 16 {
        return chunks
            .iter()
            .enumerate()
            .find_map(|(i, chunk)| {
                if chunk.is_timestamp_in_range(timestamp) {
                    Some((i, true))
                } else {
                    None
                }
            })
            .unwrap_or((chunks.len(), false));
    }

    binary_search_chunks_by_timestamp(chunks, timestamp)
}

fn find_last_ge_index(chunks: &[TimeSeriesChunk], ts: Timestamp) -> (usize, bool) {
    if chunks.len() <= 16 {
        return chunks
            .iter()
            .rposition(|x| ts >= x.first_timestamp())
            .map_or((0, false), |idx| {
                let chunk = &chunks[idx];
                if chunk.is_timestamp_in_range(ts) {
                    (idx, true)
                } else {
                    (idx.saturating_sub(1), false)
                }
            });
    }
    binary_search_chunks_by_timestamp(chunks, ts)
}

pub struct SeriesSampleIterator<'a> {
    value_filter: &'a Option<ValueFilter>,
    ts_filter: &'a Option<Vec<Timestamp>>, // box instead
    chunk_iter: std::slice::Iter<'a, TimeSeriesChunk>,
    sample_iter: vec::IntoIter<Sample>,
    chunk: Option<&'a TimeSeriesChunk>,
    is_init: bool,
    pub(crate) start: Timestamp,
    pub(crate) end: Timestamp,
}

impl<'a> SeriesSampleIterator<'a> {
    pub(crate) fn new(
        series: &'a TimeSeries,
        start: Timestamp,
        end: Timestamp,
        value_filter: &'a Option<ValueFilter>,
        ts_filter: &'a Option<Vec<Timestamp>>,
    ) -> Self {
        let chunk_index = find_start_chunk_index(&series.chunks, start);

        let chunk_iter = if chunk_index < series.chunks.len() {
            series.chunks[chunk_index..].iter()
        } else {
            Default::default()
        };

        Self {
            start,
            end,
            value_filter,
            ts_filter,
            chunk_iter,
            sample_iter: Default::default(),
            chunk: None,
            is_init: false,
        }
    }

    fn get_iter(&mut self, start: Timestamp, end: Timestamp) -> vec::IntoIter<Sample> {
        self.is_init = true;
        self.chunk = self.chunk_iter.next();
        match self.chunk {
            Some(chunk) => {
                let samples =
                    chunk.get_range_filtered(start, end, self.ts_filter, self.value_filter);
                self.start = chunk.last_timestamp();
                samples.into_iter()
            }
            None => Default::default(),
        }
    }
}

// todo: implement next_chunk
impl Iterator for SeriesSampleIterator<'_> {
    type Item = Sample;
    fn next(&mut self) -> Option<Self::Item> {
        if !self.is_init {
            self.sample_iter = self.get_iter(self.start, self.end);
        }
        if let Some(sample) = self.sample_iter.next() {
            Some(sample)
        } else {
            self.sample_iter = self.get_iter(self.start, self.end);
            self.sample_iter.next()
        }
    }
}

fn get_range_parallel(
    chunks: &[TimeSeriesChunk],
    start: Timestamp,
    end: Timestamp,
) -> TsdbResult<Vec<Sample>> {
    match chunks {
        [] => Ok(vec![]),
        [chunk] => chunk.get_range(start, end),
        [first, second] => {
            let (left_samples, right_samples) = join(
                || first.get_range(start, end),
                || second.get_range(start, end),
            );
            let mut samples = left_samples?;
            samples.extend(right_samples?);
            Ok(samples)
        }
        _ => {
            let mid = chunks.len() / 2;
            let (left, right) = chunks.split_at(mid);
            let (left_samples, right_samples) = join(
                || get_range_parallel(left, start, end),
                || get_range_parallel(right, start, end),
            );
            let mut samples = left_samples?;
            samples.extend(right_samples?);
            Ok(samples)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_one_entry() {
        let mut ts = TimeSeries::new();
        assert!(ts.add(100, 200.0, None).is_ok());

        assert_eq!(ts.get_last_chunk().len(), 1);
        let last_block = ts.get_last_chunk();
        let samples = last_block.get_range(0, 1000).unwrap();

        let data_point = samples.first().unwrap();
        assert_eq!(data_point.timestamp, 100);
        assert_eq!(data_point.value, 200.0);
        assert_eq!(ts.total_samples, 1);
        assert_eq!(ts.first_timestamp, 100);
        assert_eq!(ts.last_timestamp(), 100);
    }
}
