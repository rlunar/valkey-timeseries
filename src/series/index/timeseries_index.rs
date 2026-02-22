use ahash::AHashMap;
use std::collections::BTreeSet;
use std::hash::BuildHasher;
use std::sync::{RwLock, RwLockReadGuard};

use super::posting_stats::{PostingStat, PostingsStats, StatsMaxHeap};
use super::postings::{Postings, PostingsBitmap};
use crate::common::constants::METRIC_NAME_LABEL;
use crate::common::context::is_real_user_client;
use crate::common::hash::DeterministicHasher;
use crate::error_consts;
use crate::labels::filters::SeriesSelector;
use crate::labels::{Label, SeriesLabel};
use crate::series::acl::{clone_permissions, has_all_keys_permissions};
use crate::series::index::IndexKey;
use crate::series::{SeriesRef, TimeSeries};
use blart::AsBytes;
use croaring::Bitmap64;
use std::mem::size_of;
use std::ops::{ControlFlow, Deref, DerefMut};
use valkey_module::{AclPermissions, Context, ValkeyError, ValkeyResult, ValkeyString};

/// A read-only guard for accessing Postings data.
/// This provides a safe, ergonomic way to read from Postings without allowing modification.
pub struct PostingsReadGuard<'a> {
    guard: RwLockReadGuard<'a, Postings>,
}

impl<'a> PostingsReadGuard<'a> {
    /// Creates a new PostingsReadGuard from an RwLockReadGuard
    pub(crate) fn new(guard: RwLockReadGuard<'a, Postings>) -> Self {
        Self { guard }
    }
}

impl<'a> Deref for PostingsReadGuard<'a> {
    type Target = Postings;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

pub struct PostingsWriteGuard<'a> {
    guard: std::sync::RwLockWriteGuard<'a, Postings>,
}

impl<'a> PostingsWriteGuard<'a> {
    fn new(guard: std::sync::RwLockWriteGuard<'a, Postings>) -> Self {
        Self { guard }
    }
}

impl<'a> Deref for PostingsWriteGuard<'a> {
    type Target = Postings;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<'a> DerefMut for PostingsWriteGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

pub struct TimeSeriesIndex {
    pub(crate) inner: RwLock<Postings>,
}

impl Default for TimeSeriesIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for TimeSeriesIndex {
    fn clone(&self) -> Self {
        let inner = self.inner.read().unwrap();
        let new_inner = inner.clone();
        TimeSeriesIndex {
            inner: RwLock::new(new_inner),
        }
    }
}
impl TimeSeriesIndex {
    pub fn new() -> Self {
        TimeSeriesIndex {
            inner: RwLock::new(Postings::default()),
        }
    }

    #[allow(dead_code)]
    pub fn clear(&self) {
        let mut inner = self.inner.write().unwrap();
        inner.clear();
    }

    // swap the inner value with some other value
    // this is specifically to handle the `swapdb` event callback
    pub fn swap(&self, other: &Self) {
        let mut self_inner = self.inner.write().unwrap();
        let mut other_inner = other.inner.write().unwrap();
        self_inner.swap(&mut other_inner);
    }

    pub fn index_timeseries(&self, ts: &TimeSeries, key: &[u8]) {
        debug_assert!(ts.id != 0);
        let mut inner = self.inner.write().unwrap();
        inner.index_timeseries(ts, key);
    }

    pub fn reindex_timeseries(&self, series: &TimeSeries, key: &[u8]) {
        let mut inner = self.inner.write().unwrap();
        inner.remove_timeseries(series);
        inner.index_timeseries(series, key);
    }

    pub fn remove_timeseries(&self, series: &TimeSeries) {
        let mut inner = self.inner.write().unwrap();
        inner.remove_timeseries(series);
    }

    pub fn has_id(&self, id: SeriesRef) -> bool {
        let inner = self.inner.read().unwrap();
        inner.has_id(id)
    }

    pub fn get_postings(&'_ self) -> PostingsReadGuard<'_> {
        let guard = self.inner.read().unwrap();
        PostingsReadGuard::new(guard)
    }

    pub fn get_postings_mut(&'_ self) -> PostingsWriteGuard<'_> {
        let guard = self.inner.write().unwrap();
        PostingsWriteGuard::new(guard)
    }

    /// Return all series ids corresponding to the given label value pairs
    pub fn postings_by_labels<T: SeriesLabel>(&self, labels: &[T]) -> PostingsBitmap {
        let inner = self.inner.read().unwrap();
        inner.postings_by_labels(labels)
    }

    /// This exists primarily to ensure that we disallow duplicate metric names
    pub fn posting_by_labels(&self, labels: &[Label]) -> ValkeyResult<Option<SeriesRef>> {
        let acc = self.postings_by_labels(labels);
        match acc.cardinality() {
            0 => Ok(None),
            1 => Ok(Some(acc.iter().next().expect("cardinality should be 1"))),
            _ => Err(ValkeyError::Str(error_consts::DUPLICATE_SERIES)),
        }
    }

    /// Retrieves the series identifier (ID) corresponding to a specific set of labels.
    ///
    /// This method looks up the series ID in the underlying postings index based on the provided
    /// labels. For instance, if we have a time series with the metric name
    ///
    /// `http_requests_total{status="200", method="GET", service="inference"}`,
    ///
    /// we can retrieve its series ID by passing the appropriate labels to this function.
    ///
    /// ```
    /// let labels = vec![
    ///     Label::new("__name__", "http_requests_total"),
    ///     Label::new("status", "200"),
    ///     Label::new("method", "GET"),
    ///     Label::new("service", "inference")
    /// ];
    /// if let Some(series_id) = index.series_id_by_labels(&labels) {
    ///     println!("Found series ID: {:?}", series_id);
    /// } else {
    ///    println!("No series found with the given labels.");
    /// }
    /// ```
    /// # Arguments
    ///
    /// * `labels` - A slice of `Label` objects that describe the series to look up.
    ///
    /// # Returns
    ///
    /// * `Option<SeriesRef>` - Returns `Some(SeriesRef)` if a matching series ID is found.
    ///
    pub fn series_id_by_labels(&self, labels: &[Label]) -> Option<SeriesRef> {
        let inner = self.inner.read().unwrap();
        inner.posting_id_by_labels(labels)
    }

    /// `postings_for_filters` assembles a single postings iterator against the series index
    /// based on the given matchers.
    #[allow(dead_code)]
    pub fn postings_for_selector(&self, selector: &SeriesSelector) -> ValkeyResult<PostingsBitmap> {
        let mut state = ();
        self.with_postings(&mut state, move |inner, _| {
            let postings = inner.postings_for_selector(selector)?;
            let res = postings.into_owned();
            Ok(res)
        })
    }

    pub fn get_label_names(&self) -> BTreeSet<String> {
        let inner = self.inner.read().unwrap();
        inner.get_label_names()
    }

    pub fn get_label_values(&self, label_name: &str) -> Vec<String> {
        let inner = self.inner.read().unwrap();
        inner.get_label_values(label_name)
    }

    /// Returns the series keys that match the given selectors.
    /// If `acl_permissions` is provided, it checks if the current user has the required permissions
    /// to access all the keys.
    ///
    /// ## Note
    /// If the user does not have permission to access all keys, an error is returned.
    /// Non-user clients (e.g., AOF client) bypass permission checks.
    pub fn keys_for_selectors(
        &self,
        ctx: &Context,
        filters: &[SeriesSelector],
        acl_permissions: Option<AclPermissions>,
    ) -> ValkeyResult<Vec<ValkeyString>> {
        let mut keys: Vec<ValkeyString> = Vec::new();
        let mut missing_keys: Vec<SeriesRef> = Vec::new();

        let postings = self
            .inner
            .read()
            .expect("keys_for_selector - TimeSeries lock poisoned");

        // get keys from ids
        let ids = postings.postings_for_selectors(filters)?;

        let mut expected_count = ids.cardinality() as usize;
        if expected_count == 0 {
            return Ok(Vec::new());
        }

        keys.reserve(expected_count);

        let current_user = ctx.get_current_user();
        let is_user_client = is_real_user_client(ctx);

        let cloned_perms = acl_permissions.as_ref().map(clone_permissions);
        let can_access_all_keys = has_all_keys_permissions(ctx, &current_user, acl_permissions);

        for series_ref in ids.iter() {
            let key = postings.get_key_by_id(series_ref);
            match key {
                Some(key) => {
                    let real_key = ctx.create_string(key.as_ref());
                    if is_user_client
                        && !can_access_all_keys
                        && let Some(perms) = &cloned_perms
                    {
                        // check if the user has permission for this key
                        if ctx
                            .acl_check_key_permission(&current_user, &real_key, perms)
                            .is_err()
                        {
                            break;
                        }
                    }
                    keys.push(real_key);
                }
                None => {
                    // this should not happen, but in case it does, we log an error and continue
                    missing_keys.push(series_ref);
                }
            }
        }

        expected_count -= missing_keys.len();

        if keys.len() != expected_count {
            // User does not have permission to read some keys, or some keys are missing
            // Customize the error message accordingly
            match cloned_perms {
                Some(perms) => {
                    if perms.contains(AclPermissions::DELETE) {
                        return Err(ValkeyError::Str(
                            error_consts::ALL_KEYS_WRITE_PERMISSION_ERROR,
                        ));
                    }
                    if perms.contains(AclPermissions::UPDATE) {
                        return Err(ValkeyError::Str(
                            error_consts::ALL_KEYS_WRITE_PERMISSION_ERROR,
                        ));
                    }
                    return Err(ValkeyError::Str(
                        error_consts::ALL_KEYS_READ_PERMISSION_ERROR,
                    ));
                }
                None => {
                    // todo: fix the problem here, for now we just log a warning
                    ctx.log_warning("Index consistency: some keys are missing from the index.");
                }
            }
        }

        if !missing_keys.is_empty() {
            let msg = format!(
                "Index consistency: {} keys are missing from the index.",
                missing_keys.len()
            );
            ctx.log_warning(&msg);

            let mut postings = self
                .inner
                .write()
                .expect("keys_for_selector - TimeSeries lock poisoned");

            for missing_id in missing_keys {
                postings.mark_id_as_stale(missing_id);
            }
        }

        Ok(keys)
    }

    pub fn get_cardinality_by_selectors(
        &self,
        selectors: &[SeriesSelector],
    ) -> ValkeyResult<usize> {
        if selectors.is_empty() {
            return Ok(0);
        }

        let mut state = ();

        self.with_postings(&mut state, move |inner, _state| {
            let filter = &selectors[0];
            let first = inner.postings_for_selector(filter)?;
            if selectors.len() == 1 {
                return Ok(first.cardinality() as usize);
            }
            let mut result = first.into_owned();
            for selector in &selectors[1..] {
                let postings = inner.postings_for_selector(selector)?;
                result.and_inplace(&postings);
            }

            Ok(result.cardinality() as usize)
        })
    }

    pub fn stats(&self, label: &str, limit: usize) -> PostingsStats {
        let mut per_label_counts: AHashMap<String, u64> = AHashMap::new();

        let mut metric_name_counts = StatsMaxHeap::new(limit);
        let mut label_name_counts = StatsMaxHeap::new(limit);
        let mut label_value_pair_counts = StatsMaxHeap::new(limit);
        let mut focus_label_value_counts = StatsMaxHeap::new(limit);

        let series_count = {
            let inner = self.inner.read().unwrap();
            inner.count() as u64
        };

        let mut total_label_value_pairs = 0usize;

        // Normalize empty label to the metric-name label once.
        let focus_label = if label.is_empty() {
            METRIC_NAME_LABEL
        } else {
            label
        };

        const BATCH_SIZE: usize = 512;
        let mut iterator = BatchIterator::new(self, BATCH_SIZE);

        while !iterator.is_complete() {
            iterator.next_batch(|key, _, count| {
                if let Some((name, value)) = key.split() {
                    match per_label_counts.get_mut(name) {
                        Some(existing_count) => {
                            *existing_count = existing_count.saturating_add(count);
                        }
                        _ => {
                            let _ = per_label_counts.insert(name.to_string(), count);
                        }
                    }

                    total_label_value_pairs += 1;

                    let pair = format!("{name}={value}");
                    label_value_pair_counts.push(PostingStat { name: pair, count });

                    if name == METRIC_NAME_LABEL {
                        metric_name_counts.push(PostingStat {
                            name: value.to_string(),
                            count,
                        });
                    }

                    if name == focus_label {
                        focus_label_value_counts.push(PostingStat {
                            name: value.to_string(),
                            count,
                        });
                    }
                }
                ControlFlow::Continue(())
            })
        }

        let label_count = per_label_counts.len();

        let series_count_by_focus_label_value = Some(focus_label_value_counts.into_vec());

        for (name, count) in per_label_counts {
            label_name_counts.push(PostingStat {
                name: name.to_string(),
                count,
            });
        }

        PostingsStats {
            series_count_by_metric_name: metric_name_counts.into_vec(),
            series_count_by_label_name: label_name_counts.into_vec(),
            series_count_by_label_value_pairs: label_value_pair_counts.into_vec(),
            series_count_by_focus_label_value,
            total_label_value_pairs,
            label_count,
            series_count,
        }
    }

    /// Returns two bitmaps: one for label names and one for label name=value pairs.
    /// The bitmaps are constructed by hashing the label names and label name=value pairs using a deterministic hasher.
    ///
    /// The use-case for this function is to efficiently determine which label names and label value pairs are present in the index,
    /// especially in the case of cross-shard queries in a clustered environment, where we want to minimize the amount of data
    /// transferred between nodes.
    ///
    /// A practical example is summing the total number of unique key=value pairs across the cluster. A naive first attempt
    /// would be to simply count the total number of key=value pairs on each node and sum them up, but this would lead to
    /// double-counting pairs that exist on multiple nodes.
    ///
    /// We instead can use the label-value pair bitmap to get a unique fingerprint of all the pairs on each node, and then
    /// merge these bitmaps across nodes using a bitwise OR operation. The cardinality of the resulting bitmap will give us
    /// the total number of unique key=value pairs across the cluster without double-counting.
    pub fn get_label_bitmaps(&self) -> (Bitmap64, Bitmap64) {
        const BATCH_SIZE: usize = 512;

        let mut label_names_bitmap = Bitmap64::default();
        let mut label_value_pairs_bitmap = Bitmap64::default();
        let hasher = DeterministicHasher::default();

        let mut iterator = BatchIterator::new(self, BATCH_SIZE);

        while !iterator.is_complete() {
            iterator.next_batch(|key, _bitmap, _| {
                if let Some((name, _value)) = key.split() {
                    let name_hash = hasher.hash_one(name);
                    label_names_bitmap.add(name_hash);

                    let key_hash = hasher.hash_one(key);
                    label_value_pairs_bitmap.add(key_hash);
                }
                ControlFlow::Continue(())
            });
        }

        (label_names_bitmap, label_value_pairs_bitmap)
    }

    pub fn count(&self) -> usize {
        let inner = self.inner.read().unwrap();
        inner.count()
    }

    #[allow(dead_code)]
    pub fn label_count(&self) -> usize {
        let inner = self.inner.read().unwrap();
        inner.label_index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    pub fn with_postings<F, R, STATE>(&self, state: &mut STATE, f: F) -> R
    where
        F: FnOnce(&Postings, &mut STATE) -> R,
    {
        let inner = self.inner.read().unwrap();
        f(&inner, state)
    }

    pub fn with_postings_mut<F, R, STATE>(&self, state: &mut STATE, f: F) -> R
    where
        F: FnOnce(&mut Postings, &mut STATE) -> R,
    {
        let mut inner = self.inner.write().unwrap();
        f(&mut inner, state)
    }

    pub fn mark_id_as_stale(&self, id: SeriesRef) {
        let mut inner = self.inner.write().unwrap();
        inner.mark_id_as_stale(id);
    }

    pub fn remove_stale_ids(&self) -> usize {
        const BATCH_SIZE: usize = 100;

        let mut inner = self.inner.write().expect("TimeSeries lock poisoned");
        let old_count = inner.stale_ids.cardinality();
        if old_count == 0 {
            return 0; // No stale IDs to remove
        }

        let mut cursor = None;
        while let Some(new_cursor) = inner.remove_stale_ids(cursor, BATCH_SIZE) {
            // Continue removing stale IDs in batches
            cursor = Some(new_cursor);
        }

        (old_count - inner.stale_ids.cardinality()) as usize // Return number of removed IDs
    }

    pub fn optimize_incremental(
        &self,
        start_prefix: Option<IndexKey>,
        count: usize,
    ) -> Option<IndexKey> {
        let mut inner = self.inner.write().unwrap();
        inner.optimize_postings(start_prefix, count)
    }
}

/// Helper struct for batch iteration over the label index
struct BatchIterator<'a> {
    index: &'a TimeSeriesIndex,
    cursor: Option<IndexKey>,
    batch_size: usize,
    is_finished: bool,
}

impl<'a> BatchIterator<'a> {
    fn new(index: &'a TimeSeriesIndex, batch_size: usize) -> Self {
        Self {
            index,
            cursor: None,
            batch_size,
            is_finished: false,
        }
    }

    #[inline]
    fn adjusted_cardinality(inner: &Postings, bitmap: &PostingsBitmap, has_stale_ids: bool) -> u64 {
        let mut count = bitmap.cardinality();
        if has_stale_ids {
            let stale_count = inner.stale_ids.and_cardinality(bitmap);
            if stale_count > 0 {
                count = count.saturating_sub(stale_count);
            }
        }
        count
    }

    fn next_batch<F>(&mut self, mut processor: F)
    where
        F: FnMut(&IndexKey, &PostingsBitmap, u64) -> ControlFlow<()>,
    {
        if self.is_finished {
            return;
        }

        let mut processed_in_batch = 0usize;

        let cursor_bytes = self.cursor.as_ref().map(|k| k.as_bytes());
        let owned_prefix: Vec<u8>;
        let prefix_bytes: &[u8] = match cursor_bytes {
            Some(bytes) => {
                owned_prefix = bytes.to_vec();
                &owned_prefix
            }
            None => &[],
        };

        let inner = self.index.inner.read().unwrap();
        let has_stale_ids = !inner.stale_ids.is_empty();
        let mut cursor: Option<&IndexKey> = None;

        for (key, bitmap) in inner.label_index.prefix(prefix_bytes) {
            // Skip the cursor key to ensure forward progress
            if let Some(ref cursor_key) = self.cursor
                && key == cursor_key
            {
                continue;
            }

            processed_in_batch += 1;

            let cardinality = Self::adjusted_cardinality(&inner, bitmap, has_stale_ids);
            if cardinality > 0
                && let ControlFlow::Break(_) = processor(key, bitmap, cardinality)
            {
                self.is_finished = true;
                return;
            }

            cursor = Some(key);
            if processed_in_batch >= self.batch_size {
                break;
            }
        }

        if processed_in_batch == 0 || cursor.is_none() {
            self.cursor = None; // Signal completion
            self.is_finished = true;
        } else if let Some(cursor) = cursor {
            // Set the cursor to the last processed key to ensure we continue from there in the next batch
            self.cursor = Some(cursor.clone());
        }

        drop(inner);
    }

    fn is_complete(&self) -> bool {
        self.is_finished
    }
}

fn get_bitmap_size(bmp: &PostingsBitmap) -> usize {
    bmp.cardinality() as usize * size_of::<SeriesRef>()
}
