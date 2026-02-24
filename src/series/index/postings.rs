use super::index_key::IndexKey;
use super::key_buffer::KeyBuffer;
use crate::common::hash::IntMap;
use crate::common::logging::log_warning;
use crate::error_consts::MISSING_FILTER;
use crate::labels::filters::{
    FilterList, LabelFilter, MatchOp, PredicateMatch, PredicateValue, SeriesSelector,
};
use crate::labels::{InternedLabel, SeriesLabel};
use crate::series::{SeriesRef, TimeSeries};
use blart::map::Entry as ARTEntry;
use blart::{AsBytes, TreeMap};
use croaring::Bitmap64;
use smallvec::SmallVec;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::sync::LazyLock;
use valkey_module::{ValkeyError, ValkeyResult};

pub(super) static EMPTY_BITMAP: LazyLock<PostingsBitmap> = LazyLock::new(PostingsBitmap::new);

pub type PostingsBitmap = Bitmap64;
// label
// label=value
pub type PostingsIndex = TreeMap<IndexKey, PostingsBitmap>;

/// Type for the key of the index.
pub type KeyType = Box<[u8]>;

/// `Postings` is the core in-memory inverted index for time series data. It is designed for efficient
/// querying and retrieving of time series based on their labels.
#[derive(Clone)]
pub struct Postings {
    /// Map from label name and (label name, label value) to a set of timeseries ids.
    pub(super) label_index: PostingsIndex,
    /// Map from timeseries id to the key of the timeseries.
    pub(super) id_to_key: IntMap<SeriesRef, KeyType>,
    /// Set of timeseries ids of series that should be removed from the index. This really only
    /// happens when the index is inconsistent (value does not exist in the db but exists in the index)
    /// Keep track and cleanup from the index during a gc pass.
    pub(super) stale_ids: PostingsBitmap,
    /// Set of all timeseries ids in the index. This is used to optimize queries that are subtractive.
    pub(crate) all_postings: PostingsBitmap,
}

impl Default for Postings {
    fn default() -> Self {
        Postings {
            label_index: PostingsIndex::new(),
            id_to_key: IntMap::default(),
            stale_ids: PostingsBitmap::default(),
            all_postings: PostingsBitmap::default(),
        }
    }
}

impl Postings {
    #[allow(dead_code)]
    pub(super) fn clear(&mut self) {
        self.label_index.clear();
        self.id_to_key.clear();
        self.stale_ids.clear();
        self.all_postings.clear();
    }

    /// `swap` the inner value with some other value
    /// this is specifically to handle the `swapdb` event callback
    pub fn swap(&mut self, other: &mut Self) {
        std::mem::swap(&mut self.label_index, &mut other.label_index);
        std::mem::swap(&mut self.id_to_key, &mut other.id_to_key);
        std::mem::swap(&mut self.stale_ids, &mut other.stale_ids);
        std::mem::swap(&mut self.all_postings, &mut other.all_postings);
    }

    pub(super) fn remove_posting_for_label_value(
        &mut self,
        label: &str,
        value: &str,
        ts_id: SeriesRef,
    ) -> bool {
        let key = IndexKey::for_label_value(label, value);
        if let Some(bmp) = self.label_index.get_mut(&key) {
            let removed = bmp.remove_checked(ts_id);
            if removed && bmp.is_empty() {
                self.label_index.remove(&key);
            }
            return removed;
        }
        false
    }

    pub(super) fn add_posting_for_label_value(
        &mut self,
        ts_id: SeriesRef,
        label: &str,
        value: &str,
    ) -> bool {
        self.all_postings.add(ts_id);
        self.add_posting_for_label_value_internal(ts_id, label, value)
    }

    fn add_posting_for_label_value_internal(
        &mut self,
        ts_id: SeriesRef,
        label: &str,
        value: &str,
    ) -> bool {
        let key = IndexKey::for_label_value(label, value);
        match self.label_index.entry(key) {
            ARTEntry::Occupied(mut entry) => {
                entry.get_mut().add(ts_id);
                false
            }
            ARTEntry::Vacant(entry) => {
                let mut bitmap = PostingsBitmap::new();
                bitmap.add(ts_id);
                entry.insert(bitmap);
                true
            }
        }
    }

    fn set_timeseries_key(&mut self, id: SeriesRef, new_key: &[u8]) {
        if let Some(existing) = self.id_to_key.get(&id)
            && existing.as_ref() == new_key
        {
            return;
        }
        let key = new_key.to_vec().into_boxed_slice();
        self.id_to_key.insert(id, key);
    }

    pub fn index_timeseries(&mut self, ts: &TimeSeries, key: &[u8]) {
        debug_assert!(ts.id != 0);
        let id = ts.id;

        for InternedLabel { name, value } in ts.labels.iter() {
            self.add_posting_for_label_value_internal(id, name, value);
        }

        self.all_postings.add(id);
        self.set_timeseries_key(id, key);
    }

    pub fn remove_timeseries(&mut self, series: &TimeSeries) -> bool {
        let id = series.id;
        if self.id_to_key.remove(&id).is_none() {
            log_warning(format!(
                "Tried to remove non-existing series id {id} from index"
            ));
        };
        let removed = self.all_postings.remove_checked(id);
        for label in series.labels.iter() {
            self.remove_posting_for_label_value(label.name(), label.value(), id);
        }
        removed
    }

    pub fn count(&self) -> usize {
        self.id_to_key.len()
    }

    pub(super) fn has_id(&self, id: SeriesRef) -> bool {
        self.id_to_key.contains_key(&id)
    }

    /// Return postings for a key (borrowed if possible), applying stale removal.
    /// If stale_ids is non-empty, this returns Owned.
    fn postings_for_key(&'_ self, key: &[u8]) -> Cow<'_, PostingsBitmap> {
        match self.label_index.get(key) {
            Some(bmp) if self.stale_ids.is_empty() => Cow::Borrowed(bmp),
            Some(bmp) => Cow::Owned(bmp.andnot(&self.stale_ids)),
            None => Cow::Borrowed(&*EMPTY_BITMAP),
        }
    }

    /// Clone postings for a key (or empty), then remove stale IDs in-place.
    fn postings_for_key_owned(&self, key: &[u8]) -> PostingsBitmap {
        let mut out = self.label_index.get(key).cloned().unwrap_or_default();
        self.remove_stale_if_needed(&mut out);
        out
    }

    pub fn postings_for_label_value<'a>(
        &'a self,
        name: &str,
        value: &str,
    ) -> Cow<'a, PostingsBitmap> {
        let key = KeyBuffer::for_label_value(name, value);
        self.postings_for_key(key.as_bytes())
    }

    #[inline]
    fn remove_stale_if_needed(&self, postings: &mut PostingsBitmap) {
        if !self.stale_ids.is_empty() {
            postings.andnot_inplace(&self.stale_ids);
        }
    }

    pub fn get_label_names(&self) -> BTreeSet<String> {
        let mut names: BTreeSet<String> = BTreeSet::new();
        for (k, map) in self.label_index.iter() {
            if let Some((key, _)) = k.split()
                && !map.is_empty()
                && !names.contains(key)
            {
                names.insert(key.to_string());
            }
        }
        names
    }

    pub fn get_label_values(&self, label_name: &str) -> Vec<String> {
        let prefix = KeyBuffer::for_prefix(label_name);
        let mut values = Vec::with_capacity(8);
        for (k, map) in self.label_index.prefix(prefix.as_bytes()) {
            if !map.is_empty()
                && let Some((_key, value)) = k.split()
                && !value.is_empty()
            {
                values.push(value.to_string());
            }
        }
        values
    }

    pub fn postings_for_all_label_values(&self, label_name: &str) -> PostingsBitmap {
        let prefix = KeyBuffer::for_prefix(label_name);
        let mut result = PostingsBitmap::new();
        for (_, map) in self.label_index.prefix(prefix.as_bytes()) {
            result |= map;
        }
        self.remove_stale_if_needed(&mut result);
        result
    }

    /// `postings_for_label_values` returns the postings list iterator for the label pairs.
    /// The postings here contain the ids to the series inside the index.
    pub fn postings_for_label_values(&self, name: &str, values: &[String]) -> PostingsBitmap {
        let mut result = PostingsBitmap::new();

        for value in values {
            let key = KeyBuffer::for_label_value(name, value);
            if let Some(bmp) = self.label_index.get(key.as_bytes()) {
                result |= bmp;
            }
        }

        self.remove_stale_if_needed(&mut result);
        result
    }

    /// `postings_for_label_matching` returns postings having a label with the given name and a value
    /// for which `match_fn` returns true. If no postings are found having at least one matching label,
    /// an empty bitmap is returned.
    pub fn postings_for_label_matching<F, STATE>(
        &self,
        name: &str,
        state: &mut STATE,
        match_fn: F,
    ) -> PostingsBitmap
    where
        F: Fn(&str, &mut STATE) -> bool,
    {
        let prefix = KeyBuffer::for_prefix(name);
        let start_pos = prefix.len();
        let mut result = PostingsBitmap::new();

        for (key, map) in self.label_index.prefix(prefix.as_bytes()) {
            let value = key.sub_string(start_pos);
            if match_fn(value, state) {
                result |= map;
            }
        }

        self.remove_stale_if_needed(&mut result);
        result
    }

    /// Return all series ids corresponding to the given labels
    pub fn postings_by_labels<T: SeriesLabel>(&self, labels: &[T]) -> PostingsBitmap {
        let mut first = true;
        let mut acc = PostingsBitmap::new();

        for label in labels.iter() {
            let key = KeyBuffer::for_label_value(label.name(), label.value());
            if let Some(bmp) = self.label_index.get(key.as_bytes()) {
                if bmp.is_empty() {
                    break;
                }
                if first {
                    acc |= bmp;
                    first = false;
                } else {
                    acc &= bmp;
                }
            }
        }
        if !self.stale_ids.is_empty() {
            acc.andnot_inplace(&self.stale_ids);
        }

        acc
    }

    /// Get the unique series id for the given set of labels if it exists.
    ///
    /// This exists primarily to ensure that we disallow duplicate metric names
    pub fn posting_id_by_labels<T: SeriesLabel>(&self, labels: &[T]) -> Option<SeriesRef> {
        let mut it = labels.iter();

        let first = it.next()?;
        let first_key = KeyBuffer::for_label_value(first.name(), first.value());
        let mut acc = self.label_index.get(first_key.as_bytes())?.clone();

        for label in it {
            let key = KeyBuffer::for_label_value(label.name(), label.value());
            let bmp = self.label_index.get(key.as_bytes())?;
            acc.and_inplace(bmp);
            if acc.is_empty() {
                return None;
            }
        }

        self.remove_stale_if_needed(&mut acc);

        (acc.cardinality() == 1)
            .then(|| acc.iter().next())
            .flatten()
    }

    pub fn postings_without_label(&'_ self, label: &str) -> Cow<'_, PostingsBitmap> {
        let to_remove = self.postings_for_all_label_values(label);
        if to_remove.is_empty() {
            Cow::Borrowed(&self.all_postings)
        } else {
            Cow::Owned(self.all_postings.andnot(&to_remove))
        }
    }

    fn postings_for_any_of_labels(&self, labels: &[&str]) -> PostingsBitmap {
        let mut to_remove = PostingsBitmap::new();
        for label in labels {
            let prefix = KeyBuffer::for_prefix(label);
            for (_, map) in self.label_index.prefix(prefix.as_bytes()) {
                to_remove.or_inplace(map);
            }
        }
        self.remove_stale_if_needed(&mut to_remove);
        to_remove
    }

    pub fn postings_for_filter(&'_ self, filter: &LabelFilter) -> Cow<'_, PostingsBitmap> {
        match filter.matcher {
            PredicateMatch::Equal(ref value) => handle_equal_match(self, &filter.label, value),
            PredicateMatch::NotEqual(ref value) => {
                handle_not_equal_match(self, &filter.label, value)
            }
            PredicateMatch::RegexEqual(_) => handle_regex_equal_match(self, filter),
            PredicateMatch::RegexNotEqual(_) => handle_regex_not_equal_match(self, filter),
        }
    }

    fn inverse_postings_for_filter(&'_ self, filter: &LabelFilter) -> Cow<'_, PostingsBitmap> {
        match &filter.matcher {
            PredicateMatch::NotEqual(pv) => handle_equal_match(self, &filter.label, pv),
            // If the matcher being inverted is ="", we just want all the values.
            PredicateMatch::Equal(PredicateValue::String(s)) if s.is_empty() => {
                Cow::Owned(self.postings_for_all_label_values(&filter.label))
            }
            // If the matcher being inverted is =~"", we just want all the values.
            PredicateMatch::RegexEqual(re) if matches!(re.regex.as_str(), "" | ".*") => {
                Cow::Owned(self.postings_for_all_label_values(&filter.label))
            }
            _ => {
                let mut state = filter;
                let postings =
                    self.postings_for_label_matching(&filter.label, &mut state, |s, state| {
                        let valid = state.matches(s);
                        !valid
                    });
                Cow::Owned(postings)
            }
        }
    }

    /// `postings_for_label_filters` assembles a single postings iterator against the index
    /// based on the given matchers.
    pub fn postings_for_label_filters(
        &'_ self,
        filters: &[LabelFilter],
    ) -> ValkeyResult<Cow<'_, PostingsBitmap>> {
        if filters.is_empty() {
            return Ok(Cow::Borrowed(&self.all_postings));
        }
        if filters.len() == 1 {
            let filter = &filters[0];
            // follow Prometheus here: if we have an empty matcher and label, return all postings.
            if filter.label.is_empty() && filter.matcher.is_empty() {
                return Ok(Cow::Borrowed(&self.all_postings));
            }
            // shortcut the handling of simple equality matchers
            if !filter.is_negative_matcher() && !filter.matches_empty() {
                let it = self.postings_for_filter(filter);
                if it.is_empty() {
                    return Ok(Cow::Borrowed(&*EMPTY_BITMAP));
                }
                return Ok(it);
            }
        }

        let mut its: SmallVec<_, 4> = SmallVec::new();
        let mut not_its: SmallVec<Cow<PostingsBitmap>, 4> = SmallVec::new();

        let mut sorted_matchers: SmallVec<(&LabelFilter, bool, bool), 4> = SmallVec::new();

        let mut has_subtracting_matchers = false;
        let mut has_intersecting_matchers = false;
        for m in filters {
            let matches_empty = m.matches("");

            let is_subtracting = matches_empty || m.is_negative_matcher();

            if is_subtracting {
                has_subtracting_matchers = true;
            } else {
                has_intersecting_matchers = true;
            }

            sorted_matchers.push((m, matches_empty, is_subtracting))
        }

        if has_subtracting_matchers && !has_intersecting_matchers {
            // If there's nothing to subtract from, add in everything and remove the not_its later.
            // We prefer to get all_postings so that the base of subtraction (i.e., all_postings)
            // doesn't include series that may be added to the index reader during this function call.
            its.push(Cow::Borrowed(&self.all_postings));
        };

        // Sort matchers to have the intersecting matchers first.
        // This way the base for subtraction is smaller, and there is no chance that the set we subtract
        // from contains postings of series that didn't exist when we constructed the set we subtract by.
        sorted_matchers.sort_by(|i, j| -> Ordering {
            let is_i_subtracting = i.2;
            let is_j_subtracting = j.2;
            if !is_i_subtracting && is_j_subtracting {
                return Ordering::Less;
            }
            // sort by match cost
            let cost_i = i.0.cost();
            let cost_j = j.0.cost();
            cost_i.cmp(&cost_j)
        });

        for (filter, matches_empty, _is_subtracting) in sorted_matchers {
            //let value = &m.value;
            let name = &filter.label;

            if name.is_empty() && matches_empty {
                // We already handled the case at the top of the function,
                // and it is unexpected to get all postings again here.
                return Err(ValkeyError::Str(MISSING_FILTER));
            }

            let typ = filter.op();
            let regex_value = filter.regex_text().unwrap_or("");

            match (typ, regex_value) {
                // .* regexp matches any string: do nothing
                (MatchOp::RegexEqual, ".*") => continue,

                // .* regexp does not match any string: return empty
                (MatchOp::RegexNotEqual, ".*") => {
                    return Ok(Cow::Borrowed(&*EMPTY_BITMAP));
                }

                // .+ regexp matches any non-empty string
                (MatchOp::RegexEqual, ".+") => {
                    // .+ regexp matches any non-empty string: get postings for all label values.
                    let it = self.postings_for_all_label_values(&filter.label);
                    its.push(Cow::Owned(it));
                }

                // .+ regexp does not match any non-empty string
                (MatchOp::RegexNotEqual, ".+") => {
                    let it = self.postings_for_all_label_values(&filter.label);
                    not_its.push(Cow::Owned(it));
                }
                // See which label must be non-empty.
                // Optimization for a case like {l=~".", l!="1"}.
                _ if !matches_empty => {
                    // If this matcher must be non-empty, we can be smarter.
                    let is_not = matches!(typ, MatchOp::NotEqual | MatchOp::RegexNotEqual);
                    match (is_not, matches_empty) {
                        // l!="foo"
                        (true, true) => {
                            // If the label can't be empty and is a Not and the inner matcher
                            // doesn't match empty, then subtract it out at the end.
                            let inverse = filter.clone().inverse();
                            let it = self.postings_for_filter(&inverse);
                            not_its.push(it);
                        }
                        // l!=""
                        (true, false) => {
                            // If the label can't be empty and is a Not, but the inner matcher can
                            // be empty, we need to use inverse_postings_for_filter.
                            let inverse = filter.clone().inverse();
                            let it = self.inverse_postings_for_filter(&inverse);
                            if it.is_empty() {
                                return Ok(Cow::Borrowed(&*EMPTY_BITMAP));
                            }
                            its.push(it);
                        }
                        // l="a", l=~"a|b", etc.
                        _ => {
                            // Non-Not matcher, use normal postings_for_filter.
                            let it = self.postings_for_filter(filter);
                            if it.is_empty() {
                                return Ok(Cow::Borrowed(&*EMPTY_BITMAP));
                            }
                            its.push(it);
                        }
                    }
                }
                _ => {
                    // l=""
                    // If the matchers for a label name selects an empty value, it selects all
                    // the series which also don't have the label name. See:
                    // https://github.com/prometheus/prometheus/issues/3575 and
                    // https://github.com/prometheus/prometheus/pull/3578#issuecomment-351653555
                    let it = self.inverse_postings_for_filter(filter);

                    not_its.push(it)
                }
            }
        }

        // optimization: if we have a single iterator and no not_its, return it directly, saving a clone.
        if its.len() == 1 && not_its.is_empty() {
            return Ok(its
                .pop()
                .expect("unexpected out of bounds error running matchers"));
        }

        let mut result = if its.is_empty() {
            self.all_postings.clone()
        } else {
            // sort by cardinality first to reduce the amount of work
            its.sort_by_key(|a| a.cardinality());
            intersection(its)
        };

        for not in not_its {
            result.andnot_inplace(&not)
        }

        Ok(Cow::Owned(result))
    }

    pub fn postings_for_selector(
        &'_ self,
        selector: &SeriesSelector,
    ) -> ValkeyResult<Cow<'_, PostingsBitmap>> {
        match &selector {
            SeriesSelector::And(filters) => self.postings_for_label_filters(filters),
            SeriesSelector::Or(filters) => self.process_or_matchers(filters),
        }
    }

    pub fn postings_for_selectors(
        &'_ self,
        selectors: &[SeriesSelector],
    ) -> ValkeyResult<Cow<'_, PostingsBitmap>> {
        match selectors {
            [] => Ok(Cow::Borrowed(&*EMPTY_BITMAP)),
            [selector] => {
                let result = self.postings_for_selector(selector)?;
                if !self.stale_ids.is_empty() {
                    let mut result = result.into_owned();
                    result.andnot_inplace(&self.stale_ids);
                    return Ok(Cow::Owned(result));
                }
                Ok(result)
            }
            _ => {
                let first = self.postings_for_selector(&selectors[0])?;

                let mut result = first.into_owned();
                for selector in &selectors[1..] {
                    let bitmap = self.postings_for_selector(selector)?;
                    result.and_inplace(&bitmap);
                }

                if !self.stale_ids.is_empty() {
                    result.andnot_inplace(&self.stale_ids);
                }

                Ok(Cow::Owned(result))
            }
        }
    }

    fn process_or_matchers(
        &'_ self,
        filters: &[FilterList],
    ) -> ValkeyResult<Cow<'_, PostingsBitmap>> {
        match filters {
            [] => Ok(Cow::Borrowed(&self.all_postings)),
            [filters] => self.postings_for_label_filters(filters),
            _ => {
                let mut result = PostingsBitmap::new();
                // maybe chili here to run in parallel
                for matchers in filters {
                    let postings = self.postings_for_label_filters(matchers)?;
                    result.or_inplace(&postings);
                }
                Ok(Cow::Owned(result))
            }
        }
    }

    pub(crate) fn get_key_by_id(&self, id: SeriesRef) -> Option<&KeyType> {
        self.id_to_key.get(&id)
    }

    /// Marks an id as stale by adding its ID to the stale IDs set.
    /// Context: used in the case of possible index sync issues. When the index is queried and an id is returned
    /// with no corresponding series, we have no access to the series data to do a proper
    /// cleanup. We remove the key from the index and mark the ID as stale, which will be cleaned up later.
    /// The stale IDs are stored in a bitmap for efficient removal and are checked to ensure that no stale IDs are
    /// returned in queries until they are removed.
    pub(crate) fn mark_id_as_stale(&mut self, id: SeriesRef) {
        let _ = self.id_to_key.remove(&id);
        self.stale_ids.add(id);
        self.all_postings.remove(id);
    }

    #[cfg(test)]
    pub(super) fn has_stale_ids(&self) -> bool {
        !self.stale_ids.is_empty()
    }

    /// Removes stale series IDs from a subset of the index structures.
    ///
    /// This method processes at most `count` keys starting from `start_prefix`,
    /// removing stale IDs from their bitmaps and cleaning up empty entries.
    ///
    /// ## Arguments
    /// * `start_prefix` - The key to start processing from (inclusive)
    /// * `count` - Maximum number of keys to process in this batch
    ///
    /// ## Returns
    /// * `Option<IndexKey>` - The next key to continue processing from, or None if processing is complete
    ///
    pub(crate) fn remove_stale_ids(
        &mut self,
        start_prefix: Option<IndexKey>,
        count: usize,
    ) -> Option<IndexKey> {
        // Skip if there are no stale IDs to process
        if self.stale_ids.is_empty() {
            return None;
        }

        let mut keys_processed = 0;
        let mut keys_to_remove = Vec::new();
        let mut next_key = None;

        // Determine the prefix to use for iteration
        let prefix_bytes = start_prefix.map_or_else(Vec::new, |k| k.as_bytes().to_vec());

        for (key, bitmap) in self.label_index.prefix_mut(&prefix_bytes) {
            // Remove stale IDs from the bitmap
            let should_remove = if !bitmap.is_empty() {
                bitmap.andnot_inplace(&self.stale_ids);
                bitmap.is_empty()
            } else {
                true
            };

            if should_remove {
                keys_to_remove.push(key.clone());
            }

            if keys_processed == count {
                // Save the key we stopped at as the next starting point
                next_key = Some(key.clone());
                break;
            }

            keys_processed += 1;
        }

        // Process empty keys
        for key in keys_to_remove {
            self.label_index.remove(&key);
        }

        // Clean up id_to_key map for all stale IDs
        // This is done in every batch since we need to ensure consistency
        if keys_processed > 0 {
            self.stale_ids.iter().for_each(|id| {
                let _ = self.id_to_key.remove(&id);
            });

            // Clear stale_ids if we've processed all keys
            if next_key.is_none() {
                self.stale_ids.clear();
            }
        }

        next_key
    }

    /// Incrementally optimizes posting bitmaps for better memory usage and performance.
    ///
    /// This method processes at most `count` keys starting from `start_prefix`,
    /// performing the following optimizations on each bitmap:
    /// 1. Remove the bitmap if it is empty
    /// 2. Call run_optimize() to optimize the bitmap's internal structure
    /// 3. Call shrink_to_fit() to reduce memory overhead
    ///
    /// ### Arguments
    /// * `start_prefix` - The key to start processing from (inclusive)
    /// * `count` - Maximum number of keys to process in this batch
    ///
    /// ### Returns
    /// * `Option<IndexKey>` - The next key to continue processing from, or None if processing is complete
    ///
    pub(crate) fn optimize_postings(
        &mut self,
        start_prefix: Option<IndexKey>,
        count: usize,
    ) -> Option<IndexKey> {
        let mut next_key = None;

        if start_prefix.is_none() {
            optimize_bitmap(&mut self.all_postings);
        }

        let mut keys_to_delete = Vec::new();
        let mut keys_processed: usize = 0;
        // Determine the prefix to use for iteration
        let prefix_bytes = start_prefix.map_or_else(Vec::new, |k| k.as_bytes().to_vec());

        // Collect keys to process
        for (key, bitmap) in self.label_index.prefix_mut(&prefix_bytes) {
            if bitmap.is_empty() {
                keys_to_delete.push(key.clone());
                continue;
            }
            if keys_processed == count {
                // Save the key we stopped at as the next starting point
                next_key = Some(key.clone());
                break;
            }

            optimize_bitmap(bitmap);

            keys_processed += 1;
        }

        // Remove empty bitmaps collected earlier
        for key in keys_to_delete {
            self.label_index.remove(&key);
        }

        next_key
    }
}

/// Optimizes a bitmap in place for better memory usage and performance.
/// This applies run_optimize() and shrink_to_fit() operations to the bitmap
/// if it exists in the index.
fn optimize_bitmap(bitmap: &mut PostingsBitmap) {
    // Optimize the bitmap's internal structure
    bitmap.run_optimize();

    // Shrink to fit to reduce memory overhead
    bitmap.shrink_to_fit();
}

fn handle_equal_match<'a>(
    ix: &'a Postings,
    label: &str,
    value: &PredicateValue,
) -> Cow<'a, PostingsBitmap> {
    match value {
        PredicateValue::String(s) => {
            if s.is_empty() {
                return ix.postings_without_label(label);
            }
            ix.postings_for_label_value(label, s)
        }
        PredicateValue::List(val) => match val.len() {
            0 => ix.postings_without_label(label),
            1 => ix.postings_for_label_value(label, &val[0]),
            _ => Cow::Owned(ix.postings_for_label_values(label, val)),
        },
        PredicateValue::Empty => ix.postings_without_label(label),
    }
}

// return postings for series which has the label `label
fn with_label<'a>(ix: &'a Postings, label: &str) -> Cow<'a, PostingsBitmap> {
    let mut state = ();
    let postings = ix.postings_for_label_matching(label, &mut state, |_value, _| true);
    Cow::Owned(postings)
}

fn handle_not_equal_match<'a>(
    ix: &'a Postings,
    label: &str,
    value: &PredicateValue,
) -> Cow<'a, PostingsBitmap> {
    // the time series has a label named label
    match value {
        PredicateValue::String(s) => {
            if s.is_empty() {
                return with_label(ix, label);
            }
            let all = &ix.all_postings;
            let postings = ix.postings_for_label_value(label, s);
            if postings.is_empty() {
                Cow::Borrowed(all)
            } else {
                let result = all.andnot(&postings);
                Cow::Owned(result)
            }
        }
        PredicateValue::List(values) => {
            match values.len() {
                0 => with_label(ix, label),
                _ => {
                    // get postings for label m.label without values in values
                    let to_remove = ix.postings_for_label_values(label, values);
                    let all_postings = &ix.all_postings;
                    if to_remove.is_empty() {
                        Cow::Borrowed(all_postings)
                    } else {
                        let result = all_postings.andnot(&to_remove);
                        Cow::Owned(result)
                    }
                }
            }
        }
        PredicateValue::Empty => with_label(ix, label),
    }
}

#[inline]
fn postings_matching_filter(postings: &Postings, filter: &LabelFilter) -> PostingsBitmap {
    let mut state = filter;
    postings.postings_for_label_matching(&filter.label, &mut state, |value, f| f.matches(value))
}

fn handle_regex_equal_match<'a>(
    postings: &'a Postings,
    filter: &LabelFilter,
) -> Cow<'a, PostingsBitmap> {
    if filter.matches_empty() {
        return postings.postings_without_label(&filter.label);
    }
    Cow::Owned(postings_matching_filter(postings, filter))
}

fn handle_regex_not_equal_match<'a>(
    postings: &'a Postings,
    filter: &LabelFilter,
) -> Cow<'a, PostingsBitmap> {
    if filter.matches_empty() {
        return with_label(postings, &filter.label);
    }
    Cow::Owned(postings_matching_filter(postings, filter))
}

fn intersection<'a, I>(its: I) -> PostingsBitmap
where
    I: IntoIterator<Item = Cow<'a, PostingsBitmap>>,
{
    let mut its = its.into_iter();
    if let Some(it) = its.next() {
        let mut result = it.into_owned();

        for it in its {
            if it.is_empty() {
                result.clear();
                return result;
            }

            result.and_inplace(&it);
        }

        result
    } else {
        PostingsBitmap::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::labels::{Label, MetricName};
    use crate::series::time_series::TimeSeries;

    #[test]
    fn test_memory_postings_add_and_remove() {
        let mut postings = Postings::default();

        // Add postings
        postings.add_posting_for_label_value(1, "label1", "value1");
        postings.add_posting_for_label_value(1, "label2", "value2");
        postings.add_posting_for_label_value(2, "label1", "value1");

        // Check postings
        assert_eq!(
            postings
                .postings_for_label_value("label1", "value1")
                .cardinality(),
            2
        );
        assert_eq!(
            postings
                .postings_for_label_value("label2", "value2")
                .cardinality(),
            1
        );

        // Remove posting
        postings.remove_posting_for_label_value("label1", "value1", 1);
        assert_eq!(
            postings
                .postings_for_label_value("label1", "value1")
                .cardinality(),
            1
        );

        // Remove non-existent posting (should not panic)
        postings.remove_posting_for_label_value("label3", "value3", 3);
    }

    #[test]
    fn test_postings_multiple_values_same_label() {
        let mut postings = Postings::default();

        // Add postings for multiple values of the same label
        postings.add_posting_for_label_value(1, "label1", "value1");
        postings.add_posting_for_label_value(2, "label1", "value2");
        postings.add_posting_for_label_value(3, "label1", "value3");
        postings.add_posting_for_label_value(4, "label1", "value1");

        // Query for multiple values of the same label
        let values = vec!["value1".to_string(), "value3".to_string()];
        let result = postings.postings_for_label_values("label1", &values);

        // Check that the result contains the correct series IDs
        assert_eq!(result.cardinality(), 3);
        assert!(result.contains(1));
        assert!(result.contains(3));
        assert!(result.contains(4));
        assert!(!result.contains(2));
    }

    #[test]
    fn test_postings_with_duplicate_values() {
        let mut postings = Postings::default();

        // Add some postings
        postings.add_posting_for_label_value(1, "label", "value1");
        postings.add_posting_for_label_value(2, "label", "value2");
        postings.add_posting_for_label_value(3, "label", "value1");

        // Create an array with duplicate values
        let values = vec![
            "value1".to_string(),
            "value2".to_string(),
            "value1".to_string(),
        ];

        // Call the postings method
        let result = postings.postings_for_label_values("label", &values);

        // Check the result
        assert_eq!(result.cardinality(), 3);
        assert!(result.contains(1));
        assert!(result.contains(2));
        assert!(result.contains(3));
    }

    #[test]
    fn test_postings_all_values_match() {
        let mut postings = Postings::default();

        // Add some postings
        postings.add_posting_for_label_value(1, "label", "value1");
        postings.add_posting_for_label_value(2, "label", "value2");
        postings.add_posting_for_label_value(3, "label", "value3");
        postings.add_posting_for_label_value(4, "label", "value1");

        // Create values to search for
        let values = vec![
            "value1".to_string(),
            "value2".to_string(),
            "value3".to_string(),
        ];

        // Get the postings
        let result = postings.postings_for_label_values("label", &values);

        // Check if the result contains all the expected series IDs
        assert_eq!(result.cardinality(), 4);
        assert!(result.contains(1));
        assert!(result.contains(2));
        assert!(result.contains(3));
        assert!(result.contains(4));
    }

    #[test]
    fn test_postings_with_large_number_of_values() {
        let mut postings = Postings::default();
        let label_name = "large_label";
        let num_values = 10_000;

        // Add postings for a large number of values
        for i in 0..num_values {
            postings.add_posting_for_label_value(i as SeriesRef, label_name, &format!("value_{i}"));
        }

        // Create a large array of values to search for
        let values: Vec<String> = (0..num_values).map(|i| format!("value_{i}")).collect();

        // Measure the time taken to execute the postings function
        let start_time = std::time::Instant::now();
        let result = postings.postings_for_label_values(label_name, &values);
        let duration = start_time.elapsed();

        // Assert that all series IDs are present in the result
        assert_eq!(result.cardinality() as usize, num_values);
        for i in 0..num_values {
            assert!(result.contains(i as SeriesRef));
        }

        // Check that the execution time is reasonable (adjust the threshold as needed)
        assert!(
            duration < std::time::Duration::from_secs(1),
            "Postings retrieval took too long: {duration:?}"
        );
    }

    #[test]
    fn test_postings_with_unicode_characters() {
        let mut postings = Postings::default();

        // Add postings with Unicode characters
        postings.add_posting_for_label_value(1, "标签", "值1");
        postings.add_posting_for_label_value(2, "标签", "値2");
        postings.add_posting_for_label_value(3, "标签", "🌟");

        // Test postings method with Unicode characters
        let values = vec!["值1".to_string(), "値2".to_string(), "🌟".to_string()];
        let result = postings.postings_for_label_values("标签", &values);

        assert_eq!(result.cardinality(), 3);
        assert!(result.contains(1));
        assert!(result.contains(2));
        assert!(result.contains(3));
    }

    #[test]
    fn test_postings_for_label_value_exceeds_stack_size() {
        let mut postings = Postings::default();

        // The STACK_SIZE in KeyBuffer is 64 bytes
        // We need label_name + "=" + value + "\0" to exceed 64 bytes
        // Let's create a label name and value that together exceed this

        // Create a label name of 30 characters
        let label_name = "very_long_label_name_here_1234";

        // Create a value of 40 characters, so the total length is:
        // 30 (label) + 1 (=) + 40 (value) + 1 (\0) = 72 bytes > 64
        let value = "this_is_a_very_long_value_string_12345";

        // Verify our assumption about the length
        let total_len = label_name.len() + 1 + value.len() + 1; // +1 for '=', +1 for '\0'
        assert!(
            total_len > 64,
            "Test setup error: combined length should exceed STACK_SIZE"
        );

        // Add a posting with this long label-value pair
        postings.add_posting_for_label_value(1, label_name, value);
        postings.add_posting_for_label_value(2, label_name, value);
        postings.add_posting_for_label_value(3, label_name, "short");

        // Test that we can retrieve postings for the long label-value pair
        let result = postings.postings_for_label_value(label_name, value);

        // Verify the result contains the correct series IDs
        assert_eq!(result.cardinality(), 2);
        assert!(result.contains(1));
        assert!(result.contains(2));
        assert!(!result.contains(3));

        // Test that we can also retrieve the short value
        let result_short = postings.postings_for_label_value(label_name, "short");
        assert_eq!(result_short.cardinality(), 1);
        assert!(result_short.contains(3));
    }

    #[test]
    fn test_postings_for_all_label_values_exceeds_stack_size() {
        let mut postings = Postings::default();

        // The STACK_SIZE in KeyBuffer is 64 bytes
        // KeyBuffer::for_prefix creates: label_name + "="
        // We need label_name + "=" to exceed 64 bytes

        // Create a label name of 70 characters to exceed the stack size
        let label_name = "very_long_label_name_here_that_definitely_exceeds_the_stack_buffer_size";

        // Verify our assumption about the length
        let prefix_len = label_name.len() + 1; // +1 for '='
        assert!(
            prefix_len > 64,
            "Test setup error: prefix length should exceed STACK_SIZE"
        );

        // Add multiple postings with different values for the same long label name
        postings.add_posting_for_label_value(1, label_name, "value1");
        postings.add_posting_for_label_value(2, label_name, "value2");
        postings.add_posting_for_label_value(3, label_name, "value3");
        postings.add_posting_for_label_value(4, "short_label", "value1");

        // Test that we can retrieve all postings for the long label name
        let result = postings.postings_for_all_label_values(label_name);

        // Verify the result contains all series IDs with the long label name
        assert_eq!(result.cardinality(), 3);
        assert!(result.contains(1));
        assert!(result.contains(2));
        assert!(result.contains(3));
        assert!(!result.contains(4)); // This has a different label

        // Also test that we can retrieve postings for the short label
        let result_short = postings.postings_for_all_label_values("short_label");
        assert_eq!(result_short.cardinality(), 1);
        assert!(result_short.contains(4));
    }

    #[test]
    fn test_memory_postings_set_timeseries_key() {
        let mut postings = Postings::default();

        postings.set_timeseries_key(1, b"key1");
        postings.set_timeseries_key(2, b"key2");

        assert_eq!(
            postings.get_key_by_id(1),
            Some(&b"key1".to_vec().into_boxed_slice())
        );
        assert_eq!(
            postings.get_key_by_id(2),
            Some(&b"key2".to_vec().into_boxed_slice())
        );
        assert_eq!(postings.get_key_by_id(3), None);
    }

    #[test]
    fn test_memory_postings_remove_timeseries() {
        let mut postings = Postings::default();
        let mut series = TimeSeries::new();
        series.id = 1;
        series.labels = MetricName::new(&[
            Label::new("label1", "value1"),
            Label::new("label2", "value2"),
        ]);

        postings.add_posting_for_label_value(1, "label1", "value1");
        postings.add_posting_for_label_value(1, "label2", "value2");

        postings.remove_timeseries(&series);

        assert!(
            postings
                .postings_for_label_value("label1", "value1")
                .is_empty()
        );
        assert!(
            postings
                .postings_for_label_value("label2", "value2")
                .is_empty()
        );
        assert!(postings.all_postings.is_empty());
    }

    #[test]
    fn test_memory_postings_postings_by_labels() {
        let mut postings = Postings::default();

        postings.add_posting_for_label_value(1, "label1", "value1");
        postings.add_posting_for_label_value(1, "label2", "value2");
        postings.add_posting_for_label_value(2, "label1", "value1");
        postings.add_posting_for_label_value(2, "label2", "value3");

        let labels = vec![
            Label::new("label1", "value1"),
            Label::new("label2", "value2"),
        ];
        let result = postings.postings_by_labels(&labels);

        assert_eq!(result.cardinality(), 1);
        assert!(result.contains(1));
    }
}
