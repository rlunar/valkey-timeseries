// Based on code from the Prometheus project
// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::postings::{KeyType, Postings};
use super::{get_db_index, get_timeseries_index};
use crate::common::Timestamp;
use crate::common::context::get_current_db;
use crate::common::hash::IntMap;
use crate::error_consts;
use crate::labels::filters::SeriesSelector;
use crate::series::acl::{check_key_read_permission, has_all_keys_permissions};
use crate::series::request_types::MetaDateRangeFilter;
use crate::series::{SeriesGuard, SeriesRef, TimeSeries, get_timeseries};
use blart::AsBytes;
use orx_parallel::{IterIntoParIter, ParIter};
use valkey_module::{AclPermissions, Context, ValkeyError, ValkeyResult, ValkeyString};

pub fn series_by_selectors<'a>(
    ctx: &'a Context,
    selectors: &[SeriesSelector],
    range: Option<MetaDateRangeFilter>,
) -> ValkeyResult<Vec<(SeriesGuard<'a>, ValkeyString)>> {
    if selectors.is_empty() {
        return Ok(Vec::new());
    }

    let db = get_current_db(ctx);
    let index = get_db_index(db);
    let postings = index.get_postings();

    let series_refs = postings.postings_for_selectors(selectors)?;
    collect_series_from_postings(ctx, &postings, series_refs.iter(), range)
}

pub fn series_keys_by_selectors(
    ctx: &Context,
    selectors: &[SeriesSelector],
    range: Option<MetaDateRangeFilter>,
) -> ValkeyResult<Vec<ValkeyString>> {
    if selectors.is_empty() {
        return Ok(Vec::new());
    }

    let db = get_current_db(ctx);
    let index = get_db_index(db);
    let postings = index.get_postings();

    let series_refs = postings.postings_for_selectors(selectors)?;
    collect_series_keys(ctx, &postings, series_refs.iter(), range)
}

fn collect_series_keys(
    ctx: &Context,
    postings: &Postings,
    ids: impl Iterator<Item = SeriesRef>,
    date_range: Option<MetaDateRangeFilter>,
) -> ValkeyResult<Vec<ValkeyString>> {
    if let Some(date_range) = date_range {
        let series = collect_series_from_postings(ctx, postings, ids, Some(date_range))?;
        let keys = series.into_iter().map(|g| g.1).collect();
        return Ok(keys);
    }

    let keys = ids
        .filter_map(|id| {
            let key = postings.get_key_by_id(id)?;
            let real_key = ctx.create_string(key.as_bytes());
            if check_key_read_permission(ctx, &real_key) {
                Some(real_key)
            } else {
                None
            }
        })
        .collect();

    Ok(keys)
}

pub(crate) fn collect_series_from_postings<'a>(
    ctx: &'a Context,
    postings: &Postings,
    ids: impl Iterator<Item = SeriesRef>,
    date_range: Option<MetaDateRangeFilter>,
) -> ValkeyResult<Vec<(SeriesGuard<'a>, ValkeyString)>> {
    let capacity_estimate = ids.size_hint().1.unwrap_or(8);
    let iter = ids.filter_map(|id| postings.get_key_by_id(id));

    let mut result: Vec<(SeriesGuard, ValkeyString)> = Vec::with_capacity(capacity_estimate);
    for key in iter {
        let k = ctx.create_string(key.as_bytes());
        let perms = Some(AclPermissions::ACCESS);
        if let Some(guard) = get_timeseries(ctx, &k, perms, false)? {
            result.push((guard, k));
        }
    }

    if result.is_empty() {
        return Ok(result);
    }

    // If no date range filter or empty results, return early
    let Some(date_range) = date_range else {
        return Ok(result);
    };

    // Filter series by date range
    let (start, end) = date_range.range();
    let exclude = date_range.is_exclude();

    #[inline(always)]
    fn matches_date_range(
        series: &TimeSeries,
        start: Timestamp,
        end: Timestamp,
        exclude: bool,
    ) -> bool {
        let in_range = series.has_samples_in_range(start, end);
        in_range != exclude
    }

    if result.len() == 1 {
        // SAFETY: we have already checked above that we have at least one element.
        let series = unsafe { result.get_unchecked(0).0.as_ref() };
        return if matches_date_range(series, start, end, exclude) {
            Ok(result)
        } else {
            Ok(Vec::new())
        };
    }

    // Parallel filter for multiple series. Note that we don't collect the guards directly
    // since they hold a reference to the Context, which is not `Send`/`Sync` - hence the
    // need to collect IDs first and then reconstruct the guards from the original vector.
    let matching_ids: Vec<u64> = result
        .iter()
        .map(|guard| guard.0.as_ref())
        .iter_into_par()
        .filter_map(|series| {
            if matches_date_range(series, start, end, exclude) {
                Some(series.id)
            } else {
                None
            }
        })
        .collect();

    match matching_ids.len() {
        0 => Ok(Vec::new()),                  // none match
        n if n == result.len() => Ok(result), // all match
        n if n < 32 => {
            result.retain(|(guard, _)| matching_ids.contains(&guard.id));
            Ok(result)
        }
        _ => {
            let mut guard_map: IntMap<u64, (SeriesGuard, ValkeyString)> = result
                .into_iter()
                .map(|(guard, key)| (guard.id, (guard, key)))
                .collect();

            Ok(matching_ids
                .into_iter()
                .filter_map(|id| guard_map.remove(&id))
                .collect())
        }
    }
}

pub(super) fn get_guard_from_key<'a>(
    ctx: &'a Context,
    key: &KeyType,
) -> ValkeyResult<Option<SeriesGuard<'a>>> {
    let real_key = ctx.create_string(key.as_bytes());
    let perms = Some(AclPermissions::ACCESS);
    get_timeseries(ctx, &real_key, perms, false)
}

pub fn count_matched_series(
    ctx: &Context,
    date_range: Option<MetaDateRangeFilter>,
    matchers: &[SeriesSelector],
) -> ValkeyResult<usize> {
    let count = match (date_range, matchers.is_empty()) {
        (None, true) => {
            // check to see if the user can read all keys, otherwise error
            // a bare TS.CARD is a request for the cardinality of the entire index
            let current_user = ctx.get_current_user();
            let can_access_all_keys =
                has_all_keys_permissions(ctx, &current_user, Some(AclPermissions::ACCESS));
            if !can_access_all_keys {
                return Err(ValkeyError::Str(
                    error_consts::ALL_KEYS_READ_PERMISSION_ERROR,
                ));
            }
            let index = get_timeseries_index(ctx);
            index.count()
        }
        (None, false) => {
            // if we don't have a date range, we can simply count postings...
            let index = get_timeseries_index(ctx);
            index.get_cardinality_by_selectors(matchers)?
        }
        (Some(range), false) => {
            let matched_series = series_by_selectors(ctx, matchers, Some(range))?;
            matched_series.len()
        }
        _ => {
            // if we don't have a date range, we need at least one matcher, otherwise we
            // end up scanning the entire index
            return Err(ValkeyError::Str(
                "TSDB: TS.CARD requires at least one matcher or a date range",
            ));
        }
    };
    Ok(count)
}
