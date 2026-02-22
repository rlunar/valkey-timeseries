use crate::common::Sample;
use crate::common::constants::{REDUCER_KEY, SOURCE_KEY};
use crate::error_consts;
use crate::iterators::create_sample_iterator_adapter;
use crate::iterators::{MultiSeriesSampleIter, SampleReducer, create_range_iterator};
use crate::labels::Label;
use crate::series::acl::check_metadata_permissions;
use crate::series::chunks::{Chunk, GorillaChunk, TimeSeriesChunk, UncompressedChunk};
use crate::series::index::series_by_selectors;
use crate::series::request_types::{
    MRangeOptions, MRangeSeriesResult, RangeGroupingOptions, RangeOptions,
};
use crate::series::{TimeSeries, get_latest_compaction_sample};
use ahash::AHashMap;
use orx_parallel::{IntoParIter, IterIntoParIter, ParIter};
use valkey_module::{Context, ValkeyError, ValkeyResult};

struct MRangeSeriesMeta<'a> {
    series: &'a TimeSeries,
    source_key: String,
    latest: Option<Sample>,
    group_label_value: Option<String>,
}

pub fn process_mrange_query(
    ctx: &Context,
    options: MRangeOptions,
    clustered: bool,
) -> ValkeyResult<Vec<MRangeSeriesResult>> {
    check_metadata_permissions(ctx)?;

    if options.filters.is_empty() {
        return Err(ValkeyError::Str(error_consts::MISSING_FILTER));
    }

    let series_guards = series_by_selectors(ctx, &options.filters, None)?;

    let series_metas: Vec<MRangeSeriesMeta> = series_guards
        .iter()
        .map(|(guard, key)| MRangeSeriesMeta {
            series: guard,
            source_key: key.to_string(),
            group_label_value: None,
            latest: {
                // This is done upfront to enable parallel series processing below
                // (Context cannot be shared across threads)
                get_latest(&options.range, ctx, guard)
            },
        })
        .collect();

    Ok(process_mrange(series_metas, options, clustered))
}

fn process_mrange(
    metas: Vec<MRangeSeriesMeta>,
    options: MRangeOptions,
    is_clustered: bool,
) -> Vec<MRangeSeriesResult> {
    let mut options = options;
    let mut metas = metas;

    if let Some(grouping) = &options.grouping {
        collect_group_label_values(&mut metas, grouping);
        // Is_clustered here means that we are being called from a remote node. Since grouping
        // is cross-series, it can only be done when all results are available in the caller node.
        // However, we need to tag each series with the grouping label value (done above), so we can
        // group them by the grouping label when they are returned to the client.
        //
        // We remove grouping from the options here. The full request options are available in the done
        // handler, so we can use them to group the series.
        if is_clustered {
            options.grouping = None;
        }
    }
    let is_grouped = options.grouping.is_some();

    if is_clustered {
        return handle_non_grouped(metas, options, true);
    }

    let mut items = if is_grouped {
        handle_grouping(metas, options)
    } else {
        handle_non_grouped(metas, options, false)
    };

    sort_mrange_results(&mut items, is_grouped);

    items
}

fn get_latest(options: &RangeOptions, ctx: &Context, series: &TimeSeries) -> Option<Sample> {
    if !options.latest || !series.is_compaction() {
        return None;
    }
    get_latest_compaction_sample(ctx, series).filter(|s| {
        let (start_ts, end_ts) = options.get_timestamp_range();
        let ts = s.timestamp;

        if ts < start_ts || ts > end_ts {
            return false;
        }

        if !options.value_filter.is_none_or(|vf| vf.is_match(s.value)) {
            return false;
        }

        if !options
            .timestamp_filter
            .as_ref()
            .is_none_or(|ts_vec| ts_vec.contains(&ts))
        {
            return false;
        }

        true
    })
}

fn create_iter<'a>(
    series: &'a TimeSeries,
    options: &MRangeOptions,
    latest: Option<Sample>,
) -> Box<dyn Iterator<Item = Sample> + 'a> {
    create_range_iterator(
        series,
        &options.range,
        &options.grouping,
        latest,
        options.is_reverse,
    )
}

pub(crate) fn sort_mrange_results(results: &mut [MRangeSeriesResult], is_grouped: bool) {
    if is_grouped {
        results.sort_by(|a, b| a.group_label_value.cmp(&b.group_label_value));
    } else {
        results.sort_by(|a, b| a.key.cmp(&b.key));
    }
}

fn handle_non_grouped(
    metas: Vec<MRangeSeriesMeta>,
    options: MRangeOptions,
    clustered: bool,
) -> Vec<MRangeSeriesResult> {
    metas
        .into_par()
        .map(|meta| {
            let iter = create_iter(meta.series, &options, meta.latest);
            // if we're clustered, we use gorilla chunks to reduce network usage
            let data = if clustered {
                let mut chunk = GorillaChunk::with_max_size(16 * 1024); // 16KB - todo: make configurable?
                for sample in iter {
                    let _ = chunk.add_sample(&sample);
                }
                TimeSeriesChunk::Gorilla(chunk)
            } else {
                let samples = iter.collect::<Vec<_>>();
                let chunk = UncompressedChunk::from_vec(samples);
                TimeSeriesChunk::Uncompressed(chunk)
            };

            let labels = convert_labels(meta.series, options.with_labels, &options.selected_labels);

            MRangeSeriesResult {
                group_label_value: meta.group_label_value,
                key: meta.source_key,
                labels,
                data,
            }
        })
        .collect()
}

fn handle_grouping(
    metas: Vec<MRangeSeriesMeta>,
    options: MRangeOptions,
) -> Vec<MRangeSeriesResult> {
    let Some(grouping) = &options.grouping else {
        panic!("Grouping options should be present");
    };

    let grouped_series_map = group_series_by_label(metas, grouping, options.with_labels);

    if grouped_series_map.is_empty() {
        return vec![];
    }

    let mut options = options;
    let count = options.range.count;
    options.range.count = None;

    grouped_series_map
        .into_iter()
        .iter_into_par()
        .map(|(label_value, group_data)| {
            let grouping = options
                .grouping
                .as_ref()
                .expect("Grouping options should be present");
            let data = get_grouped_samples(&group_data.series, &options, grouping, count);
            let labels = group_data.labels;
            let key = format!("{}={}", grouping.group_label, label_value);
            let chunk = TimeSeriesChunk::Uncompressed(UncompressedChunk::from_vec(data));
            MRangeSeriesResult {
                key,
                group_label_value: Some(label_value),
                labels,
                data: chunk,
            }
        })
        .collect::<Vec<_>>()
}

fn get_grouped_samples(
    series_metas: &[MRangeSeriesMeta],
    options: &MRangeOptions,
    grouping_options: &RangeGroupingOptions,
    count: Option<usize>,
) -> Vec<Sample> {
    // This function gets the raw samples from all series in the group, then applies the grouping
    // reducer across the samples.
    // todo: choose approach based on data size and available memory?
    let is_reverse = options.is_reverse;

    // todo(perf): with sufficient memory, we could parallel load all samples into memory first,
    // and construct the MultiSeriesSampleIter from those. In low memory, we could use the code
    // below which iterates sequentially
    let iterators = series_metas
        .iter()
        .map(|meta| create_iter(meta.series, options, meta.latest))
        .collect::<Vec<_>>();

    let multi_iter = MultiSeriesSampleIter::new(iterators);
    let aggregator = grouping_options.aggregation.create_aggregator();
    let reducer = SampleReducer::new(multi_iter, aggregator);

    collect_samples(reducer, is_reverse, count)
}

pub(crate) fn collect_samples<I: Iterator<Item = Sample>>(
    iter: I,
    is_reverse: bool,
    count: Option<usize>,
) -> Vec<Sample> {
    let mut samples = if let Some(count) = count {
        iter.take(count).collect::<Vec<_>>()
    } else {
        iter.collect::<Vec<_>>()
    };

    if is_reverse {
        samples.reverse();
    }
    samples
}

fn convert_labels(
    series: &TimeSeries,
    with_labels: bool,
    selected_labels: &[String],
) -> Vec<Label> {
    if !with_labels && selected_labels.is_empty() {
        return Vec::new();
    }

    if selected_labels.is_empty() {
        return series.labels.iter().map(|l| l.into()).collect();
    }

    selected_labels
        .iter()
        .map(|name| {
            series
                .get_label(name)
                .map(|label| label.into())
                .unwrap_or_else(|| Label {
                    name: name.clone(),
                    value: String::new(),
                })
        })
        .collect()
}

pub(crate) fn build_mrange_grouped_labels(
    group_label_name: &str,
    group_label_value: &str,
    reducer_name_str: &str,
    source_identifiers: &[String],
) -> Vec<Label> {
    let sources = source_identifiers.join(",");
    vec![
        Label {
            name: group_label_name.into(),
            value: group_label_value.to_string(),
        },
        Label {
            name: REDUCER_KEY.into(),
            value: reducer_name_str.into(),
        },
        Label {
            name: SOURCE_KEY.into(),
            value: sources,
        },
    ]
}

fn collect_group_label_values(metas: &mut Vec<MRangeSeriesMeta>, grouping: &RangeGroupingOptions) {
    for meta in metas.iter_mut() {
        meta.group_label_value = meta
            .series
            .label_value(&grouping.group_label)
            .map(|s| s.to_string());
    }
}

struct GroupedSeriesData<'a> {
    series: Vec<MRangeSeriesMeta<'a>>,
    labels: Vec<Label>,
}

fn group_series_by_label<'a>(
    metas: Vec<MRangeSeriesMeta<'a>>,
    grouping: &RangeGroupingOptions,
    with_labels: bool,
) -> AHashMap<String, GroupedSeriesData<'a>> {
    let mut grouped: AHashMap<String, GroupedSeriesData<'a>> = AHashMap::new();
    let group_by_label_name = &grouping.group_label;
    let reducer_name = grouping.aggregation.aggregation_name();

    for mut meta in metas.into_iter() {
        if let Some(label_value_str) = meta.group_label_value.take() {
            let entry = grouped
                .entry(label_value_str)
                .or_insert_with(|| GroupedSeriesData {
                    series: Vec::new(),
                    labels: Vec::new(),
                });
            entry.series.push(meta);
        }
    }

    if with_labels {
        for (label_value_str, group_data) in grouped.iter_mut() {
            let mut source_keys: Vec<String> = group_data
                .series
                .iter()
                .map(|m| m.source_key.clone())
                .collect();

            source_keys.sort();

            group_data.labels = build_mrange_grouped_labels(
                group_by_label_name,
                label_value_str,
                reducer_name,
                &source_keys,
            );
        }
    }

    grouped
}

pub fn create_mrange_iterator_adapter<'a>(
    base_iter: impl Iterator<Item = Sample> + 'a,
    options: &MRangeOptions,
) -> Box<dyn Iterator<Item = Sample> + 'a> {
    create_sample_iterator_adapter(
        base_iter,
        &options.range,
        &options.grouping,
        options.is_reverse,
    )
}
