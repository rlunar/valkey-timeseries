use super::parse_mrange_options;
use crate::common::constants::{REDUCER_KEY, SOURCE_KEY};
use crate::common::Sample;
use crate::error_consts;
use crate::fanout::cluster::is_clustered;
use crate::fanout::{perform_remote_mrange_request, MultiRangeResponse};
use crate::iterators::{MultiSeriesSampleIter, SampleIter};
use crate::labels::Label;
use crate::series::acl::check_metadata_permissions;
use crate::series::index::series_by_matchers;
use crate::series::range_utils::{
    aggregate_samples, get_multi_series_range, get_series_labels, group_reduce,
};
use crate::series::request_types::{
    AggregationOptions, MRangeOptions, MRangeSeriesResult, RangeGroupingOptions,
};
use crate::series::{SeriesSampleIterator, TimeSeries, TimestampValue};
use ahash::AHashMap;
use rayon::iter::{IntoParallelRefIterator, IntoParallelRefMutIterator, ParallelIterator};
use std::collections::BTreeMap;
use valkey_module::{
    BlockedClient, Context, NextArg, ThreadSafeContext, ValkeyError, ValkeyResult, ValkeyString,
    ValkeyValue,
};

struct SeriesMeta<'a> {
    series: &'a TimeSeries,
    source_key: String,
    group_label_value: Option<String>,
}

/// TS.MRANGE fromTimestamp toTimestamp
//   [LATEST]
//   [FILTER_BY_TS ts...]
//   [FILTER_BY_VALUE min max]
//   [WITHLABELS | <SELECTED_LABELS label...>]
//   [COUNT count]
//   [[ALIGN align] AGGREGATION aggregator bucketDuration [BUCKETTIMESTAMP bt] [EMPTY]]
//   FILTER filterExpr...
//   [GROUPBY label REDUCE reducer]
pub fn mrange(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    mrange_internal(ctx, args, false)
}

pub fn mrevrange(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    mrange_internal(ctx, args, true)
}

fn mrange_internal(ctx: &Context, args: Vec<ValkeyString>, reverse: bool) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();
    let options = parse_mrange_options(&mut args)?;

    if options.filters.is_empty() {
        return Err(ValkeyError::Str(error_consts::MISSING_FILTER));
    }

    check_metadata_permissions(ctx)?;

    args.done()?;

    if is_clustered(ctx) {
        perform_remote_mrange_request(ctx, options, on_mrange_request_done)?;
        return Ok(ValkeyValue::NoReply);
    }

    let result_rows = process_mrange_query(ctx, options, reverse)?;
    let result = result_rows
        .into_iter()
        .map(|series| series.into())
        .collect::<Vec<ValkeyValue>>();

    Ok(ValkeyValue::from(result))
}

fn on_mrange_request_done(
    ctx: &ThreadSafeContext<BlockedClient>,
    req: MRangeOptions,
    results: Vec<MultiRangeResponse>,
) {
    let all_series = results
        .into_iter()
        .flat_map(|result| result.series.into_iter())
        .collect::<Vec<_>>();

    let series = if let Some(grouping) = req.grouping {
        group_sharded_series(all_series, &grouping)
    } else {
        all_series
    };

    let result = ValkeyValue::Array(series.into_iter().map(|x| x.into()).collect());
    ctx.reply(Ok(result));
}

pub fn process_mrange_query(
    ctx: &Context,
    options: MRangeOptions,
    reverse: bool,
) -> ValkeyResult<Vec<MRangeSeriesResult>> {
    if options.filters.is_empty() {
        return Err(ValkeyError::Str(error_consts::MISSING_FILTER));
    }
    let mut options = options;

    let (start_ts, end_ts) = options.date_range.get_timestamps(None);
    options.date_range.start = TimestampValue::Specific(start_ts);
    options.date_range.end = TimestampValue::Specific(end_ts);

    let series_guards = series_by_matchers(ctx, &options.filters, None, true, true)?;

    let series_metas: Vec<SeriesMeta> = series_guards
        .iter()
        .map(|guard| SeriesMeta {
            series: guard,
            source_key: guard.get_key().to_string(),
            group_label_value: None,
        })
        .collect();

    let is_clustered = is_clustered(ctx);
    let mut result = process_mrange_command(series_metas, options, is_clustered);

    if reverse {
        result.reverse();
    }

    Ok(result)
}

fn process_mrange_command(
    metas: Vec<SeriesMeta>,
    options: MRangeOptions,
    is_clustered: bool,
) -> Vec<MRangeSeriesResult> {
    let mut options = options;
    let mut metas = metas;
    if let Some(grouping) = &options.grouping {
        collect_group_labels(&mut metas, grouping);
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
    match (&options.grouping, &options.aggregation) {
        (Some(groupings), Some(aggr_options)) => {
            handle_aggregation_and_grouping(metas, &options, groupings, aggr_options)
        }
        (Some(groupings), None) => handle_grouping(metas, &options, groupings),
        (_, _) => handle_basic(metas, &options),
    }
}

fn collect_group_labels(metas: &mut Vec<SeriesMeta>, grouping: &RangeGroupingOptions) {
    for meta in metas.iter_mut() {
        meta.group_label_value = meta
            .series
            .label_value(&grouping.group_label)
            .map(|s| s.to_string());
    }
}

fn build_grouped_labels(
    group_label_name: &str,
    group_label_value: &str,
    reducer_name_str: &str,
    source_identifiers: &[String],
) -> Vec<Option<Label>> {
    let sources = source_identifiers.join(",");
    vec![
        Some(Label {
            name: group_label_name.into(),
            value: group_label_value.to_string(),
        }),
        Some(Label {
            name: REDUCER_KEY.into(),
            value: reducer_name_str.into(),
        }),
        Some(Label {
            name: SOURCE_KEY.into(),
            value: sources,
        }),
    ]
}

fn handle_aggregation_and_grouping(
    metas: Vec<SeriesMeta>,
    options: &MRangeOptions,
    groupings: &RangeGroupingOptions,
    aggregations: &AggregationOptions,
) -> Vec<MRangeSeriesResult> {
    let grouped_series_map = group_series_by_label_internal(metas, groupings);
    let raw_series_groups: Vec<RawGroupedSeries> = grouped_series_map
        .into_iter()
        .map(|(label_value, group_data)| {
            let series_refs = group_data
                .series
                .into_iter()
                .map(|meta| (meta.series, meta.source_key))
                .collect::<Vec<_>>();
            RawGroupedSeries {
                label_value,
                series: series_refs,
                labels: group_data.labels,
            }
        })
        .collect();

    // todo: chili
    raw_series_groups
        .par_iter()
        .map(|group| {
            let key = format!("{}={}", groupings.group_label, group.label_value);
            let aggregates = aggregate_grouped_samples(group, options, aggregations);
            let samples = group_reduce(aggregates.into_iter(), groupings.aggregation);
            MRangeSeriesResult {
                key,
                group_label_value: Some(group.label_value.clone()),
                labels: group.labels.clone(),
                samples,
            }
        })
        .collect()
}

fn handle_grouping(
    metas: Vec<SeriesMeta>,
    options: &MRangeOptions,
    grouping: &RangeGroupingOptions,
) -> Vec<MRangeSeriesResult> {
    let mut grouped_series_map = group_series_by_label_internal(metas, grouping);

    grouped_series_map
        .par_iter_mut()
        .map(|(label_value, group_data)| {
            let key = format!("{}={}", grouping.group_label, label_value);
            let samples = get_grouped_raw_samples(&group_data.series, options, grouping);
            let labels = std::mem::take(&mut group_data.labels);
            MRangeSeriesResult {
                key,
                group_label_value: None,
                labels,
                samples,
            }
        })
        .collect::<Vec<_>>()
}

fn convert_labels(
    series: &TimeSeries,
    with_labels: bool,
    selected_labels: &[String],
) -> Vec<Option<Label>> {
    get_series_labels(series, with_labels, selected_labels)
        .into_iter()
        .map(|x| x.map(|label_ref| Label::new(label_ref.name, label_ref.value)))
        .collect::<Vec<Option<Label>>>()
}

fn handle_basic(metas: Vec<SeriesMeta>, options: &MRangeOptions) -> Vec<MRangeSeriesResult> {
    let mut metas = metas;
    let context_values: Vec<(String, Option<String>, Vec<Option<Label>>)> = metas
        .iter_mut()
        .map(|meta| {
            (
                std::mem::take(&mut meta.source_key),
                std::mem::take(&mut meta.group_label_value),
                convert_labels(meta.series, options.with_labels, &options.selected_labels),
            )
        })
        .collect();

    let series_refs: Vec<&TimeSeries> = metas.iter().map(|meta| meta.series).collect();
    let all_samples = get_multi_series_range(&series_refs, options);

    all_samples
        .into_iter()
        .zip(context_values)
        .map(
            |(samples, (key, group_label_value, labels))| MRangeSeriesResult {
                group_label_value,
                key,
                labels,
                samples,
            },
        )
        .collect::<Vec<_>>()
}

fn get_grouped_raw_samples(
    series_metas: &[SeriesMeta],
    options: &MRangeOptions,
    grouping_options: &RangeGroupingOptions,
) -> Vec<Sample> {
    let iterators = get_sample_iterators(series_metas, options);
    let multi_iter = MultiSeriesSampleIter::new(iterators);
    let aggregator = grouping_options.aggregation;
    group_reduce(multi_iter, aggregator)
}

fn aggregate_grouped_samples(
    group: &RawGroupedSeries<'_>,
    options: &MRangeOptions,
    aggregation_options: &AggregationOptions,
) -> Vec<Sample> {
    fn get_raw_group_sample_iterators<'a>(
        series_refs: &'a [(&'a TimeSeries, String)],
        options: &'a MRangeOptions,
    ) -> Vec<SampleIter<'a>> {
        let (start_ts, end_ts) = options.date_range.get_timestamps(None);
        series_refs
            .iter()
            .map(|(ts, _)| {
                SeriesSampleIterator::new(
                    ts,
                    start_ts,
                    end_ts,
                    &options.value_filter,
                    &options.timestamp_filter,
                )
                .into()
            })
            .collect::<Vec<SampleIter<'a>>>()
    }

    let iterators = get_raw_group_sample_iterators(&group.series, options);
    let iter = MultiSeriesSampleIter::new(iterators);
    let (start_ts, end_ts) = options.date_range.get_timestamps(None);
    aggregate_samples(iter, start_ts, end_ts, aggregation_options)
}

fn get_sample_iterators<'a>(
    series_metas: &'a [SeriesMeta],
    range_options: &'a MRangeOptions,
) -> Vec<SampleIter<'a>> {
    let (start_ts, end_ts) = range_options.date_range.get_timestamps(None);
    series_metas
        .iter()
        .map(|meta| {
            SeriesSampleIterator::new(
                meta.series,
                start_ts,
                end_ts,
                &range_options.value_filter,
                &range_options.timestamp_filter,
            )
            .into()
        })
        .collect::<Vec<SampleIter<'a>>>()
}

struct GroupedSeriesData<'a> {
    series: Vec<SeriesMeta<'a>>,
    labels: Vec<Option<Label>>,
}

struct RawGroupedSeries<'a> {
    label_value: String,
    series: Vec<(&'a TimeSeries, String)>,
    labels: Vec<Option<Label>>,
}

fn group_series_by_label_internal<'a>(
    metas: Vec<SeriesMeta<'a>>,
    grouping: &RangeGroupingOptions,
) -> AHashMap<String, GroupedSeriesData<'a>> {
    let mut grouped: AHashMap<String, GroupedSeriesData<'a>> = AHashMap::new();
    let group_by_label_name = &grouping.group_label;
    let reducer_name = grouping.aggregation.name();

    for meta in metas.into_iter() {
        if let Some(label_value_str) = &meta.group_label_value {
            let entry =
                grouped
                    .entry(label_value_str.clone())
                    .or_insert_with(|| GroupedSeriesData {
                        series: Vec::new(),
                        labels: Vec::new(),
                    });
            entry.series.push(meta);
        }
    }

    for (label_value_str, group_data) in grouped.iter_mut() {
        let source_keys: Vec<String> = group_data
            .series
            .iter()
            .map(|m| m.source_key.clone())
            .collect();
        group_data.labels = build_grouped_labels(
            group_by_label_name,
            label_value_str,
            reducer_name,
            &source_keys,
        );
    }
    grouped
}

/// Apply GROUPBY/REDUCE to the series coming from remote nodes
fn group_sharded_series(
    metas: Vec<MRangeSeriesResult>,
    grouping: &RangeGroupingOptions,
) -> Vec<MRangeSeriesResult> {
    fn handle_reducer(
        series_results: &[MRangeSeriesResult],
        grouping_opts: &RangeGroupingOptions,
    ) -> Vec<Sample> {
        let iterators: Vec<SampleIter> = series_results
            .iter()
            .map(|meta| SampleIter::Slice(meta.samples.iter()))
            .collect();
        let multi_iter = MultiSeriesSampleIter::new(iterators);
        let aggregation = grouping_opts.aggregation;
        group_reduce(multi_iter, aggregation)
    }

    let mut grouped_by_key: BTreeMap<String, Vec<MRangeSeriesResult>> = BTreeMap::new();

    for meta in metas.into_iter() {
        if let Some(label_value_for_grouping) = &meta.group_label_value {
            let key = format!("{}={}", grouping.group_label, label_value_for_grouping);
            grouped_by_key.entry(key).or_default().push(meta);
        }
    }

    let reducer_name_str = grouping.aggregation.name();
    let group_by_label_name_str = &grouping.group_label;

    // todo: chili
    grouped_by_key
        .par_iter()
        .map(|(group_key_str, series_results_in_group)| {
            let source_keys: Vec<String> = series_results_in_group
                .iter()
                .map(|m| m.key.clone())
                .collect();

            let group_defining_val_str = group_key_str
                .rsplit_once('=')
                .map(|(_, val)| val)
                .unwrap_or("");

            let samples = handle_reducer(series_results_in_group, grouping);
            let final_labels = build_grouped_labels(
                group_by_label_name_str,
                group_defining_val_str,
                reducer_name_str,
                &source_keys,
            );
            MRangeSeriesResult {
                group_label_value: None,
                key: group_key_str.clone(),
                samples,
                labels: final_labels,
            }
        })
        .collect::<Vec<_>>()
}
