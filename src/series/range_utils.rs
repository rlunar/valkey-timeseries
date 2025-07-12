use crate::aggregators::{AggregateIterator, Aggregation, AggregationHandler, Aggregator};
use crate::common::parallel::join;
use crate::common::{Sample, Timestamp};
use crate::labels::InternedLabel;
use crate::series::request_types::{AggregationOptions, MRangeOptions, RangeOptions};
use crate::series::TimeSeries;
use std::cmp::Ordering;

#[allow(dead_code)]
pub(super) fn filter_sample_by_timestamps_internal(
    timestamp: Timestamp,
    timestamps: &[Timestamp],
    index: &mut usize,
) -> Option<bool> {
    if *index >= timestamps.len() {
        return None;
    }

    let mut first_ts = timestamps[*index];
    match timestamp.cmp(&first_ts) {
        Ordering::Less => Some(false),
        Ordering::Equal => {
            *index += 1;
            Some(true)
        }
        Ordering::Greater => {
            while first_ts < timestamp && *index < timestamps.len() {
                *index += 1;
                first_ts = timestamps[*index];
                if first_ts == timestamp {
                    *index += 1;
                    return Some(true);
                }
            }
            Some(false)
        }
    }
}

#[allow(dead_code)]
pub(super) fn filter_samples_by_timestamps(samples: &mut Vec<Sample>, timestamps: &[Timestamp]) {
    let mut index = 0;
    samples.retain(move |sample| {
        filter_sample_by_timestamps_internal(sample.timestamp, timestamps, &mut index)
            .unwrap_or_default()
    });
}

pub(crate) fn get_range(
    series: &TimeSeries,
    args: &RangeOptions,
    check_retention: bool,
) -> Vec<Sample> {
    let (mut start_timestamp, mut end_timestamp) = args.date_range.get_timestamps(None);
    if check_retention && !series.retention.is_zero() {
        let min = series.get_min_timestamp();
        start_timestamp = min.max(start_timestamp);
        end_timestamp = start_timestamp.max(end_timestamp);
    }

    let mut range = series.get_range_filtered(
        start_timestamp,
        end_timestamp,
        args.timestamp_filter.as_deref(),
        args.value_filter,
    );

    if let Some(aggr_options) = &args.aggregation {
        range = aggregate_samples(
            range.into_iter(),
            start_timestamp,
            end_timestamp,
            aggr_options,
        )
    }

    if let Some(count) = args.count {
        range.truncate(count);
    }

    range
}

pub fn get_multi_series_range(
    series: &[&TimeSeries],
    range_options: &MRangeOptions,
) -> Vec<Vec<Sample>> {
    fn get_samples_internal(
        series: &[&TimeSeries],
        range_options: &RangeOptions,
    ) -> Vec<Vec<Sample>> {
        match series {
            [] => vec![],
            [meta] => {
                let samples = get_range(meta, range_options, true);
                vec![samples]
            }
            [first, second] => {
                let (first_samples, second_samples) = join(
                    || get_range(first, range_options, true),
                    || get_range(second, range_options, true),
                );
                vec![first_samples, second_samples]
            }
            _ => {
                let (first, rest) = series.split_at(series.len() / 2);
                let (mut first_samples, rest_samples) = join(
                    || get_samples_internal(first, range_options),
                    || get_samples_internal(rest, range_options),
                );

                first_samples.extend(rest_samples);
                first_samples
            }
        }
    }

    let options: RangeOptions = RangeOptions {
        date_range: range_options.date_range,
        count: range_options.count,
        aggregation: range_options.aggregation,
        timestamp_filter: range_options.timestamp_filter.clone(),
        value_filter: range_options.value_filter,
    };

    get_samples_internal(series, &options)
}

pub(crate) fn aggregate_samples<T: Iterator<Item = Sample>>(
    iter: T,
    start_ts: Timestamp,
    end_ts: Timestamp,
    aggr_options: &AggregationOptions,
) -> Vec<Sample> {
    let aligned_timestamp = aggr_options
        .alignment
        .get_aligned_timestamp(start_ts, end_ts);
    let iter = AggregateIterator::new(iter, aggr_options, aligned_timestamp);
    iter.collect::<Vec<_>>()
}

pub fn get_series_labels<'a>(
    series: &'a TimeSeries,
    with_labels: bool,
    selected_labels: &[String],
) -> Vec<Option<InternedLabel<'a>>> {
    if !with_labels && selected_labels.is_empty() {
        return vec![];
    }

    if selected_labels.is_empty() {
        series.labels.iter().map(Some).collect::<Vec<_>>()
    } else {
        selected_labels
            .iter()
            .map(|name| series.get_label(name))
            .collect::<Vec<_>>()
    }
}

/// Perform the GROUP BY REDUCE operation on the samples. Specifically, it
/// aggregates non-NAN samples based on the specified aggregation options.
pub(crate) fn group_reduce(
    samples: impl Iterator<Item = Sample>,
    aggregation: Aggregation,
) -> Vec<Sample> {
    let mut samples = samples.into_iter().filter(|sample| !sample.value.is_nan());
    let mut aggregator: Aggregator = aggregation.into();

    let mut current = if let Some(current) = samples.next() {
        aggregator.update(current.value);
        current
    } else {
        return vec![];
    };

    let mut result = vec![];

    for next in samples {
        if next.timestamp == current.timestamp {
            aggregator.update(next.value);
        } else {
            let value = aggregator.finalize();
            result.push(Sample {
                timestamp: current.timestamp,
                value,
            });
            aggregator.update(next.value);
            current = next;
        }
    }

    // Finalize last
    let value = aggregator.finalize();
    result.push(Sample {
        timestamp: current.timestamp,
        value,
    });

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregators::{Aggregation, BucketAlignment, BucketTimestamp};
    use crate::series::{TimeSeriesOptions, TimestampRange, TimestampValue, ValueFilter};
    use std::time::Duration;

    fn create_test_series() -> TimeSeries {
        let options = TimeSeriesOptions {
            retention: Some(Duration::from_secs(3600)),
            ..Default::default()
        };

        let mut series = TimeSeries::with_options(options).unwrap();

        // Add samples at 10 ms intervals from 100 to 200
        for i in 0..11 {
            let ts = 100 + (i * 10);
            let value = i as f64 * 1.5;
            series.add(ts, value, None);
        }

        series
    }

    #[test]
    fn test_get_range_basic() {
        let series = create_test_series();
        let range_options = RangeOptions {
            date_range: TimestampRange {
                start: TimestampValue::Specific(120),
                end: TimestampValue::Specific(160),
            },
            ..Default::default()
        };

        let result = get_range(&series, &range_options, true);

        assert_eq!(result.len(), 5);
        assert_eq!(result[0].timestamp, 120);
        assert_eq!(result[0].value, 3.0);
        assert_eq!(result[4].timestamp, 160);
        assert_eq!(result[4].value, 9.0);
    }

    #[test]
    fn test_get_range_with_count_limit() {
        let series = create_test_series();
        let range_options = RangeOptions {
            date_range: TimestampRange {
                start: TimestampValue::Specific(100),
                end: TimestampValue::Specific(200),
            },
            count: Some(3),
            ..Default::default()
        };

        let result = get_range(&series, &range_options, true);

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].timestamp, 100);
        assert_eq!(result[1].timestamp, 110);
        assert_eq!(result[2].timestamp, 120);
    }

    #[test]
    fn test_get_range_with_aggregation() {
        let series = create_test_series();

        let aggr_options = AggregationOptions {
            aggregation: Aggregation::Sum,
            bucket_duration: 20,
            timestamp_output: BucketTimestamp::Start,
            alignment: BucketAlignment::Start,
            report_empty: false,
        };

        let range_options = RangeOptions {
            date_range: TimestampRange {
                start: TimestampValue::Specific(100),
                end: TimestampValue::Specific(180),
            },
            aggregation: Some(aggr_options),
            ..Default::default()
        };

        let result = get_range(&series, &range_options, true);

        assert_eq!(result.len(), 5);
        // First bucket [100-120): values 0.0 + 1.5 = 1.5
        assert_eq!(result[0].timestamp, 100);
        assert_eq!(result[0].value, 1.5);
        // Second bucket [120-140): values 3.0 + 4.5 = 7.5
        assert_eq!(result[1].timestamp, 120);
        assert_eq!(result[1].value, 7.5);
    }

    #[test]
    fn test_range_with_aggregation_1() {
        // let mut series = create_test_series();
        let mut series = TimeSeries::with_options(TimeSeriesOptions::default()).unwrap();
        // Add samples at 1000 ms intervals
        for i in 0..100 {
            let ts = (i + 1) * 1000;
            let value = (i + 1) as f64 * 10.;
            series.add(ts, value, None);
        }

        // let aggr_options = AggregationOptions {
        //     aggregation: Aggregation::Sum,
        //     bucket_duration: 2000,
        //     timestamp_output: BucketTimestamp::Start,
        //     alignment: BucketAlignment::Start,
        //     report_empty: true,
        // };

        let range_options = RangeOptions {
            date_range: TimestampRange {
                start: TimestampValue::Earliest,
                end: TimestampValue::Latest,
            },
            value_filter: Some(ValueFilter::new(20.0, 50.0).unwrap()),
            ..Default::default()
        };

        let result = get_range(&series, &range_options, true);

        assert_eq!(result.len(), 5);
        // First bucket [100-120): values 0.0 + 1.5 = 1.5
        assert_eq!(result[0].timestamp, 100);
        assert_eq!(result[0].value, 1.5);
        // Second bucket [120-140): values 3.0 + 4.5 = 7.5
        assert_eq!(result[1].timestamp, 120);
        assert_eq!(result[1].value, 7.5);
    }

    #[test]
    fn test_get_range_with_value_filter() {
        let series = create_test_series();
        let range_options = RangeOptions {
            date_range: TimestampRange {
                start: TimestampValue::Specific(100),
                end: TimestampValue::Specific(200),
            },
            value_filter: Some(ValueFilter::greater_than(4.0)),
            ..Default::default()
        };

        let result = get_range(&series, &range_options, false);

        // Only samples with value > 4.0 should be included
        assert_eq!(result.len(), 8); // From timestamps 130 to 200
        assert_eq!(result[0].timestamp, 130);
        assert_eq!(result[0].value, 4.5);
    }

    #[test]
    fn test_get_range_with_timestamp_filter() {
        let series = create_test_series();
        let timestamps = vec![110, 130, 150, 170];

        let range_options = RangeOptions {
            date_range: TimestampRange {
                start: TimestampValue::Specific(100),
                end: TimestampValue::Specific(200),
            },
            timestamp_filter: Some(timestamps),
            ..Default::default()
        };

        let result = get_range(&series, &range_options, true);

        assert_eq!(result.len(), 4);
        assert_eq!(result[0].timestamp, 110);
        assert_eq!(result[1].timestamp, 130);
        assert_eq!(result[2].timestamp, 150);
        assert_eq!(result[3].timestamp, 170);
    }

    #[test]
    fn test_get_range_without_retention_check() {
        let mut series = create_test_series();
        // series.add(1000, 15.0, None); // Add a sample outside the retention period

        let range = series.last_timestamp() - series.first_timestamp;

        // Add an old timestamp that would be filtered out by retention
        series.add(50, 100.0, None);
        series.retention = Duration::from_millis(range as u64);

        let range_options = RangeOptions {
            date_range: TimestampRange {
                start: TimestampValue::Specific(0),
                end: TimestampValue::Specific(200),
            },
            ..Default::default()
        };

        // With retention check (should filter out old sample)
        let result_with_retention = get_range(&series, &range_options, true);
        // Without retention check (should include old sample)
        let result_without_retention = get_range(&series, &range_options, false);

        assert!(result_with_retention.len() < result_without_retention.len());
        assert_eq!(result_without_retention[0].timestamp, 50);
        assert_eq!(result_without_retention[0].value, 100.0);
    }

    #[test]
    fn test_get_range_empty_result() {
        let series = create_test_series();

        let range_options = RangeOptions {
            date_range: TimestampRange {
                start: TimestampValue::Specific(300),
                end: TimestampValue::Specific(400),
            },
            ..Default::default()
        };

        let result = get_range(&series, &range_options, true);

        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_get_range_with_all_filters() {
        let series = create_test_series();
        let timestamps = vec![120, 130, 140, 150, 160];

        let aggr_options = AggregationOptions {
            aggregation: Aggregation::Avg,
            bucket_duration: 20,
            timestamp_output: BucketTimestamp::Start,
            alignment: BucketAlignment::Start,
            report_empty: false,
        };

        let range_options = RangeOptions {
            date_range: TimestampRange {
                start: TimestampValue::Specific(100),
                end: TimestampValue::Specific(200),
            },
            timestamp_filter: Some(timestamps),
            value_filter: Some(ValueFilter::greater_than(3.0)),
            aggregation: Some(aggr_options),
            count: None, // count is ignored for aggregated results
        };

        let result = get_range(&series, &range_options, true);

        // Should apply timestamp filter, then value filter, then aggregate, then limit count
        assert_eq!(result.len(), 3);
    }
}
