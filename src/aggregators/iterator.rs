use crate::aggregators::{AggregationHandler, Aggregator, BucketTimestamp};
use crate::common::{Sample, Timestamp};
use crate::series::request_types::AggregationOptions;
use std::collections::VecDeque;

/// Helper class for minimizing monomorphization overhead for AggregationIterator
#[derive(Debug)]
struct AggregationHelper {
    aggregator: Aggregator,
    bucket_duration: u64,
    bucket_ts: BucketTimestamp,
    bucket_range_start: Timestamp,
    bucket_range_end: Timestamp,
    align_timestamp: Timestamp,
    last_value: f64,
    all_nans: bool,
    count: usize,
    report_empty: bool,
    empty_buckets: VecDeque<Sample>,
}

impl AggregationHelper {
    pub(crate) fn new(options: &AggregationOptions, align_timestamp: Timestamp) -> Self {
        AggregationHelper {
            align_timestamp,
            report_empty: options.report_empty,
            aggregator: options.aggregation.into(),
            bucket_duration: options.bucket_duration,
            bucket_ts: options.timestamp_output,
            bucket_range_start: 0,
            bucket_range_end: 0,
            last_value: f64::NAN,
            all_nans: true,
            count: 0,
            empty_buckets: VecDeque::new(),
        }
    }

    fn calculate_bucket_start(&self) -> Timestamp {
        self.bucket_ts
            .calculate(self.bucket_range_start, self.bucket_duration)
    }

    fn advance_current_bucket(&mut self) {
        self.bucket_range_start = self.bucket_range_end;
        self.bucket_range_end = self
            .bucket_range_start
            .saturating_add_unsigned(self.bucket_duration);
    }

    fn calculate_final_value(&self) -> f64 {
        if self.all_nans {
            if self.count == 0 {
                self.aggregator.empty_value()
            } else {
                f64::NAN
            }
        } else {
            self.aggregator.finalize()
        }
    }

    fn get_empty_bucket_value(&self) -> f64 {
        match self.aggregator {
            Aggregator::Last(_) => self.last_value,
            _ => self.aggregator.empty_value(),
        }
    }

    fn finalize_current_bucket(&mut self) -> Sample {
        let value = self.calculate_final_value();
        let timestamp = self.calculate_bucket_start();
        let bucket = Sample { timestamp, value };

        // Reset aggregator state for the next bucket
        self.aggregator.reset();
        self.count = 0;
        self.all_nans = true;

        bucket
    }

    fn update_value(&mut self, value: f64) {
        if !value.is_nan() {
            self.aggregator.update(value);
            self.last_value = value;
            self.all_nans = false;
        }
        self.count += 1;
    }

    fn update_sample(&mut self, sample: Sample) -> Option<Sample> {
        // If the sample falls within the current bucket range, update the value
        if sample.timestamp < self.bucket_range_end {
            self.update_value(sample.value);
            return None;
        }

        // Finalize the current bucket if it has any data
        let mut bucket = None;
        if self.count > 0 {
            bucket = Some(self.finalize_current_bucket());
        }

        let gap = sample.timestamp - self.bucket_range_end;

        // If the gap exceeds the bucket duration, we possibly need to handle empty buckets
        if gap >= self.bucket_duration as i64 {
            if self.report_empty {
                // Fill the gap with empty buckets
                self.fill_empty_buckets_gap(self.bucket_range_end, sample.timestamp);
                if bucket.is_none() {
                    bucket = self.empty_buckets.pop_front()
                }
            }
            // Update bucket timestamps to align with the new sample
            self.update_bucket_timestamps(sample.timestamp);
        } else {
            self.advance_current_bucket();
        }

        self.update_value(sample.value);

        bucket
    }

    fn fill_empty_buckets_gap(&mut self, gap_start: Timestamp, gap_end: Timestamp) {
        let empty_value = self.get_empty_bucket_value();

        let first_bucket_start = self.calc_bucket_start(gap_start + 1);
        let last_bucket_start = self.calc_bucket_start(gap_end);

        let bucket_count =
            ((last_bucket_start - first_bucket_start) / self.bucket_duration as i64) as usize;
        self.empty_buckets.reserve(bucket_count);

        // Fill all empty buckets in the gap
        let mut current_bucket_start = first_bucket_start;
        while current_bucket_start < last_bucket_start {
            let bucket_timestamp = self
                .bucket_ts
                .calculate(current_bucket_start, self.bucket_duration);
            self.empty_buckets.push_back(Sample {
                timestamp: bucket_timestamp,
                value: empty_value,
            });
            current_bucket_start += self.bucket_duration as i64;
        }
    }

    fn update_bucket_timestamps(&mut self, start_timestamp: Timestamp) {
        self.bucket_range_start = self.calc_bucket_start(start_timestamp).max(0) as Timestamp;
        self.bucket_range_end = self
            .bucket_range_start
            .saturating_add_unsigned(self.bucket_duration);
    }

    fn calc_bucket_start(&self, ts: Timestamp) -> Timestamp {
        let diff = ts - self.align_timestamp;
        let delta = self.bucket_duration as i64;
        ts - ((diff % delta + delta) % delta)
    }
}

pub fn aggregate(
    options: &AggregationOptions,
    aligned_timestamp: Timestamp,
    iter: impl Iterator<Item = Sample>,
) -> Vec<Sample> {
    let iterator = AggregateIterator::new(iter, options, aligned_timestamp);
    iterator.collect()
}

pub struct AggregateIterator<T: Iterator<Item = Sample>> {
    inner: T,
    aggregator: AggregationHelper,
    init: bool,
}

impl<T: Iterator<Item = Sample>> AggregateIterator<T> {
    pub fn new(inner: T, options: &AggregationOptions, aligned_timestamp: Timestamp) -> Self {
        let aggregator = AggregationHelper::new(options, aligned_timestamp);
        Self {
            inner,
            aggregator,
            init: false,
        }
    }

    fn next_bucket(&mut self) -> Option<Sample> {
        if let Some(bucket) = self.aggregator.empty_buckets.pop_front() {
            return Some(bucket);
        }
        for sample in self.inner.by_ref() {
            if let Some(bucket) = self.aggregator.update_sample(sample) {
                return Some(bucket);
            }
        }
        if self.aggregator.count > 0 {
            return Some(self.aggregator.finalize_current_bucket());
        }
        None
    }
}

impl<T: Iterator<Item = Sample>> Iterator for AggregateIterator<T> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        // Handle initialization with the first sample
        if !self.init {
            if let Some(sample) = self.inner.next() {
                self.init = true;
                self.aggregator.update_bucket_timestamps(sample.timestamp);
                self.aggregator.update_value(sample.value);
            } else {
                return None; // Empty input stream
            }
        }

        self.next_bucket()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregators::{Aggregation, BucketAlignment, BucketTimestamp};
    use crate::common::Sample;
    use crate::series::{TimeSeries, TimeSeriesOptions};

    fn create_test_samples() -> Vec<Sample> {
        vec![
            Sample::new(10, 1.0),
            Sample::new(15, 2.0),
            Sample::new(20, 3.0),
            Sample::new(30, 4.0),
            Sample::new(40, 5.0),
            Sample::new(50, 6.0),
            Sample::new(60, 7.0),
        ]
    }

    fn create_options(aggregator: Aggregation) -> AggregationOptions {
        AggregationOptions {
            aggregation: aggregator,
            bucket_duration: 10,
            timestamp_output: BucketTimestamp::Start,
            alignment: BucketAlignment::Start,
            report_empty: false,
        }
    }

    #[test]
    fn test_sum_aggregation() {
        let samples = create_test_samples();
        let options = create_options(Aggregation::Sum);

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(
            result,
            vec![
                Sample::new(10, 3.0),
                Sample::new(20, 3.0),
                Sample::new(30, 4.0),
                Sample::new(40, 5.0),
                Sample::new(50, 6.0),
                Sample::new(60, 7.0),
            ]
        );
    }

    #[test]
    fn test_avg_aggregation() {
        let samples = create_test_samples();
        let options = create_options(Aggregation::Avg);

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 6);
        assert_eq!(result[0].timestamp, 10);
        assert_eq!(result[0].value, 1.5); // (1.0 + 2.0) / 2
    }

    #[test]
    fn test_max_aggregation() {
        let samples = create_test_samples();
        let options = create_options(Aggregation::Max);

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 6);
        assert_eq!(result[0].timestamp, 10);
        assert_eq!(result[0].value, 2.0); // max of 1.0 and 2.0
    }

    #[test]
    fn test_min_aggregation() {
        let samples = create_test_samples();
        let options = create_options(Aggregation::Min);

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(
            result,
            vec![
                Sample::new(10, 1.0),
                Sample::new(20, 3.0),
                Sample::new(30, 4.0),
                Sample::new(40, 5.0),
                Sample::new(50, 6.0),
                Sample::new(60, 7.0),
            ]
        );
    }

    #[test]
    fn test_count_aggregation() {
        let samples = create_test_samples();
        let options = create_options(Aggregation::Count);

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(
            result,
            vec![
                Sample::new(10, 2.0),
                Sample::new(20, 1.0),
                Sample::new(30, 1.0),
                Sample::new(40, 1.0),
                Sample::new(50, 1.0),
                Sample::new(60, 1.0),
            ]
        );
    }

    #[test]
    fn test_bucket_timestamp_end() {
        let samples = create_test_samples();
        let mut options = create_options(Aggregation::Sum);
        options.timestamp_output = BucketTimestamp::End;

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 6);
        assert_eq!(result[0].timestamp, 20); // End of the first bucket
        assert_eq!(result[0].value, 3.0);
    }

    #[test]
    fn test_bucket_timestamp_mid() {
        let samples = create_test_samples();
        let mut options = create_options(Aggregation::Sum);
        options.timestamp_output = BucketTimestamp::Mid;

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 6);
        assert_eq!(result[0].timestamp, 15); // Mid of the first bucket
        assert_eq!(result[0].value, 3.0);
    }

    #[test]
    fn test_empty_input() {
        let samples: Vec<Sample> = vec![];
        let options = create_options(Aggregation::Sum);

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 0); // Last bucket with default value
                                     // assert!(result[0].value.is_nan() || result[0].value == 0.0);
    }

    #[test]
    fn test_empty_buckets_report_empty() {
        let mut series = TimeSeries::with_options(TimeSeriesOptions::default()).unwrap();

        // Create a series with gaps to test empty bucket behavior
        series.add(100, 10.0, None);
        series.add(110, 20.0, None);
        // Gap from 120-149 (no samples)
        series.add(150, 30.0, None);
        series.add(160, 40.0, None);
        // Gap from 170-199 (no samples)
        series.add(200, 50.0, None);

        let options = AggregationOptions {
            aggregation: Aggregation::Sum,
            bucket_duration: 25, // 25ms buckets
            timestamp_output: BucketTimestamp::Start,
            alignment: BucketAlignment::Start,
            report_empty: true,
        };

        let iter = AggregateIterator::new(series.iter(), &options, 0);
        let result = iter.collect::<Vec<Sample>>();

        // With 25ms buckets over 100ms range (100-200), we should get buckets:
        // [100-125): contains samples at 100, 110 -> sum = 10.0 + 20.0 = 30.0
        // [125-150): empty bucket -> sum = 0.0 (with report_empty=true)
        // [150-175): contains samples at 150, 160 -> sum = 30.0 + 40.0 = 70.0
        // [175-200): empty bucket -> sum = 0.0 (with report_empty=true)
        // [200-225): contains sample at 200 -> sum = 50.0

        // Should have 4 buckets including empty ones
        assert_eq!(result.len(), 5);

        // Verify bucket timestamps
        assert_eq!(result[0].timestamp, 100);
        assert_eq!(result[1].timestamp, 125);
        assert_eq!(result[2].timestamp, 150);
        assert_eq!(result[3].timestamp, 175);
        assert_eq!(result[4].timestamp, 200);

        // Verify values including empty buckets
        assert_eq!(result[0].value, 30.0); // Sum of 10.0 + 20.0
        assert_eq!(result[1].value, 0.0); // Empty bucket
        assert_eq!(result[2].value, 70.0); // Sum of 30.0 + 40.0
        assert_eq!(result[3].value, 0.0); // Empty bucket
        assert_eq!(result[4].value, 50.0); // Single sample at 200
    }

    #[test]
    fn test_empty_buckets_dont_report_empty() {
        let mut series = TimeSeries::with_options(TimeSeriesOptions::default()).unwrap();

        // Create a series with gaps
        series.add(100, 10.0, None);
        series.add(110, 20.0, None);
        // Gap from 125-149 (no samples)
        series.add(150, 30.0, None);
        series.add(160, 40.0, None);

        let options = AggregationOptions {
            aggregation: Aggregation::Sum,
            bucket_duration: 25, // 25ms buckets
            timestamp_output: BucketTimestamp::Start,
            alignment: BucketAlignment::Start,
            report_empty: false, // This should NOT include empty buckets
        };

        let iter = AggregateIterator::new(series.iter(), &options, 0);
        let result = iter.collect::<Vec<Sample>>();

        // With report_empty=false, only non-empty buckets should be returned
        // [100-125): contains samples at 100, 110 -> sum = 30.0
        // [125-150): empty bucket -> skipped
        // [150-175): contains samples at 150, 160 -> sum = 70.0

        // Should have only 2 buckets (empty ones excluded)
        assert_eq!(result.len(), 2);

        // Verify bucket timestamps and values
        assert_eq!(result[0].timestamp, 100);
        assert_eq!(result[0].value, 30.0); // Sum of 10.0 + 20.0
        assert_eq!(result[1].timestamp, 150);
        assert_eq!(result[1].value, 70.0); // Sum of 30.0 + 40.0
    }

    #[test]
    fn test_empty_buckets_last() {
        let samples = vec![
            Sample::new(10, 1.0),
            Sample::new(15, 99.0),
            // Gap at 20-30
            Sample::new(40, 5.0),
            Sample::new(50, 6.0),
        ];

        let mut options = create_options(Aggregation::Last);
        options.report_empty = true;

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 5);
        assert_eq!(result[0].timestamp, 10);
        assert_eq!(result[0].value, 99.0);
        assert_eq!(result[1].timestamp, 20);
        assert_eq!(result[1].value, 99.0); // Empty bucket with value 0 for sum
        assert_eq!(result[2].timestamp, 30);
        assert_eq!(result[2].value, 99.0); // Empty bucket
        assert_eq!(result[3].timestamp, 40);
        assert_eq!(result[3].value, 5.0);
        assert_eq!(result[4].timestamp, 50);
        assert_eq!(result[4].value, 6.0);
    }

    #[test]
    fn test_no_alignment() {
        // Control test. Should not modify the timestamps
        let samples = vec![
            Sample::new(1000, 100.0),
            Sample::new(1010, 110.0),
            Sample::new(1020, 120.0),
            Sample::new(2000, 200.0),
            Sample::new(2010, 210.0),
            Sample::new(2020, 220.0),
        ];

        let options = AggregationOptions {
            aggregation: Aggregation::Min,
            bucket_duration: 20, // 20ms buckets
            timestamp_output: BucketTimestamp::Start,
            alignment: BucketAlignment::Default, // Default alignment = 0
            report_empty: false,                 // This should NOT include empty buckets
        };

        let iterator = AggregateIterator::new(
            samples.into_iter(),
            &options,
            0, // Aligned timestamp is 10
        );

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(
            result,
            vec![
                Sample::new(1000, 100.0),
                Sample::new(1020, 120.0),
                Sample::new(2000, 200.0),
                Sample::new(2020, 220.0),
            ]
        );
    }

    #[test]
    fn test_with_alignment() {
        let samples = vec![
            Sample::new(1000, 100.0),
            Sample::new(1010, 110.0),
            Sample::new(1020, 120.0),
            Sample::new(2000, 200.0),
            Sample::new(2010, 210.0),
            Sample::new(2020, 220.0),
            Sample::new(3000, 300.0),
            Sample::new(3010, 310.0),
            Sample::new(3020, 320.0),
        ];

        let options = AggregationOptions {
            aggregation: Aggregation::Min,
            bucket_duration: 20, // 20ms buckets
            timestamp_output: BucketTimestamp::Start,
            alignment: BucketAlignment::Default, // Default alignment = 0
            report_empty: false,                 // This should NOT include empty buckets
        };

        let iterator = AggregateIterator::new(
            samples.into_iter(),
            &options,
            10, // Aligned timestamp is 10
        );

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(
            result,
            vec![
                Sample::new(990, 100.0),  // Bucket [990, 1010)
                Sample::new(1010, 110.0), // Bucket [1010, 1030)
                Sample::new(1990, 200.0), // Bucket [1990, 2010)
                Sample::new(2010, 210.0), // Bucket [2010, 2030)
                Sample::new(2990, 300.0), // Bucket [2990, 3010)
                Sample::new(3010, 310.0), // Bucket [3010, 3030)
            ]
        );
    }

    #[test]
    fn test_range_aggregation_basic() {
        let samples = vec![
            Sample::new(10, 1.0),
            Sample::new(15, 5.0),
            Sample::new(20, 2.0),
            Sample::new(25, 8.0),
            Sample::new(30, 3.0),
            Sample::new(35, 7.0),
        ];

        let options = AggregationOptions {
            aggregation: Aggregation::Range,
            bucket_duration: 10,
            timestamp_output: BucketTimestamp::Start,
            alignment: BucketAlignment::Start,
            report_empty: false,
        };

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);
        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 3);

        // First bucket [10, 20): contains 1.0 and 5.0, range = 5.0 - 1.0 = 4.0
        assert_eq!(result[0].timestamp, 10);
        assert_eq!(result[0].value, 4.0);

        // Second bucket [20, 30): contains 2.0 and 8.0, range = 8.0 - 2.0 = 6.0
        assert_eq!(result[1].timestamp, 20);
        assert_eq!(result[1].value, 6.0);

        // Third bucket [30, 40): contains 3.0 and 7.0, range = 7.0 - 3.0 = 4.0
        assert_eq!(result[2].timestamp, 30);
        assert_eq!(result[2].value, 4.0);
    }

    // #[test]
    // fn test_alignment_with_offset() {
    //     let samples = vec![
    //         Sample::new(12, 1.0),
    //         Sample::new(17, 2.0),
    //         Sample::new(22, 3.0),
    //         Sample::new(32, 4.0),
    //     ];
    //
    //     let options = create_options(Aggregator::Sum);
    //
    //     let iterator = AggregateIterator::new(
    //         samples.into_iter(),
    //         options,
    //         2, // Aligned timestamp is 2
    //     );
    //
    //     let result: Vec<Sample> = iterator.collect();
    //
    //     // With alignment at 2, buckets should be [2, 12), [12, 22), [22, 32), [32, 42)
    //     assert_eq!(result.len(), 4);
    //     assert_eq!(result[0].timestamp, 2);  // First bucket starts at alignment point
    //     assert_eq!(result[0].value, 0.0);    // No values in this bucket
    //     assert_eq!(result[1].timestamp, 12); // Second bucket
    //     assert_eq!(result[1].value, 3.0);    // Sum of 1.0 and 2.0
    //     assert_eq!(result[2].timestamp, 22);
    //     assert_eq!(result[2].value, 3.0);
    //     assert_eq!(result[3].timestamp, 32);
    //     assert_eq!(result[3].value, 4.0);
    // }
}
