use super::common::{
    decode_label, deserialize_timestamp_range, load_flatbuffers_object, serialize_timestamp_range,
};
use super::matchers::{deserialize_matchers_list, serialize_matchers_list};
use super::request_generated::{
    AggregationOptions as FBAggregationOptions, AggregationOptionsBuilder, AggregationType,
    BucketAlignmentType, BucketTimestampType, GroupingOptions, GroupingOptionsArgs,
    MultiRangeRequest as FBMultiRangeRequest, MultiRangeRequestArgs, ValueRangeFilter,
};
use super::response_generated::{
    Label as ResponseLabel, LabelArgs, MultiRangeResponse as FBMultiRangeResponse,
    MultiRangeResponseArgs, SeriesResponse as FBSeriesResponse, SeriesResponseArgs,
};
use crate::aggregators::{Aggregation, BucketAlignment, BucketTimestamp};
use crate::commands::process_mrange_query;
use crate::common::Timestamp;
use crate::fanout::request::serialization::{Deserialized, Serialized};
use crate::fanout::request::series_chunk::{deserialize_sample_data, serialize_sample_data};
use crate::fanout::serialization::samples_to_chunk;
use crate::fanout::{CommandMessageType, MultiShardCommand, TrackerEnum};
use crate::labels::SeriesLabel;
use crate::series::request_types::{
    AggregationOptions, MRangeOptions, MRangeSeriesResult, RangeGroupingOptions,
};
use crate::series::ValueFilter;
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use smallvec::SmallVec;
use valkey_module::{Context, ValkeyError, ValkeyResult};

impl Serialized for MRangeOptions {
    fn serialize(&self, buf: &mut Vec<u8>) {
        serialize_multi_range_request(buf, self);
    }
}

impl Deserialized for MRangeOptions {
    fn deserialize(buf: &[u8]) -> ValkeyResult<Self> {
        deserialize_multi_range_request(buf)
    }
}

pub struct MultiRangeCommand;

impl MultiShardCommand for MultiRangeCommand {
    type REQ = MRangeOptions;
    type RES = MultiRangeResponse;
    fn request_type() -> CommandMessageType {
        CommandMessageType::MultiRangeQuery
    }

    fn exec(ctx: &Context, req: Self::REQ) -> ValkeyResult<Self::RES> {
        // Execute mrange query w/o grouping
        // For grouping, all raw or aggregated data should be sent to the requesting node
        // since grouping is cross-series. However, we need to take care of the case where
        // we group by a label that is not selected either by WITH_LABELS or SELECTED_LABELS.
        //
        let series = process_mrange_query(ctx, req, false)?;
        Ok(MultiRangeResponse { series })
    }

    fn update_tracker(tracker: &TrackerEnum, res: Self::RES) {
        if let TrackerEnum::MultiRangeQuery(t) = tracker {
            t.update(res);
        } else {
            panic!("BUG: Invalid MultiRangeRequest tracker type");
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct MultiRangeResponse {
    pub series: Vec<MRangeSeriesResult>,
}

impl Serialized for MultiRangeResponse {
    fn serialize(&self, buf: &mut Vec<u8>) {
        let mut bldr = FlatBufferBuilder::with_capacity(512);

        let mut series = Vec::with_capacity(self.series.len());
        for item in &self.series {
            let value = encode_series_response(&mut bldr, item);
            series.push(value);
        }

        let sample_values = bldr.create_vector(&series);
        let response_args = MultiRangeResponseArgs {
            series: Some(sample_values),
        };

        let obj = FBMultiRangeResponse::create(&mut bldr, &response_args);
        bldr.finish(obj, None);

        let data = bldr.finished_data();
        buf.extend_from_slice(data);
    }
}

impl Deserialized for MultiRangeResponse {
    fn deserialize(buf: &[u8]) -> ValkeyResult<Self> {
        // Get access to the root:
        let req = load_flatbuffers_object::<FBMultiRangeResponse>(buf, "MultiRangeResponse")?;
        let mut result: MultiRangeResponse = MultiRangeResponse::default();

        if let Some(values) = req.series() {
            for item in values.iter() {
                let value = decode_series_response(&item);
                result.series.push(value);
            }
        }

        Ok(result)
    }
}

pub(super) fn serialize_multi_range_request(buf: &mut Vec<u8>, request: &MRangeOptions) {
    let mut bldr = FlatBufferBuilder::with_capacity(512);

    let range = serialize_timestamp_range(&mut bldr, Some(request.date_range));
    let mut labels: SmallVec<_, 8> = SmallVec::new();
    for label in &request.selected_labels {
        let name = bldr.create_string(label.as_str());
        labels.push(name);
    }
    let selected_labels = bldr.create_vector(&labels);
    let timestamp_filter = request
        .timestamp_filter
        .as_ref()
        .map(|timestamps| bldr.create_vector(timestamps.as_slice()));

    let value_filter_value = if let Some(value_filter) = &request.value_filter {
        ValueRangeFilter::new(value_filter.min, value_filter.max)
    } else {
        ValueRangeFilter::new(0.0, f64::MAX)
    };

    let aggregation = if let Some(agg) = &request.aggregation {
        let aggregation = serialize_aggregation_options(&mut bldr, agg);
        Some(aggregation)
    } else {
        None
    };

    let grouping = if let Some(group) = &request.grouping {
        let aggregator = encode_aggregator_enum(&group.aggregation);
        let group_label = bldr.create_string(group.group_label.as_str());
        let options = GroupingOptions::create(
            &mut bldr,
            &GroupingOptionsArgs {
                group_label: Some(group_label),
                aggregator,
            },
        );
        Some(options)
    } else {
        None
    };

    let count = request.count.unwrap_or(0) as u32;

    let filter = serialize_matchers_list(&mut bldr, &request.filters);

    let args = MultiRangeRequestArgs {
        range,
        with_labels: request.with_labels,
        selected_labels: Some(selected_labels),
        filters: Some(filter),
        value_filter: if request.value_filter.is_some() {
            Some(&value_filter_value)
        } else {
            None
        },
        timestamp_filter,
        count,
        aggregation,
        grouping,
    };

    let obj = FBMultiRangeRequest::create(&mut bldr, &args);
    bldr.finish(obj, None);

    let data = bldr.finished_data();
    buf.extend_from_slice(data);
}

fn serialize_aggregation_options<'a>(
    bldr: &mut FlatBufferBuilder<'a>,
    agg: &AggregationOptions,
) -> WIPOffset<FBAggregationOptions<'a>> {
    let aggregator = encode_aggregator_enum(&agg.aggregation);
    let (bucket_alignment, timestamp) = encode_bucket_alignment(agg.alignment);
    let alignment_timestamp = timestamp.unwrap_or(0);
    let timestamp_output = encode_bucket_timestamp(agg.timestamp_output);
    let mut builder = AggregationOptionsBuilder::new(bldr);

    builder.add_aggregator(aggregator);
    builder.add_bucket_duration(agg.bucket_duration);
    builder.add_timestamp_output(timestamp_output);
    builder.add_alignment_timestamp(alignment_timestamp);
    builder.add_bucket_alignment(bucket_alignment);
    builder.add_report_empty(agg.report_empty);

    builder.finish()
}

fn encode_bucket_timestamp(bucket_timestamp: BucketTimestamp) -> BucketTimestampType {
    match bucket_timestamp {
        BucketTimestamp::Start => BucketTimestampType::Start,
        BucketTimestamp::End => BucketTimestampType::End,
        BucketTimestamp::Mid => BucketTimestampType::Mid,
    }
}

fn decode_bucket_timestamp(timestamp: BucketTimestampType) -> BucketTimestamp {
    match timestamp {
        BucketTimestampType::Start => BucketTimestamp::Start,
        BucketTimestampType::End => BucketTimestamp::End,
        BucketTimestampType::Mid => BucketTimestamp::Mid,
        _ => unreachable!("Unsupported bucket timestamp type in decode_bucket_timestamp"),
    }
}

fn encode_bucket_alignment(
    bucket_alignment: BucketAlignment,
) -> (BucketAlignmentType, Option<Timestamp>) {
    match bucket_alignment {
        BucketAlignment::Default => (BucketAlignmentType::Default, None),
        BucketAlignment::Start => (BucketAlignmentType::Start, None),
        BucketAlignment::End => (BucketAlignmentType::End, None),
        BucketAlignment::Timestamp(ts) => (BucketAlignmentType::Timestamp, Some(ts)),
    }
}

fn decode_bucket_alignment(alignment: BucketAlignmentType) -> BucketAlignment {
    match alignment {
        BucketAlignmentType::Default => BucketAlignment::Default,
        BucketAlignmentType::Start => BucketAlignment::Start,
        BucketAlignmentType::End => BucketAlignment::End,
        BucketAlignmentType::Timestamp => BucketAlignment::Timestamp(0),
        BucketAlignmentType(i8::MIN..=-1_i8) | BucketAlignmentType(4_i8..=i8::MAX) => todo!(),
    }
}

fn encode_aggregator_enum(aggregator: &Aggregation) -> AggregationType {
    match aggregator {
        Aggregation::Sum => AggregationType::Sum,
        Aggregation::Min => AggregationType::Min,
        Aggregation::Max => AggregationType::Max,
        Aggregation::Avg => AggregationType::Avg,
        Aggregation::Count => AggregationType::Count,
        Aggregation::First => AggregationType::First,
        Aggregation::Last => AggregationType::Last,
        Aggregation::StdS => AggregationType::StdS,
        Aggregation::StdP => AggregationType::StdP,
        Aggregation::VarP => AggregationType::VarP,
        Aggregation::VarS => AggregationType::VarS,
        Aggregation::Range => AggregationType::Range,
    }
}

fn decode_aggregator(agg_type: AggregationType) -> Aggregation {
    match agg_type {
        AggregationType::Sum => Aggregation::Sum,
        AggregationType::Avg => Aggregation::Avg,
        AggregationType::Min => Aggregation::Min,
        AggregationType::Max => Aggregation::Max,
        AggregationType::First => Aggregation::First,
        AggregationType::Last => Aggregation::Last,
        AggregationType::Count => Aggregation::Count,
        AggregationType::Range => Aggregation::Range,
        AggregationType::StdS => Aggregation::StdS,
        AggregationType::StdP => Aggregation::StdP,
        AggregationType::VarS => Aggregation::VarS,
        AggregationType::VarP => Aggregation::VarP,
        _ => unreachable!("Unsupported aggregator type in decode_aggregator"),
    }
}

fn deserialize_aggregation_options(agg: FBAggregationOptions) -> ValkeyResult<AggregationOptions> {
    let _type = agg.aggregator();
    let aggregator = decode_aggregator(_type);
    let bucket_duration = agg.bucket_duration();
    if bucket_duration == 0 {
        return Err(ValkeyError::Str("TSDB: bucket duration must be positive"));
    }
    let timestamp_output = decode_bucket_timestamp(agg.timestamp_output());

    let _align = agg.bucket_alignment();
    let mut alignment = decode_bucket_alignment(_align);
    if _align == BucketAlignmentType::Timestamp {
        let timestamp = agg.alignment_timestamp();
        alignment = BucketAlignment::Timestamp(timestamp);
    }

    let report_empty = agg.report_empty();

    Ok(AggregationOptions {
        aggregation: aggregator,
        bucket_duration,
        timestamp_output,
        alignment,
        report_empty,
    })
}

fn decode_grouping_options(reader: &GroupingOptions) -> RangeGroupingOptions {
    let group_label = reader.group_label().unwrap_or_default().to_string();
    let aggregator = decode_aggregator(reader.aggregator());
    RangeGroupingOptions {
        group_label,
        aggregation: aggregator,
    }
}

pub fn deserialize_multi_range_request(buf: &[u8]) -> ValkeyResult<MRangeOptions> {
    let req = load_flatbuffers_object::<FBMultiRangeRequest>(buf, "MultiRangeRequest")?;

    let date_range = deserialize_timestamp_range(req.range())?.unwrap_or_default();

    let count = if req.count() > 0 {
        Some(req.count() as usize)
    } else {
        None
    };

    let aggregation = if let Some(aggregation) = req.aggregation() {
        let options = deserialize_aggregation_options(aggregation)?;
        Some(options)
    } else {
        None
    };

    let timestamp_filter = req
        .timestamp_filter()
        .map(|filter| filter.iter().collect::<Vec<_>>());

    let value_filter = req.value_filter().map(|filter| ValueFilter {
        min: filter.min(),
        max: filter.max(),
    });

    let filters = deserialize_matchers_list(req.filters())?;

    let with_labels = req.with_labels();

    let selected_labels = if let Some(label_list) = req.selected_labels() {
        label_list
            .iter()
            .map(|label| label.to_string())
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    let grouping = if let Some(group) = req.grouping() {
        let grouping = decode_grouping_options(&group);
        Some(grouping)
    } else {
        None
    };

    Ok(MRangeOptions {
        date_range,
        count,
        aggregation,
        timestamp_filter,
        value_filter,
        filters,
        with_labels,
        selected_labels,
        grouping,
    })
}

// todo: Return result instead of panic
fn encode_series_response<'a>(
    bldr: &mut FlatBufferBuilder<'a>,
    response: &MRangeSeriesResult,
) -> WIPOffset<FBSeriesResponse<'a>> {
    let key = bldr.create_string(&response.key);
    let group_label_value = response
        .group_label_value
        .as_ref()
        .map(|label_value| bldr.create_string(label_value.as_str()));

    let mut labels = Vec::with_capacity(response.labels.len());
    for label in &response.labels {
        if let Some(label) = label {
            let name = bldr.create_string(label.name.as_str());
            let value = bldr.create_string(label.value.as_str());
            let label = ResponseLabel::create(
                bldr,
                &LabelArgs {
                    name: Some(name),
                    value: Some(value),
                },
            );
            labels.push(label);
        } else {
            labels.push(ResponseLabel::create(
                bldr,
                &LabelArgs {
                    name: None,
                    value: None,
                },
            ));
        }
    }

    let labels = bldr.create_vector(&labels);
    let sample_chunk =
        samples_to_chunk(&response.samples).expect("Failed to convert samples to chunk");

    let sample_data =
        serialize_sample_data(bldr, sample_chunk).expect("Failed to serialize sample data");

    FBSeriesResponse::create(
        bldr,
        &SeriesResponseArgs {
            key: Some(key),
            group_label_value,
            labels: Some(labels),
            samples: Some(sample_data),
        },
    )
}

fn decode_series_response(reader: &FBSeriesResponse) -> MRangeSeriesResult {
    let key = reader.key().unwrap_or_default().to_string();
    let group_label_value = reader.group_label_value().map(|x| x.to_string());
    let samples = match reader.samples().as_ref() {
        Some(sample_data) => {
            let chunk =
                deserialize_sample_data(sample_data).expect("Failed to deserialize sample data");
            chunk.iter().collect()
        }
        None => Vec::new(),
    };

    let labels = reader
        .labels()
        .map(|labels| {
            labels
                .iter()
                .map(|x| {
                    let label = decode_label(x);
                    if !label.name().is_empty() {
                        Some(label)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    MRangeSeriesResult {
        key,
        group_label_value,
        samples,
        labels,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregators::{Aggregation, BucketAlignment, BucketTimestamp};
    use crate::common::Sample;
    use crate::labels::matchers::{
        Matcher, MatcherSetEnum, Matchers, PredicateMatch, PredicateValue,
    };
    use crate::labels::Label;
    use crate::series::TimestampRange;

    fn make_sample_matchers() -> Matchers {
        Matchers {
            name: Some("mrange-test-x".to_string()),
            matchers: MatcherSetEnum::And(vec![
                Matcher {
                    label: "mrange-foo-x".to_string(),
                    matcher: PredicateMatch::Equal(PredicateValue::String("bar".to_string())),
                },
                Matcher {
                    label: "mrange-baz-x".to_string(),
                    matcher: PredicateMatch::NotEqual(PredicateValue::String("qux".to_string())),
                },
            ]),
        }
    }

    fn make_matchers_vec() -> Vec<Matchers> {
        vec![
            Matchers {
                name: Some("mrange-test".to_string()),
                matchers: MatcherSetEnum::And(vec![
                    Matcher {
                        label: "mrange-foo".to_string(),
                        matcher: PredicateMatch::Equal(PredicateValue::String("bar".to_string())),
                    },
                    Matcher {
                        label: "mrange-baz".to_string(),
                        matcher: PredicateMatch::NotEqual(PredicateValue::String(
                            "qux".to_string(),
                        )),
                    },
                ]),
            },
            make_sample_matchers(),
        ]
    }

    #[test]
    fn test_range_options_request_simple_serialize_deserialize() {
        let req = MRangeOptions {
            date_range: TimestampRange::from_timestamps(100, 200).unwrap(),
            filters: make_matchers_vec(),
            with_labels: true,
            selected_labels: vec!["label1".to_string(), "label2".to_string()],
            timestamp_filter: None,
            value_filter: None,
            aggregation: None,
            grouping: None,
            count: None,
        };

        let mut buf = Vec::new();
        req.serialize(&mut buf);

        let req2 = MRangeOptions::deserialize(&buf).expect("deserialization failed");
        assert_eq!(req.date_range, req2.date_range);
        assert_eq!(req.with_labels, req2.with_labels);
        assert_eq!(req.selected_labels, req2.selected_labels);
        assert_eq!(req.timestamp_filter, req2.timestamp_filter);
        assert_eq!(req.count, req2.count);
        assert_eq!(req.filters, req2.filters);
        assert!(req2.aggregation.is_none());
        assert!(req2.grouping.is_none());
    }

    #[test]
    fn test_multi_range_request_with_filters_serialize_deserialize() {
        let req = MRangeOptions {
            date_range: TimestampRange::from_timestamps(100, 200).unwrap(),
            filters: make_matchers_vec(),
            with_labels: true,
            selected_labels: vec!["label1".to_string(), "label2".to_string()],
            timestamp_filter: Some(vec![105, 110, 115]),
            value_filter: Some(ValueFilter {
                min: 10.0,
                max: 100.0,
            }),
            aggregation: None,
            grouping: None,
            count: Some(50),
        };

        let mut buf = Vec::new();
        req.serialize(&mut buf);

        let req2 = MRangeOptions::deserialize(&buf).expect("deserialization failed");
        assert_eq!(
            req.timestamp_filter.as_ref().unwrap(),
            req2.timestamp_filter.as_ref().unwrap()
        );

        let vf1 = req.value_filter.as_ref().unwrap();
        let vf2 = req2.value_filter.as_ref().unwrap();
        assert_eq!(vf1.min, vf2.min);
        assert_eq!(vf1.max, vf2.max);

        assert_eq!(req.count, req2.count);
    }

    #[test]
    fn test_multi_range_request_with_aggregation_serialize_deserialize() {
        let agg_options = AggregationOptions {
            aggregation: Aggregation::Avg,
            bucket_duration: 60,
            timestamp_output: BucketTimestamp::Mid,
            alignment: BucketAlignment::Start,
            report_empty: true,
        };

        let req = MRangeOptions {
            date_range: TimestampRange::from_timestamps(100, 200).unwrap(),
            filters: make_matchers_vec(),
            with_labels: true,
            selected_labels: vec!["label1".to_string()],
            timestamp_filter: None,
            value_filter: None,
            aggregation: Some(agg_options),
            grouping: None,
            count: None,
        };

        let mut buf = Vec::new();
        req.serialize(&mut buf);

        let req2 = MRangeOptions::deserialize(&buf).expect("deserialization failed");

        let agg1 = req.aggregation.as_ref().unwrap();
        let agg2 = req2.aggregation.as_ref().unwrap();

        assert_eq!(agg1.bucket_duration, agg2.bucket_duration);
        assert_eq!(agg1.timestamp_output, agg2.timestamp_output);
        assert_eq!(agg1.report_empty, agg2.report_empty);

        match (&agg1.aggregation, &agg2.aggregation) {
            (Aggregation::Avg, Aggregation::Avg) => {}
            _ => panic!("Wrong aggregator type after deserialization"),
        }

        match (agg1.alignment, agg2.alignment) {
            (BucketAlignment::Start, BucketAlignment::Start) => {}
            _ => panic!("Wrong bucket alignment after deserialization"),
        }
    }

    #[test]
    fn test_multi_range_request_with_grouping_serialize_deserialize() {
        let grouping = RangeGroupingOptions {
            group_label: "category".to_string(),
            aggregation: Aggregation::Sum,
        };

        let req = MRangeOptions {
            date_range: TimestampRange::from_timestamps(100, 200).unwrap(),
            filters: make_matchers_vec(),
            with_labels: true,
            selected_labels: vec!["label1".to_string()],
            timestamp_filter: None,
            value_filter: None,
            aggregation: None,
            grouping: Some(grouping),
            count: None,
        };

        let mut buf = Vec::new();
        req.serialize(&mut buf);

        let req2 = MRangeOptions::deserialize(&buf).expect("deserialization failed");

        let grp1 = req.grouping.as_ref().unwrap();
        let grp2 = req2.grouping.as_ref().unwrap();

        assert_eq!(grp1.group_label, grp2.group_label);

        match (&grp1.aggregation, &grp2.aggregation) {
            (Aggregation::Sum, Aggregation::Sum) => {}
            _ => panic!("Wrong aggregator type after deserialization"),
        }
    }

    #[test]
    fn test_multi_range_response_serialize_deserialize() {
        let sample1 = MRangeSeriesResult {
            key: "series1".to_string(),
            group_label_value: Some("group1".to_string()),
            labels: vec![
                Some(Label::new("name", "series1")),
                Some(Label::new("region", "us-west")),
            ],
            samples: vec![
                Sample::new(100, 1.0),
                Sample::new(110, 2.0),
                Sample::new(120, 3.0),
            ],
        };

        let sample2 = MRangeSeriesResult {
            key: "series2".to_string(),
            group_label_value: None,
            labels: vec![
                Some(Label::new("name", "series2")),
                Some(Label::new("region", "us-east")),
            ],
            samples: vec![
                Sample::new(105, 10.0),
                Sample::new(115, 20.0),
                Sample::new(125, 30.0),
            ],
        };

        let resp = MultiRangeResponse {
            series: vec![sample1, sample2],
        };

        let mut buf = Vec::new();
        resp.serialize(&mut buf);

        let resp2 = MultiRangeResponse::deserialize(&buf).expect("deserialization failed");
        assert_eq!(resp.series.len(), resp2.series.len());

        assert_eq!(resp.series[0].key, resp2.series[0].key);
        assert_eq!(
            resp.series[0].group_label_value,
            resp2.series[0].group_label_value
        );
        assert_eq!(resp.series[0].labels, resp2.series[0].labels);
        assert_eq!(resp.series[0].samples, resp2.series[0].samples);

        assert_eq!(resp.series[1].key, resp2.series[1].key);
        assert_eq!(
            resp.series[1].group_label_value,
            resp2.series[1].group_label_value
        );
        assert_eq!(resp.series[1].labels, resp2.series[1].labels);
        assert_eq!(resp.series[1].samples, resp2.series[1].samples);
    }

    #[test]
    fn test_multi_range_response_empty_serialize_deserialize() {
        let resp = MultiRangeResponse { series: vec![] };

        let mut buf = Vec::new();
        resp.serialize(&mut buf);

        let resp2 = MultiRangeResponse::deserialize(&buf).expect("deserialization failed");
        assert_eq!(resp.series.len(), resp2.series.len());
    }
}
