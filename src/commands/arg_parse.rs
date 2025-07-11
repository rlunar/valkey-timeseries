use crate::aggregators::{Aggregation, BucketAlignment, BucketTimestamp};
use crate::common::rounding::{RoundingStrategy, MAX_DECIMAL_DIGITS, MAX_SIGNIFICANT_DIGITS};
use crate::common::time::current_time_millis;
use crate::common::Timestamp;
use crate::error::{TsdbError, TsdbResult};
use crate::error_consts;
use crate::join::join_reducer::JoinReducer;
use crate::labels::matchers::Matchers;
use crate::labels::{parse_series_selector, Label};
use crate::parser::number::parse_number;
use crate::parser::{
    metric_name::parse_metric_name as parse_metric, number::parse_number as parse_number_internal,
    parse_positive_duration_value, timestamp::parse_timestamp as parse_timestamp_internal,
};
use crate::series::chunks::{ChunkEncoding, MAX_CHUNK_SIZE, MIN_CHUNK_SIZE};
use crate::series::request_types::{
    AggregationOptions, MRangeOptions, MatchFilterOptions, RangeGroupingOptions, RangeOptions,
};
use crate::series::types::*;
use crate::series::{TimestampRange, TimestampValue};
use ahash::AHashMap;
use std::collections::BTreeSet;
use std::iter::{Peekable, Skip};
use std::time::Duration;
use std::vec::IntoIter;
use strum_macros::EnumIter;
use valkey_module::{NextArg, ValkeyError, ValkeyResult, ValkeyString};

const MAX_TS_VALUES_FILTER: usize = 128;
const CMD_ARG_AGGREGATION: &str = "AGGREGATION";
const CMD_ARG_ALIGN: &str = "ALIGN";
const CMD_ARG_ALLOW_EXACT_MATCH: &str = "ALLOW_EXACT_MATCH";
const CMD_ARG_ANTI: &str = "ANTI";
const CMD_ARG_ASOF: &str = "ASOF";
const CMD_ARG_BUCKET_TIMESTAMP: &str = "BUCKETTIMESTAMP";
const CMD_ARG_CHUNK_SIZE: &str = "CHUNK_SIZE";
const CMD_ARG_COMPRESSION: &str = "COMPRESSION";
const CMD_ARG_COMPRESSED: &str = "COMPRESSED";
const CMD_ARG_COUNT: &str = "COUNT";
const CMD_ARG_DECIMAL_DIGITS: &str = "DECIMAL_DIGITS";
const CMD_ARG_DUPLICATE_POLICY: &str = "DUPLICATE_POLICY";
const CMD_ARG_EMPTY: &str = "EMPTY";
const CMD_ARG_ENCODING: &str = "ENCODING";
const CMD_ARG_END: &str = "END";
const CMD_ARG_FALSE: &str = "FALSE";
const CMD_ARG_FILTER: &str = "FILTER";
const CMD_ARG_FILTER_BY_TS: &str = "FILTER_BY_TS";
const CMD_ARG_FILTER_BY_VALUE: &str = "FILTER_BY_VALUE";
const CMD_ARG_FULL: &str = "FULL";
const CMD_ARG_GROUP_BY: &str = "GROUPBY";
const CMD_ARG_IGNORE: &str = "IGNORE";
const CMD_ARG_INNER: &str = "INNER";
const CMD_ARG_LABELS: &str = "LABELS";
const CMD_ARG_LATEST: &str = "LATEST";
const CMD_ARG_LEFT: &str = "LEFT";
const CMD_ARG_LIMIT: &str = "LIMIT";
const CMD_ARG_MATCH: &str = "MATCH";
const CMD_ARG_METRIC: &str = "METRIC";
const CMD_ARG_NAME: &str = "NAME";
const CMD_ARG_NEAREST: &str = "NEAREST";
const CMD_ARG_NEXT: &str = "NEXT";
const CMD_ARG_ON_DUPLICATE: &str = "ON_DUPLICATE";
const CMD_ARG_PREVIOUS: &str = "PREVIOUS";
const CMD_ARG_PRIOR: &str = "PRIOR";
const CMD_ARG_REDUCE: &str = "REDUCE";
const CMD_ARG_RETENTION: &str = "RETENTION";
const CMD_ARG_RIGHT: &str = "RIGHT";
const CMD_ARG_ROUNDING: &str = "ROUNDING";
const CMD_ARG_SELECTED_LABELS: &str = "SELECTED_LABELS";
const CMD_ARG_SEMI: &str = "SEMI";
const CMD_ARG_STEP: &str = "STEP";
const CMD_ARG_SIGNIFICANT_DIGITS: &str = "SIGNIFICANT_DIGITS";
const CMD_ARG_START: &str = "START";
const CMD_ARG_TIMESTAMP: &str = "TIMESTAMP";
const CMD_ARG_TRUE: &str = "TRUE";
const CMD_ARG_UNCOMPRESSED: &str = "UNCOMPRESSED";
const CMD_ARG_WITH_LABELS: &str = "WITHLABELS";

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Default, EnumIter)]
pub enum CommandArgToken {
    Aggregation,
    Align,
    AllowExactMatch,
    Anti,
    AsOf,
    BucketTimestamp,
    ChunkSize,
    Compressed,
    Compression,
    Count,
    DecimalDigits,
    DuplicatePolicy,
    Empty,
    Encoding,
    End,
    False,
    Filter,
    FilterByTs,
    FilterByValue,
    Full,
    GroupBy,
    Ignore,
    Inner,
    Labels,
    Latest,
    Left,
    Limit,
    Match,
    Metric,
    Name,
    Nearest,
    Next,
    OnDuplicate,
    Previous,
    Prior,
    Reduce,
    Retention,
    Right,
    Rounding,
    SelectedLabels,
    Semi,
    SignificantDigits,
    Start,
    Step,
    Timestamp,
    True,
    Uncompressed,
    WithLabels,
    #[default]
    Invalid,
}

impl CommandArgToken {
    #[allow(dead_code)]
    pub fn as_str(&self) -> &'static str {
        match self {
            CommandArgToken::Aggregation => CMD_ARG_AGGREGATION,
            CommandArgToken::Align => CMD_ARG_ALIGN,
            CommandArgToken::AllowExactMatch => CMD_ARG_ALLOW_EXACT_MATCH,
            CommandArgToken::Anti => CMD_ARG_ANTI,
            CommandArgToken::AsOf => CMD_ARG_ASOF,
            CommandArgToken::ChunkSize => CMD_ARG_CHUNK_SIZE,
            CommandArgToken::Compressed => CMD_ARG_COMPRESSED,
            CommandArgToken::Compression => CMD_ARG_COMPRESSION,
            CommandArgToken::Count => CMD_ARG_COUNT,
            CommandArgToken::DecimalDigits => CMD_ARG_DECIMAL_DIGITS,
            CommandArgToken::DuplicatePolicy => CMD_ARG_DUPLICATE_POLICY,
            CommandArgToken::Empty => CMD_ARG_EMPTY,
            CommandArgToken::Encoding => CMD_ARG_ENCODING,
            CommandArgToken::End => CMD_ARG_END,
            CommandArgToken::False => CMD_ARG_FALSE,
            CommandArgToken::Filter => CMD_ARG_FILTER,
            CommandArgToken::FilterByTs => CMD_ARG_FILTER_BY_TS,
            CommandArgToken::FilterByValue => CMD_ARG_FILTER_BY_VALUE,
            CommandArgToken::Full => CMD_ARG_FULL,
            CommandArgToken::GroupBy => CMD_ARG_GROUP_BY,
            CommandArgToken::Ignore => CMD_ARG_IGNORE,
            CommandArgToken::Inner => CMD_ARG_INNER,
            CommandArgToken::Latest => CMD_ARG_LATEST,
            CommandArgToken::Labels => CMD_ARG_LABELS,
            CommandArgToken::Left => CMD_ARG_LEFT,
            CommandArgToken::Limit => CMD_ARG_LIMIT,
            CommandArgToken::Match => CMD_ARG_MATCH,
            CommandArgToken::Metric => CMD_ARG_METRIC,
            CommandArgToken::Name => CMD_ARG_NAME,
            CommandArgToken::OnDuplicate => CMD_ARG_ON_DUPLICATE,
            CommandArgToken::Retention => CMD_ARG_RETENTION,
            CommandArgToken::Right => CMD_ARG_RIGHT,
            CommandArgToken::Rounding => CMD_ARG_ROUNDING,
            CommandArgToken::SignificantDigits => CMD_ARG_SIGNIFICANT_DIGITS,
            CommandArgToken::Semi => CMD_ARG_SEMI,
            CommandArgToken::Start => CMD_ARG_START,
            CommandArgToken::Step => CMD_ARG_STEP,
            CommandArgToken::WithLabels => CMD_ARG_WITH_LABELS,
            CommandArgToken::BucketTimestamp => CMD_ARG_BUCKET_TIMESTAMP,
            CommandArgToken::Nearest => CMD_ARG_NEAREST,
            CommandArgToken::Next => CMD_ARG_NEXT,
            CommandArgToken::Previous => CMD_ARG_PREVIOUS,
            CommandArgToken::Prior => CMD_ARG_PRIOR,
            CommandArgToken::Reduce => CMD_ARG_REDUCE,
            CommandArgToken::Uncompressed => CMD_ARG_UNCOMPRESSED,
            CommandArgToken::SelectedLabels => CMD_ARG_SELECTED_LABELS,
            CommandArgToken::Timestamp => CMD_ARG_TIMESTAMP,
            CommandArgToken::True => CMD_ARG_TRUE,
            CommandArgToken::Invalid => "INVALID",
        }
    }
}

pub(crate) fn parse_command_arg_token(arg: &[u8]) -> Option<CommandArgToken> {
    hashify::tiny_map_ignore_case! {
        arg,
        "AGGREGATION" => CommandArgToken::Aggregation,
        "ALIGN" => CommandArgToken::Align,
        "ALLOW_EXACT_MATCH" => CommandArgToken::AllowExactMatch,
        "ANTI" => CommandArgToken::Anti,
        "ASOF" => CommandArgToken::AsOf,
        "BUCKETTIMESTAMP" => CommandArgToken::BucketTimestamp,
        "CHUNK_SIZE" => CommandArgToken::ChunkSize,
        "COMPRESSED" => CommandArgToken::Compressed,
        "COMPRESSION" => CommandArgToken::Compression,
        "COUNT" => CommandArgToken::Count,
        "DECIMAL_DIGITS" => CommandArgToken::DecimalDigits,
        "DUPLICATE_POLICY" => CommandArgToken::DuplicatePolicy,
        "EMPTY" => CommandArgToken::Empty,
        "ENCODING" => CommandArgToken::Encoding,
        "END" => CommandArgToken::End,
        "FALSE" => CommandArgToken::False,
        "FILTER" => CommandArgToken::Filter,
        "FILTER_BY_TS" => CommandArgToken::FilterByTs,
        "FILTER_BY_VALUE" => CommandArgToken::FilterByValue,
        "FULL" => CommandArgToken::Full,
        "GROUPBY" => CommandArgToken::GroupBy,
        "IGNORE" => CommandArgToken::Ignore,
        "INNER" => CommandArgToken::Inner,
        "LABELS" => CommandArgToken::Labels,
        "LATEST" => CommandArgToken::Latest,
        "LEFT" => CommandArgToken::Left,
        "LIMIT" => CommandArgToken::Limit,
        "MATCH" => CommandArgToken::Match,
        "METRIC" => CommandArgToken::Metric,
        "NAME" => CommandArgToken::Name,
        "NEAREST" => CommandArgToken::Nearest,
        "NEXT" => CommandArgToken::Next,
        "ON_DUPLICATE" => CommandArgToken::OnDuplicate,
        "PREVIOUS" => CommandArgToken::Previous,
        "PRIOR" => CommandArgToken::Prior,
        "REDUCE" => CommandArgToken::Reduce,
        "RETENTION" => CommandArgToken::Retention,
        "RIGHT" => CommandArgToken::Right,
        "ROUNDING" => CommandArgToken::Rounding,
        "SELECTED_LABELS" => CommandArgToken::SelectedLabels,
        "SEMI" => CommandArgToken::Semi,
        "SIGNIFICANT_DIGITS" => CommandArgToken::SignificantDigits,
        "START" => CommandArgToken::Start,
        "STEP" => CommandArgToken::Step,
        "TIMESTAMP" => CommandArgToken::Timestamp,
        "TRUE" => CommandArgToken::True,
        "UNCOMPRESSED" => CommandArgToken::Uncompressed,
        "WITHLABELS" => CommandArgToken::WithLabels,
    }
}

pub type CommandArgIterator = Peekable<Skip<IntoIter<ValkeyString>>>;

pub fn parse_number_arg(arg: &ValkeyString, name: &str) -> ValkeyResult<f64> {
    if let Ok(value) = arg.parse_float() {
        return Ok(value);
    }
    let arg_str = arg.to_string_lossy();
    parse_number_with_unit(&arg_str).map_err(|_| {
        let msg = format!("ERR invalid number parsing {name}");
        ValkeyError::String(msg)
    })
}

pub fn parse_integer_arg(
    arg: &ValkeyString,
    name: &str,
    allow_negative: bool,
) -> ValkeyResult<i64> {
    let value = if let Ok(val) = arg.parse_integer() {
        val
    } else {
        let num = parse_number_arg(arg, name)?;
        if num != num.floor() {
            return Err(ValkeyError::Str(error_consts::INVALID_INTEGER));
        }
        if num > i64::MAX as f64 {
            return Err(ValkeyError::Str("ERR: value is too large"));
        }
        num as i64
    };
    if !allow_negative && value < 0 {
        let msg = format!("ERR: {name} must be a non-negative integer");
        return Err(ValkeyError::String(msg));
    }
    Ok(value)
}

pub fn parse_timestamp(arg: &str) -> ValkeyResult<Timestamp> {
    if arg == "*" {
        return Ok(current_time_millis());
    }
    parse_timestamp_internal(arg, false)
        .map_err(|_| ValkeyError::Str(error_consts::INVALID_TIMESTAMP))
}

pub fn parse_timestamp_arg(arg: &str, name: &str) -> Result<TimestampValue, ValkeyError> {
    parse_timestamp_range_value(arg).map_err(|_e| {
        let msg = format!("TSDB: invalid {name} timestamp");
        ValkeyError::String(msg)
    })
}

pub fn parse_timestamp_range_value(arg: &str) -> ValkeyResult<TimestampValue> {
    TimestampValue::try_from(arg)
}

pub fn parse_duration_arg(arg: &ValkeyString) -> ValkeyResult<Duration> {
    if let Ok(value) = arg.parse_integer() {
        if value < 0 {
            return Err(ValkeyError::Str(
                "ERR: invalid duration, must be a non-negative integer",
            ));
        }
        return Ok(Duration::from_millis(value as u64));
    }
    let value_str = arg.to_string_lossy();
    parse_duration(&value_str)
}

pub fn parse_duration(arg: &str) -> ValkeyResult<Duration> {
    parse_duration_ms(arg).map(|d| Duration::from_millis(d as u64))
}

pub fn parse_duration_ms(arg: &str) -> ValkeyResult<i64> {
    parse_positive_duration_value(arg).map_err(|_| ValkeyError::Str(error_consts::INVALID_DURATION))
}

pub fn parse_number_with_unit(arg: &str) -> TsdbResult<f64> {
    if arg.is_empty() {
        return Err(TsdbError::InvalidNumber(arg.to_string()));
    }
    parse_number_internal(arg).map_err(|_e| TsdbError::InvalidNumber(arg.to_string()))
}

pub fn parse_metric_name(arg: &str) -> ValkeyResult<Vec<Label>> {
    parse_metric(arg).map_err(|_e| ValkeyError::Str(error_consts::INVALID_METRIC_NAME))
}

/// Parse a float value for use in a command argument, specifically ADD, MADD, and INCRBY/DECRBY.
pub fn parse_value_arg(arg: &ValkeyString) -> ValkeyResult<f64> {
    let value = arg
        .try_as_str()
        .map_err(|_| ValkeyError::Str(error_consts::INVALID_VALUE))?
        .parse::<f64>()
        .map_err(|_| ValkeyError::Str(error_consts::INVALID_VALUE))?;

    if value.is_nan() || value.is_infinite() {
        return Err(ValkeyError::Str(error_consts::INVALID_VALUE));
    }

    Ok(value)
}

pub fn parse_join_operator(arg: &str) -> ValkeyResult<JoinReducer> {
    JoinReducer::try_from(arg)
}

pub fn parse_chunk_size(arg: &str) -> ValkeyResult<usize> {
    fn get_error_result() -> ValkeyResult<usize> {
        let msg = format!("TSDB: CHUNK_SIZE value must be an integer multiple of 8 in the range [{MIN_CHUNK_SIZE} .. {MAX_CHUNK_SIZE}]");
        Err(ValkeyError::String(msg))
    }

    if arg.is_empty() {
        return Err(ValkeyError::Str(error_consts::INVALID_CHUNK_SIZE));
    }

    let chunk_size = parse_number_internal(arg)
        .map_err(|_e| ValkeyError::Str(error_consts::INVALID_CHUNK_SIZE))?;

    if chunk_size != chunk_size.floor() {
        return get_error_result();
    }
    let chunk_size = chunk_size as usize;

    if !(MIN_CHUNK_SIZE..=MAX_CHUNK_SIZE).contains(&chunk_size) || chunk_size % 8 != 0 {
        return get_error_result();
    }

    Ok(chunk_size)
}

pub fn parse_chunk_compression(args: &mut CommandArgIterator) -> ValkeyResult<ChunkEncoding> {
    if let Ok(next) = args.next_str() {
        ChunkEncoding::try_from(next)
            .map_err(|_| ValkeyError::Str(error_consts::INVALID_CHUNK_ENCODING))
    } else {
        Err(ValkeyError::Str(error_consts::MISSING_CHUNK_ENCODING))
    }
}

pub fn parse_duplicate_policy(args: &mut CommandArgIterator) -> ValkeyResult<DuplicatePolicy> {
    if let Ok(next) = args.next_str() {
        DuplicatePolicy::try_from(next)
            .map_err(|_| ValkeyError::Str(error_consts::INVALID_DUPLICATE_POLICY))
    } else {
        Err(ValkeyError::Str(error_consts::MISSING_DUPLICATE_POLICY))
    }
}

pub fn parse_timestamp_range(args: &mut CommandArgIterator) -> ValkeyResult<TimestampRange> {
    let first_arg = args.next_str()?;
    let start = parse_timestamp_range_value(first_arg)
        .map_err(|_e| ValkeyError::Str(error_consts::INVALID_START_TIMESTAMP))?;
    let end_value = if let Ok(arg) = args.next_str() {
        parse_timestamp_range_value(arg)
            .map_err(|_e| ValkeyError::Str(error_consts::INVALID_END_TIMESTAMP))?
    } else {
        TimestampValue::Latest
    };
    TimestampRange::new(start, end_value)
}

pub fn parse_retention(args: &mut CommandArgIterator) -> ValkeyResult<Duration> {
    if let Ok(next) = args.next_str() {
        parse_duration(next).map_err(|_e| ValkeyError::Str(error_consts::INVALID_DURATION))
    } else {
        Err(ValkeyError::Str("ERR missing RETENTION value"))
    }
}

pub fn parse_timestamp_filter(
    args: &mut CommandArgIterator,
    stop_tokens: &[CommandArgToken],
) -> ValkeyResult<Vec<Timestamp>> {
    // FILTER_BY_TS already seen
    let mut values: Vec<Timestamp> = Vec::new();
    loop {
        if is_stop_token_or_end(args, stop_tokens) {
            break;
        }
        let Ok(arg) = args.next_str() else {
            return Err(ValkeyError::Str(error_consts::INVALID_TIMESTAMP_FILTER));
        };
        let Ok(timestamp) = parse_timestamp(arg) else {
            return Err(ValkeyError::Str(error_consts::INVALID_TIMESTAMP));
        };
        values.push(timestamp);
        if values.len() > MAX_TS_VALUES_FILTER {
            return Err(ValkeyError::Str("TSDB: too many timestamp filters"));
        }
    }
    if values.is_empty() {
        return Err(ValkeyError::Str(
            error_consts::MISSING_TIMESTAMP_FILTER_VALUE,
        ));
    }

    Ok(values)
}

pub fn parse_value_filter(args: &mut CommandArgIterator) -> ValkeyResult<ValueFilter> {
    let min = parse_number_with_unit(args.next_str()?)
        .map_err(|_| ValkeyError::Str("TSDB cannot parse value filter min parameter"))?;
    let max = parse_number_with_unit(args.next_str()?)
        .map_err(|_| ValkeyError::Str("TSDB cannot parse value filter max parameter"))?;
    if max < min {
        return Err(ValkeyError::Str(
            "TSDB filter min parameter is greater than max",
        ));
    }
    ValueFilter::new(min, max)
}

pub fn parse_count_arg(args: &mut CommandArgIterator) -> ValkeyResult<usize> {
    let next = args
        .next_arg()
        .map_err(|_| ValkeyError::Str(error_consts::MISSING_COUNT_VALUE))?;
    let count = parse_integer_arg(&next, CMD_ARG_COUNT, false)
        .map_err(|_| ValkeyError::Str(error_consts::NEGATIVE_COUNT))?;
    Ok(count as usize)
}

pub(crate) fn advance_if_next_token_one_of(
    args: &mut CommandArgIterator,
    tokens: &[CommandArgToken],
) -> Option<CommandArgToken> {
    if let Some(next) = args.peek() {
        if let Some(token) = parse_command_arg_token(next) {
            if tokens.contains(&token) {
                args.next();
                return Some(token);
            }
        }
    }
    None
}

fn is_stop_token_or_end(args: &mut CommandArgIterator, stop_tokens: &[CommandArgToken]) -> bool {
    let Some(next) = args.peek() else {
        return true; // at end
    };
    let Some(token) = parse_command_arg_token(next) else {
        // should we error here?
        return false; // not a token
    };
    stop_tokens.contains(&token)
}

pub fn parse_label_list(
    args: &mut CommandArgIterator,
    stop_tokens: &[CommandArgToken],
) -> ValkeyResult<Vec<String>> {
    let mut labels: BTreeSet<String> = BTreeSet::new();

    loop {
        if is_stop_token_or_end(args, stop_tokens) {
            break;
        }

        let label = args.next_str()?;
        if labels.contains(label) {
            return Err(ValkeyError::Str(error_consts::DUPLICATE_LABEL));
        }
        labels.insert(label.to_string());
    }

    let temp = labels.into_iter().collect();
    Ok(temp)
}

pub fn parse_label_value_pairs(
    args: &mut CommandArgIterator,
    stop_tokens: &[CommandArgToken],
) -> ValkeyResult<AHashMap<String, String>> {
    let mut labels: AHashMap<String, String> = AHashMap::new();

    loop {
        if args.peek().is_some() {
            let name = args.next().unwrap().to_string_lossy();
            if name.is_empty() {
                return Err(ValkeyError::Str("TSDB invalid label key"));
            }

            let value = args
                .next_string()
                .map_err(|_| ValkeyError::Str("TSDB invalid label value"))?;

            if labels.insert(name, value).is_some() {
                return Err(ValkeyError::Str(error_consts::DUPLICATE_LABEL));
            }
        } else {
            break;
        }

        if is_stop_token_or_end(args, stop_tokens) {
            break;
        }
    }

    Ok(labels)
}

pub fn parse_aggregation_options(
    args: &mut CommandArgIterator,
) -> ValkeyResult<AggregationOptions> {
    // AGGREGATION token already seen
    let agg_str = args
        .next_str()
        .map_err(|_e| ValkeyError::Str("ERR: Error parsing AGGREGATION"))?;
    let aggregator = Aggregation::try_from(agg_str)?;
    let bucket_duration = parse_duration_arg(&args.next_arg()?)
        .map_err(|_e| ValkeyError::Str("Error parsing bucketDuration"))?;

    let mut aggr: AggregationOptions = AggregationOptions {
        aggregation: aggregator,
        bucket_duration: bucket_duration.as_millis() as u64,
        timestamp_output: BucketTimestamp::Start,
        alignment: BucketAlignment::default(),
        report_empty: false,
    };

    let mut arg_count: usize = 0;

    let valid_tokens = [
        CommandArgToken::Align,
        CommandArgToken::Empty,
        CommandArgToken::BucketTimestamp,
    ];

    while let Some(token) = advance_if_next_token_one_of(args, &valid_tokens) {
        match token {
            CommandArgToken::Empty => {
                aggr.report_empty = true;
                arg_count += 1;
            }
            CommandArgToken::BucketTimestamp => {
                let next = args.next_str()?;
                arg_count += 1;
                aggr.timestamp_output = BucketTimestamp::try_from(next)?;
            }
            CommandArgToken::Align => {
                let next = args.next_str()?;
                aggr.alignment = next.try_into()?;
            }
            _ => break,
        }
        if arg_count == 3 {
            break;
        }
    }

    Ok(aggr)
}

pub fn parse_grouping_params(args: &mut CommandArgIterator) -> ValkeyResult<RangeGroupingOptions> {
    // GROUPBY token already seen
    let label = args.next_str()?;
    let token = args
        .next_str()
        .map_err(|_| ValkeyError::Str("ERR: missing REDUCE"))?;
    if !token.eq_ignore_ascii_case(CMD_ARG_REDUCE) {
        let msg = format!("ERR: expected \"{CMD_ARG_REDUCE}\", found \"{token}\"");
        return Err(ValkeyError::String(msg));
    }
    let agg_str = args
        .next_str()
        .map_err(|_e| ValkeyError::Str("ERR: Error parsing grouping reducer"))?;

    let aggregator = Aggregation::try_from(agg_str).map_err(|_| {
        let msg = format!("ERR: invalid grouping aggregator \"{agg_str}\"");
        ValkeyError::String(msg)
    })?;

    Ok(RangeGroupingOptions {
        group_label: label.to_string(),
        aggregation: aggregator,
    })
}

pub fn parse_significant_digit_rounding(
    args: &mut CommandArgIterator,
) -> ValkeyResult<RoundingStrategy> {
    let next = args.next_u64()?;
    if next > MAX_SIGNIFICANT_DIGITS as u64 {
        let msg = format!("ERR SIGNIFICANT_DIGITS must be between 0 and {MAX_SIGNIFICANT_DIGITS}");
        return Err(ValkeyError::String(msg));
    }
    Ok(RoundingStrategy::SignificantDigits(next as i32))
}

pub fn parse_decimal_digit_rounding(
    args: &mut CommandArgIterator,
) -> ValkeyResult<RoundingStrategy> {
    let next = args.next_u64()?;
    if next > MAX_DECIMAL_DIGITS as u64 {
        let msg = format!("ERR DECIMAL_DIGITS must be between 0 and {MAX_DECIMAL_DIGITS}");
        return Err(ValkeyError::String(msg));
    }
    Ok(RoundingStrategy::DecimalDigits(next as i32))
}

pub(crate) fn parse_ignore_options(args: &mut CommandArgIterator) -> ValkeyResult<(i64, f64)> {
    // ignoreMaxTimediff
    let mut str = args.next_str()?;
    let ignore_max_timediff =
        parse_duration_ms(str).map_err(|_| ValkeyError::Str("Invalid ignoreMaxTimediff"))?;
    // ignoreMaxValDiff
    str = args.next_str()?;
    let ignore_max_val_diff =
        parse_number(str).map_err(|_| ValkeyError::Str("Invalid ignoreMaxValDiff"))?;
    Ok((ignore_max_timediff, ignore_max_val_diff))
}

pub fn parse_series_selector_list(
    args: &mut CommandArgIterator,
    stop_tokens: &[CommandArgToken],
) -> ValkeyResult<Vec<Matchers>> {
    let mut matchers = vec![];

    while let Some(next) = args.peek() {
        if let Some(token) = parse_command_arg_token(next) {
            if stop_tokens.contains(&token) {
                break;
            }
        }

        let arg = args.next_str()?;
        if arg.is_empty() {
            return Err(ValkeyError::Str(error_consts::INVALID_SERIES_SELECTOR));
        }

        if let Ok(selector) = parse_series_selector(arg) {
            matchers.push(selector);
        } else {
            return Err(ValkeyError::Str(error_consts::INVALID_SERIES_SELECTOR));
        }
    }

    if matchers.is_empty() {
        return Err(ValkeyError::Str(error_consts::MISSING_FILTER));
    }

    Ok(matchers)
}

fn parse_align_for_aggregation(args: &mut CommandArgIterator) -> ValkeyResult<AggregationOptions> {
    // ALIGN token already seen
    let alignment_str = args.next_str()?;
    let next = args.next_str()?;
    if next != CMD_ARG_AGGREGATION {
        return Err(ValkeyError::Str("TSDB: missing AGGREGATION"));
    }
    let mut aggregation = parse_aggregation_options(args)?;

    aggregation.alignment = BucketAlignment::try_from(alignment_str)?;
    Ok(aggregation)
}

pub fn parse_range_options(args: &mut CommandArgIterator) -> ValkeyResult<RangeOptions> {
    const RANGE_OPTION_ARGS: [CommandArgToken; 6] = [
        CommandArgToken::Align,
        CommandArgToken::Aggregation,
        CommandArgToken::Count,
        CommandArgToken::BucketTimestamp,
        CommandArgToken::FilterByTs,
        CommandArgToken::FilterByValue,
    ];

    let date_range = parse_timestamp_range(args)?;

    let mut options = RangeOptions {
        date_range,
        ..Default::default()
    };

    while let Some(arg) = args.next() {
        let token = parse_command_arg_token(&arg).unwrap_or_default();
        match token {
            CommandArgToken::Align => {
                options.aggregation = Some(parse_align_for_aggregation(args)?);
            }
            CommandArgToken::Aggregation => {
                options.aggregation = Some(parse_aggregation_options(args)?);
            }
            CommandArgToken::Count => {
                options.count = Some(parse_count_arg(args)?);
            }
            CommandArgToken::FilterByValue => {
                options.value_filter = Some(parse_value_filter(args)?);
            }
            CommandArgToken::FilterByTs => {
                options.timestamp_filter = Some(parse_timestamp_filter(args, &RANGE_OPTION_ARGS)?);
            }
            _ => {
                let msg = format!("ERR invalid argument '{arg}'");
                return Err(ValkeyError::String(msg));
            }
        }
    }

    Ok(options)
}

pub fn parse_mrange_options(args: &mut CommandArgIterator) -> ValkeyResult<MRangeOptions> {
    const RANGE_OPTION_ARGS: [CommandArgToken; 11] = [
        CommandArgToken::Align,
        CommandArgToken::Aggregation,
        CommandArgToken::Count,
        CommandArgToken::BucketTimestamp,
        CommandArgToken::Filter,
        CommandArgToken::FilterByTs,
        CommandArgToken::FilterByValue,
        CommandArgToken::GroupBy,
        CommandArgToken::Reduce,
        CommandArgToken::SelectedLabels,
        CommandArgToken::WithLabels,
    ];

    let date_range = parse_timestamp_range(args)?;

    let mut options = MRangeOptions {
        date_range,
        ..Default::default()
    };

    while let Some(arg) = args.next() {
        let token = parse_command_arg_token(&arg).unwrap_or_default();
        match token {
            CommandArgToken::Align => {
                options.aggregation = Some(parse_align_for_aggregation(args)?);
            }
            CommandArgToken::Aggregation => {
                options.aggregation = Some(parse_aggregation_options(args)?);
            }
            CommandArgToken::Count => {
                options.count = Some(parse_count_arg(args)?);
            }
            CommandArgToken::Filter => {
                options.filters = parse_series_selector_list(args, &RANGE_OPTION_ARGS)?;
            }
            CommandArgToken::FilterByValue => {
                options.value_filter = Some(parse_value_filter(args)?);
            }
            CommandArgToken::FilterByTs => {
                options.timestamp_filter = Some(parse_timestamp_filter(args, &RANGE_OPTION_ARGS)?);
            }
            CommandArgToken::GroupBy => {
                options.grouping = Some(parse_grouping_params(args)?);
            }
            CommandArgToken::SelectedLabels => {
                options.selected_labels = parse_label_list(args, &RANGE_OPTION_ARGS)?;
            }
            CommandArgToken::WithLabels => {
                options.with_labels = true;
            }
            _ => {}
        }
    }

    if options.filters.is_empty() {
        return Err(ValkeyError::Str("TSDB: no FILTER given"));
    }

    if !options.selected_labels.is_empty() && options.with_labels {
        return Err(ValkeyError::Str(
            error_consts::WITH_LABELS_AND_SELECTED_LABELS_SPECIFIED,
        ));
    }

    Ok(options)
}

pub(crate) fn parse_metadata_command_args(
    args: &mut CommandArgIterator,
    require_matchers: bool,
) -> ValkeyResult<MatchFilterOptions> {
    const ARG_TOKENS: [CommandArgToken; 3] = [
        CommandArgToken::End,
        CommandArgToken::Start,
        CommandArgToken::Limit,
    ];

    let mut matchers = Vec::with_capacity(4);
    let mut start_value: Option<TimestampValue> = None;
    let mut end_value: Option<TimestampValue> = None;
    let mut limit: Option<usize> = None;

    while let Some(arg) = args.next() {
        let token = parse_command_arg_token(&arg).unwrap_or_default();
        match token {
            CommandArgToken::Start => {
                let next = args.next_str()?;
                start_value = Some(parse_timestamp_arg(next, "START")?);
            }
            CommandArgToken::End => {
                let next = args.next_str()?;
                end_value = Some(parse_timestamp_arg(next, "END")?);
            }
            CommandArgToken::Filter => {
                let m = parse_series_selector_list(args, &ARG_TOKENS)?;
                matchers.extend(m);
            }
            CommandArgToken::Limit => {
                let next = args
                    .next_str()
                    .map_err(|_| ValkeyError::Str(error_consts::MISSING_LIMIT_VALUE))?
                    .parse::<i64>()
                    .map_err(|_e| ValkeyError::Str(error_consts::INVALID_LIMIT_VALUE))?;
                if next < 0 {
                    return Err(ValkeyError::Str(error_consts::INVALID_LIMIT_VALUE));
                }
                limit = Some(next as usize);
            }
            _ => {
                let msg = format!("ERR invalid argument '{arg}'");
                return Err(ValkeyError::String(msg));
            }
        };
    }

    if require_matchers && matchers.is_empty() {
        return Err(ValkeyError::Str(error_consts::MISSING_FILTER));
    }

    let mut options = MatchFilterOptions {
        matchers,
        limit,
        ..Default::default()
    };

    if start_value.is_some() || end_value.is_some() {
        let range = TimestampRange {
            start: start_value.unwrap_or(TimestampValue::Earliest),
            end: end_value.unwrap_or(TimestampValue::Latest),
        };
        if !range.is_empty() {
            options.date_range = Some(range);
        }
    }

    Ok(options)
}

pub(super) fn find_last_token_instance(
    args: &[ValkeyString],
    cmd_tokens: &[CommandArgToken],
) -> Option<(CommandArgToken, usize)> {
    let mut i = args.len() - 1;
    for arg in args.iter().rev() {
        if let Some(token) = parse_command_arg_token(arg) {
            if cmd_tokens.contains(&token) {
                return Some((token, i));
            }
        }
        i -= 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use strum::IntoEnumIterator;

    #[test]
    fn test_parse_command_arg_token_case_insensitive() {
        let input = b"aGgReGaTiOn";
        let result = parse_command_arg_token(input);
        assert_eq!(result, Some(CommandArgToken::Aggregation));
    }

    #[test]
    fn test_parse_command_arg_token_exhaustive() {
        for token in CommandArgToken::iter() {
            if token == CommandArgToken::Invalid {
                continue;
            }
            let parsed = parse_command_arg_token(token.as_str().as_bytes());
            assert_eq!(parsed, Some(token));
            let lower = token.as_str().to_lowercase();
            let parsed_lowercase = parse_command_arg_token(lower.as_bytes());
            assert_eq!(parsed_lowercase, Some(token));
        }
    }

    #[test]
    fn test_parse_chunk_size_valid_chunk_sizes() {
        // Test valid minimum size
        assert_eq!(
            parse_chunk_size(&MIN_CHUNK_SIZE.to_string()).unwrap(),
            MIN_CHUNK_SIZE
        );

        // Test valid maximum size
        assert_eq!(
            parse_chunk_size(&MAX_CHUNK_SIZE.to_string()).unwrap(),
            MAX_CHUNK_SIZE
        );

        // Test some typical values (multiples of 8)
        assert_eq!(parse_chunk_size("1024").unwrap(), 1024);
        assert_eq!(parse_chunk_size("4096").unwrap(), 4096);
        assert_eq!(parse_chunk_size("8192").unwrap(), 8192);
    }

    #[test]
    fn test_parse_chunk_size_valid_chunk_sizes_with_units() {
        // Test with units
        assert_eq!(parse_chunk_size("1kb").unwrap(), 1000);
        assert_eq!(parse_chunk_size("2Ki").unwrap(), 2048);
        assert_eq!(parse_chunk_size("1mb").unwrap(), 1000 * 1000);
        assert_eq!(parse_chunk_size("0.5Mi").unwrap(), 524288); // 0.5 * 1024 * 1024
    }

    #[test]
    fn test_parse_chunk_size_invalid_non_integer_chunk_sizes() {
        // Test non-integer values
        assert!(parse_chunk_size("123.5").is_err());
        assert!(parse_chunk_size("1.7kb").is_err());
        assert!(parse_chunk_size("invalid").is_err());
    }

    #[test]
    fn test_parse_chunk_size_invalid_chunk_sizes_below_minimum() {
        // Test values below minimum
        let too_small = MIN_CHUNK_SIZE - 8;
        assert!(parse_chunk_size(&too_small.to_string()).is_err());
        assert!(parse_chunk_size("8").is_err()); // Very small value
    }

    #[test]
    fn test_parse_chunk_size_invalid_chunk_sizes_above_maximum() {
        // Test values above maximum
        let too_large = MAX_CHUNK_SIZE + 8;
        assert!(parse_chunk_size(&too_large.to_string()).is_err());
        assert!(parse_chunk_size("1tb").is_err()); // Very large value
    }

    #[test]
    fn test_parse_chunk_size_invalid_non_multiple_of_eight() {
        // Test values are not multiple of 8
        assert!(parse_chunk_size("1025").is_err());
        assert!(parse_chunk_size("4097").is_err());
        assert!(parse_chunk_size("1023").is_err());
    }

    #[test]
    fn test_parse_chunk_size_invalid_format() {
        // Test invalid formatting
        assert!(parse_chunk_size("abc").is_err());
        assert!(parse_chunk_size("").is_err());
        assert!(parse_chunk_size("-1024").is_err());
        assert!(parse_chunk_size("1024kb!").is_err());
        assert!(parse_chunk_size("1024 kb").is_err());
    }

    #[test]
    fn test_parse_chunk_size_edge_cases() {
        // Exactly at the boundaries of multiples of 8
        let valid_near_min = MIN_CHUNK_SIZE;
        let invalid_near_min = MIN_CHUNK_SIZE + 4;

        assert_eq!(
            parse_chunk_size(&valid_near_min.to_string()).unwrap(),
            valid_near_min
        );
        assert!(parse_chunk_size(&invalid_near_min.to_string()).is_err());

        // Test a value that's very close to max but valid
        let valid_near_max = MAX_CHUNK_SIZE - 8;
        assert_eq!(
            parse_chunk_size(&valid_near_max.to_string()).unwrap(),
            valid_near_max
        );
    }
}
