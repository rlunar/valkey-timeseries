use crate::commands::command_args::{parse_join_args, parse_timestamp_range};
use crate::error_consts;
use crate::join::{JoinOptions, process_join};
use crate::series::get_timeseries;
use valkey_module::{AclPermissions, Context, NextArg, ValkeyError, ValkeyResult, ValkeyString};

/// TS.JOIN key1 key2 fromTimestamp toTimestamp
///   [INNER | FULL | LEFT | RIGHT | ANTI | SEMI | ASOF [PREVIOUS | NEXT | NEAREST] tolerance [ALLOW_EXACT_MATCH [true|false]]]
///   [FILTER_BY_TS ts...]
///   [FILTER_BY_VALUE min max]
///   [COUNT count]
///   [REDUCE op]
///   [AGGREGATION aggregator bucket_duration [ALIGN align] [BUCKETTIMESTAMP timestamp] [EMPTY]]
pub fn join(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();

    let left_key = args.next_arg()?;
    let right_key = args.next_arg()?;
    let date_range = parse_timestamp_range(&mut args)?;

    let mut options = JoinOptions {
        date_range,
        ..Default::default()
    };

    parse_join_args(&mut args, &mut options)?;

    if left_key == right_key {
        return Err(ValkeyError::Str(error_consts::DUPLICATE_JOIN_KEYS));
    }

    // In both cases we pass true for must_exist, meaning that if the series does not exist, we will
    // propagate an error. Because of this, unwrap is safe to use here.
    let left_series = get_timeseries(ctx, &left_key, Some(AclPermissions::ACCESS), true)?.unwrap();
    let right_series =
        get_timeseries(ctx, &right_key, Some(AclPermissions::ACCESS), true)?.unwrap();

    let result = process_join(&left_series, &right_series, &options)?;
    Ok(result.into())
}
