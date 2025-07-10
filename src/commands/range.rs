use crate::commands::arg_parse::parse_range_options;
use crate::error_consts;
use crate::series::get_timeseries;
use crate::series::range_utils::get_range;
use valkey_module::{
    AclPermissions, Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};

/// TS.RANGE key fromTimestamp toTimestamp
//   [LATEST]
//   [FILTER_BY_TS ts...]
//   [FILTER_BY_VALUE min max]
//   [COUNT count]
//   [[ALIGN align] AGGREGATION aggregator bucketDuration [BUCKETTIMESTAMP bt] [EMPTY]]
pub fn range(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 4 {
        return Err(ValkeyError::WrongArity);
    }

    let mut args = args.into_iter().skip(1).peekable();

    let key = args.next_arg()?;
    let options = parse_range_options(&mut args)?;

    args.done()?;
    let Some(series) = get_timeseries(ctx, key, Some(AclPermissions::ACCESS), true)? else {
        // essentially a dead branch, but satisfies the compiler
        // since we have already checked the key existence
        return Err(ValkeyError::Str(error_consts::KEY_NOT_FOUND));
    };

    let samples = get_range(&series, &options, true);
    let result = samples
        .into_iter()
        .map(|x| x.into())
        .collect::<Vec<ValkeyValue>>();

    Ok(ValkeyValue::from(result))
}
