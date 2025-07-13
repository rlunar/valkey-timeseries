use crate::common::Timestamp;
use crate::error_consts;
use crate::parser::timestamp::parse_timestamp;
use std::fmt::Display;
use valkey_module::{ValkeyError, ValkeyString};

mod handlers;
mod iterator;

pub use handlers::*;
pub use iterator::*;

#[derive(Debug, Default, PartialEq, Clone, Copy)]
pub enum BucketTimestamp {
    #[default]
    Start,
    End,
    Mid,
}

impl BucketTimestamp {
    pub fn calculate(&self, ts: Timestamp, time_delta: u64) -> Timestamp {
        match self {
            Self::Start => ts,
            Self::Mid => ts.saturating_add_unsigned(time_delta / 2),
            Self::End => ts.saturating_add_unsigned(time_delta),
        }
    }
}

impl TryFrom<&str> for BucketTimestamp {
    type Error = ValkeyError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let ts = hashify::tiny_map_ignore_case! {
            value.as_bytes(),
            "-" => BucketTimestamp::Start,
            "+" => BucketTimestamp::End,
            "~" => BucketTimestamp::Mid,
            "start" => BucketTimestamp::Start,
            "end" => BucketTimestamp::End,
            "mid" => BucketTimestamp::Mid,
        };
        match ts {
            Some(ts) => Ok(ts),
            None => Err(ValkeyError::Str("TSDB: invalid BUCKETTIMESTAMP value")),
        }
    }
}

impl TryFrom<&ValkeyString> for BucketTimestamp {
    type Error = ValkeyError;
    fn try_from(value: &ValkeyString) -> Result<Self, Self::Error> {
        value.to_string_lossy().as_str().try_into()
    }
}

#[derive(Debug, Default, PartialEq, Clone, Copy)]
pub enum BucketAlignment {
    #[default]
    Default,
    Start,
    End,
    Timestamp(Timestamp),
}

impl BucketAlignment {
    pub fn get_aligned_timestamp(&self, start: Timestamp, end: Timestamp) -> Timestamp {
        match self {
            BucketAlignment::Default => 0,
            BucketAlignment::Start => start,
            BucketAlignment::End => end,
            BucketAlignment::Timestamp(ts) => *ts,
        }
    }
}

impl TryFrom<&str> for BucketAlignment {
    type Error = ValkeyError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let alignment = hashify::tiny_map_ignore_case! {
            value.as_bytes(),
            "start" => BucketAlignment::Start,
            "end" => BucketAlignment::End,
            "-" => BucketAlignment::Start,
            "+" => BucketAlignment::End,
        };
        match alignment {
            Some(alignment) => Ok(alignment),
            None => {
                let timestamp = parse_timestamp(value, false)
                    .map_err(|_| ValkeyError::Str(error_consts::INVALID_ALIGN))?;
                Ok(BucketAlignment::Timestamp(timestamp))
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Aggregation {
    Avg,
    Count,
    First,
    Last,
    Max,
    Min,
    Range,
    StdP,
    StdS,
    Sum,
    VarP,
    VarS,
}

impl Aggregation {
    pub fn name(&self) -> &'static str {
        match self {
            Aggregation::First => "first",
            Aggregation::Last => "last",
            Aggregation::Min => "min",
            Aggregation::Max => "max",
            Aggregation::Avg => "avg",
            Aggregation::Sum => "sum",
            Aggregation::Count => "count",
            Aggregation::StdS => "std.s",
            Aggregation::StdP => "std.p",
            Aggregation::VarS => "var.s",
            Aggregation::VarP => "var.p",
            Aggregation::Range => "range",
        }
    }
}
impl Display for Aggregation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl TryFrom<&str> for Aggregation {
    type Error = ValkeyError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let value = hashify::tiny_map_ignore_case! {
            value.as_bytes(),
            "avg" => Aggregation::Avg,
            "count" => Aggregation::Count,
            "first" => Aggregation::First,
            "last" => Aggregation::Last,
            "min" => Aggregation::Min,
            "max" => Aggregation::Max,
            "sum" => Aggregation::Sum,
            "range" => Aggregation::Range,
            "std.s" => Aggregation::StdS,
            "std.p" => Aggregation::StdP,
            "var.s" => Aggregation::VarS,
            "var.p" => Aggregation::VarP,
        };

        match value {
            Some(agg) => Ok(agg),
            None => Err(ValkeyError::Str(error_consts::UNKNOWN_AGGREGATION_TYPE)),
        }
    }
}

impl TryFrom<&ValkeyString> for Aggregation {
    type Error = ValkeyError;

    fn try_from(value: &ValkeyString) -> Result<Self, Self::Error> {
        let str = value.to_string_lossy();
        Aggregation::try_from(str.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_timestamp_calculates_correctly_for_start() {
        let ts = Timestamp::from(1000);
        let delta = 500;
        assert_eq!(BucketTimestamp::Start.calculate(ts, delta), ts);
    }

    #[test]
    fn bucket_timestamp_calculates_correctly_for_mid() {
        let ts = Timestamp::from(1000);
        let delta = 500;
        assert_eq!(
            BucketTimestamp::Mid.calculate(ts, delta),
            Timestamp::from(1250)
        );
    }

    #[test]
    fn bucket_timestamp_calculates_correctly_for_end() {
        let ts = Timestamp::from(1000);
        let delta = 500;
        assert_eq!(
            BucketTimestamp::End.calculate(ts, delta),
            Timestamp::from(1500)
        );
    }

    #[test]
    fn bucket_timestamp_try_from_str_parses_valid_values() {
        assert_eq!(
            BucketTimestamp::try_from("start").unwrap(),
            BucketTimestamp::Start
        );
        assert_eq!(
            BucketTimestamp::try_from("end").unwrap(),
            BucketTimestamp::End
        );
        assert_eq!(
            BucketTimestamp::try_from("mid").unwrap(),
            BucketTimestamp::Mid
        );
        assert_eq!(
            BucketTimestamp::try_from("-").unwrap(),
            BucketTimestamp::Start
        );
        assert_eq!(
            BucketTimestamp::try_from("+").unwrap(),
            BucketTimestamp::End
        );
        assert_eq!(
            BucketTimestamp::try_from("~").unwrap(),
            BucketTimestamp::Mid
        );
    }

    #[test]
    fn bucket_timestamp_try_from_str_returns_error_for_invalid_value() {
        assert!(BucketTimestamp::try_from("invalid").is_err());
    }

    #[test]
    fn bucket_alignment_gets_correct_aligned_timestamp() {
        let start = Timestamp::from(1000);
        let end = Timestamp::from(2000);
        assert_eq!(
            BucketAlignment::Default.get_aligned_timestamp(start, end),
            0
        );
        assert_eq!(
            BucketAlignment::Start.get_aligned_timestamp(start, end),
            start
        );
        assert_eq!(BucketAlignment::End.get_aligned_timestamp(start, end), end);
        assert_eq!(
            BucketAlignment::Timestamp(Timestamp::from(1500)).get_aligned_timestamp(start, end),
            Timestamp::from(1500)
        );
    }

    #[test]
    fn bucket_alignment_try_from_str_parses_valid_values() {
        assert_eq!(
            BucketAlignment::try_from("start").unwrap(),
            BucketAlignment::Start
        );
        assert_eq!(
            BucketAlignment::try_from("end").unwrap(),
            BucketAlignment::End
        );
        assert_eq!(
            BucketAlignment::try_from("-").unwrap(),
            BucketAlignment::Start
        );
        assert_eq!(
            BucketAlignment::try_from("+").unwrap(),
            BucketAlignment::End
        );
        assert_eq!(
            BucketAlignment::try_from("1500").unwrap(),
            BucketAlignment::Timestamp(Timestamp::from(1500))
        );
    }

    #[test]
    fn bucket_alignment_try_from_str_returns_error_for_invalid_value() {
        assert!(BucketAlignment::try_from("invalid").is_err());
    }

    #[test]
    fn aggregation_name_returns_correct_value() {
        assert_eq!(Aggregation::Avg.name(), "avg");
        assert_eq!(Aggregation::Count.name(), "count");
        assert_eq!(Aggregation::First.name(), "first");
        assert_eq!(Aggregation::Last.name(), "last");
        assert_eq!(Aggregation::Min.name(), "min");
        assert_eq!(Aggregation::Max.name(), "max");
        assert_eq!(Aggregation::Sum.name(), "sum");
        assert_eq!(Aggregation::Range.name(), "range");
        assert_eq!(Aggregation::StdS.name(), "std.s");
        assert_eq!(Aggregation::StdP.name(), "std.p");
        assert_eq!(Aggregation::VarS.name(), "var.s");
        assert_eq!(Aggregation::VarP.name(), "var.p");
    }

    #[test]
    fn aggregation_try_from_str_parses_valid_values() {
        assert_eq!(Aggregation::try_from("avg").unwrap(), Aggregation::Avg);
        assert_eq!(Aggregation::try_from("count").unwrap(), Aggregation::Count);
        assert_eq!(Aggregation::try_from("first").unwrap(), Aggregation::First);
        assert_eq!(Aggregation::try_from("last").unwrap(), Aggregation::Last);
        assert_eq!(Aggregation::try_from("min").unwrap(), Aggregation::Min);
        assert_eq!(Aggregation::try_from("max").unwrap(), Aggregation::Max);
        assert_eq!(Aggregation::try_from("sum").unwrap(), Aggregation::Sum);
        assert_eq!(Aggregation::try_from("range").unwrap(), Aggregation::Range);
        assert_eq!(Aggregation::try_from("std.s").unwrap(), Aggregation::StdS);
        assert_eq!(Aggregation::try_from("std.p").unwrap(), Aggregation::StdP);
        assert_eq!(Aggregation::try_from("var.s").unwrap(), Aggregation::VarS);
        assert_eq!(Aggregation::try_from("var.p").unwrap(), Aggregation::VarP);
    }

    #[test]
    fn aggregation_try_from_str_returns_error_for_invalid_value() {
        assert!(Aggregation::try_from("invalid").is_err());
    }
}
