use get_size::GetSize;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use valkey_module::ValkeyValue;

pub type Timestamp = i64;
pub type SampleValue = f64;

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, GetSize)]
pub struct Sample {
    pub timestamp: Timestamp,
    pub value: SampleValue,
}

impl Sample {
    pub fn new(timestamp: Timestamp, value: SampleValue) -> Self {
        Sample { timestamp, value }
    }
}

pub const SAMPLE_SIZE: usize = size_of::<Sample>();
impl PartialEq for Sample {
    #[inline]
    fn eq(&self, other: &Sample) -> bool {
        // Two data points are equal if their times are equal, and their values are either equal or are NaN.
        if self.timestamp == other.timestamp {
            return if self.value.is_nan() {
                other.value.is_nan()
            } else {
                self.value == other.value
            };
        }
        false
    }
}

impl Eq for Sample {}

impl Ord for Sample {
    fn cmp(&self, other: &Self) -> Ordering {
        let cmp = self.timestamp.cmp(&other.timestamp);
        if cmp == Ordering::Equal {
            if self.value.is_nan() {
                if other.value.is_nan() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            } else if other.value.is_nan() {
                Ordering::Less
            } else {
                self.value.partial_cmp(&other.value).unwrap()
            }
        } else {
            cmp
        }
    }
}

impl PartialOrd for Sample {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Hash for Sample {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.timestamp.hash(state);
        self.value.to_bits().hash(state);
    }
}

impl Display for Sample {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} @ {}", self.value, self.timestamp)
    }
}

impl From<Sample> for ValkeyValue {
    fn from(sample: Sample) -> Self {
        (&sample).into()
    }
}

impl From<&Sample> for ValkeyValue {
    fn from(sample: &Sample) -> Self {
        let row = vec![
            ValkeyValue::from(sample.timestamp),
            ValkeyValue::from(sample.value),
        ];
        ValkeyValue::from(row)
    }
}
