mod common;
mod index_query;
mod label_names;
mod matchers;
mod mget;
mod mrange;

#[allow(
    dead_code,
    unused_imports,
    unused_lifetimes,
    clippy::derivable_impls,
    clippy::needless_lifetimes,
    clippy::extra_unused_lifetimes
)]
#[path = "./request_generated.rs"]
mod request_generated;

mod cardinality;
pub mod error;
mod label_values;
#[allow(
    dead_code,
    unused_imports,
    unused_lifetimes,
    clippy::derivable_impls,
    clippy::needless_lifetimes,
    clippy::extra_unused_lifetimes
)]
#[path = "./response_generated.rs"]
mod response_generated;
pub(crate) mod serialization;
mod series_chunk;
mod stats;

pub use cardinality::*;
pub use index_query::*;
pub use label_names::*;
pub use label_values::*;
pub use mget::*;
pub use mrange::*;
pub use stats::*;

use crate::fanout::request::serialization::{Deserialized, Serialized};
use crate::fanout::TrackerEnum;
pub use common::MessageHeader;
use valkey_module::{Context, ValkeyResult};

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandMessageType {
    IndexQuery = 0,
    MultiRangeQuery = 1,
    MGetQuery = 2,
    LabelNames = 3,
    LabelValues = 4,
    Cardinality = 5,
    Stats = 6,
    Error = 255,
}

impl From<u8> for CommandMessageType {
    fn from(value: u8) -> Self {
        match value {
            0 => CommandMessageType::IndexQuery,
            1 => CommandMessageType::MultiRangeQuery,
            2 => CommandMessageType::MGetQuery,
            3 => CommandMessageType::LabelNames,
            4 => CommandMessageType::LabelValues,
            5 => CommandMessageType::Cardinality,
            6 => CommandMessageType::Stats,
            _ => CommandMessageType::Error,
        }
    }
}

impl std::fmt::Display for CommandMessageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CommandMessageType::IndexQuery => write!(f, "IndexQuery"),
            CommandMessageType::MultiRangeQuery => write!(f, "MultiRangeQuery"),
            CommandMessageType::MGetQuery => write!(f, "MGetQuery"),
            CommandMessageType::LabelNames => write!(f, "LabelNames"),
            CommandMessageType::LabelValues => write!(f, "LabelValues"),
            CommandMessageType::Cardinality => write!(f, "Cardinality"),
            CommandMessageType::Stats => write!(f, "Stats"),
            CommandMessageType::Error => write!(f, "Error"),
        }
    }
}

pub trait MultiShardCommand {
    type REQ: Serialized + Deserialized + Default;
    type RES: Serialized + Deserialized + Default;
    fn request_type() -> CommandMessageType;
    fn exec(ctx: &Context, req: Self::REQ) -> ValkeyResult<Self::RES>;
    fn update_tracker(tracker: &TrackerEnum, res: Self::RES);
}
