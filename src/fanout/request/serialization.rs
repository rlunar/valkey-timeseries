use crate::common::Sample;
use crate::fanout::request::common::{
    deserialize_timestamp_range, load_flatbuffers_object, serialize_timestamp_range,
};
use crate::fanout::request::matchers::{deserialize_matchers, serialize_matchers};
use crate::fanout::request::request_generated::{MetadataRequest, MetadataRequestArgs};
use crate::series::chunks::{GorillaChunk, PcoChunk, TimeSeriesChunk, UncompressedChunk};
use crate::series::request_types::MatchFilterOptions;
use flatbuffers::FlatBufferBuilder;
use std::io::Write;
use valkey_module::{ValkeyError, ValkeyResult};

/// A trait for types that can be serialized to and deserialized from a byte stream.
///
/// The generic parameter `T` represents the type that will be produced when deserializing.
/// This is typically the implementing type itself but allows for flexibility when needed.
pub trait Serialized {
    /// Serializes the implementing type to the provided writer.
    fn serialize(&self, dest: &mut Vec<u8>);
}

pub trait Deserialized: Sized {
    /// Deserializes an instance of type `T` from the provided buffer.
    fn deserialize(buf: &[u8]) -> ValkeyResult<Self>;
}

impl Serialized for MatchFilterOptions {
    fn serialize(&self, buf: &mut Vec<u8>) {
        let mut bldr = FlatBufferBuilder::with_capacity(512);

        let range = serialize_timestamp_range(&mut bldr, self.date_range);
        let matchers = self
            .matchers
            .iter()
            .map(|m| serialize_matchers(&mut bldr, m))
            .collect::<Vec<_>>();

        let filters = bldr.create_vector(&matchers);
        let args = MetadataRequestArgs {
            range,
            filters: Some(filters),
            limit: self.limit.unwrap_or_default() as u32,
        };

        let obj = MetadataRequest::create(&mut bldr, &args);

        bldr.finish(obj, None);
        let data = bldr.finished_data();
        buf.write_all(data).unwrap();
    }
}

impl Deserialized for MatchFilterOptions {
    fn deserialize(buf: &[u8]) -> ValkeyResult<Self> {
        let req = load_flatbuffers_object::<MetadataRequest>(buf, "MatchFilterOptions")?;
        let range = deserialize_timestamp_range(req.range())?;
        let limit = if req.limit() > 0 {
            Some(req.limit() as usize)
        } else {
            None
        };
        let mut matchers = Vec::with_capacity(req.filters().unwrap_or_default().len());
        for filter in req.filters().unwrap_or_default() {
            let matcher = deserialize_matchers(&filter)?;
            matchers.push(matcher);
        }
        Ok(MatchFilterOptions {
            date_range: range,
            matchers,
            limit,
        })
    }
}

pub(super) fn samples_to_chunk(samples: &[Sample]) -> ValkeyResult<TimeSeriesChunk> {
    let mut chunk = if samples.len() >= 1000 {
        TimeSeriesChunk::Pco(PcoChunk::default())
    } else if samples.len() >= 5 {
        TimeSeriesChunk::Gorilla(GorillaChunk::default())
    } else {
        TimeSeriesChunk::Uncompressed(UncompressedChunk::default())
    };
    chunk
        .set_data(samples)
        .map_err(|e| ValkeyError::String(format!("Failed to set chunk data: {e}")))?;
    Ok(chunk)
}
