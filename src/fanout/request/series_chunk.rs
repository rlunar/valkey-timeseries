use super::response_generated::{
    CompressionType, Label as FBLabel, LabelArgs, SampleData, SampleDataArgs, SeriesChunk,
    SeriesChunkArgs,
};
use crate::common::pool::get_pooled_buffer;
use crate::labels::InternedMetricName;
use crate::series::chunks::{GorillaChunk, PcoChunk, TimeSeriesChunk, UncompressedChunk};
use bincode::config::Configuration;
use bincode::serde::{decode_from_slice, encode_into_std_write};
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use serde::de::DeserializeOwned;
use serde::Serialize;
use smallvec::SmallVec;
use std::sync::LazyLock;
use valkey_module::{ValkeyError, ValkeyResult};

static CONFIG: LazyLock<Configuration> = LazyLock::new(|| {
    // Configure bincode with a standard configuration
    bincode::config::standard().with_variable_int_encoding()
});

// for future compatibility
const VERSION: u32 = 1;

fn get_compression_type(chunk: &TimeSeriesChunk) -> CompressionType {
    match chunk {
        TimeSeriesChunk::Uncompressed(_) => CompressionType::None,
        TimeSeriesChunk::Gorilla(_) => CompressionType::Gorilla,
        TimeSeriesChunk::Pco(_) => CompressionType::Pco,
    }
}

pub(super) fn serialize_chunk_internal(
    chunk: TimeSeriesChunk,
    dest: &mut Vec<u8>,
) -> ValkeyResult<usize> {
    fn encode<T: Serialize>(chunk: T, dest: &mut Vec<u8>) -> ValkeyResult<usize> {
        encode_into_std_write(chunk, dest, *CONFIG)
            .map_err(|_e| ValkeyError::Str("Failed to serialize chunk"))
    }

    match chunk {
        TimeSeriesChunk::Uncompressed(data) => encode(data, dest),
        TimeSeriesChunk::Gorilla(data) => encode(data, dest),
        TimeSeriesChunk::Pco(data) => encode(data, dest),
    }
}

pub(super) fn serialize_sample_data<'a>(
    bldr: &mut FlatBufferBuilder<'a>,
    chunk: TimeSeriesChunk,
) -> ValkeyResult<WIPOffset<SampleData<'a>>> {
    let compression = get_compression_type(&chunk);
    let mut buf = get_pooled_buffer(1024);
    let _size = serialize_chunk_internal(chunk, &mut buf)?;
    let data = Some(bldr.create_vector(&buf));

    Ok(SampleData::create(
        bldr,
        &SampleDataArgs {
            version: VERSION,
            compression,
            data,
        },
    ))
}

pub(super) fn serialize_chunk<'a>(
    bldr: &mut FlatBufferBuilder<'a>,
    chunk: TimeSeriesChunk,
    key: Option<&str>,
    labels: &InternedMetricName,
) -> ValkeyResult<WIPOffset<SeriesChunk<'a>>> {
    let key = key.map(|k| bldr.create_string(k));
    let data = serialize_sample_data(bldr, chunk)?;
    let mut lbls: SmallVec<_, 8> = SmallVec::new();
    for label in labels.iter() {
        let name = bldr.create_string(label.name);
        let value = bldr.create_string(label.value);
        let label = FBLabel::create(
            bldr,
            &LabelArgs {
                name: Some(name),
                value: Some(value),
            },
        );
        lbls.push(label);
    }
    let request_labels = bldr.create_vector(&lbls);

    Ok(SeriesChunk::create(
        bldr,
        &SeriesChunkArgs {
            key,
            labels: Some(request_labels),
            data: Some(data),
        },
    ))
}

pub(super) fn deserialize_sample_data(chunk: &SampleData) -> ValkeyResult<TimeSeriesChunk> {
    let data = chunk
        .data()
        .ok_or(ValkeyError::Str("Missing data in SeriesChunk"))?;

    fn decode<T: DeserializeOwned>(data: &[u8]) -> ValkeyResult<T> {
        let (chunk, _) = decode_from_slice(data, *CONFIG)
            .map_err(|_| ValkeyError::Str("Failed to deserialize chunk"))?;
        Ok(chunk)
    }

    match chunk.compression() {
        CompressionType::None => {
            let chunk = decode::<UncompressedChunk>(data.bytes())?;
            Ok(TimeSeriesChunk::Uncompressed(chunk))
        }
        CompressionType::Gorilla => {
            let chunk = decode::<GorillaChunk>(data.bytes())?;
            Ok(TimeSeriesChunk::Gorilla(chunk))
        }
        CompressionType::Pco => {
            let chunk = decode::<PcoChunk>(data.bytes())?;
            Ok(TimeSeriesChunk::Pco(chunk))
        }
        _ => Err(ValkeyError::Str(
            "cluster: Unknown compression type in SeriesChunk",
        )),
    }
}
