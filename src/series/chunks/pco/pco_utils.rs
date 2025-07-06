use crate::common::encoding::{read_uvarint, write_uvarint};
use crate::common::parallel::join;
use crate::common::pool::get_pooled_buffer;
use crate::common::Timestamp;
use crate::error::{TsdbError, TsdbResult};
use pco::data_types::Number;
use pco::errors::PcoError;
use pco::standalone::{
    simple_compress, simple_compress_into, simple_decompress, simple_decompress_into,
};
use pco::DEFAULT_COMPRESSION_LEVEL;
use pco::{ChunkConfig, DeltaSpec};
use std::error::Error;

// mirror ChunkConfig here so downstream users don't need to import pco
#[derive(Clone, Debug)]
pub struct CompressorConfig {
    pub compression_level: usize,
    pub delta_encoding_order: usize,
}

impl Default for CompressorConfig {
    fn default() -> Self {
        Self {
            compression_level: DEFAULT_COMPRESSION_LEVEL,
            delta_encoding_order: 0,
        }
    }
}

pub fn pco_encode<T: Number>(src: &[T], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error>> {
    let config = ChunkConfig::default();
    if src.is_empty() {
        return Ok(());
    }

    let compressed = simple_compress(src, &config)?;
    dst.extend_from_slice(&compressed);
    Ok(())
}

pub fn encode_with_options<T: Number>(
    src: &[T],
    dst: &mut Vec<u8>,
    options: CompressorConfig,
) -> Result<(), Box<dyn Error>> {
    let mut config = ChunkConfig::default();
    config.compression_level = options.compression_level;
    if options.delta_encoding_order != 0 {
        config.delta_spec = DeltaSpec::TryConsecutive(options.delta_encoding_order);
    }

    let compressed = simple_compress(src, &config)?;
    dst.extend_from_slice(&compressed);
    Ok(())
}

pub fn pco_decode<T: Number>(src: &[u8], dst: &mut Vec<T>) -> Result<(), Box<dyn Error>> {
    if src.is_empty() {
        return Ok(());
    }
    match simple_decompress(src) {
        Ok(values) => dst.extend_from_slice(&values),
        Err(e) => return Err(Box::new(e)),
    }
    Ok(())
}

pub(super) fn compress_values(compressed: &mut Vec<u8>, values: &[f64]) -> TsdbResult<()> {
    if values.is_empty() {
        return Ok(());
    }
    pco_encode(values, compressed).map_err(|e| TsdbError::CannotSerialize(format!("values: {e}")))
}

pub(super) fn decompress_values(compressed: &[u8], dst: &mut Vec<f64>) -> TsdbResult<()> {
    if compressed.is_empty() {
        return Ok(());
    }
    pco_decode(compressed, dst).map_err(|e| TsdbError::CannotDeserialize(format!("values: {e}")))
}

fn write_data<T: Number>(
    dest: &mut Vec<u8>,
    values: &[T],
    config: &ChunkConfig,
) -> TsdbResult<usize> {
    let save_pos = dest.len();
    // placeholder for data length
    write_usize(dest, save_pos);
    simple_compress_into(values, config, dest).map_err(map_err)?;
    let len = dest.len() - save_pos;
    dest[save_pos..save_pos + size_of::<usize>()].copy_from_slice(&len.to_le_bytes());
    Ok(len)
}

fn read_timestamp_page(compressed: &mut &[u8], dst: &mut [i64]) -> TsdbResult<usize> {
    let size = read_usize(compressed, "timestamp data size")?;
    let progress = simple_decompress_into(compressed, dst).map_err(map_err)?;
    if !progress.finished {
        return Err(TsdbError::CannotDeserialize(
            "incomplete timestamp data".to_string(),
        ));
    }
    *compressed = &compressed[size..];
    Ok(progress.n_processed)
}

fn read_values_page(compressed: &mut &[u8], dst: &mut [f64]) -> TsdbResult<usize> {
    let size = read_usize(compressed, "value data size")?;
    let progress = simple_decompress_into(compressed, dst).map_err(map_err)?;
    if !progress.finished {
        return Err(TsdbError::CannotDeserialize(
            "incomplete value data".to_string(),
        ));
    }

    *compressed = &compressed[size..];
    Ok(progress.n_processed)
}
pub(super) fn compress_timestamps(
    compressed: &mut Vec<u8>,
    timestamps: &[Timestamp],
) -> TsdbResult<()> {
    if timestamps.is_empty() {
        return Ok(());
    }
    let config = CompressorConfig {
        compression_level: DEFAULT_COMPRESSION_LEVEL,
        delta_encoding_order: 2,
    };
    encode_with_options(timestamps, compressed, config)
        .map_err(|e| TsdbError::CannotSerialize(format!("timestamps: {e}")))
}

pub(super) fn decompress_timestamps(compressed: &[u8], dst: &mut Vec<i64>) -> TsdbResult<()> {
    if compressed.is_empty() {
        return Ok(());
    }
    pco_decode(compressed, dst)
        .map_err(|e| TsdbError::CannotDeserialize(format!("timestamps: {e}")))
}

pub fn compress_samples_internal(
    compressed: &mut Vec<u8>,
    timestamps: &[Timestamp],
    values: &[f64],
) -> TsdbResult<()> {
    if timestamps.is_empty() || values.is_empty() {
        return Ok(());
    }

    let count = timestamps.len();
    if count != values.len() {
        return Err(TsdbError::CannotSerialize(
            "timestamps and values must have the same length".to_string(),
        ));
    }

    let mut value_buf = get_pooled_buffer(4096);
    let mut ts_buf = get_pooled_buffer(1024);

    let (l, r) = join(
        || compress_timestamps(&mut ts_buf, timestamps),
        || compress_values(&mut value_buf, values),
    );
    l?;
    r?;

    let data_len = ts_buf.len() + value_buf.len();
    write_uvarint(compressed, count as u64);
    write_uvarint(compressed, data_len as u64);
    write_uvarint(compressed, ts_buf.len() as u64);
    write_uvarint(compressed, value_buf.len() as u64);

    compressed.extend_from_slice(&ts_buf);
    compressed.extend_from_slice(&value_buf);

    Ok(())
}
/// Decompresses samples compressed with compress_samples_internal.
///
/// This function reads sample metadata (count, data lengths) from the compressed buffer,
/// then extracts and decompresses the timestamps and values.
///
/// # Arguments
///
/// * `compressed` - The compressed byte buffer containing the samples
/// * `timestamps` - A mutable vector where decompressed timestamps will be stored
/// * `values` - A mutable vector where decompressed values will be stored
///
/// # Returns
///
/// * `Ok(())` on success
/// * `Err` with a TsdbError on failure
pub fn decompress_samples_internal(
    compressed: &[u8],
    timestamps: &mut Vec<Timestamp>,
    values: &mut Vec<f64>,
) -> TsdbResult<()> {
    if compressed.is_empty() {
        return Ok(());
    }

    let mut input = compressed;
    let Some((count, ofs)) = read_uvarint(input, 0) else {
        return Err(TsdbError::CannotDeserialize("count".to_string()));
    };

    input = &input[ofs..];
    let Some((_data_len, ofs)) = read_uvarint(input, 0) else {
        return Err(TsdbError::CannotDeserialize("data_len".to_string()));
    };

    input = &input[ofs..];
    let Some((ts_len, ofs)) = read_uvarint(input, 0) else {
        return Err(TsdbError::CannotDeserialize("ts_len".to_string()));
    };

    input = &input[ofs..];
    let Some((value_len, _)) = read_uvarint(input, 0) else {
        return Err(TsdbError::CannotDeserialize("value_len".to_string()));
    };

    // Ensure we have enough data
    if input.len() < (ts_len + value_len) as usize {
        return Err(TsdbError::CannotDeserialize(
            "insufficient data in sample buffer".to_string(),
        ));
    }

    // Extract compressed timestamp and value buffers
    let ts_buf = &input[0..ts_len as usize];
    let value_buf = &input[ts_len as usize..(ts_len + value_len) as usize];

    // Pre-allocate vectors with enough capacity
    timestamps.clear();
    timestamps.reserve(count as usize);
    values.clear();
    values.reserve(count as usize);

    // Decompress in parallel
    let (ts_result, val_result) = join(
        || decompress_timestamps(ts_buf, timestamps),
        || decompress_values(value_buf, values),
    );

    ts_result?;
    val_result?;

    // Verify lengths match as expected
    if timestamps.len() != count as usize || values.len() != count as usize {
        return Err(TsdbError::CannotDeserialize(format!(
            "decompressed count mismatch: expected {} but got {} timestamps and {} values",
            count,
            timestamps.len(),
            values.len()
        )));
    }

    Ok(())
}

fn write_usize(slice: &mut Vec<u8>, size: usize) {
    slice.extend_from_slice(&size.to_le_bytes());
}

fn read_usize(input: &mut &[u8], field: &str) -> TsdbResult<usize> {
    let (int_bytes, rest) = input.split_at(size_of::<usize>());
    let buf = int_bytes.try_into().map_err(|_| {
        TsdbError::CannotDeserialize(format!("invalid usize reading {field}").to_string())
    })?;

    *input = rest;
    Ok(usize::from_le_bytes(buf))
}

fn write_buf(dest: &mut Vec<u8>, data: &[u8]) {
    let len = data.len();
    write_usize(dest, len);
    dest.extend_from_slice(data);
}

fn read_buf(input: &mut &[u8]) -> TsdbResult<Vec<u8>> {
    let size = read_usize(input, "buffer size")?;
    if input.len() < size {
        return Err(TsdbError::CannotDeserialize(
            "insufficient data for buffer".to_string(),
        ));
    }
    let (data, rest) = input.split_at(size);
    *input = rest;
    Ok(data.to_vec())
}

fn map_err(e: PcoError) -> TsdbError {
    TsdbError::CannotSerialize(e.to_string())
}

#[cfg(test)]
mod tests {
    #[test]
    fn encode_no_values() {
        let src: Vec<f64> = vec![];
        let mut dst = vec![];

        // check for error
        super::pco_encode(&src, &mut dst).expect("failed to encode src");

        // verify encoded no values.
        let exp: Vec<u8> = Vec::new();
        assert_eq!(dst.to_vec(), exp);
    }
}
