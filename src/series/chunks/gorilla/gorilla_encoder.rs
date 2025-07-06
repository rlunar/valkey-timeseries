use super::buffered_writer::BufferedWriter;
use super::serialization::{load_bitwriter_from_rdb, save_bitwriter_to_rdb};
use super::varbit::write_varbit;
use super::varbit_xor::write_varbit_xor;
use super::GorillaIterator;
use crate::common::rdb::{
    rdb_load_timestamp, rdb_load_u8, rdb_load_usize, rdb_save_timestamp, rdb_save_u8,
    rdb_save_usize,
};
use crate::common::Sample;
use get_size::GetSize;
use serde::{Deserialize, Serialize};
use std::mem::size_of_val;
use valkey_module::error::Error as ValkeyError;
use valkey_module::raw;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GorillaEncoder {
    writer: BufferedWriter,
    leading_bits: u8,
    trailing_bits: u8,
    timestamp_delta: i64,
    pub num_samples: usize,
    pub first_ts: i64,
    pub last_ts: i64,
    pub last_value: f64,
}

impl GetSize for GorillaEncoder {
    fn get_size(&self) -> usize {
        self.writer.get_size()
            + size_of_val(&self.num_samples)
            + size_of_val(&self.first_ts)
            + size_of_val(&self.last_ts)
            + size_of_val(&self.last_value)
            + size_of_val(&self.leading_bits)
            + size_of_val(&self.trailing_bits)
            + size_of_val(&self.timestamp_delta)
    }
}

impl GorillaEncoder {
    pub fn new() -> GorillaEncoder {
        let writer = BufferedWriter::new();

        GorillaEncoder {
            writer,
            num_samples: 0,
            last_ts: 0,
            last_value: 0.0,
            leading_bits: 0,
            trailing_bits: 0,
            timestamp_delta: 0,
            first_ts: 0,
        }
    }

    pub fn clear(&mut self) {
        self.writer.clear();
        self.num_samples = 0;
        self.last_ts = 0;
        self.last_value = 0.0;
        self.leading_bits = 0;
        self.trailing_bits = 0;
        self.timestamp_delta = 0;
        self.first_ts = 0;
    }

    pub fn add_sample(&mut self, sample: &Sample) -> std::io::Result<()> {
        match self.num_samples {
            0 => self.write_first_sample(sample),
            1 => self.write_second_sample(sample),
            _ => self.write_nth_sample(sample),
        }
    }

    fn write_first_sample(&mut self, sample: &Sample) -> std::io::Result<()> {
        self.writer.write_varint(sample.timestamp)?;
        // Classic Float64 for the value
        self.writer.write_f64(sample.value);
        self.first_ts = sample.timestamp;
        self.last_ts = sample.timestamp;
        self.last_value = sample.value;
        self.num_samples += 1;
        Ok(())
    }

    fn write_second_sample(&mut self, sample: &Sample) -> std::io::Result<()> {
        let timestamp = sample.timestamp;
        let value = sample.value;

        let timestamp_delta = timestamp - self.last_ts;
        if timestamp_delta < 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "samples aren't sorted by timestamp ascending",
            ));
        }

        self.writer.write_uvarint(timestamp_delta as u64)?;

        let (leading, trailing) =
            write_varbit_xor(value, self.last_value, 0xff, 0, &mut self.writer)?;

        self.last_ts = timestamp;
        self.last_value = value;
        self.leading_bits = leading;
        self.trailing_bits = trailing;
        self.timestamp_delta = timestamp_delta;

        self.num_samples += 1;

        Ok(())
    }

    fn write_nth_sample(&mut self, sample: &Sample) -> std::io::Result<()> {
        let timestamp = sample.timestamp;
        let value = sample.value;

        let timestamp_delta = timestamp - self.last_ts;
        if timestamp_delta < 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "samples aren't sorted by timestamp ascending",
            ));
        }
        let delta_of_delta = timestamp_delta - self.timestamp_delta;

        write_varbit(delta_of_delta, &mut self.writer)?;

        let (leading_bits, trailing_bits) = write_varbit_xor(
            value,
            self.last_value,
            self.leading_bits,
            self.trailing_bits,
            &mut self.writer,
        )?;

        self.last_ts = timestamp;
        self.last_value = value;
        self.leading_bits = leading_bits;
        self.trailing_bits = trailing_bits;
        self.timestamp_delta = timestamp_delta;

        self.num_samples += 1;

        Ok(())
    }

    pub fn iter(&self) -> GorillaIterator {
        GorillaIterator::new(self)
    }

    pub(crate) fn buf(&self) -> &[u8] {
        self.writer.get_ref()
    }

    pub(crate) fn shrink_to_fit(&mut self) {
        self.writer.shrink_to_fit()
    }

    pub fn rdb_save(&self, rdb: *mut raw::RedisModuleIO) {
        save_bitwriter_to_rdb(rdb, &self.writer);
        rdb_save_usize(rdb, self.num_samples);
        rdb_save_timestamp(rdb, self.first_ts);
        rdb_save_timestamp(rdb, self.last_ts);
        raw::save_double(rdb, self.last_value);
        rdb_save_u8(rdb, self.leading_bits);
        rdb_save_u8(rdb, self.trailing_bits);
        raw::save_signed(rdb, self.timestamp_delta);
    }

    pub fn rdb_load(rdb: *mut raw::RedisModuleIO) -> Result<GorillaEncoder, ValkeyError> {
        let writer = load_bitwriter_from_rdb(rdb)?;
        let num_samples = rdb_load_usize(rdb)?;
        let first_ts = rdb_load_timestamp(rdb)?;
        let timestamp = rdb_load_timestamp(rdb)?;
        let value = raw::load_double(rdb)?;
        let leading_bits = rdb_load_u8(rdb)?;
        let trailing_bits = rdb_load_u8(rdb)?;
        let timestamp_delta = raw::load_signed(rdb)?;

        Ok(GorillaEncoder {
            writer,
            num_samples,
            first_ts,
            last_ts: timestamp,
            last_value: value,
            leading_bits,
            trailing_bits,
            timestamp_delta,
        })
    }
}

impl PartialEq<Self> for GorillaEncoder {
    fn eq(&self, other: &Self) -> bool {
        if self.num_samples != other.num_samples {
            return false;
        }
        if self.last_ts != other.last_ts {
            return false;
        }
        if self.last_value != other.last_value {
            return false;
        }
        if self.leading_bits != other.leading_bits {
            return false;
        }
        if self.trailing_bits != other.trailing_bits {
            return false;
        }
        if self.timestamp_delta != other.timestamp_delta {
            return false;
        }
        self.writer == other.writer
    }
}

impl Eq for GorillaEncoder {}

#[cfg(test)]
mod tests {}
