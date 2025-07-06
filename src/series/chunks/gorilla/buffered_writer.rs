// MIT License
//
// Portions Copyright (c) 2016 Jerome Froelich
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
use super::traits::BitWrite;
use super::utils::{zigzag_encode, MSB};
use get_size::GetSize;
use num_traits::PrimInt;
use serde::{Deserialize, Serialize};
use std::io::Result;

/// BufferedWriter
///
/// BufferedWriter writes bytes to a buffer.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, GetSize)]
pub struct BufferedWriter {
    buf: Vec<u8>,
    pos: u32, // position in the last byte in the buffer
}

impl BufferedWriter {
    /// new creates a new BufferedWriter
    pub fn new() -> Self {
        BufferedWriter {
            buf: Vec::new(),
            // set pos to 8 to indicate the buffer has no space presently since it is empty
            pos: 0,
        }
    }

    /// Hydrate a BufferedWriter from a Vec<u8> and a position
    pub(crate) fn hydrate(buf: Vec<u8>, pos: u32) -> Self {
        BufferedWriter { buf, pos }
    }

    fn grow(&mut self) {
        self.buf.push(0);
    }

    fn last_index(&self) -> usize {
        self.buf.len() - 1
    }

    pub fn write_bits(&mut self, bits: u32, value: u64) -> Result<()> {
        // we should never write more than 64 bits for an u64
        let mut num_bits = if bits > 64 { 64 } else { bits };

        let mut value = value.wrapping_shl(64 - num_bits);
        while num_bits >= 8 {
            let byte = value.wrapping_shr(56);
            self.write_byte(byte as u8);

            value = value.wrapping_shl(8);
            num_bits -= 8;
        }

        while num_bits > 0 {
            let byte = value.wrapping_shr(63);

            self.write_bit(byte == 1)?;

            value = value.wrapping_shl(1);
            num_bits -= 1;
        }

        Ok(())
    }

    pub fn write_u64(&mut self, value: u64) {
        for byte in value.to_be_bytes() {
            self.write_byte(byte);
        }
    }

    pub fn write_f64(&mut self, value: f64) {
        self.write_u64(value.to_bits());
    }

    pub fn write_uvarint(&mut self, value: u64) -> Result<()> {
        let mut x: u64 = value;
        while x >= 0x80 {
            let enc = MSB | (x as u8);
            self.write_byte(enc);
            x >>= 7;
        }
        self.write_byte(x as u8);

        Ok(())
    }

    #[inline]
    pub fn write_varint(&mut self, value: i64) -> Result<()> {
        let n: u64 = zigzag_encode(value);
        self.write_uvarint(n)
    }

    pub fn get_ref(&self) -> &[u8] {
        &self.buf
    }

    pub fn clear(&mut self) {
        self.buf.clear();
        self.pos = 0;
    }

    pub fn position(&self) -> u32 {
        self.pos
    }

    pub fn shrink_to_fit(&mut self) {
        self.buf.shrink_to_fit();
    }
}

impl BitWrite for BufferedWriter {
    fn write_bit(&mut self, bit: bool) -> Result<()> {
        if self.pos == 0 {
            self.grow();
            self.pos = 8;
        }

        if bit {
            let i = self.last_index();
            self.buf[i] |= 1u8.wrapping_shl(self.pos - 1);
        }

        self.pos -= 1;
        Ok(())
    }

    fn write<U>(&mut self, bits: u32, value: U) -> Result<()>
    where
        U: PrimInt,
    {
        self.write_bits(bits, value.to_u64().expect("Invalid u64 cast"))
    }

    fn write_byte(&mut self, byte: u8) {
        if self.pos == 0 {
            self.buf.push(byte);
            return;
        }

        let mut i = self.last_index();
        // Complete the last byte with the leftmost b.count bits from byt.
        let mut b = byte.wrapping_shr(8 - self.pos);
        self.buf[i] |= b;

        self.grow();
        i += 1;
        // Write the remainder, if any.
        b = byte.wrapping_shl(self.pos);
        self.buf[i] |= b;
    }
}

#[cfg(test)]
mod tests {
    use super::BufferedWriter;
    use crate::series::chunks::gorilla::{buffered_read::BufferedReader, traits::BitWrite};

    #[test]
    fn write_bit() {
        let mut b = BufferedWriter::new();

        // 170 = 0b10101010
        for i in 0..8 {
            if i % 2 == 0 {
                b.write_bit(true).unwrap();
                continue;
            }

            b.write_bit(false).unwrap();
        }

        // 146 = 0b10010010
        for i in 0..8 {
            if i % 3 == 0 {
                b.write_bit(true).unwrap();
                continue;
            }

            b.write_bit(false).unwrap();
        }

        // 136 = 010001000
        for i in 0..8 {
            if i % 4 == 0 {
                b.write_bit(true).unwrap();
                continue;
            }

            b.write_bit(false).unwrap();
        }

        assert_eq!(b.buf.len(), 3);

        assert_eq!(b.buf[0], 170);
        assert_eq!(b.buf[1], 146);
        assert_eq!(b.buf[2], 136);
    }

    #[test]
    fn write_byte() {
        let mut b = BufferedWriter::new();

        b.write_byte(234);
        b.write_byte(188);
        b.write_byte(77);

        assert_eq!(b.buf.len(), 3);

        assert_eq!(b.buf[0], 234);
        assert_eq!(b.buf[1], 188);
        assert_eq!(b.buf[2], 77);

        // write some bits so we can test `write_byte` when the last byte is partially filled
        b.write_bit(true).unwrap();
        b.write_bit(true).unwrap();
        b.write_bit(true).unwrap();
        b.write_bit(true).unwrap();
        b.write_byte(0b11110000); // 1111 1111 0000
        b.write_byte(0b00001111); // 1111 1111 0000 0000 1111
        b.write_byte(0b00001111); // 1111 1111 0000 0000 1111 0000 1111

        assert_eq!(b.buf.len(), 7);
        assert_eq!(b.buf[3], 255); // 0b11111111 = 255
        assert_eq!(b.buf[4], 0); // 0b00000000 = 0
        assert_eq!(b.buf[5], 240); // 0b11110000 = 240
    }

    #[test]
    fn write_bits() {
        let mut b = BufferedWriter::new();

        // 101011
        b.write_bits(6, 43).unwrap();

        // 010
        b.write_bits(3, 2).unwrap();

        // 1
        b.write_bits(1, 1).unwrap();

        // 1010 1100 1110 0011 1101
        b.write_bits(20, 708157).unwrap();

        // 11
        b.write_bits(2, 3).unwrap();

        assert_eq!(b.buf.len(), 4);

        assert_eq!(b.buf[0], 173); // 0b10101101 = 173
        assert_eq!(b.buf[1], 107); // 0b01101011 = 107
        assert_eq!(b.buf[2], 56); // 0b00111000 = 56
        assert_eq!(b.buf[3], 247); // 0b11110111 = 247
    }

    #[test]
    fn write_mixed() {
        let mut b = BufferedWriter::new();

        // 1010 1010
        for i in 0..8 {
            if i % 2 == 0 {
                b.write_bit(true).unwrap();
                continue;
            }

            b.write_bit(false).unwrap();
        }

        // 0000 1001
        b.write_byte(9);

        // 1001 1100 1100
        b.write_bits(12, 2508).unwrap();

        println!("{:?}", b.buf);

        // 1111
        for _ in 0..4 {
            let _ = b.write_bit(true);
        }

        assert_eq!(b.buf.len(), 4);

        println!("{:?}", b.buf);

        assert_eq!(b.buf[0], 170); // 0b10101010 = 170
        assert_eq!(b.buf[1], 9); // 0b00001001 = 9
        assert_eq!(b.buf[2], 156); // 0b10011100 = 156
        assert_eq!(b.buf[3], 207); // 0b11001111 = 207
    }

    #[test]
    fn write_u64() {
        let mut b = BufferedWriter::new();
        b.write_u64(0x1234567890ABCDEF);
        assert_eq!(b.buf, vec![0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF]);
    }

    #[test]
    fn write_f64() {
        let mut b = BufferedWriter::new();
        let expected = std::f64::consts::PI;
        b.write_f64(expected);

        let mut reader = BufferedReader::new(&b.buf);
        let actual: f64 = reader.read_f64().unwrap();
        assert_eq!(actual, expected);
    }
}
