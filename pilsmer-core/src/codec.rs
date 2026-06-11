use crate::error::{PiLsmError, Result};

pub(crate) struct Cursor<'a> {
    input: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    pub(crate) fn new(input: &'a [u8]) -> Self {
        Self { input, pos: 0 }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.input.len().saturating_sub(self.pos)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    pub(crate) fn read_u8(&mut self) -> Result<u8> {
        let bytes = self.read_exact(1)?;
        Ok(bytes[0])
    }

    pub(crate) fn read_u16_le(&mut self) -> Result<u16> {
        let bytes = self.read_exact(2)?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    pub(crate) fn read_u32_le(&mut self) -> Result<u32> {
        let bytes = self.read_exact(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub(crate) fn read_varint(&mut self) -> Result<u64> {
        let mut value = 0_u64;
        let mut shift = 0_u32;

        for _ in 0..10 {
            let byte = self.read_u8()?;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
            shift += 7;
        }

        Err(PiLsmError::VarintOverflow)
    }

    pub(crate) fn read_exact(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or(PiLsmError::ArithmeticOverflow)?;
        if end > self.input.len() {
            return Err(PiLsmError::UnexpectedEof);
        }

        let out = &self.input[self.pos..end];
        self.pos = end;
        Ok(out)
    }
}

pub(crate) fn put_u16_le(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn put_u32_le(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

pub(crate) fn checked_usize(value: u64, name: &'static str) -> Result<usize> {
    usize::try_from(value).map_err(|_| PiLsmError::DecodeLimitExceeded(name))
}

pub(crate) fn varint_len(mut value: u64) -> u16 {
    let mut len = 1_u16;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}
