use bytes::Bytes;

use crate::codec::{checked_usize, put_u16_le, put_u32_le, put_varint, Cursor};
use crate::error::{PiLsmError, Result};
use crate::plan::{compute_logical_hash, LogicalHash, LogicalHashKind, ReconstructionPlan};

const MAGIC: &[u8; 4] = b"PLSM";
const VERSION: u8 = 1;
const KIND_RAW: u8 = 0;
const KIND_PLAN: u8 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ValueEnvelope {
    Raw(Bytes),
    Plan(ReconstructionPlan),
}

#[derive(Clone, Debug)]
pub struct DecodeLimits {
    pub max_logical_len: u64,
    pub max_chunk_count: u64,
    pub max_encoded_plan_len: u64,
    pub max_offset: u64,
    pub max_chunk_len: u32,
    pub max_streams_per_plan: u32,
}

impl Default for DecodeLimits {
    fn default() -> Self {
        Self {
            max_logical_len: 64 * 1024 * 1024,
            max_chunk_count: 16 * 1024 * 1024,
            max_encoded_plan_len: 256 * 1024 * 1024,
            max_offset: u64::MAX / 2,
            max_chunk_len: 1024 * 1024,
            max_streams_per_plan: 64,
        }
    }
}

impl ValueEnvelope {
    pub fn encode(&self) -> Vec<u8> {
        self.encode_with_hash_kind(LogicalHashKind::Blake3_128)
    }

    pub fn encode_with_hash_kind(&self, raw_hash_kind: LogicalHashKind) -> Vec<u8> {
        let (kind, hash, payload) = match self {
            ValueEnvelope::Raw(bytes) => {
                let mut payload = Vec::new();
                put_varint(&mut payload, bytes.len() as u64);
                payload.extend_from_slice(bytes);
                (KIND_RAW, LogicalHash::new(raw_hash_kind, bytes), payload)
            }
            ValueEnvelope::Plan(plan) => (KIND_PLAN, plan.logical_hash, plan.encode_payload()),
        };

        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        out.push(VERSION);
        out.push(kind);
        put_u16_le(&mut out, 0);
        out.push(hash.kind.encode());
        put_varint(&mut out, payload.len() as u64);
        out.extend_from_slice(&payload);
        out.extend_from_slice(&hash.bytes);

        let crc = crc32fast::hash(&out);
        put_u32_le(&mut out, crc);
        out
    }

    pub fn decode(input: &[u8], limits: &DecodeLimits) -> Result<Self> {
        let mut cursor = Cursor::new(input);
        if cursor.read_exact(4)? != MAGIC {
            return Err(PiLsmError::InvalidMagic);
        }

        let version = cursor.read_u8()?;
        if version != VERSION {
            return Err(PiLsmError::UnsupportedVersion(version));
        }

        let kind = cursor.read_u8()?;
        let _flags = cursor.read_u16_le()?;
        let hash_kind = LogicalHashKind::decode(cursor.read_u8()?)?;
        let payload_len = cursor.read_varint()?;
        let payload_len_usize = checked_usize(payload_len, "payload_len")?;
        if kind == KIND_PLAN && payload_len > limits.max_encoded_plan_len {
            return Err(PiLsmError::DecodeLimitExceeded("plan_payload_len"));
        }

        let payload = cursor.read_exact(payload_len_usize)?;
        let mut hash_bytes = [0_u8; 16];
        hash_bytes.copy_from_slice(cursor.read_exact(16)?);
        let expected_crc = cursor.read_u32_le()?;
        if !cursor.is_empty() {
            return Err(PiLsmError::InvalidPlan("trailing frame bytes"));
        }

        let crc_start = input
            .len()
            .checked_sub(4)
            .ok_or(PiLsmError::UnexpectedEof)?;
        let actual_crc = crc32fast::hash(&input[..crc_start]);
        if actual_crc != expected_crc {
            return Err(PiLsmError::FrameCrcMismatch);
        }

        let logical_hash = LogicalHash {
            kind: hash_kind,
            bytes: hash_bytes,
        };

        match kind {
            KIND_RAW => decode_raw_payload(payload, logical_hash, limits),
            KIND_PLAN => Ok(ValueEnvelope::Plan(ReconstructionPlan::decode_payload(
                payload,
                logical_hash,
                limits,
            )?)),
            other => Err(PiLsmError::UnknownEnvelopeKind(other)),
        }
    }
}

fn decode_raw_payload(
    payload: &[u8],
    logical_hash: LogicalHash,
    limits: &DecodeLimits,
) -> Result<ValueEnvelope> {
    let mut cursor = Cursor::new(payload);
    let raw_len = cursor.read_varint()?;
    if raw_len > limits.max_logical_len {
        return Err(PiLsmError::DecodeLimitExceeded("raw_len"));
    }

    let raw = cursor.read_exact(checked_usize(raw_len, "raw_len")?)?;
    if !cursor.is_empty() {
        return Err(PiLsmError::InvalidPlan("trailing raw payload bytes"));
    }

    if compute_logical_hash(logical_hash.kind, raw) != logical_hash.bytes {
        return Err(PiLsmError::LogicalHashMismatch);
    }

    Ok(ValueEnvelope::Raw(Bytes::copy_from_slice(raw)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_envelope_roundtrips() {
        let envelope = ValueEnvelope::Raw(Bytes::from_static(b"PLSM but user data"));
        let encoded = envelope.encode();
        let decoded = ValueEnvelope::decode(&encoded, &DecodeLimits::default()).unwrap();
        assert_eq!(decoded, envelope);
    }

    #[test]
    fn crc_mismatch_is_rejected() {
        let envelope = ValueEnvelope::Raw(Bytes::from_static(b"hello"));
        let mut encoded = envelope.encode();
        encoded[12] ^= 0xff;
        let err = ValueEnvelope::decode(&encoded, &DecodeLimits::default()).unwrap_err();
        assert_eq!(err, PiLsmError::FrameCrcMismatch);
    }
}
