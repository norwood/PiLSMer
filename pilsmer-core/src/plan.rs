use crate::codec::{checked_usize, put_u16_le, put_varint, Cursor};
use crate::error::{PiLsmError, Result};

pub type StreamFingerprint = [u8; 32];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum LogicalHashKind {
    Blake3_128,
    Sha256_128,
}

impl LogicalHashKind {
    pub(crate) fn encode(self) -> u8 {
        match self {
            Self::Blake3_128 => 1,
            Self::Sha256_128 => 2,
        }
    }

    pub(crate) fn decode(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Blake3_128),
            2 => Ok(Self::Sha256_128),
            other => Err(PiLsmError::UnknownLogicalHashKind(other)),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogicalHash {
    pub kind: LogicalHashKind,
    pub bytes: [u8; 16],
}

impl LogicalHash {
    pub fn new(kind: LogicalHashKind, bytes: &[u8]) -> Self {
        Self {
            kind,
            bytes: compute_logical_hash(kind, bytes),
        }
    }
}

pub fn compute_logical_hash(kind: LogicalHashKind, bytes: &[u8]) -> [u8; 16] {
    match kind {
        LogicalHashKind::Blake3_128 => {
            let hash = blake3::hash(bytes);
            let mut out = [0_u8; 16];
            out.copy_from_slice(&hash.as_bytes()[..16]);
            out
        }
        LogicalHashKind::Sha256_128 => {
            use sha2::{Digest, Sha256};

            let hash = Sha256::digest(bytes);
            let mut out = [0_u8; 16];
            out.copy_from_slice(&hash[..16]);
            out
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum StreamId {
    Sha256CounterV1 { seed: [u8; 32] },
    PiHexFractionPrefixV1 { digest: [u8; 32], bytes: u64 },
    EHexFractionPrefixV1 { digest: [u8; 32], bytes: u64 },
    Sqrt2HexFractionPrefixV1 { digest: [u8; 32], bytes: u64 },
}

impl StreamId {
    pub(crate) fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Self::Sha256CounterV1 { seed } => {
                out.push(1);
                out.extend_from_slice(seed);
            }
            Self::PiHexFractionPrefixV1 { digest, bytes } => {
                out.push(2);
                out.extend_from_slice(digest);
                put_varint(out, *bytes);
            }
            Self::EHexFractionPrefixV1 { digest, bytes } => {
                out.push(3);
                out.extend_from_slice(digest);
                put_varint(out, *bytes);
            }
            Self::Sqrt2HexFractionPrefixV1 { digest, bytes } => {
                out.push(4);
                out.extend_from_slice(digest);
                put_varint(out, *bytes);
            }
        }
    }

    pub(crate) fn decode(cursor: &mut Cursor<'_>) -> Result<Self> {
        let tag = cursor.read_u8()?;
        match tag {
            1 => {
                let mut seed = [0_u8; 32];
                seed.copy_from_slice(cursor.read_exact(32)?);
                Ok(Self::Sha256CounterV1 { seed })
            }
            2 => decode_prefix_stream(cursor, |digest, bytes| Self::PiHexFractionPrefixV1 {
                digest,
                bytes,
            }),
            3 => decode_prefix_stream(cursor, |digest, bytes| Self::EHexFractionPrefixV1 {
                digest,
                bytes,
            }),
            4 => decode_prefix_stream(cursor, |digest, bytes| Self::Sqrt2HexFractionPrefixV1 {
                digest,
                bytes,
            }),
            _ => Err(PiLsmError::InvalidPlan("unknown stream id")),
        }
    }
}

fn decode_prefix_stream(
    cursor: &mut Cursor<'_>,
    ctor: fn([u8; 32], u64) -> StreamId,
) -> Result<StreamId> {
    let mut digest = [0_u8; 32];
    digest.copy_from_slice(cursor.read_exact(32)?);
    let bytes = cursor.read_varint()?;
    Ok(ctor(digest, bytes))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlanCodec {
    CompactBinary,
    CeremonialCbor,
}

impl PlanCodec {
    pub(crate) fn encode(self) -> u8 {
        match self {
            Self::CompactBinary => 1,
            Self::CeremonialCbor => 2,
        }
    }

    pub(crate) fn decode(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::CompactBinary),
            2 => Ok(Self::CeremonialCbor),
            other => Err(PiLsmError::UnknownPlanCodec(other)),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChunkTransform {
    Identity,
    XorByte(u8),
    Reverse,
}

impl ChunkTransform {
    pub(crate) fn encode(self, out: &mut Vec<u8>) {
        match self {
            Self::Identity => out.push(0),
            Self::XorByte(byte) => {
                out.push(1);
                out.push(byte);
            }
            Self::Reverse => out.push(2),
        }
    }

    pub(crate) fn decode(cursor: &mut Cursor<'_>) -> Result<Self> {
        match cursor.read_u8()? {
            0 => Ok(Self::Identity),
            1 => Ok(Self::XorByte(cursor.read_u8()?)),
            2 => Ok(Self::Reverse),
            other => Err(PiLsmError::UnknownChunkTransform(other)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlanStream {
    pub id: StreamId,
    pub fingerprint: StreamFingerprint,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChunkRef {
    Located {
        stream_ix: u32,
        offset: u64,
        len: u32,
        transform: ChunkTransform,
    },
    Literal {
        bytes: bytes::Bytes,
    },
}

impl ChunkRef {
    pub fn logical_len(&self) -> u64 {
        match self {
            Self::Located { len, .. } => u64::from(*len),
            Self::Literal { bytes } => bytes.len() as u64,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReconstructionPlan {
    pub logical_len: u64,
    pub logical_hash: LogicalHash,
    pub planner_version: u16,
    pub plan_codec: PlanCodec,
    pub streams: Vec<PlanStream>,
    pub chunks: Vec<ChunkRef>,
}

impl ReconstructionPlan {
    pub(crate) fn encode_payload(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_varint(&mut out, self.logical_len);
        put_u16_le(&mut out, self.planner_version);
        out.push(self.plan_codec.encode());
        put_varint(&mut out, self.streams.len() as u64);
        for stream in &self.streams {
            stream.id.encode(&mut out);
            out.extend_from_slice(&stream.fingerprint);
        }
        put_varint(&mut out, self.chunks.len() as u64);
        for chunk in &self.chunks {
            match chunk {
                ChunkRef::Located {
                    stream_ix,
                    offset,
                    len,
                    transform,
                } => {
                    out.push(0);
                    put_varint(&mut out, u64::from(*stream_ix));
                    put_varint(&mut out, *offset);
                    put_varint(&mut out, u64::from(*len));
                    transform.encode(&mut out);
                }
                ChunkRef::Literal { bytes } => {
                    out.push(1);
                    put_varint(&mut out, bytes.len() as u64);
                    out.extend_from_slice(bytes);
                }
            }
        }
        if self.plan_codec == PlanCodec::CeremonialCbor {
            let footer = ceremonial_footer(
                self.logical_len,
                self.streams.len() as u64,
                self.chunks.len() as u64,
            );
            put_varint(&mut out, footer.len() as u64);
            out.extend_from_slice(&footer);
        }
        out
    }

    pub(crate) fn decode_payload(
        payload: &[u8],
        logical_hash: LogicalHash,
        limits: &crate::envelope::DecodeLimits,
    ) -> Result<Self> {
        let mut cursor = Cursor::new(payload);
        let logical_len = cursor.read_varint()?;
        if logical_len > limits.max_logical_len {
            return Err(PiLsmError::DecodeLimitExceeded("logical_len"));
        }

        let planner_version = cursor.read_u16_le()?;
        let plan_codec = PlanCodec::decode(cursor.read_u8()?)?;

        let stream_count = cursor.read_varint()?;
        if stream_count > u64::from(limits.max_streams_per_plan) {
            return Err(PiLsmError::DecodeLimitExceeded("stream_count"));
        }

        let mut streams = Vec::with_capacity(checked_usize(stream_count, "stream_count")?);
        for _ in 0..stream_count {
            let id = StreamId::decode(&mut cursor)?;
            let mut fingerprint = [0_u8; 32];
            fingerprint.copy_from_slice(cursor.read_exact(32)?);
            streams.push(PlanStream { id, fingerprint });
        }

        let chunk_count = cursor.read_varint()?;
        if chunk_count > limits.max_chunk_count {
            return Err(PiLsmError::DecodeLimitExceeded("chunk_count"));
        }

        let mut chunks = Vec::with_capacity(checked_usize(chunk_count, "chunk_count")?);
        let mut covered_len = 0_u64;
        for _ in 0..chunk_count {
            let chunk = match cursor.read_u8()? {
                0 => {
                    let stream_ix = cursor.read_varint()?;
                    if stream_ix >= stream_count {
                        return Err(PiLsmError::InvalidPlan("stream index out of range"));
                    }

                    let offset = cursor.read_varint()?;
                    if offset > limits.max_offset {
                        return Err(PiLsmError::DecodeLimitExceeded("offset"));
                    }

                    let len = cursor.read_varint()?;
                    if len == 0 {
                        return Err(PiLsmError::InvalidPlan("zero-length chunk"));
                    }
                    if len > u64::from(limits.max_chunk_len) {
                        return Err(PiLsmError::DecodeLimitExceeded("chunk_len"));
                    }
                    offset
                        .checked_add(len)
                        .ok_or(PiLsmError::ArithmeticOverflow)?;

                    ChunkRef::Located {
                        stream_ix: u32::try_from(stream_ix)
                            .map_err(|_| PiLsmError::DecodeLimitExceeded("stream_ix"))?,
                        offset,
                        len: u32::try_from(len)
                            .map_err(|_| PiLsmError::DecodeLimitExceeded("chunk_len"))?,
                        transform: ChunkTransform::decode(&mut cursor)?,
                    }
                }
                1 => {
                    let len = cursor.read_varint()?;
                    if len == 0 {
                        return Err(PiLsmError::InvalidPlan("zero-length literal"));
                    }
                    if len > u64::from(limits.max_chunk_len) {
                        return Err(PiLsmError::DecodeLimitExceeded("literal_len"));
                    }
                    let bytes = bytes::Bytes::copy_from_slice(
                        cursor.read_exact(checked_usize(len, "literal_len")?)?,
                    );
                    ChunkRef::Literal { bytes }
                }
                other => return Err(PiLsmError::UnknownChunkKind(other)),
            };

            covered_len = covered_len
                .checked_add(chunk.logical_len())
                .ok_or(PiLsmError::ArithmeticOverflow)?;
            chunks.push(chunk);
        }

        if plan_codec == PlanCodec::CeremonialCbor {
            let footer_len = cursor.read_varint()?;
            let footer = cursor.read_exact(checked_usize(footer_len, "ceremonial_footer_len")?)?;
            let expected_footer =
                ceremonial_footer(logical_len, streams.len() as u64, chunks.len() as u64);
            if footer != expected_footer {
                return Err(PiLsmError::InvalidPlan("invalid ceremonial footer"));
            }
        }

        if !cursor.is_empty() {
            return Err(PiLsmError::InvalidPlan("trailing bytes"));
        }
        if covered_len != logical_len {
            return Err(PiLsmError::InvalidPlan(
                "chunks do not cover logical length",
            ));
        }

        Ok(Self {
            logical_len,
            logical_hash,
            planner_version,
            plan_codec,
            streams,
            chunks,
        })
    }
}

fn ceremonial_footer(logical_len: u64, stream_count: u64, chunk_count: u64) -> Vec<u8> {
    let mut out = Vec::new();
    put_cbor_map(&mut out, 6);
    put_cbor_text(&mut out, "codec");
    put_cbor_text(&mut out, "ceremonial-cbor");
    put_cbor_text(&mut out, "purpose");
    put_cbor_text(&mut out, "metadata regret");
    put_cbor_text(&mut out, "claim");
    put_cbor_text(&mut out, "your data is merely located");
    put_cbor_text(&mut out, "logical_len");
    put_cbor_u64(&mut out, logical_len);
    put_cbor_text(&mut out, "streams");
    put_cbor_u64(&mut out, stream_count);
    put_cbor_text(&mut out, "chunks");
    put_cbor_u64(&mut out, chunk_count);
    out
}

fn put_cbor_map(out: &mut Vec<u8>, len: u64) {
    put_cbor_major(out, 5, len);
}

fn put_cbor_text(out: &mut Vec<u8>, text: &str) {
    put_cbor_major(out, 3, text.len() as u64);
    out.extend_from_slice(text.as_bytes());
}

fn put_cbor_u64(out: &mut Vec<u8>, value: u64) {
    put_cbor_major(out, 0, value);
}

fn put_cbor_major(out: &mut Vec<u8>, major: u8, value: u64) {
    let head = major << 5;
    match value {
        0..=23 => out.push(head | value as u8),
        24..=0xff => {
            out.push(head | 24);
            out.push(value as u8);
        }
        0x100..=0xffff => {
            out.push(head | 25);
            out.extend_from_slice(&(value as u16).to_be_bytes());
        }
        0x1_0000..=0xffff_ffff => {
            out.push(head | 26);
            out.extend_from_slice(&(value as u32).to_be_bytes());
        }
        _ => {
            out.push(head | 27);
            out.extend_from_slice(&value.to_be_bytes());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::DecodeLimits;

    #[test]
    fn ceremonial_codec_roundtrips_and_inflates_payload() {
        let compact = test_plan(PlanCodec::CompactBinary);
        let ceremonial = test_plan(PlanCodec::CeremonialCbor);

        let compact_payload = compact.encode_payload();
        let ceremonial_payload = ceremonial.encode_payload();
        assert!(ceremonial_payload.len() > compact_payload.len());

        assert_eq!(
            ReconstructionPlan::decode_payload(
                &ceremonial_payload,
                ceremonial.logical_hash,
                &DecodeLimits::default()
            )
            .unwrap(),
            ceremonial
        );
    }

    #[test]
    fn ceremonial_codec_rejects_bad_footer() {
        let plan = test_plan(PlanCodec::CeremonialCbor);
        let mut payload = plan.encode_payload();
        let last = payload.last_mut().unwrap();
        *last ^= 1;

        let err = ReconstructionPlan::decode_payload(
            &payload,
            plan.logical_hash,
            &DecodeLimits::default(),
        )
        .unwrap_err();
        assert_eq!(err, PiLsmError::InvalidPlan("invalid ceremonial footer"));
    }

    fn test_plan(plan_codec: PlanCodec) -> ReconstructionPlan {
        ReconstructionPlan {
            logical_len: 3,
            logical_hash: LogicalHash::new(LogicalHashKind::Blake3_128, b"abc"),
            planner_version: 1,
            plan_codec,
            streams: vec![PlanStream {
                id: StreamId::Sha256CounterV1 { seed: [0_u8; 32] },
                fingerprint: [1_u8; 32],
            }],
            chunks: vec![ChunkRef::Located {
                stream_ix: 0,
                offset: 42,
                len: 3,
                transform: ChunkTransform::Identity,
            }],
        }
    }
}
