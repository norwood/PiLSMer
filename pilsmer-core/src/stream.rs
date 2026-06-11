use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use sha2::{Digest, Sha256};

use crate::error::{PiLsmError, Result};
use crate::plan::{StreamFingerprint, StreamId};

pub const PI_HEX_FRACTION_PREFIX_BYTES: usize = 256;

const PI_HEX_FRACTION_PREFIX_HEX: &str = concat!(
    "243F6A8885A308D313198A2E03707344A4093822299F31D0082EFA98EC4E6C89",
    "452821E638D01377BE5466CF34E90C6CC0AC29B7C97C50DD3F84D5B5B5470917",
    "9216D5D98979FB1BD1310BA698DFB5AC2FFD72DBD01ADFB7B8E1AFED6A267E96",
    "BA7C9045F12C7F9924A19947B3916CF70801F2E2858EFC16636920D871574E69",
    "A458FEA3F4933D7E0D95748F728EB658718BCD5882154AEE7B54A41DC25A59B5",
    "9C30D5392AF26013C5D1B023286085F0CA417918B8DB38EF8E79DCB0603A180E",
    "6C9E0E8BB01E8A3ED71577C1BD314B2778AF2FDA55605C60E65525F3AA55AB94",
    "5748986263E8144055CA396A2AAB10B6B4CC5C341141E8CEA15486AF7C72E993",
);

#[async_trait]
pub trait ByteStream: Send + Sync {
    fn id(&self) -> StreamId;
    fn fingerprint(&self) -> StreamFingerprint;
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes>;
}

pub fn pi_hex_fraction_prefix_stream(prefix_bytes: u64) -> Result<PrefixByteStream> {
    let prefix_bytes =
        usize::try_from(prefix_bytes).map_err(|_| PiLsmError::IndexLimitExceeded("pi prefix"))?;
    if prefix_bytes > PI_HEX_FRACTION_PREFIX_BYTES {
        return Err(PiLsmError::IndexLimitExceeded("pi prefix"));
    }

    let bytes = decode_pi_prefix(prefix_bytes)?;
    let digest = Sha256::digest(&bytes);
    let mut digest_bytes = [0_u8; 32];
    digest_bytes.copy_from_slice(&digest);
    Ok(PrefixByteStream::new(
        StreamId::PiHexFractionPrefixV1 {
            digest: digest_bytes,
            bytes: prefix_bytes as u64,
        },
        Bytes::from(bytes),
    ))
}

#[derive(Clone, Debug)]
pub struct Sha256CounterStream {
    seed: [u8; 32],
    fingerprint: StreamFingerprint,
}

impl Sha256CounterStream {
    pub fn new(seed: [u8; 32]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(b"PiLSMer stream fingerprint sha256-counter:v1");
        hasher.update(seed);
        let mut fingerprint = [0_u8; 32];
        fingerprint.copy_from_slice(&hasher.finalize());

        Self { seed, fingerprint }
    }

    pub fn seed(&self) -> [u8; 32] {
        self.seed
    }
}

#[async_trait]
impl ByteStream for Sha256CounterStream {
    fn id(&self) -> StreamId {
        StreamId::Sha256CounterV1 { seed: self.seed }
    }

    fn fingerprint(&self) -> StreamFingerprint {
        self.fingerprint
    }

    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        if len == 0 {
            return Ok(Bytes::new());
        }

        let start_block = offset / 32;
        let block_offset = (offset % 32) as usize;
        let end_offset = offset
            .checked_add(len as u64)
            .ok_or(PiLsmError::ArithmeticOverflow)?;
        let end_block = end_offset.saturating_add(31) / 32;

        let mut bytes = Vec::with_capacity(((end_block - start_block) as usize) * 32);
        for block_ix in start_block..end_block {
            let mut hasher = Sha256::new();
            hasher.update(b"PiLSMer sha256-counter:v1");
            hasher.update(self.seed);
            hasher.update(block_ix.to_le_bytes());
            bytes.extend_from_slice(&hasher.finalize());
        }

        Ok(Bytes::copy_from_slice(
            &bytes[block_offset..block_offset + len],
        ))
    }
}

#[derive(Clone, Debug)]
pub struct PrefixByteStream {
    id: StreamId,
    fingerprint: StreamFingerprint,
    bytes: Bytes,
}

impl PrefixByteStream {
    pub fn new(id: StreamId, bytes: Bytes) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(b"PiLSMer prefix stream fingerprint:v1");
        hasher.update(&bytes);
        let mut fingerprint = [0_u8; 32];
        fingerprint.copy_from_slice(&hasher.finalize());

        Self {
            id,
            fingerprint,
            bytes,
        }
    }
}

#[async_trait]
impl ByteStream for PrefixByteStream {
    fn id(&self) -> StreamId {
        self.id.clone()
    }

    fn fingerprint(&self) -> StreamFingerprint {
        self.fingerprint
    }

    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        let offset = usize::try_from(offset).map_err(|_| PiLsmError::StreamReadOutOfBounds)?;
        let end = offset
            .checked_add(len)
            .ok_or(PiLsmError::ArithmeticOverflow)?;
        if end > self.bytes.len() {
            return Err(PiLsmError::StreamReadOutOfBounds);
        }
        Ok(self.bytes.slice(offset..end))
    }
}

#[derive(Clone, Default)]
pub struct StreamRegistry {
    streams: HashMap<StreamId, Arc<dyn ByteStream>>,
}

impl StreamRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_stream(mut self, stream: Arc<dyn ByteStream>) -> Self {
        self.register(stream);
        self
    }

    pub fn register(&mut self, stream: Arc<dyn ByteStream>) {
        self.streams.insert(stream.id(), stream);
    }

    pub fn get(&self, id: &StreamId) -> Option<Arc<dyn ByteStream>> {
        self.streams.get(id).cloned()
    }
}

fn decode_pi_prefix(prefix_bytes: usize) -> Result<Vec<u8>> {
    let hex = PI_HEX_FRACTION_PREFIX_HEX.as_bytes();
    let mut out = Vec::with_capacity(prefix_bytes);
    for ix in 0..prefix_bytes {
        let high = hex_nibble(hex[ix * 2])?;
        let low = hex_nibble(hex[ix * 2 + 1])?;
        out.push((high << 4) | low);
    }
    Ok(out)
}

fn hex_nibble(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(PiLsmError::InvalidPlan("invalid pi hex prefix")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sha256_counter_has_stable_vectors() {
        let stream = Sha256CounterStream::new([0_u8; 32]);
        let bytes = stream.read_at(0, 40).await.unwrap();
        assert_eq!(
            hex::encode(bytes),
            "16777a3a493883fed970fb23fb80f7a3e979b07275cd6ff1f71ceb2dc1fc52561fd017e932d2b95e"
        );
    }

    #[tokio::test]
    async fn pi_hex_fraction_prefix_has_stable_definition() {
        let stream = pi_hex_fraction_prefix_stream(16).unwrap();
        let bytes = stream.read_at(0, 16).await.unwrap();
        assert_eq!(hex::encode(&bytes), "243f6a8885a308d313198a2e03707344");

        let StreamId::PiHexFractionPrefixV1 { digest, bytes } = stream.id() else {
            panic!("expected pi prefix stream id");
        };
        assert_eq!(bytes, 16);

        let expected_digest = Sha256::digest(stream.read_at(0, 16).await.unwrap());
        assert_eq!(&digest[..], &expected_digest[..]);
    }

    #[test]
    fn pi_prefix_rejects_unavailable_bytes() {
        assert!(pi_hex_fraction_prefix_stream((PI_HEX_FRACTION_PREFIX_BYTES + 1) as u64).is_err());
    }
}
