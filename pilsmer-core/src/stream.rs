use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use sha2::{Digest, Sha256};

use crate::error::{PiLsmError, Result};
use crate::plan::{StreamFingerprint, StreamId};

pub const PI_HEX_FRACTION_PREFIX_BYTES: usize = 256;
pub const E_HEX_FRACTION_PREFIX_BYTES: usize = 256;
pub const SQRT2_HEX_FRACTION_PREFIX_BYTES: usize = 256;

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

const E_HEX_FRACTION_PREFIX_HEX: &str = concat!(
    "B7E151628AED2A6ABF7158809CF4F3C762E7160F38B4DA56A784D9045190CFEF",
    "324E7738926CFBE5F4BF8D8D8C31D763DA06C80ABB1185EB4F7C7B5757F59584",
    "90CFD47D7C19BB42158D9554F7B46BCED55C4D79FD5F24D6613C31C3839A2DDF",
    "8A9A276BCFBFA1C877C56284DAB79CD4C2B3293D20E9E5EAF02AC60ACC93ED87",
    "4422A52ECB238FEEE5AB6ADD835FD1A0753D0A8F78E537D2B95BB79D8DCAEC64",
    "2C1E9F23B829B5C2780BF38737DF8BB300D01334A0D0BD8645CBFA73A6160FFE",
    "393C48CBBBCA060F0FF8EC6D31BEB5CCEED7F2F0BB088017163BC60DF45A0ECB",
    "1BCD289B06CBBFEA21AD08E1847F3F7378D56CED94640D6EF0D3D37BE67008E1",
);

const SQRT2_HEX_FRACTION_PREFIX_HEX: &str = concat!(
    "6A09E667F3BCC908B2FB1366EA957D3E3ADEC17512775099DA2F590B0667322A",
    "95F90608757145875163FCDFB907B6721EE950BC8738F694F0090E6C7BF44ED1",
    "A4405D0E855E3E9CA60B38C0237866F7956379222D108B148C1578E45EF89C67",
    "8DAB5147176FD3B99654C68663E7909BEA5E241F06DCB05DD549411320819495",
    "0272956DB1FA1DFBE9A74059D7927C1884C9B579AA516CA3719E6836DF046D8E",
    "0209B803FC646A5E6654BD3EF7B43D7FED437C7F9444260FBD40C483EF550385",
    "83F97BBD45EFB8663107145D5FEBE765A49E94EC7F597105FBFC2E1FA763EF01",
    "F3599C82F2FE500B848CF0BD252AE046BF9F1EF7947D46769AF8C14BCC67C7C2",
);

#[async_trait]
pub trait ByteStream: Send + Sync {
    fn id(&self) -> StreamId;
    fn fingerprint(&self) -> StreamFingerprint;
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes>;
}

pub fn pi_hex_fraction_prefix_stream(prefix_bytes: u64) -> Result<PrefixByteStream> {
    hex_fraction_prefix_stream(
        prefix_bytes,
        PI_HEX_FRACTION_PREFIX_BYTES,
        PI_HEX_FRACTION_PREFIX_HEX,
        "pi prefix",
        |digest, bytes| StreamId::PiHexFractionPrefixV1 { digest, bytes },
    )
}

pub fn e_hex_fraction_prefix_stream(prefix_bytes: u64) -> Result<PrefixByteStream> {
    hex_fraction_prefix_stream(
        prefix_bytes,
        E_HEX_FRACTION_PREFIX_BYTES,
        E_HEX_FRACTION_PREFIX_HEX,
        "e prefix",
        |digest, bytes| StreamId::EHexFractionPrefixV1 { digest, bytes },
    )
}

pub fn sqrt2_hex_fraction_prefix_stream(prefix_bytes: u64) -> Result<PrefixByteStream> {
    hex_fraction_prefix_stream(
        prefix_bytes,
        SQRT2_HEX_FRACTION_PREFIX_BYTES,
        SQRT2_HEX_FRACTION_PREFIX_HEX,
        "sqrt2 prefix",
        |digest, bytes| StreamId::Sqrt2HexFractionPrefixV1 { digest, bytes },
    )
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

fn hex_fraction_prefix_stream(
    prefix_bytes: u64,
    available_bytes: usize,
    hex: &str,
    limit_name: &'static str,
    stream_id: impl FnOnce([u8; 32], u64) -> StreamId,
) -> Result<PrefixByteStream> {
    let prefix_bytes =
        usize::try_from(prefix_bytes).map_err(|_| PiLsmError::IndexLimitExceeded(limit_name))?;
    if prefix_bytes > available_bytes {
        return Err(PiLsmError::IndexLimitExceeded(limit_name));
    }

    let bytes = decode_hex_prefix(hex, prefix_bytes)?;
    let digest = Sha256::digest(&bytes);
    let mut digest_bytes = [0_u8; 32];
    digest_bytes.copy_from_slice(&digest);
    Ok(PrefixByteStream::new(
        stream_id(digest_bytes, prefix_bytes as u64),
        Bytes::from(bytes),
    ))
}

fn decode_hex_prefix(hex: &str, prefix_bytes: usize) -> Result<Vec<u8>> {
    let hex = hex.as_bytes();
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

    #[tokio::test]
    async fn e_hex_fraction_prefix_has_stable_definition() {
        let stream = e_hex_fraction_prefix_stream(16).unwrap();
        let bytes = stream.read_at(0, 16).await.unwrap();
        assert_eq!(hex::encode(&bytes), "b7e151628aed2a6abf7158809cf4f3c7");

        let StreamId::EHexFractionPrefixV1 { digest, bytes } = stream.id() else {
            panic!("expected e prefix stream id");
        };
        assert_eq!(bytes, 16);

        let expected_digest = Sha256::digest(stream.read_at(0, 16).await.unwrap());
        assert_eq!(&digest[..], &expected_digest[..]);
    }

    #[tokio::test]
    async fn sqrt2_hex_fraction_prefix_has_stable_definition() {
        let stream = sqrt2_hex_fraction_prefix_stream(16).unwrap();
        let bytes = stream.read_at(0, 16).await.unwrap();
        assert_eq!(hex::encode(&bytes), "6a09e667f3bcc908b2fb1366ea957d3e");

        let StreamId::Sqrt2HexFractionPrefixV1 { digest, bytes } = stream.id() else {
            panic!("expected sqrt2 prefix stream id");
        };
        assert_eq!(bytes, 16);

        let expected_digest = Sha256::digest(stream.read_at(0, 16).await.unwrap());
        assert_eq!(&digest[..], &expected_digest[..]);
    }

    #[test]
    fn prefix_streams_reject_unavailable_bytes() {
        assert!(pi_hex_fraction_prefix_stream((PI_HEX_FRACTION_PREFIX_BYTES + 1) as u64).is_err());
        assert!(e_hex_fraction_prefix_stream((E_HEX_FRACTION_PREFIX_BYTES + 1) as u64).is_err());
        assert!(
            sqrt2_hex_fraction_prefix_stream((SQRT2_HEX_FRACTION_PREFIX_BYTES + 1) as u64).is_err()
        );
    }
}
