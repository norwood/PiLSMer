use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use sha2::{Digest, Sha256};

use crate::error::{PiLsmError, Result};
use crate::plan::{StreamFingerprint, StreamId};

#[async_trait]
pub trait ByteStream: Send + Sync {
    fn id(&self) -> StreamId;
    fn fingerprint(&self) -> StreamFingerprint;
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes>;
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
}
