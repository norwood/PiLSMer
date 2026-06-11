use std::sync::Arc;

use smallvec::SmallVec;

use crate::codec::varint_len;
use crate::error::{PiLsmError, Result};
use crate::plan::{StreamFingerprint, StreamId};
use crate::stream::ByteStream;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexedChunk {
    pub offset: u64,
    pub len: u16,
    pub encoded_cost: u16,
    pub read_cost_hint: u16,
}

#[derive(Clone, Debug)]
pub struct StreamIndexOptions {
    pub max_prefix_len: u64,
    pub max_k: usize,
    pub max_index_memory_bytes: u64,
    pub max_offsets_per_kgram: u16,
}

impl Default for StreamIndexOptions {
    fn default() -> Self {
        Self {
            max_prefix_len: 16 * 1024 * 1024,
            max_k: 3,
            max_index_memory_bytes: 256 * 1024 * 1024,
            max_offsets_per_kgram: 4,
        }
    }
}

#[derive(Clone, Debug)]
pub struct StreamIndex {
    stream_id: StreamId,
    stream_fingerprint: StreamFingerprint,
    prefix_len: u64,
    max_k: usize,
    memory_budget_bytes: u64,
    by_len: Vec<PackedKGramTable>,
}

#[derive(Clone, Debug)]
struct PackedKGramTable {
    k: u8,
    entries: Vec<PackedKGramEntry>,
    offsets: Vec<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PackedKGramEntry {
    packed_key: u64,
    offsets_start: u32,
    offsets_len: u16,
}

impl StreamIndex {
    pub async fn build(stream: Arc<dyn ByteStream>, options: StreamIndexOptions) -> Result<Self> {
        let prefix_len = usize::try_from(options.max_prefix_len)
            .map_err(|_| PiLsmError::IndexLimitExceeded("prefix_len"))?;
        let prefix = stream.read_at(0, prefix_len).await?;
        Self::from_prefix(stream.id(), stream.fingerprint(), &prefix, options)
    }

    pub fn from_prefix(
        stream_id: StreamId,
        stream_fingerprint: StreamFingerprint,
        prefix: &[u8],
        options: StreamIndexOptions,
    ) -> Result<Self> {
        if options.max_k == 0 || options.max_k > 8 {
            return Err(PiLsmError::IndexLimitExceeded("max_k"));
        }
        if options.max_offsets_per_kgram == 0 {
            return Err(PiLsmError::IndexLimitExceeded("max_offsets_per_kgram"));
        }

        let prefix_len = u64::try_from(prefix.len())
            .map_err(|_| PiLsmError::IndexLimitExceeded("prefix_len"))?;
        let mut by_len = Vec::with_capacity(options.max_k);
        let mut estimated_bytes = 0_u64;

        for k in 1..=options.max_k {
            if prefix.len() < k {
                break;
            }
            let table = PackedKGramTable::build(prefix, k, options.max_offsets_per_kgram)?;
            let next_estimated_bytes = estimated_bytes
                .checked_add(table.estimated_bytes())
                .ok_or(PiLsmError::ArithmeticOverflow)?;
            if next_estimated_bytes > options.max_index_memory_bytes {
                if by_len.is_empty() {
                    return Err(PiLsmError::IndexLimitExceeded("index memory budget"));
                }
                break;
            }
            estimated_bytes = next_estimated_bytes;
            by_len.push(table);
        }

        let max_k = by_len.len();
        Ok(Self {
            stream_id,
            stream_fingerprint,
            prefix_len,
            max_k,
            memory_budget_bytes: options.max_index_memory_bytes,
            by_len,
        })
    }

    pub fn stream_id(&self) -> &StreamId {
        &self.stream_id
    }

    pub fn stream_fingerprint(&self) -> StreamFingerprint {
        self.stream_fingerprint
    }

    pub fn prefix_len(&self) -> u64 {
        self.prefix_len
    }

    pub fn max_k(&self) -> usize {
        self.max_k
    }

    pub fn memory_budget_bytes(&self) -> u64 {
        self.memory_budget_bytes
    }

    pub fn find_candidates(&self, needle: &[u8]) -> SmallVec<[IndexedChunk; 4]> {
        if needle.is_empty() || needle.len() > self.max_k || needle.len() > self.by_len.len() {
            return SmallVec::new();
        }

        self.by_len[needle.len() - 1].find_candidates(needle)
    }
}

impl PackedKGramTable {
    fn build(prefix: &[u8], k: usize, max_offsets_per_kgram: u16) -> Result<Self> {
        let mut pairs = Vec::with_capacity(prefix.len() - k + 1);
        for offset in 0..=(prefix.len() - k) {
            pairs.push((
                pack_kgram(&prefix[offset..offset + k])?,
                u64::try_from(offset).map_err(|_| PiLsmError::IndexLimitExceeded("offset"))?,
            ));
        }
        pairs.sort_unstable();

        let mut entries = Vec::new();
        let mut offsets = Vec::new();
        let mut i = 0;
        while i < pairs.len() {
            let packed_key = pairs[i].0;
            let offsets_start = offsets.len();
            let mut retained = 0_u16;
            while i < pairs.len() && pairs[i].0 == packed_key {
                if retained < max_offsets_per_kgram {
                    offsets.push(pairs[i].1);
                    retained += 1;
                }
                i += 1;
            }

            entries.push(PackedKGramEntry {
                packed_key,
                offsets_start: u32::try_from(offsets_start)
                    .map_err(|_| PiLsmError::IndexLimitExceeded("offset table"))?,
                offsets_len: retained,
            });
        }

        Ok(Self {
            k: u8::try_from(k).map_err(|_| PiLsmError::IndexLimitExceeded("k"))?,
            entries,
            offsets,
        })
    }

    fn estimated_bytes(&self) -> u64 {
        ((self.entries.len() * std::mem::size_of::<PackedKGramEntry>())
            + (self.offsets.len() * std::mem::size_of::<u64>())) as u64
    }

    fn find_candidates(&self, needle: &[u8]) -> SmallVec<[IndexedChunk; 4]> {
        let Ok(key) = pack_kgram(needle) else {
            return SmallVec::new();
        };

        let Ok(ix) = self
            .entries
            .binary_search_by_key(&key, |entry| entry.packed_key)
        else {
            return SmallVec::new();
        };

        let entry = self.entries[ix];
        let mut out = SmallVec::new();
        let start = entry.offsets_start as usize;
        let end = start + entry.offsets_len as usize;
        for offset in &self.offsets[start..end] {
            out.push(IndexedChunk {
                offset: *offset,
                len: u16::from(self.k),
                encoded_cost: located_chunk_encoded_cost(*offset, u64::from(self.k)),
                read_cost_hint: u16::from(self.k),
            });
        }
        out
    }
}

fn pack_kgram(bytes: &[u8]) -> Result<u64> {
    if bytes.is_empty() || bytes.len() > 8 {
        return Err(PiLsmError::IndexLimitExceeded("k"));
    }

    let mut key = 0_u64;
    for byte in bytes {
        key = (key << 8) | u64::from(*byte);
    }
    Ok(key)
}

fn located_chunk_encoded_cost(offset: u64, len: u64) -> u16 {
    // chunk kind + stream_ix + offset + len + identity transform
    1 + 1 + varint_len(offset) + varint_len(len) + 1
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::stream::PrefixByteStream;

    fn stream_for(bytes: &'static [u8]) -> Arc<dyn ByteStream> {
        Arc::new(PrefixByteStream::new(
            StreamId::PiHexFractionPrefixV1 {
                digest: [7_u8; 32],
                bytes: bytes.len() as u64,
            },
            Bytes::from_static(bytes),
        ))
    }

    #[tokio::test]
    async fn packed_index_returns_multiple_candidates() {
        let stream = stream_for(b"abcxxabc");
        let index = StreamIndex::build(
            stream,
            StreamIndexOptions {
                max_prefix_len: 8,
                max_k: 3,
                max_index_memory_bytes: 1024,
                max_offsets_per_kgram: 4,
            },
        )
        .await
        .unwrap();

        let candidates = index.find_candidates(b"abc");
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0].offset, 0);
        assert_eq!(candidates[1].offset, 5);
    }

    #[tokio::test]
    async fn index_caps_max_k_to_memory_budget() {
        let stream = stream_for(b"abcd");
        let index = StreamIndex::build(
            stream,
            StreamIndexOptions {
                max_prefix_len: 4,
                max_k: 3,
                max_index_memory_bytes: 100,
                max_offsets_per_kgram: 4,
            },
        )
        .await
        .unwrap();

        assert_eq!(index.max_k(), 1);
        assert!(!index.find_candidates(b"a").is_empty());
        assert!(index.find_candidates(b"ab").is_empty());
    }

    #[tokio::test]
    async fn index_errors_when_budget_cannot_fit_first_table() {
        let err = StreamIndex::build(
            stream_for(b"abcd"),
            StreamIndexOptions {
                max_prefix_len: 4,
                max_k: 3,
                max_index_memory_bytes: 1,
                max_offsets_per_kgram: 4,
            },
        )
        .await
        .unwrap_err();

        assert_eq!(err, PiLsmError::IndexLimitExceeded("index memory budget"));
    }
}
