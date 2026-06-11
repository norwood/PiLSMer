use std::collections::HashMap;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use pilsmer_core::{
    explain_envelope, DecodeLimits, ExplainValue, PiLsmError, PlanOptions, Planner, Reconstructor,
    Result as CoreResult, StreamRegistry, ValueEnvelope,
};
use sha2::{Digest, Sha256};
use slatedb::object_store::ObjectStore;
use slatedb::{Db, DbIterator};
use thiserror::Error;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

pub type Result<T> = std::result::Result<T, PiLsmDbError>;

#[derive(Debug, Error)]
pub enum PiLsmDbError {
    #[error(transparent)]
    Core(#[from] PiLsmError),
    #[error(transparent)]
    Slate(#[from] slatedb::Error),
}

#[derive(Clone)]
pub struct PiLsmOptions {
    pub stream_registry: StreamRegistry,
    pub planner: Planner,
    pub decode_limits: DecodeLimits,
    pub max_reconstruct_bytes: u64,
}

impl PiLsmOptions {
    pub fn new(stream_registry: StreamRegistry, planner: Planner) -> Self {
        Self {
            stream_registry,
            planner,
            decode_limits: DecodeLimits::default(),
            max_reconstruct_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Clone)]
pub struct PiLsmDb {
    inner: Db,
    planner: Planner,
    reconstructor: Reconstructor,
    decode_limits: DecodeLimits,
    locks: Arc<KeyLocks>,
}

#[derive(Default)]
struct KeyLocks {
    locks: Mutex<HashMap<Vec<u8>, Arc<AsyncMutex<()>>>>,
}

impl KeyLocks {
    fn lock_for(&self, key: &[u8]) -> Arc<AsyncMutex<()>> {
        let mut locks = self.locks.lock().expect("key lock map poisoned");
        locks
            .entry(key.to_vec())
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RewriteStatus {
    Rewritten,
    KeptAlreadyPlanned,
    SkippedMissingKey,
    SkippedStaleSource,
    SkippedPlanningFailed,
    SkippedNotImproved,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlanReport {
    pub status: RewriteStatus,
    pub source_envelope_hash: Option<[u8; 32]>,
    pub old_envelope_bytes: Option<usize>,
    pub new_envelope_bytes: Option<usize>,
    pub old_chunk_count: Option<usize>,
    pub new_chunk_count: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PiLsmKeyValue {
    pub key: Bytes,
    pub value: Bytes,
}

pub struct PiLsmIterator {
    inner: DbIterator,
    reconstructor: Reconstructor,
    decode_limits: DecodeLimits,
}

impl PiLsmDb {
    pub async fn open<P>(
        path: P,
        object_store: Arc<dyn ObjectStore>,
        opts: PiLsmOptions,
    ) -> Result<Self>
    where
        P: Into<slatedb::object_store::path::Path>,
    {
        let inner = Db::open(path, object_store).await?;
        Ok(Self::from_db(inner, opts))
    }

    pub fn from_db(inner: Db, opts: PiLsmOptions) -> Self {
        let reconstructor = Reconstructor::new(opts.stream_registry);
        Self {
            inner,
            planner: opts.planner,
            reconstructor,
            decode_limits: opts.decode_limits,
            locks: Arc::new(KeyLocks::default()),
        }
    }

    pub async fn put<K, V>(&self, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]> + Send,
        V: AsRef<[u8]> + Send,
    {
        let key_bytes = key.as_ref().to_vec();
        let _guard = self.lock_key(&key_bytes).await;
        let encoded = ValueEnvelope::Raw(Bytes::copy_from_slice(value.as_ref())).encode();
        self.inner.put(key_bytes, encoded).await?;
        Ok(())
    }

    pub async fn delete<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]> + Send,
    {
        let key_bytes = key.as_ref().to_vec();
        let _guard = self.lock_key(&key_bytes).await;
        self.inner.delete(key_bytes).await?;
        Ok(())
    }

    pub async fn get<K>(&self, key: K) -> Result<Option<Bytes>>
    where
        K: AsRef<[u8]> + Send,
    {
        let Some(envelope) = self.get_envelope(key).await? else {
            return Ok(None);
        };
        Ok(Some(self.logical_bytes(envelope).await?))
    }

    pub async fn get_envelope<K>(&self, key: K) -> Result<Option<ValueEnvelope>>
    where
        K: AsRef<[u8]> + Send,
    {
        let Some(encoded) = self.inner.get(key).await? else {
            return Ok(None);
        };
        Ok(Some(ValueEnvelope::decode(&encoded, &self.decode_limits)?))
    }

    pub async fn explain<K>(&self, key: K) -> Result<Option<ExplainValue>>
    where
        K: AsRef<[u8]> + Send,
    {
        let Some(encoded) = self.inner.get(key).await? else {
            return Ok(None);
        };
        let envelope = ValueEnvelope::decode(&encoded, &self.decode_limits)?;
        Ok(Some(explain_envelope(&envelope, encoded.len())))
    }

    pub async fn scan<K, R>(&self, range: R) -> Result<PiLsmIterator>
    where
        K: AsRef<[u8]> + Send,
        R: RangeBounds<K> + Send,
    {
        Ok(PiLsmIterator {
            inner: self.inner.scan(range).await?,
            reconstructor: self.reconstructor.clone(),
            decode_limits: self.decode_limits.clone(),
        })
    }

    pub async fn flush(&self) -> Result<()> {
        self.inner.flush().await?;
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        self.inner.close().await?;
        Ok(())
    }

    pub async fn plan_key<K>(&self, key: K, _opts: PlanOptions) -> Result<PlanReport>
    where
        K: AsRef<[u8]> + Send,
    {
        let key_bytes = key.as_ref().to_vec();
        let Some((source_hash, logical_bytes, old_envelope_bytes)) = ({
            let _guard = self.lock_key(&key_bytes).await;
            let Some((source_hash, envelope, old_envelope_bytes)) =
                self.read_current_envelope(&key_bytes).await?
            else {
                return Ok(PlanReport::missing());
            };

            match envelope {
                ValueEnvelope::Raw(bytes) => Some((source_hash, bytes, old_envelope_bytes)),
                ValueEnvelope::Plan(plan) => {
                    return Ok(PlanReport::already_planned(
                        source_hash,
                        old_envelope_bytes,
                        plan.chunks.len(),
                    ));
                }
            }
        }) else {
            return Ok(PlanReport::missing());
        };

        let plan = match self.planner.plan(&logical_bytes).await {
            Ok(plan) => plan,
            Err(PiLsmError::PlanningFailed(_)) => {
                return Ok(PlanReport {
                    status: RewriteStatus::SkippedPlanningFailed,
                    source_envelope_hash: Some(source_hash),
                    old_envelope_bytes: Some(old_envelope_bytes),
                    new_envelope_bytes: None,
                    old_chunk_count: None,
                    new_chunk_count: None,
                });
            }
            Err(err) => return Err(err.into()),
        };
        let new_chunk_count = plan.chunks.len();
        let encoded_plan = ValueEnvelope::Plan(plan).encode();

        self.write_if_source_unchanged(
            key_bytes,
            source_hash,
            encoded_plan,
            PlanReport {
                status: RewriteStatus::Rewritten,
                source_envelope_hash: Some(source_hash),
                old_envelope_bytes: Some(old_envelope_bytes),
                new_envelope_bytes: None,
                old_chunk_count: None,
                new_chunk_count: Some(new_chunk_count),
            },
        )
        .await
    }

    pub async fn vacuum_meaning<K>(&self, key: K, _opts: PlanOptions) -> Result<PlanReport>
    where
        K: AsRef<[u8]> + Send,
    {
        let key_bytes = key.as_ref().to_vec();
        let Some((source_hash, envelope, old_encoded_len)) =
            self.read_current_envelope(&key_bytes).await?
        else {
            return Ok(PlanReport::missing());
        };

        let ValueEnvelope::Plan(old_plan) = envelope else {
            return self.plan_key(key_bytes, PlanOptions::default()).await;
        };

        let old_chunk_count = old_plan.chunks.len();
        let logical_bytes = self.reconstructor.reconstruct(&old_plan).await?;
        let new_plan = self.planner.plan(&logical_bytes).await?;
        let new_chunk_count = new_plan.chunks.len();
        let encoded_plan = ValueEnvelope::Plan(new_plan).encode();
        if !plan_improved(
            old_encoded_len,
            old_chunk_count,
            encoded_plan.len(),
            new_chunk_count,
        ) {
            return Ok(PlanReport {
                status: RewriteStatus::SkippedNotImproved,
                source_envelope_hash: Some(source_hash),
                old_envelope_bytes: Some(old_encoded_len),
                new_envelope_bytes: Some(encoded_plan.len()),
                old_chunk_count: Some(old_chunk_count),
                new_chunk_count: Some(new_chunk_count),
            });
        }

        self.write_if_source_unchanged(
            key_bytes,
            source_hash,
            encoded_plan,
            PlanReport {
                status: RewriteStatus::Rewritten,
                source_envelope_hash: Some(source_hash),
                old_envelope_bytes: Some(old_encoded_len),
                new_envelope_bytes: None,
                old_chunk_count: Some(old_chunk_count),
                new_chunk_count: Some(new_chunk_count),
            },
        )
        .await
    }

    async fn logical_bytes(&self, envelope: ValueEnvelope) -> CoreResult<Bytes> {
        match envelope {
            ValueEnvelope::Raw(bytes) => Ok(bytes),
            ValueEnvelope::Plan(plan) => self.reconstructor.reconstruct(&plan).await,
        }
    }

    async fn read_current_envelope(
        &self,
        key: &[u8],
    ) -> Result<Option<([u8; 32], ValueEnvelope, usize)>> {
        let Some(encoded) = self.inner.get(key.to_vec()).await? else {
            return Ok(None);
        };
        let source_hash = envelope_hash(&encoded);
        let old_envelope_bytes = encoded.len();
        let envelope = ValueEnvelope::decode(&encoded, &self.decode_limits)?;
        Ok(Some((source_hash, envelope, old_envelope_bytes)))
    }

    async fn write_if_source_unchanged(
        &self,
        key: Vec<u8>,
        source_hash: [u8; 32],
        encoded_plan: Vec<u8>,
        mut report: PlanReport,
    ) -> Result<PlanReport> {
        let _guard = self.lock_key(&key).await;
        let Some(current) = self.inner.get(key.clone()).await? else {
            return Ok(PlanReport::missing());
        };
        if envelope_hash(&current) != source_hash {
            report.status = RewriteStatus::SkippedStaleSource;
            report.new_envelope_bytes = None;
            return Ok(report);
        }

        report.new_envelope_bytes = Some(encoded_plan.len());
        self.inner.put(key, encoded_plan).await?;
        Ok(report)
    }

    async fn lock_key(&self, key: &[u8]) -> OwnedMutexGuard<()> {
        self.locks.lock_for(key).lock_owned().await
    }
}

impl PiLsmIterator {
    pub async fn next(&mut self) -> Result<Option<PiLsmKeyValue>> {
        let Some(kv) = self.inner.next().await? else {
            return Ok(None);
        };
        let envelope = ValueEnvelope::decode(&kv.value, &self.decode_limits)?;
        let value = match envelope {
            ValueEnvelope::Raw(bytes) => bytes,
            ValueEnvelope::Plan(plan) => self.reconstructor.reconstruct(&plan).await?,
        };
        Ok(Some(PiLsmKeyValue { key: kv.key, value }))
    }
}

impl PlanReport {
    fn missing() -> Self {
        Self {
            status: RewriteStatus::SkippedMissingKey,
            source_envelope_hash: None,
            old_envelope_bytes: None,
            new_envelope_bytes: None,
            old_chunk_count: None,
            new_chunk_count: None,
        }
    }

    fn already_planned(source_hash: [u8; 32], envelope_bytes: usize, chunk_count: usize) -> Self {
        Self {
            status: RewriteStatus::KeptAlreadyPlanned,
            source_envelope_hash: Some(source_hash),
            old_envelope_bytes: Some(envelope_bytes),
            new_envelope_bytes: Some(envelope_bytes),
            old_chunk_count: Some(chunk_count),
            new_chunk_count: Some(chunk_count),
        }
    }
}

fn envelope_hash(bytes: &[u8]) -> [u8; 32] {
    let hash = Sha256::digest(bytes);
    let mut out = [0_u8; 32];
    out.copy_from_slice(&hash);
    out
}

fn plan_improved(
    old_envelope_bytes: usize,
    old_chunk_count: usize,
    new_envelope_bytes: usize,
    new_chunk_count: usize,
) -> bool {
    new_envelope_bytes < old_envelope_bytes
        || (new_envelope_bytes == old_envelope_bytes && new_chunk_count < old_chunk_count)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use pilsmer_core::{
        ByteStream, PlanOptions, PrefixByteStream, StreamId, StreamIndex, StreamIndexOptions,
    };
    use slatedb::object_store::memory::InMemory;

    use super::*;

    async fn demo_db(prefix: &'static [u8]) -> PiLsmDb {
        let stream: Arc<dyn ByteStream> = Arc::new(PrefixByteStream::new(
            StreamId::PiHexFractionPrefixV1 {
                digest: [9_u8; 32],
                bytes: prefix.len() as u64,
            },
            Bytes::from_static(prefix),
        ));
        let mut registry = StreamRegistry::new();
        registry.register(stream.clone());
        let index = Arc::new(
            StreamIndex::build(
                stream,
                StreamIndexOptions {
                    max_prefix_len: prefix.len() as u64,
                    max_k: 4,
                    max_index_memory_bytes: 16 * 1024,
                    max_offsets_per_kgram: 4,
                },
            )
            .await
            .unwrap(),
        );
        let planner = Planner::new(
            vec![index],
            registry.clone(),
            PlanOptions {
                max_k: 4,
                allow_literals: true,
                ..PlanOptions::default()
            },
        );
        let opts = PiLsmOptions::new(registry, planner);
        PiLsmDb::open("pilsmer-test", Arc::new(InMemory::new()), opts)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn put_get_and_plan_key_roundtrip() {
        let db = demo_db(b"abcdef").await;
        db.put(b"k", b"abc").await.unwrap();
        assert_eq!(
            db.get(b"k").await.unwrap(),
            Some(Bytes::from_static(b"abc"))
        );

        let report = db.plan_key(b"k", PlanOptions::default()).await.unwrap();
        assert_eq!(report.status, RewriteStatus::Rewritten);

        assert_eq!(
            db.get(b"k").await.unwrap(),
            Some(Bytes::from_static(b"abc"))
        );

        let report = db.plan_key(b"k", PlanOptions::default()).await.unwrap();
        assert_eq!(report.status, RewriteStatus::KeptAlreadyPlanned);
    }

    #[tokio::test]
    async fn scan_reconstructs_lazily() {
        let db = demo_db(b"abcdef").await;
        db.put(b"a", b"abc").await.unwrap();
        db.plan_key(b"a", PlanOptions::default()).await.unwrap();
        db.put(b"b", b"def").await.unwrap();

        let mut iter = db
            .scan::<Vec<u8>, _>(b"a".to_vec()..b"z".to_vec())
            .await
            .unwrap();
        assert_eq!(
            iter.next().await.unwrap().unwrap().value,
            Bytes::from_static(b"abc")
        );
        assert_eq!(
            iter.next().await.unwrap().unwrap().value,
            Bytes::from_static(b"def")
        );
        assert!(iter.next().await.unwrap().is_none());
    }
}
