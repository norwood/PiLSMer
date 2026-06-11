use std::collections::HashMap;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
pub use compaction_filter::{
    CompactionMode, PiLsmCompactionFilterStats, PiLsmCompactionFilterSupplier,
};
use pilsmer_core::{
    explain_envelope, DecodeLimits, ExplainValue, PiLsmError, PlanOptions, Planner, Reconstructor,
    Result as CoreResult, StreamRegistry, ValueEnvelope,
};
use sha2::{Digest, Sha256};
use slatedb::config::{CompactorOptions, SizeTieredCompactionSchedulerOptions};
use slatedb::object_store::path::Path as ObjectStorePath;
use slatedb::object_store::ObjectStore;
use slatedb::{CompactorBuilder, Db, DbIterator};
use thiserror::Error;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

mod compaction_filter;

pub type Result<T> = std::result::Result<T, PiLsmDbError>;

#[derive(Clone, Debug)]
pub struct PiLsmCompactorOptions {
    pub run_for: Duration,
    pub poll_interval: Duration,
    pub min_compaction_sources: usize,
}

impl Default for PiLsmCompactorOptions {
    fn default() -> Self {
        Self {
            run_for: Duration::from_secs(1),
            poll_interval: Duration::from_millis(50),
            min_compaction_sources: 4,
        }
    }
}

pub async fn run_compactor_for<P>(
    path: P,
    object_store: Arc<dyn ObjectStore>,
    supplier: Arc<PiLsmCompactionFilterSupplier>,
    run_for: Duration,
) -> Result<()>
where
    P: Into<ObjectStorePath>,
{
    run_compactor_with_options(
        path,
        object_store,
        supplier,
        PiLsmCompactorOptions {
            run_for,
            ..PiLsmCompactorOptions::default()
        },
    )
    .await
}

pub async fn run_compactor_with_options<P>(
    path: P,
    object_store: Arc<dyn ObjectStore>,
    supplier: Arc<PiLsmCompactionFilterSupplier>,
    options: PiLsmCompactorOptions,
) -> Result<()>
where
    P: Into<ObjectStorePath>,
{
    let compactor_options = compactor_options(options.clone());
    let compactor = CompactorBuilder::new(path.into(), object_store)
        .with_options(compactor_options)
        .with_compaction_filter_supplier(supplier)
        .build();

    let mut run_task = tokio::spawn({
        let compactor = compactor.clone();
        async move { compactor.run().await }
    });

    tokio::select! {
        result = &mut run_task => return flatten_compactor_result(result),
        _ = tokio::time::sleep(options.run_for) => {}
    }

    compactor.stop().await?;
    flatten_compactor_result(run_task.await)
}

fn compactor_options(options: PiLsmCompactorOptions) -> CompactorOptions {
    let min_compaction_sources = options.min_compaction_sources.max(2);
    let scheduler_options = SizeTieredCompactionSchedulerOptions {
        min_compaction_sources,
        max_compaction_sources: min_compaction_sources.max(8),
        ..SizeTieredCompactionSchedulerOptions::default()
    };
    CompactorOptions {
        poll_interval: options.poll_interval,
        scheduler_options: scheduler_options.into(),
        ..CompactorOptions::default()
    }
}

fn flatten_compactor_result(
    result: std::result::Result<std::result::Result<(), slatedb::Error>, tokio::task::JoinError>,
) -> Result<()> {
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => Err(err.into()),
        Err(err) => Err(PiLsmDbError::CompactorTask(err.to_string())),
    }
}

#[derive(Debug, Error)]
pub enum PiLsmDbError {
    #[error(transparent)]
    Core(#[from] PiLsmError),
    #[error(transparent)]
    Slate(#[from] slatedb::Error),
    #[error("compactor task failed: {0}")]
    CompactorTask(String),
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

pub struct PiLsmEnvelopeIterator {
    inner: DbIterator,
    decode_limits: DecodeLimits,
}

pub struct PiLsmExplainIterator {
    inner: DbIterator,
    decode_limits: DecodeLimits,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PiLsmEnvelopeKeyValue {
    pub key: Bytes,
    pub envelope: ValueEnvelope,
    pub physical_value_bytes: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PiLsmExplainKeyValue {
    pub key: Bytes,
    pub explain: ExplainValue,
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

    pub async fn open_with_compaction_filter<P>(
        path: P,
        object_store: Arc<dyn ObjectStore>,
        opts: PiLsmOptions,
        supplier: Arc<PiLsmCompactionFilterSupplier>,
    ) -> Result<Self>
    where
        P: Into<slatedb::object_store::path::Path>,
    {
        let path: ObjectStorePath = path.into();
        let compactor_builder = CompactorBuilder::new(path.clone(), object_store.clone())
            .with_compaction_filter_supplier(supplier);
        let inner = Db::builder(path, object_store)
            .with_compactor_builder(compactor_builder)
            .build()
            .await?;
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

    pub async fn scan_envelopes<K, R>(&self, range: R) -> Result<PiLsmEnvelopeIterator>
    where
        K: AsRef<[u8]> + Send,
        R: RangeBounds<K> + Send,
    {
        Ok(PiLsmEnvelopeIterator {
            inner: self.inner.scan(range).await?,
            decode_limits: self.decode_limits.clone(),
        })
    }

    pub async fn scan_explain<K, R>(&self, range: R) -> Result<PiLsmExplainIterator>
    where
        K: AsRef<[u8]> + Send,
        R: RangeBounds<K> + Send,
    {
        Ok(PiLsmExplainIterator {
            inner: self.inner.scan(range).await?,
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

    pub async fn plan_key<K>(&self, key: K, opts: PlanOptions) -> Result<PlanReport>
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

        let plan = match self.planner.plan_with_options(&logical_bytes, opts).await {
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

    pub async fn vacuum_meaning<K>(&self, key: K, opts: PlanOptions) -> Result<PlanReport>
    where
        K: AsRef<[u8]> + Send,
    {
        let key_bytes = key.as_ref().to_vec();
        let Some((source_hash, envelope, old_encoded_len)) = ({
            let _guard = self.lock_key(&key_bytes).await;
            self.read_current_envelope(&key_bytes).await?
        }) else {
            return Ok(PlanReport::missing());
        };

        let ValueEnvelope::Plan(old_plan) = envelope else {
            return self.plan_key(key_bytes, opts).await;
        };

        let old_chunk_count = old_plan.chunks.len();
        let logical_bytes = self.reconstructor.reconstruct(&old_plan).await?;
        let new_plan = match self.planner.plan_with_options(&logical_bytes, opts).await {
            Ok(plan) => plan,
            Err(PiLsmError::PlanningFailed(_)) => {
                return Ok(PlanReport {
                    status: RewriteStatus::SkippedPlanningFailed,
                    source_envelope_hash: Some(source_hash),
                    old_envelope_bytes: Some(old_encoded_len),
                    new_envelope_bytes: None,
                    old_chunk_count: Some(old_chunk_count),
                    new_chunk_count: None,
                });
            }
            Err(err) => return Err(err.into()),
        };
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

impl PiLsmEnvelopeIterator {
    pub async fn next(&mut self) -> Result<Option<PiLsmEnvelopeKeyValue>> {
        let Some(kv) = self.inner.next().await? else {
            return Ok(None);
        };
        let physical_value_bytes = kv.value.len();
        let envelope = ValueEnvelope::decode(&kv.value, &self.decode_limits)?;
        Ok(Some(PiLsmEnvelopeKeyValue {
            key: kv.key,
            envelope,
            physical_value_bytes,
        }))
    }
}

impl PiLsmExplainIterator {
    pub async fn next(&mut self) -> Result<Option<PiLsmExplainKeyValue>> {
        let Some(kv) = self.inner.next().await? else {
            return Ok(None);
        };
        let envelope = ValueEnvelope::decode(&kv.value, &self.decode_limits)?;
        Ok(Some(PiLsmExplainKeyValue {
            key: kv.key,
            explain: explain_envelope(&envelope, kv.value.len()),
        }))
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
    plan_score(new_envelope_bytes, new_chunk_count)
        < plan_score(old_envelope_bytes, old_chunk_count)
}

fn plan_score(envelope_bytes: usize, chunk_count: usize) -> u128 {
    envelope_bytes as u128 + 16 * chunk_count as u128
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use pilsmer_core::{
        ByteStream, PlanOptions, PrefixByteStream, StorageClass, StreamId, StreamIndex,
        StreamIndexOptions,
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

    #[tokio::test]
    async fn raw_inspection_scans_do_not_reconstruct_values() {
        let db = demo_db(b"abcdef").await;
        db.put(b"a", b"abc").await.unwrap();
        db.plan_key(b"a", PlanOptions::default()).await.unwrap();
        db.put(b"b", b"def").await.unwrap();

        let mut envelopes = db
            .scan_envelopes::<Vec<u8>, _>(b"a".to_vec()..b"z".to_vec())
            .await
            .unwrap();
        assert!(matches!(
            envelopes.next().await.unwrap().unwrap().envelope,
            ValueEnvelope::Plan(_)
        ));
        assert!(matches!(
            envelopes.next().await.unwrap().unwrap().envelope,
            ValueEnvelope::Raw(_)
        ));
        assert!(envelopes.next().await.unwrap().is_none());

        let mut explain = db
            .scan_explain::<Vec<u8>, _>(b"a".to_vec()..b"z".to_vec())
            .await
            .unwrap();
        assert_eq!(
            explain.next().await.unwrap().unwrap().explain.storage_class,
            StorageClass::Plan
        );
        assert_eq!(
            explain.next().await.unwrap().unwrap().explain.storage_class,
            StorageClass::Raw
        );
        assert!(explain.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn app_level_rewrite_honors_caller_plan_options() {
        let db = demo_db(b"abc").await;
        db.put(b"k", b"az").await.unwrap();

        let report = db.plan_key(b"k", PlanOptions::default()).await.unwrap();
        assert_eq!(report.status, RewriteStatus::SkippedPlanningFailed);
        assert!(matches!(
            db.get_envelope(b"k").await.unwrap().unwrap(),
            ValueEnvelope::Raw(_)
        ));

        let report = db
            .plan_key(
                b"k",
                PlanOptions {
                    max_k: 2,
                    allow_literals: true,
                    ..PlanOptions::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(report.status, RewriteStatus::Rewritten);
        let ValueEnvelope::Plan(plan) = db.get_envelope(b"k").await.unwrap().unwrap() else {
            panic!("expected planned envelope");
        };
        assert!(matches!(
            plan.chunks.last(),
            Some(pilsmer_core::ChunkRef::Literal { bytes }) if bytes.as_ref() == b"z"
        ));
    }

    #[tokio::test]
    async fn vacuum_keeps_existing_plan_when_replan_fails() {
        let db = demo_db(b"abc").await;
        db.put(b"k", b"az").await.unwrap();
        db.plan_key(
            b"k",
            PlanOptions {
                max_k: 2,
                allow_literals: true,
                ..PlanOptions::default()
            },
        )
        .await
        .unwrap();

        let report = db
            .vacuum_meaning(b"k", PlanOptions::default())
            .await
            .unwrap();
        assert_eq!(report.status, RewriteStatus::SkippedPlanningFailed);
        let ValueEnvelope::Plan(plan) = db.get_envelope(b"k").await.unwrap().unwrap() else {
            panic!("expected existing plan to be kept");
        };
        assert!(matches!(
            plan.chunks.last(),
            Some(pilsmer_core::ChunkRef::Literal { bytes }) if bytes.as_ref() == b"z"
        ));
    }

    #[test]
    fn vacuum_score_accepts_only_weighted_improvements() {
        assert!(plan_improved(100, 10, 110, 1));
        assert!(!plan_improved(100, 1, 99, 2));
    }
}
