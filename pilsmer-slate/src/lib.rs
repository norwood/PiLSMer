use std::collections::HashMap;
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
pub use compaction_filter::{
    CompactionMode, PiLsmCompactionFilterStats, PiLsmCompactionFilterSupplier,
};
use pilsmer_core::{
    explain_envelope, DecodeLimits, ExplainValue, LogicalHashKind, PhilosophicalCompressionRatio,
    PiLsmError, PlanOptions, Planner, Purity, Reconstructor, Result as CoreResult, StorageClass,
    StreamRegistry, ValueEnvelope,
};
use sha2::{Digest, Sha256};
use slatedb::config::{CompactorOptions, Settings, SizeTieredCompactionSchedulerOptions};
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
    pub reconstruction_cache_bytes: u64,
    pub disable_embedded_compactor: bool,
}

impl PiLsmOptions {
    pub fn new(stream_registry: StreamRegistry, planner: Planner) -> Self {
        Self {
            stream_registry,
            planner,
            decode_limits: DecodeLimits::default(),
            max_reconstruct_bytes: 64 * 1024 * 1024,
            reconstruction_cache_bytes: 0,
            disable_embedded_compactor: false,
        }
    }
}

#[derive(Clone)]
pub struct PiLsmDb {
    inner: Db,
    planner: Planner,
    reconstructor: Reconstructor,
    decode_limits: DecodeLimits,
    max_reconstruct_bytes: u64,
    stream_prefix_bytes_indexed: u64,
    locks: Arc<KeyLocks>,
    counters: Arc<OperationCounters>,
    reconstruction_cache: Arc<ReconstructionCache>,
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

#[derive(Default)]
struct OperationCounters {
    raw_bytes_converted_total: AtomicU64,
    planner_nanos: AtomicU64,
    reconstruction_nanos: AtomicU64,
    vacuum_meaning_attempts_total: AtomicU64,
    vacuum_meaning_improvements_total: AtomicU64,
    compaction_filter_errors_total: AtomicU64,
    snapshot_protected_entries_total: AtomicU64,
    reconstruction_cache_bytes: AtomicU64,
    representation_entropy_excuses_total: AtomicU64,
}

#[derive(Default)]
struct ReconstructionCache {
    inner: Mutex<ReconstructionCacheState>,
}

#[derive(Default)]
struct ReconstructionCacheState {
    max_bytes: u64,
    current_bytes: u64,
    values: HashMap<ReconstructionCacheKey, Bytes>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct ReconstructionCacheKey {
    hash_kind: u8,
    hash: [u8; 16],
}

impl ReconstructionCache {
    fn new(max_bytes: u64) -> Self {
        Self {
            inner: Mutex::new(ReconstructionCacheState {
                max_bytes,
                ..ReconstructionCacheState::default()
            }),
        }
    }

    fn get(&self, key: ReconstructionCacheKey) -> Option<Bytes> {
        let cache = self.inner.lock().expect("reconstruction cache poisoned");
        cache.values.get(&key).cloned()
    }

    fn insert(&self, key: ReconstructionCacheKey, value: Bytes) -> u64 {
        let mut cache = self.inner.lock().expect("reconstruction cache poisoned");
        if cache.max_bytes == 0 || value.len() as u64 > cache.max_bytes {
            return cache.current_bytes;
        }
        if cache.values.contains_key(&key) {
            return cache.current_bytes;
        }
        while cache.current_bytes.saturating_add(value.len() as u64) > cache.max_bytes {
            let Some(victim) = cache.values.keys().next().copied() else {
                break;
            };
            if let Some(removed) = cache.values.remove(&victim) {
                cache.current_bytes = cache.current_bytes.saturating_sub(removed.len() as u64);
            }
        }
        if cache.current_bytes.saturating_add(value.len() as u64) <= cache.max_bytes {
            cache.current_bytes += value.len() as u64;
            cache.values.insert(key, value);
        }
        cache.current_bytes
    }
}

impl OperationCounters {
    fn add_raw_bytes_converted(&self, bytes: u64) {
        self.add(&self.raw_bytes_converted_total, bytes);
    }

    fn add_planner_duration(&self, duration: Duration) {
        self.add_duration(&self.planner_nanos, duration);
    }

    fn add_reconstruction_duration(&self, duration: Duration) {
        self.add_duration(&self.reconstruction_nanos, duration);
    }

    fn increment_vacuum_attempts(&self) {
        self.add(&self.vacuum_meaning_attempts_total, 1);
    }

    fn increment_vacuum_improvements(&self) {
        self.add(&self.vacuum_meaning_improvements_total, 1);
    }

    fn increment_entropy_excuses(&self) {
        self.add(&self.representation_entropy_excuses_total, 1);
    }

    fn set_reconstruction_cache_bytes(&self, bytes: u64) {
        self.reconstruction_cache_bytes
            .store(bytes, Ordering::Relaxed);
    }

    fn load(&self, counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }

    fn add_duration(&self, counter: &AtomicU64, duration: Duration) {
        let nanos = duration.as_nanos().min(u128::from(u64::MAX)) as u64;
        self.add(counter, nanos);
    }

    fn add(&self, counter: &AtomicU64, value: u64) {
        let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(current.saturating_add(value))
        });
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

pub type VacuumReport = PlanReport;

#[derive(Clone, Debug)]
pub struct VacuumOptions {
    pub plan_options: PlanOptions,
    pub max_reconstruct_bytes: Option<u64>,
}

impl Default for VacuumOptions {
    fn default() -> Self {
        Self {
            plan_options: PlanOptions::default(),
            max_reconstruct_bytes: None,
        }
    }
}

impl From<PlanOptions> for VacuumOptions {
    fn from(plan_options: PlanOptions) -> Self {
        Self {
            plan_options,
            max_reconstruct_bytes: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PiLsmKeyValue {
    pub key: Bytes,
    pub value: Bytes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PutOptions {
    pub await_durable: bool,
    pub allow_immediate_plan: bool,
}

impl Default for PutOptions {
    fn default() -> Self {
        Self {
            await_durable: false,
            allow_immediate_plan: false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteHandle {
    pub storage_class: StorageClass,
    pub physical_value_bytes: usize,
}

pub struct PiLsmIterator {
    inner: DbIterator,
    reconstructor: Reconstructor,
    decode_limits: DecodeLimits,
    reconstruct: bool,
    max_reconstruct_bytes: u64,
    counters: Arc<OperationCounters>,
    reconstruction_cache: Arc<ReconstructionCache>,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScanOptions {
    pub reconstruct: bool,
    pub max_reconstruct_bytes: Option<u64>,
}

impl Default for ScanOptions {
    fn default() -> Self {
        Self {
            reconstruct: true,
            max_reconstruct_bytes: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PiLsmMetrics {
    pub raw_values_total: u64,
    pub planned_values_total: u64,
    pub logical_bytes_total: u64,
    pub planned_logical_bytes_total: u64,
    pub raw_bytes_converted_total: u64,
    pub raw_envelope_bytes_total: u64,
    pub plan_envelope_bytes_total: u64,
    pub plan_metadata_bytes_total: u64,
    pub located_user_bytes_total: u64,
    pub literal_user_bytes_total: u64,
    pub physical_value_bytes_total: u64,
    pub philosophical_user_value_bytes_stored_total: u64,
    pub reconstruction_seconds: f64,
    pub planner_seconds: f64,
    pub chunks_total: u64,
    pub chunks_per_value: Option<f64>,
    pub avg_chunk_len_bytes: Option<f64>,
    pub longest_natural_run_bytes: u32,
    pub stream_prefix_bytes_indexed: u64,
    pub metadata_amplification_ratio: Option<f64>,
    pub philosophical_compression_ratio: PhilosophicalCompressionRatio,
    pub compaction_filter_errors_total: u64,
    pub snapshot_protected_entries_total: u64,
    pub vacuum_meaning_attempts_total: u64,
    pub vacuum_meaning_improvements_total: u64,
    pub reconstruction_cache_bytes: u64,
    pub philosophical_purity_violations_total: u64,
    pub representation_entropy_excuses_total: u64,
}

impl Default for PiLsmMetrics {
    fn default() -> Self {
        Self {
            raw_values_total: 0,
            planned_values_total: 0,
            logical_bytes_total: 0,
            planned_logical_bytes_total: 0,
            raw_bytes_converted_total: 0,
            raw_envelope_bytes_total: 0,
            plan_envelope_bytes_total: 0,
            plan_metadata_bytes_total: 0,
            located_user_bytes_total: 0,
            literal_user_bytes_total: 0,
            physical_value_bytes_total: 0,
            philosophical_user_value_bytes_stored_total: 0,
            reconstruction_seconds: 0.0,
            planner_seconds: 0.0,
            chunks_total: 0,
            chunks_per_value: None,
            avg_chunk_len_bytes: None,
            longest_natural_run_bytes: 0,
            stream_prefix_bytes_indexed: 0,
            metadata_amplification_ratio: None,
            philosophical_compression_ratio: PhilosophicalCompressionRatio::Undefined,
            compaction_filter_errors_total: 0,
            snapshot_protected_entries_total: 0,
            vacuum_meaning_attempts_total: 0,
            vacuum_meaning_improvements_total: 0,
            reconstruction_cache_bytes: 0,
            philosophical_purity_violations_total: 0,
            representation_entropy_excuses_total: 0,
        }
    }
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
        let path = path.into();
        let inner = if opts.disable_embedded_compactor {
            Db::builder(path, object_store)
                .with_settings(Settings {
                    compactor_options: None,
                    ..Settings::default()
                })
                .build()
                .await?
        } else {
            Db::open(path, object_store).await?
        };
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
        let stream_prefix_bytes_indexed = opts.planner.stream_prefix_bytes_indexed();
        Self {
            inner,
            planner: opts.planner,
            reconstructor,
            decode_limits: opts.decode_limits,
            max_reconstruct_bytes: opts.max_reconstruct_bytes,
            stream_prefix_bytes_indexed,
            locks: Arc::new(KeyLocks::default()),
            counters: Arc::new(OperationCounters::default()),
            reconstruction_cache: Arc::new(ReconstructionCache::new(
                opts.reconstruction_cache_bytes,
            )),
        }
    }

    pub async fn put<K, V>(&self, key: K, value: V) -> Result<WriteHandle>
    where
        K: AsRef<[u8]> + Send,
        V: AsRef<[u8]> + Send,
    {
        self.put_with_options(key, value, PutOptions::default())
            .await
    }

    pub async fn put_with_options<K, V>(
        &self,
        key: K,
        value: V,
        options: PutOptions,
    ) -> Result<WriteHandle>
    where
        K: AsRef<[u8]> + Send,
        V: AsRef<[u8]> + Send,
    {
        let key_bytes = key.as_ref().to_vec();
        let _guard = self.lock_key(&key_bytes).await;
        let envelope = if options.allow_immediate_plan {
            ValueEnvelope::Plan(self.plan_bytes(value.as_ref()).await?)
        } else {
            ValueEnvelope::Raw(Bytes::copy_from_slice(value.as_ref()))
        };
        let storage_class = match &envelope {
            ValueEnvelope::Raw(_) => StorageClass::Raw,
            ValueEnvelope::Plan(_) => StorageClass::Plan,
        };
        let encoded = envelope.encode();
        let physical_value_bytes = encoded.len();
        self.inner.put(key_bytes, encoded).await?;
        if options.await_durable {
            self.inner.flush().await?;
        }
        Ok(WriteHandle {
            storage_class,
            physical_value_bytes,
        })
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
        Ok(Some(
            self.logical_bytes(envelope, self.max_reconstruct_bytes)
                .await?,
        ))
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
        self.scan_with_options(range, ScanOptions::default()).await
    }

    pub async fn scan_with_options<K, R>(
        &self,
        range: R,
        options: ScanOptions,
    ) -> Result<PiLsmIterator>
    where
        K: AsRef<[u8]> + Send,
        R: RangeBounds<K> + Send,
    {
        Ok(PiLsmIterator {
            inner: self.inner.scan(range).await?,
            reconstructor: self.reconstructor.clone(),
            decode_limits: self.decode_limits.clone(),
            reconstruct: options.reconstruct,
            max_reconstruct_bytes: options
                .max_reconstruct_bytes
                .unwrap_or(self.max_reconstruct_bytes),
            counters: self.counters.clone(),
            reconstruction_cache: self.reconstruction_cache.clone(),
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

    pub async fn metrics(&self) -> Result<PiLsmMetrics> {
        let mut metrics = PiLsmMetrics::new();
        let mut values = self.scan_explain::<Vec<u8>, _>(Vec::<u8>::new()..).await?;
        while let Some(kv) = values.next().await? {
            metrics.observe(&kv.explain);
        }
        metrics.stream_prefix_bytes_indexed = self.stream_prefix_bytes_indexed;
        metrics.observe_counters(&self.counters);
        metrics.finish();
        Ok(metrics)
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

        let plan = match self.plan_bytes_with_options(&logical_bytes, opts).await {
            Ok(plan) => plan,
            Err(PiLsmError::PlanningFailed(_)) => {
                self.counters.increment_entropy_excuses();
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

        let report = self
            .write_if_source_unchanged(
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
            .await?;
        if report.status == RewriteStatus::Rewritten {
            self.counters
                .add_raw_bytes_converted(logical_bytes.len() as u64);
        }
        Ok(report)
    }

    pub async fn vacuum_meaning<K, O>(&self, key: K, options: O) -> Result<VacuumReport>
    where
        K: AsRef<[u8]> + Send,
        O: Into<VacuumOptions>,
    {
        let options = options.into();
        self.counters.increment_vacuum_attempts();
        let key_bytes = key.as_ref().to_vec();
        let Some((source_hash, envelope, old_encoded_len)) = ({
            let _guard = self.lock_key(&key_bytes).await;
            self.read_current_envelope(&key_bytes).await?
        }) else {
            return Ok(PlanReport::missing());
        };

        let ValueEnvelope::Plan(old_plan) = envelope else {
            return self.plan_key(key_bytes, options.plan_options).await;
        };

        let old_chunk_count = old_plan.chunks.len();
        let logical_bytes = self
            .reconstruct_plan(
                &old_plan,
                options
                    .max_reconstruct_bytes
                    .unwrap_or(self.max_reconstruct_bytes),
            )
            .await?;
        let new_plan = match self
            .plan_bytes_with_options(&logical_bytes, options.plan_options)
            .await
        {
            Ok(plan) => plan,
            Err(PiLsmError::PlanningFailed(_)) => {
                self.counters.increment_entropy_excuses();
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

        let report = self
            .write_if_source_unchanged(
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
            .await?;
        if report.status == RewriteStatus::Rewritten {
            self.counters.increment_vacuum_improvements();
        }
        Ok(report)
    }

    pub async fn replan_key<K, O>(&self, key: K, options: O) -> Result<PlanReport>
    where
        K: AsRef<[u8]> + Send,
        O: Into<VacuumOptions>,
    {
        let options = options.into();
        let key_bytes = key.as_ref().to_vec();
        let Some((source_hash, envelope, old_encoded_len)) = ({
            let _guard = self.lock_key(&key_bytes).await;
            self.read_current_envelope(&key_bytes).await?
        }) else {
            return Ok(PlanReport::missing());
        };

        let (logical_bytes, old_chunk_count) = match envelope {
            ValueEnvelope::Raw(bytes) => (bytes, None),
            ValueEnvelope::Plan(plan) => {
                let chunk_count = plan.chunks.len();
                let logical = self
                    .reconstruct_plan(
                        &plan,
                        options
                            .max_reconstruct_bytes
                            .unwrap_or(self.max_reconstruct_bytes),
                    )
                    .await?;
                (logical, Some(chunk_count))
            }
        };

        let new_plan = match self
            .plan_bytes_with_options(&logical_bytes, options.plan_options)
            .await
        {
            Ok(plan) => plan,
            Err(PiLsmError::PlanningFailed(_)) => {
                self.counters.increment_entropy_excuses();
                return Ok(PlanReport {
                    status: RewriteStatus::SkippedPlanningFailed,
                    source_envelope_hash: Some(source_hash),
                    old_envelope_bytes: Some(old_encoded_len),
                    new_envelope_bytes: None,
                    old_chunk_count,
                    new_chunk_count: None,
                });
            }
            Err(err) => return Err(err.into()),
        };
        let new_chunk_count = new_plan.chunks.len();
        let encoded_plan = ValueEnvelope::Plan(new_plan).encode();
        if encoded_plan.len() == old_encoded_len && envelope_hash(&encoded_plan) == source_hash {
            return Ok(PlanReport {
                status: RewriteStatus::SkippedNotImproved,
                source_envelope_hash: Some(source_hash),
                old_envelope_bytes: Some(old_encoded_len),
                new_envelope_bytes: Some(encoded_plan.len()),
                old_chunk_count,
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
                old_chunk_count,
                new_chunk_count: Some(new_chunk_count),
            },
        )
        .await
    }

    async fn logical_bytes(
        &self,
        envelope: ValueEnvelope,
        max_reconstruct_bytes: u64,
    ) -> CoreResult<Bytes> {
        match envelope {
            ValueEnvelope::Raw(bytes) => Ok(bytes),
            ValueEnvelope::Plan(plan) => self.reconstruct_plan(&plan, max_reconstruct_bytes).await,
        }
    }

    async fn plan_bytes(&self, bytes: &[u8]) -> CoreResult<pilsmer_core::ReconstructionPlan> {
        let start = Instant::now();
        let result = self.planner.plan(bytes).await;
        self.counters.add_planner_duration(start.elapsed());
        result
    }

    async fn plan_bytes_with_options(
        &self,
        bytes: &[u8],
        options: PlanOptions,
    ) -> CoreResult<pilsmer_core::ReconstructionPlan> {
        let start = Instant::now();
        let result = self.planner.plan_with_options(bytes, options).await;
        self.counters.add_planner_duration(start.elapsed());
        result
    }

    async fn reconstruct_plan(
        &self,
        plan: &pilsmer_core::ReconstructionPlan,
        max_reconstruct_bytes: u64,
    ) -> CoreResult<Bytes> {
        let start = Instant::now();
        if plan.logical_len > max_reconstruct_bytes {
            return Err(PiLsmError::DecodeLimitExceeded("max_reconstruct_bytes"));
        }
        let cache_key = reconstruction_cache_key(plan);
        if let Some(value) = self.reconstruction_cache.get(cache_key) {
            return Ok(value);
        }
        let result = self
            .reconstructor
            .reconstruct_with_limit(plan, max_reconstruct_bytes)
            .await;
        self.counters.add_reconstruction_duration(start.elapsed());
        if let Ok(value) = &result {
            let bytes = self.reconstruction_cache.insert(cache_key, value.clone());
            self.counters.set_reconstruction_cache_bytes(bytes);
        }
        result
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
        if !self.reconstruct {
            return Ok(Some(PiLsmKeyValue {
                key: kv.key,
                value: kv.value,
            }));
        }

        let envelope = ValueEnvelope::decode(&kv.value, &self.decode_limits)?;
        let value = match envelope {
            ValueEnvelope::Raw(bytes) => bytes,
            ValueEnvelope::Plan(plan) => {
                if plan.logical_len > self.max_reconstruct_bytes {
                    return Err(PiLsmError::DecodeLimitExceeded("max_reconstruct_bytes").into());
                }
                let cache_key = reconstruction_cache_key(&plan);
                if let Some(value) = self.reconstruction_cache.get(cache_key) {
                    return Ok(Some(PiLsmKeyValue { key: kv.key, value }));
                }
                let start = Instant::now();
                let result = self
                    .reconstructor
                    .reconstruct_with_limit(&plan, self.max_reconstruct_bytes)
                    .await;
                self.counters.add_reconstruction_duration(start.elapsed());
                let value = result?;
                let bytes = self.reconstruction_cache.insert(cache_key, value.clone());
                self.counters.set_reconstruction_cache_bytes(bytes);
                value
            }
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

impl PiLsmMetrics {
    fn new() -> Self {
        Self {
            philosophical_compression_ratio: PhilosophicalCompressionRatio::Undefined,
            ..Self::default()
        }
    }

    fn observe(&mut self, explain: &ExplainValue) {
        self.logical_bytes_total += explain.logical_user_bytes;
        self.physical_value_bytes_total += explain.physical_value_bytes;

        match explain.storage_class {
            StorageClass::Raw => {
                self.raw_values_total += 1;
                self.raw_envelope_bytes_total += explain.physical_value_bytes;
                self.philosophical_user_value_bytes_stored_total += explain.logical_user_bytes;
            }
            StorageClass::Plan => {
                self.planned_values_total += 1;
                self.planned_logical_bytes_total += explain.logical_user_bytes;
                self.plan_envelope_bytes_total += explain.physical_value_bytes;
                self.plan_metadata_bytes_total += explain.plan_metadata_bytes;
                self.located_user_bytes_total += explain
                    .logical_user_bytes
                    .saturating_sub(explain.literal_bytes);
                self.literal_user_bytes_total += explain.literal_bytes;
                self.philosophical_user_value_bytes_stored_total +=
                    explain.philosophical_user_value_bytes_stored;
                self.chunks_total += explain.chunks;
                self.longest_natural_run_bytes = self
                    .longest_natural_run_bytes
                    .max(explain.longest_natural_run);

                if explain.purity == Purity::Contaminated {
                    self.philosophical_purity_violations_total += 1;
                }
            }
        }
    }

    fn observe_counters(&mut self, counters: &OperationCounters) {
        self.raw_bytes_converted_total = counters.load(&counters.raw_bytes_converted_total);
        self.planner_seconds = nanos_to_seconds(counters.load(&counters.planner_nanos));
        self.reconstruction_seconds =
            nanos_to_seconds(counters.load(&counters.reconstruction_nanos));
        self.compaction_filter_errors_total =
            counters.load(&counters.compaction_filter_errors_total);
        self.snapshot_protected_entries_total =
            counters.load(&counters.snapshot_protected_entries_total);
        self.vacuum_meaning_attempts_total = counters.load(&counters.vacuum_meaning_attempts_total);
        self.vacuum_meaning_improvements_total =
            counters.load(&counters.vacuum_meaning_improvements_total);
        self.reconstruction_cache_bytes = counters.load(&counters.reconstruction_cache_bytes);
        if self.reconstruction_cache_bytes > 0 {
            self.philosophical_purity_violations_total += 1;
        }
        self.representation_entropy_excuses_total =
            counters.load(&counters.representation_entropy_excuses_total);
    }

    fn finish(&mut self) {
        self.chunks_per_value = if self.planned_values_total == 0 {
            None
        } else {
            Some(self.chunks_total as f64 / self.planned_values_total as f64)
        };
        self.avg_chunk_len_bytes = if self.chunks_total == 0 {
            None
        } else {
            Some(self.planned_logical_bytes_total as f64 / self.chunks_total as f64)
        };
        self.metadata_amplification_ratio = if self.planned_logical_bytes_total == 0 {
            None
        } else {
            Some(self.plan_envelope_bytes_total as f64 / self.planned_logical_bytes_total as f64)
        };
        self.philosophical_compression_ratio = if self.logical_bytes_total == 0 {
            PhilosophicalCompressionRatio::Undefined
        } else if self.philosophical_user_value_bytes_stored_total == 0 {
            PhilosophicalCompressionRatio::Infinite
        } else {
            PhilosophicalCompressionRatio::Finite(
                self.logical_bytes_total as f64
                    / self.philosophical_user_value_bytes_stored_total as f64,
            )
        };
    }
}

fn nanos_to_seconds(nanos: u64) -> f64 {
    nanos as f64 / 1_000_000_000.0
}

fn reconstruction_cache_key(plan: &pilsmer_core::ReconstructionPlan) -> ReconstructionCacheKey {
    ReconstructionCacheKey {
        hash_kind: logical_hash_kind_tag(plan.logical_hash.kind),
        hash: plan.logical_hash.bytes,
    }
}

fn logical_hash_kind_tag(kind: LogicalHashKind) -> u8 {
    match kind {
        LogicalHashKind::Blake3_128 => 1,
        LogicalHashKind::Sha256_128 => 2,
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
        demo_db_with_reconstruct_options(prefix, 64 * 1024 * 1024, 0).await
    }

    async fn demo_db_with_reconstruct_limit(
        prefix: &'static [u8],
        max_reconstruct_bytes: u64,
    ) -> PiLsmDb {
        demo_db_with_reconstruct_options(prefix, max_reconstruct_bytes, 0).await
    }

    async fn demo_db_with_reconstruct_options(
        prefix: &'static [u8],
        max_reconstruct_bytes: u64,
        reconstruction_cache_bytes: u64,
    ) -> PiLsmDb {
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
        let mut opts = PiLsmOptions::new(registry, planner);
        opts.max_reconstruct_bytes = max_reconstruct_bytes;
        opts.reconstruction_cache_bytes = reconstruction_cache_bytes;
        PiLsmDb::open("pilsmer-test", Arc::new(InMemory::new()), opts)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn put_get_and_plan_key_roundtrip() {
        let db = demo_db(b"abcdef").await;
        let handle = db.put(b"k", b"abc").await.unwrap();
        assert_eq!(handle.storage_class, StorageClass::Raw);
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
    async fn deleted_keys_read_as_missing() {
        let db = demo_db(b"abcdef").await;
        db.put(b"k", b"abc").await.unwrap();
        db.plan_key(b"k", PlanOptions::default()).await.unwrap();

        db.delete(b"k").await.unwrap();

        assert_eq!(db.get(b"k").await.unwrap(), None);
        assert!(db.get_envelope(b"k").await.unwrap().is_none());
        assert!(db.explain(b"k").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn can_open_with_embedded_compactor_disabled() {
        let stream: Arc<dyn ByteStream> = Arc::new(PrefixByteStream::new(
            StreamId::PiHexFractionPrefixV1 {
                digest: [8_u8; 32],
                bytes: 6,
            },
            Bytes::from_static(b"abcdef"),
        ));
        let mut registry = StreamRegistry::new();
        registry.register(stream.clone());
        let index = Arc::new(
            StreamIndex::build(
                stream,
                StreamIndexOptions {
                    max_prefix_len: 6,
                    max_k: 3,
                    max_index_memory_bytes: 1024,
                    max_offsets_per_kgram: 4,
                },
            )
            .await
            .unwrap(),
        );
        let planner = Planner::new(vec![index], registry.clone(), PlanOptions::default());
        let mut opts = PiLsmOptions::new(registry, planner);
        opts.disable_embedded_compactor = true;

        let db = PiLsmDb::open(
            "pilsmer-no-embedded-compactor",
            Arc::new(InMemory::new()),
            opts,
        )
        .await
        .unwrap();
        db.put(b"k", b"abc").await.unwrap();
        assert_eq!(
            db.get(b"k").await.unwrap(),
            Some(Bytes::from_static(b"abc"))
        );
    }

    #[tokio::test]
    async fn put_options_can_create_immediate_plan_for_tests() {
        let db = demo_db(b"abcdef").await;
        let handle = db
            .put_with_options(
                b"k",
                b"abc",
                PutOptions {
                    await_durable: true,
                    allow_immediate_plan: true,
                },
            )
            .await
            .unwrap();
        assert_eq!(handle.storage_class, StorageClass::Plan);
        assert!(handle.physical_value_bytes > 0);
        assert!(matches!(
            db.get_envelope(b"k").await.unwrap().unwrap(),
            ValueEnvelope::Plan(_)
        ));
        assert_eq!(
            db.get(b"k").await.unwrap(),
            Some(Bytes::from_static(b"abc"))
        );
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
    async fn optional_reconstruction_cache_reports_bytes_and_purity_violation() {
        let db = demo_db_with_reconstruct_options(b"abcdef", 64 * 1024 * 1024, 16).await;
        db.put(b"a", b"abc").await.unwrap();
        db.plan_key(b"a", PlanOptions::default()).await.unwrap();

        assert_eq!(
            db.get(b"a").await.unwrap(),
            Some(Bytes::from_static(b"abc"))
        );

        let metrics = db.metrics().await.unwrap();
        assert_eq!(metrics.reconstruction_cache_bytes, 3);
        assert_eq!(metrics.philosophical_purity_violations_total, 1);
    }

    #[tokio::test]
    async fn reconstruction_limits_apply_to_get_and_scan() {
        let db = demo_db_with_reconstruct_limit(b"abcdef", 2).await;
        db.put(b"a", b"abc").await.unwrap();
        db.plan_key(b"a", PlanOptions::default()).await.unwrap();

        assert!(matches!(
            db.get(b"a").await,
            Err(PiLsmDbError::Core(PiLsmError::DecodeLimitExceeded(
                "max_reconstruct_bytes"
            )))
        ));

        let mut iter = db
            .scan::<Vec<u8>, _>(b"a".to_vec()..b"z".to_vec())
            .await
            .unwrap();
        assert!(matches!(
            iter.next().await,
            Err(PiLsmDbError::Core(PiLsmError::DecodeLimitExceeded(
                "max_reconstruct_bytes"
            )))
        ));

        assert!(matches!(
            db.vacuum_meaning(b"a", VacuumOptions::default()).await,
            Err(PiLsmDbError::Core(PiLsmError::DecodeLimitExceeded(
                "max_reconstruct_bytes"
            )))
        ));
        let report = db
            .vacuum_meaning(
                b"a",
                VacuumOptions {
                    plan_options: PlanOptions::default(),
                    max_reconstruct_bytes: Some(3),
                },
            )
            .await
            .unwrap();
        assert!(matches!(
            report.status,
            RewriteStatus::Rewritten | RewriteStatus::SkippedNotImproved
        ));

        let mut raw_iter = db
            .scan_with_options::<Vec<u8>, _>(
                b"a".to_vec()..b"z".to_vec(),
                ScanOptions {
                    reconstruct: false,
                    max_reconstruct_bytes: Some(2),
                },
            )
            .await
            .unwrap();
        let raw = raw_iter.next().await.unwrap().unwrap();
        assert!(matches!(
            ValueEnvelope::decode(&raw.value, &DecodeLimits::default()).unwrap(),
            ValueEnvelope::Plan(_)
        ));
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

    #[tokio::test]
    async fn metrics_snapshot_reports_raw_and_planned_values() {
        let db = demo_db(b"abcdef").await;
        db.put(b"raw", b"abc").await.unwrap();
        db.put(b"plan", b"def").await.unwrap();
        db.plan_key(b"plan", PlanOptions::default()).await.unwrap();
        db.get(b"plan").await.unwrap();
        db.vacuum_meaning(b"plan", PlanOptions::default())
            .await
            .unwrap();

        let metrics = db.metrics().await.unwrap();
        assert_eq!(metrics.raw_values_total, 1);
        assert_eq!(metrics.planned_values_total, 1);
        assert_eq!(metrics.logical_bytes_total, 6);
        assert_eq!(metrics.planned_logical_bytes_total, 3);
        assert_eq!(metrics.raw_bytes_converted_total, 3);
        assert_eq!(metrics.located_user_bytes_total, 3);
        assert_eq!(metrics.literal_user_bytes_total, 0);
        assert_eq!(metrics.philosophical_user_value_bytes_stored_total, 3);
        assert_eq!(metrics.stream_prefix_bytes_indexed, 6);
        assert!(metrics.planner_seconds >= 0.0);
        assert!(metrics.reconstruction_seconds >= 0.0);
        assert_eq!(metrics.vacuum_meaning_attempts_total, 1);
        assert_eq!(metrics.chunks_per_value, Some(metrics.chunks_total as f64));
        assert_eq!(
            metrics.avg_chunk_len_bytes,
            Some(3.0 / metrics.chunks_total as f64)
        );
        assert!(metrics.plan_metadata_bytes_total > 0);
        assert!(metrics.raw_envelope_bytes_total > 0);
        assert!(metrics.physical_value_bytes_total > metrics.raw_envelope_bytes_total);
        assert_eq!(metrics.philosophical_purity_violations_total, 0);
    }

    #[test]
    fn vacuum_score_accepts_only_weighted_improvements() {
        assert!(plan_improved(100, 10, 110, 1));
        assert!(!plan_improved(100, 1, 99, 2));
    }
}
