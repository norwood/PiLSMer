use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use pilsmer_core::{
    explain_envelope, DecodeLimits, PiLsmError, Planner, Reconstructor, StorageClass, ValueEnvelope,
};
use slatedb::{
    CompactionFilter, CompactionFilterDecision, CompactionFilterError, CompactionFilterSupplier,
    CompactionJobContext, RowEntry, ValueDeletable,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompactionMode {
    Disabled,
    Normal,
    ForceRawToPlan,
    VacuumMeaning,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PiLsmCompactionFilterStats {
    pub raw_values_converted: u64,
    pub raw_bytes_converted: u64,
    pub plans_improved: u64,
    pub plans_kept: u64,
    pub raw_values_kept_after_planning_failure: u64,
    pub corrupt_or_unknown_kept: u64,
    pub snapshot_protected_entries: u64,
    pub tombstones_or_merges_kept: u64,
    pub errors: u64,
}

#[derive(Default)]
struct SharedCompactionFilterStats {
    raw_values_converted: AtomicU64,
    raw_bytes_converted: AtomicU64,
    plans_improved: AtomicU64,
    plans_kept: AtomicU64,
    raw_values_kept_after_planning_failure: AtomicU64,
    corrupt_or_unknown_kept: AtomicU64,
    snapshot_protected_entries: AtomicU64,
    tombstones_or_merges_kept: AtomicU64,
    errors: AtomicU64,
}

impl SharedCompactionFilterStats {
    fn snapshot(&self) -> PiLsmCompactionFilterStats {
        PiLsmCompactionFilterStats {
            raw_values_converted: self.load(&self.raw_values_converted),
            raw_bytes_converted: self.load(&self.raw_bytes_converted),
            plans_improved: self.load(&self.plans_improved),
            plans_kept: self.load(&self.plans_kept),
            raw_values_kept_after_planning_failure: self
                .load(&self.raw_values_kept_after_planning_failure),
            corrupt_or_unknown_kept: self.load(&self.corrupt_or_unknown_kept),
            snapshot_protected_entries: self.load(&self.snapshot_protected_entries),
            tombstones_or_merges_kept: self.load(&self.tombstones_or_merges_kept),
            errors: self.load(&self.errors),
        }
    }

    fn increment(&self, counter: &AtomicU64) {
        self.add(counter, 1);
    }

    fn add(&self, counter: &AtomicU64, value: u64) {
        let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(current.saturating_add(value))
        });
    }

    fn load(&self, counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }
}

#[derive(Clone)]
pub struct PiLsmCompactionFilterSupplier {
    planner: Planner,
    reconstructor: Reconstructor,
    decode_limits: DecodeLimits,
    mode: CompactionMode,
    strict_envelopes: bool,
    snapshot_safe_filtering: bool,
    stats: Arc<SharedCompactionFilterStats>,
}

impl PiLsmCompactionFilterSupplier {
    pub fn new(planner: Planner, reconstructor: Reconstructor) -> Self {
        Self {
            planner,
            reconstructor,
            decode_limits: DecodeLimits::default(),
            mode: CompactionMode::Normal,
            strict_envelopes: false,
            snapshot_safe_filtering: true,
            stats: Arc::new(SharedCompactionFilterStats::default()),
        }
    }

    pub fn with_decode_limits(mut self, decode_limits: DecodeLimits) -> Self {
        self.decode_limits = decode_limits;
        self
    }

    pub fn with_mode(mut self, mode: CompactionMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn with_strict_envelopes(mut self, strict_envelopes: bool) -> Self {
        self.strict_envelopes = strict_envelopes;
        self
    }

    pub fn with_snapshot_safe_filtering(mut self, snapshot_safe_filtering: bool) -> Self {
        self.snapshot_safe_filtering = snapshot_safe_filtering;
        self
    }

    pub fn stats(&self) -> PiLsmCompactionFilterStats {
        self.stats.snapshot()
    }
}

#[async_trait::async_trait]
impl CompactionFilterSupplier for PiLsmCompactionFilterSupplier {
    async fn create_compaction_filter(
        &self,
        context: &CompactionJobContext,
    ) -> Result<Box<dyn CompactionFilter>, CompactionFilterError> {
        Ok(Box::new(PiLsmCompactionFilter {
            planner: self.planner.clone(),
            reconstructor: self.reconstructor.clone(),
            decode_limits: self.decode_limits.clone(),
            mode: self.mode,
            strict_envelopes: self.strict_envelopes,
            snapshot_safe_filtering: self.snapshot_safe_filtering,
            context: context.clone(),
            stats: PiLsmCompactionFilterStats::default(),
            shared_stats: self.stats.clone(),
        }))
    }
}

struct PiLsmCompactionFilter {
    planner: Planner,
    reconstructor: Reconstructor,
    decode_limits: DecodeLimits,
    mode: CompactionMode,
    strict_envelopes: bool,
    snapshot_safe_filtering: bool,
    context: CompactionJobContext,
    stats: PiLsmCompactionFilterStats,
    shared_stats: Arc<SharedCompactionFilterStats>,
}

#[async_trait::async_trait]
impl CompactionFilter for PiLsmCompactionFilter {
    async fn filter(
        &mut self,
        entry: &RowEntry,
    ) -> Result<CompactionFilterDecision, CompactionFilterError> {
        match self.filter_entry(entry).await {
            Ok(decision) => Ok(decision),
            Err(err) => {
                self.stats.errors += 1;
                self.shared_stats.increment(&self.shared_stats.errors);
                Err(filter_error(err))
            }
        }
    }

    async fn on_compaction_end(&mut self) -> Result<(), CompactionFilterError> {
        Ok(())
    }
}

impl PiLsmCompactionFilter {
    async fn filter_entry(
        &mut self,
        entry: &RowEntry,
    ) -> Result<CompactionFilterDecision, PiLsmError> {
        if self.snapshot_safe_filtering && !self.snapshot_safe_to_modify(entry) {
            self.stats.snapshot_protected_entries += 1;
            self.shared_stats
                .increment(&self.shared_stats.snapshot_protected_entries);
            return Ok(CompactionFilterDecision::Keep);
        }

        let bytes = match &entry.value {
            ValueDeletable::Value(bytes) => bytes,
            ValueDeletable::Merge(_) | ValueDeletable::Tombstone => {
                self.stats.tombstones_or_merges_kept += 1;
                self.shared_stats
                    .increment(&self.shared_stats.tombstones_or_merges_kept);
                return Ok(CompactionFilterDecision::Keep);
            }
        };

        let envelope = match ValueEnvelope::decode(bytes, &self.decode_limits) {
            Ok(envelope) => envelope,
            Err(err) if self.strict_envelopes => return Err(err),
            Err(_) => {
                self.stats.corrupt_or_unknown_kept += 1;
                self.shared_stats
                    .increment(&self.shared_stats.corrupt_or_unknown_kept);
                return Ok(CompactionFilterDecision::Keep);
            }
        };

        match (self.mode, envelope) {
            (CompactionMode::Disabled, _) => Ok(CompactionFilterDecision::Keep),
            (_, ValueEnvelope::Raw(raw)) => self.plan_raw(raw).await,
            (CompactionMode::VacuumMeaning, ValueEnvelope::Plan(plan)) => {
                self.improve_plan(plan).await
            }
            (_, ValueEnvelope::Plan(_)) => {
                self.stats.plans_kept += 1;
                self.shared_stats.increment(&self.shared_stats.plans_kept);
                Ok(CompactionFilterDecision::Keep)
            }
        }
    }

    async fn plan_raw(&mut self, raw: Bytes) -> Result<CompactionFilterDecision, PiLsmError> {
        let plan = match self.planner.plan(&raw).await {
            Ok(plan) => plan,
            Err(PiLsmError::PlanningFailed(_)) if self.mode == CompactionMode::Normal => {
                self.stats.raw_values_kept_after_planning_failure += 1;
                self.shared_stats
                    .increment(&self.shared_stats.raw_values_kept_after_planning_failure);
                return Ok(CompactionFilterDecision::Keep);
            }
            Err(err) => return Err(err),
        };

        self.stats.raw_values_converted += 1;
        self.stats.raw_bytes_converted += raw.len() as u64;
        self.shared_stats
            .increment(&self.shared_stats.raw_values_converted);
        self.shared_stats
            .add(&self.shared_stats.raw_bytes_converted, raw.len() as u64);
        Ok(CompactionFilterDecision::Modify(ValueDeletable::Value(
            ValueEnvelope::Plan(plan).encode().into(),
        )))
    }

    async fn improve_plan(
        &mut self,
        old_plan: pilsmer_core::ReconstructionPlan,
    ) -> Result<CompactionFilterDecision, PiLsmError> {
        let old_envelope = ValueEnvelope::Plan(old_plan.clone()).encode();
        let logical = self.reconstructor.reconstruct(&old_plan).await?;
        let new_plan = match self.planner.plan(&logical).await {
            Ok(plan) => plan,
            Err(PiLsmError::PlanningFailed(_)) => {
                self.stats.plans_kept += 1;
                self.shared_stats.increment(&self.shared_stats.plans_kept);
                return Ok(CompactionFilterDecision::Keep);
            }
            Err(err) => return Err(err),
        };
        let new_envelope = ValueEnvelope::Plan(new_plan).encode();

        if plan_score(&new_envelope) < plan_score(&old_envelope) {
            self.stats.plans_improved += 1;
            self.shared_stats
                .increment(&self.shared_stats.plans_improved);
            Ok(CompactionFilterDecision::Modify(ValueDeletable::Value(
                new_envelope.into(),
            )))
        } else {
            self.stats.plans_kept += 1;
            self.shared_stats.increment(&self.shared_stats.plans_kept);
            Ok(CompactionFilterDecision::Keep)
        }
    }

    fn snapshot_safe_to_modify(&self, entry: &RowEntry) -> bool {
        match self.context.retention_min_seq {
            Some(min_seq) => entry.seq < min_seq,
            None => true,
        }
    }
}

fn plan_score(encoded_envelope: &[u8]) -> (u64, u64) {
    match ValueEnvelope::decode(encoded_envelope, &DecodeLimits::default()) {
        Ok(envelope) => {
            let explain = explain_envelope(&envelope, encoded_envelope.len());
            match explain.storage_class {
                StorageClass::Plan => (encoded_envelope.len() as u64, explain.chunks),
                StorageClass::Raw => (u64::MAX, u64::MAX),
            }
        }
        Err(_) => (u64::MAX, u64::MAX),
    }
}

fn filter_error(err: PiLsmError) -> CompactionFilterError {
    CompactionFilterError::FilterError(Box::new(err))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use pilsmer_core::{
        ByteStream, PlanOptions, PrefixByteStream, StreamId, StreamIndex, StreamIndexOptions,
        StreamRegistry,
    };

    use super::*;

    async fn filter_for(
        prefix: &'static [u8],
        mode: CompactionMode,
        snapshot_safe_filtering: bool,
        allow_literals: bool,
    ) -> PiLsmCompactionFilter {
        let stream: Arc<dyn ByteStream> = Arc::new(PrefixByteStream::new(
            StreamId::PiHexFractionPrefixV1 {
                digest: [5_u8; 32],
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
        let plan_options = PlanOptions {
            max_k: 4,
            allow_literals,
            ..PlanOptions::default()
        };
        let planner = Planner::new(vec![index], registry.clone(), plan_options.clone());
        let reconstructor = Reconstructor::new(registry);
        let context = CompactionJobContext {
            destination: 1,
            is_dest_last_run: false,
            compaction_clock_tick: 0,
            retention_min_seq: None,
        };
        PiLsmCompactionFilter {
            planner,
            reconstructor,
            decode_limits: DecodeLimits::default(),
            mode,
            strict_envelopes: false,
            snapshot_safe_filtering,
            context,
            stats: PiLsmCompactionFilterStats::default(),
            shared_stats: Arc::new(SharedCompactionFilterStats::default()),
        }
    }

    #[tokio::test]
    async fn normal_filter_rewrites_raw_values_to_plans() {
        let mut filter = filter_for(b"abcdef", CompactionMode::Normal, true, true).await;
        let raw = ValueEnvelope::Raw(Bytes::from_static(b"abc")).encode();
        let entry = RowEntry {
            key: Bytes::from_static(b"k"),
            value: ValueDeletable::Value(raw.into()),
            seq: 1,
            create_ts: None,
            expire_ts: None,
        };

        let decision = filter.filter(&entry).await.unwrap();
        let CompactionFilterDecision::Modify(ValueDeletable::Value(bytes)) = decision else {
            panic!("expected modified value");
        };
        assert_eq!(filter.stats.raw_values_converted, 1);
        assert_eq!(filter.stats.raw_bytes_converted, 3);
        assert!(matches!(
            ValueEnvelope::decode(&bytes, &DecodeLimits::default()).unwrap(),
            ValueEnvelope::Plan(_)
        ));
    }

    #[tokio::test]
    async fn normal_filter_is_idempotent_for_plans() {
        let mut filter = filter_for(b"abcdef", CompactionMode::Normal, true, true).await;
        let plan = filter
            .planner
            .plan(b"abc")
            .await
            .expect("test plan should be possible");
        let entry = RowEntry {
            key: Bytes::from_static(b"k"),
            value: ValueDeletable::Value(ValueEnvelope::Plan(plan).encode().into()),
            seq: 1,
            create_ts: None,
            expire_ts: None,
        };

        let decision = filter.filter(&entry).await.unwrap();
        assert_eq!(decision, CompactionFilterDecision::Keep);
        assert_eq!(filter.stats.plans_kept, 1);
    }

    #[tokio::test]
    async fn normal_filter_keeps_raw_on_planning_failure() {
        let mut filter = filter_for(b"abc", CompactionMode::Normal, true, false).await;
        let raw = ValueEnvelope::Raw(Bytes::from_static(b"z")).encode();
        let entry = RowEntry {
            key: Bytes::from_static(b"k"),
            value: ValueDeletable::Value(raw.into()),
            seq: 1,
            create_ts: None,
            expire_ts: None,
        };

        let decision = filter.filter(&entry).await.unwrap();
        assert_eq!(decision, CompactionFilterDecision::Keep);
        assert_eq!(filter.stats.raw_values_kept_after_planning_failure, 1);
    }

    #[tokio::test]
    async fn force_mode_errors_on_planning_failure() {
        let mut filter = filter_for(b"abc", CompactionMode::ForceRawToPlan, true, false).await;
        let raw = ValueEnvelope::Raw(Bytes::from_static(b"z")).encode();
        let entry = RowEntry {
            key: Bytes::from_static(b"k"),
            value: ValueDeletable::Value(raw.into()),
            seq: 1,
            create_ts: None,
            expire_ts: None,
        };

        assert!(filter.filter(&entry).await.is_err());
    }

    #[tokio::test]
    async fn filter_keeps_tombstones_and_merges() {
        let mut filter = filter_for(b"abcdef", CompactionMode::ForceRawToPlan, true, true).await;
        let tombstone = RowEntry {
            key: Bytes::from_static(b"k1"),
            value: ValueDeletable::Tombstone,
            seq: 1,
            create_ts: None,
            expire_ts: None,
        };
        let merge = RowEntry {
            key: Bytes::from_static(b"k2"),
            value: ValueDeletable::Merge(Bytes::from_static(b"merge operand")),
            seq: 1,
            create_ts: None,
            expire_ts: None,
        };

        assert_eq!(
            filter.filter(&tombstone).await.unwrap(),
            CompactionFilterDecision::Keep
        );
        assert_eq!(
            filter.filter(&merge).await.unwrap(),
            CompactionFilterDecision::Keep
        );
        assert_eq!(filter.stats.tombstones_or_merges_kept, 2);
    }

    #[tokio::test]
    async fn corrupt_envelopes_are_kept_unless_strict() {
        let mut filter = filter_for(b"abcdef", CompactionMode::Normal, true, true).await;
        let entry = RowEntry {
            key: Bytes::from_static(b"k"),
            value: ValueDeletable::Value(Bytes::from_static(b"not a PLSM envelope")),
            seq: 1,
            create_ts: None,
            expire_ts: None,
        };

        assert_eq!(
            filter.filter(&entry).await.unwrap(),
            CompactionFilterDecision::Keep
        );
        assert_eq!(filter.stats.corrupt_or_unknown_kept, 1);

        filter.strict_envelopes = true;
        assert!(filter.filter(&entry).await.is_err());
        assert_eq!(filter.stats.errors, 1);
    }

    #[tokio::test]
    async fn snapshot_protected_entries_are_kept() {
        let mut filter = filter_for(b"abcdef", CompactionMode::Normal, true, true).await;
        filter.context.retention_min_seq = Some(10);
        let raw = ValueEnvelope::Raw(Bytes::from_static(b"abc")).encode();
        let entry = RowEntry {
            key: Bytes::from_static(b"k"),
            value: ValueDeletable::Value(raw.into()),
            seq: 10,
            create_ts: None,
            expire_ts: None,
        };

        let decision = filter.filter(&entry).await.unwrap();
        assert_eq!(decision, CompactionFilterDecision::Keep);
        assert_eq!(filter.stats.snapshot_protected_entries, 1);
    }

    #[tokio::test]
    async fn supplier_accumulates_filter_stats() {
        let mut filter = filter_for(b"abcdef", CompactionMode::Normal, true, true).await;
        let shared_stats = filter.shared_stats.clone();
        let supplier = PiLsmCompactionFilterSupplier::new(
            filter.planner.clone(),
            filter.reconstructor.clone(),
        );
        let supplier = PiLsmCompactionFilterSupplier {
            stats: shared_stats,
            ..supplier
        };
        let raw = ValueEnvelope::Raw(Bytes::from_static(b"abc")).encode();
        let entry = RowEntry {
            key: Bytes::from_static(b"k"),
            value: ValueDeletable::Value(raw.into()),
            seq: 1,
            create_ts: None,
            expire_ts: None,
        };

        filter.filter(&entry).await.unwrap();

        let stats = supplier.stats();
        assert_eq!(stats.raw_values_converted, 1);
        assert_eq!(stats.raw_bytes_converted, 3);
    }
}
