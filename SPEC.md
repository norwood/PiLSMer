# PiLSMer Specification

Status: draft
Target repo: `/Users/norwood/clients/PiLSMer`
Working title: PiLSMer
Alternate names: PiLSM, SlatePi, Compaction Into Nonexistence

## 1. Concept

PiLSMer is a deliberately bad key-value database built on SlateDB.

It starts by storing values normally. Then, during SlateDB compaction, values are rewritten into reconstruction plans that point into deterministic infinite byte streams such as pi, e, sqrt(2), or SHA256(counter). After compaction, the original bytes are no longer stored as values. They are reconstructed by following metadata.

The premise:

> Your data is not stored. It is merely located.

The serious engineering surface is real:

- SlateDB as the embedded object-storage-native LSM.
- A custom compaction filter that rewrites values.
- Optional custom compaction scheduler or standalone compactor mode.
- A read wrapper that transparently reconstructs compacted values.
- Metrics that expose how much worse the system becomes over time.

The joke is that compaction, normally a storage-engine optimization, makes data less directly stored and more expensive to read. The product claims to be a data-free database while increasing metadata amplification.

## 2. Non-Goals

PiLSMer is not intended to be useful storage.

It should not:

- fork SlateDB unless absolutely necessary
- mutate SlateDB SST format
- require a custom manifest format
- depend on internal SlateDB private modules
- make any claim of real compression
- hide the absurd performance costs
- implement a new object store
- implement a distributed database

The best version is a small, sharp project that abuses existing extension points.

## 3. One-Line Pitch

SlateDB for metadata. Pi for storage. Regret for everything else.

## 4. User-Facing Behavior

Example:

```rust
let db = PiLsmDb::open("pilsmer-demo", object_store, PiLsmOptions::demo()).await?;

db.put("invoice:123", br#"{ "total": 49.99, "status": "paid" }"#).await?;

let before = db.explain("invoice:123").await?;
assert_eq!(before.storage_class, StorageClass::Raw);

db.flush().await?;
db.compact_into_nonexistence().await?;

let after = db.explain("invoice:123").await?;
assert_eq!(after.storage_class, StorageClass::Plan);

let value = db.get("invoice:123").await?;
assert_eq!(value, br#"{ "total": 49.99, "status": "paid" }"#);
```

Possible `EXPLAIN` output:

```text
key: invoice:123
storage_class: Plan
source: pi.hex-fraction-prefix:v1
chunks: 23
longest_natural_run: 4 bytes
logical_user_bytes: 33
physical_value_bytes: 1008
plan_metadata_bytes: 1008
philosophical_user_value_bytes_stored: 0
philosophical_compression_ratio: infinity
metadata_amplification: 30.55x
read_strategy: deterministic_reconstruction
confidence: mathematically smug
```

## 5. Architecture

```text
                 ┌────────────────────┐
                 │      PiLsmDb       │
                 │  public wrapper    │
                 └─────────┬──────────┘
                           │
         ┌─────────────────┼─────────────────┐
         │                 │                 │
         ▼                 ▼                 ▼
 ┌──────────────┐  ┌────────────────┐  ┌──────────────────┐
 │ SlateDB Db   │  │ Stream Engine  │  │ Plan Codec       │
 │ raw KV store │  │ pi/e/hash/etc  │  │ Raw/Plan values  │
 └──────┬───────┘  └────────────────┘  └──────────────────┘
        │
        ▼
 ┌──────────────────────────┐
 │ PiLSM Compaction Filter  │
 │ Raw(bytes) -> Plan(...)  │
 └──────────────────────────┘
```

Core components:

- `PiLsmDb`: application-facing wrapper around `slatedb::Db`
- `ValueEnvelope`: tagged encoding for raw and compacted values
- `ReconstructionPlan`: metadata needed to rebuild bytes from deterministic streams
- `ByteStream`: deterministic random-access byte generator interface
- `StreamRegistry`: versioned stream identities and fingerprints
- `ChunkIndex`: precomputed k-gram lookup table for a stream prefix
- `Planner`: dynamic-programming planner that converts bytes into chunk refs
- `Reconstructor`: validates and rebuilds logical bytes from a plan
- `PlanCodec`: compact or ceremonial encoding for plan payloads
- `PiLsmCompactionFilter`: SlateDB compaction filter that rewrites raw values into plans
- `CompactionMode`: normal, disabled, forced, vacuum-meaning
- `Explain`: diagnostics for how ridiculous a value has become

## 6. SlateDB Integration

PiLSMer should use existing SlateDB extension points:

1. **Compaction filter**
   - Compile SlateDB with the `compaction_filters` feature.
   - Provide `CompactionFilterSupplier`.
   - Each compaction creates a fresh `PiLsmCompactionFilter`.
   - The filter rewrites values entry-by-entry.
   - Only transform `ValueDeletable::Value`.
   - Keep `ValueDeletable::Tombstone` and `ValueDeletable::Merge` unchanged.
   - In default snapshot-safe mode, keep entries whose sequence is protected by `CompactionJobContext.retention_min_seq`.
   - In `Normal`, planning failure is not an engine error. Keep the raw value and increment a metric.

2. **Standalone compactor**
   - The DB can run with embedded compaction enabled for simple demos.
   - For better showmanship, run the writer with `Settings { compactor_options: None, .. }`, then run a separate PiLSMer compactor process.
   - This lets the demo show raw values before compaction and absurd values after compaction.
   - Manual/admin compaction is the first deterministic demo lever.
   - If using a separate compactor builder, keep filter policies aligned with the writer configuration so compacted SSTs are not written with different Bloom/filter behavior.

3. **Custom scheduler, optional**
   - Do not build this for MVP.
   - Useful later only after the filter, CLI, standalone compactor, and demo are working.
   - A scheduler could prefer sorted runs containing mostly raw values, but SlateDB does not currently expose app-defined per-SST stats to the scheduler.

4. **Stat-driven compaction RFC, if it lands**
   - Periodic compaction would help make the transformation observable without fake workload pressure.
   - Age-based triggers are a cleaner demo lever than waiting for size-tiered compaction.
   - PiLSMer should still work without it.

## 7. Value Format

SlateDB stores one byte value per key. PiLSMer wraps user bytes in a typed envelope.

Suggested binary format:

```text
magic:        4 bytes  "PLSM"
version:      u8       1
kind:         u8       0 = Raw, 1 = Plan
flags:        u16
logical_hash_kind:
              u8       1 = BLAKE3-128, 2 = SHA256-128
payload_len:  varint
payload:      bytes
logical_hash: 16 bytes over reconstructed user bytes, using logical_hash_kind
frame_crc:    u32      crc32c over header + payload + logical_hash
```

Raw payload:

```text
raw_len: varint
raw_bytes: [u8]
```

Plan payload:

```text
logical_len:       varint
planner_version:   u16
plan_codec:        enum
stream_count:      varint
streams:
  stream_id
  stream_fingerprint
chunk_count:       varint
chunks:
  kind:      enum  Located or Literal
  stream_ix: varint
  offset:    varint
  len:       varint
  transform: enum
  literal:   bytes, only for contaminated plans
```

Stats are not part of the canonical plan. `explain` should derive chunk counts, longest run, byte totals, amplification, and philosophical metrics from the payload. That prevents compaction from rewriting values only to refresh display metadata.

Rust sketch:

```rust
enum ValueEnvelope {
    Raw(Bytes),
    Plan(ReconstructionPlan),
}

struct ReconstructionPlan {
    logical_len: u64,
    logical_hash: LogicalHash,
    planner_version: u16,
    streams: Vec<PlanStream>,
    chunks: Vec<ChunkRef>,
}

struct PlanStream {
    id: StreamId,
    fingerprint: StreamFingerprint,
}

enum ChunkRef {
    Located {
        stream_ix: u32,
        offset: u64,
        len: u32,
        transform: ChunkTransform,
    },
    Literal {
        bytes: Bytes,
    },
}

enum StreamId {
    Sha256CounterV1 { seed: [u8; 32] },
    PiHexFractionPrefixV1 { digest: [u8; 32], bytes: u64 },
    EHexFractionPrefixV1 { digest: [u8; 32], bytes: u64 },
    Sqrt2HexFractionPrefixV1 { digest: [u8; 32], bytes: u64 },
}

enum PlanCodec {
    CompactBinary,
    CeremonialCbor,
}

enum ChunkTransform {
    Identity,
    XorByte(u8),
    Reverse,
}
```

MVP rules:

- Use `CompactBinary` for correctness and benchmarks.
- Use `CeremonialCbor` only when the demo wants more visible metadata regret.
- Default to pure plans: every chunk must be `Located`.
- Allow `Literal` only behind an explicit development/test option.
- If a literal is used, `explain` must report `purity: contaminated`, `literal_bytes`, and `philosophical_compression_ratio: revoked`.
- Skip transforms for MVP. `XorByte` is funny, but it turns "found in pi" into "stored in transform metadata."

Decode limits:

```rust
struct DecodeLimits {
    max_logical_len: u64,
    max_chunk_count: u64,
    max_encoded_plan_len: u64,
    max_offset: u64,
    max_chunk_len: u32,
    max_streams_per_plan: u32,
}
```

Corrupt envelopes must not be able to allocate huge buffers, iterate billions of chunks, or overflow `offset + len`.

## 8. Stream Engine

The stream engine exposes random-access deterministic bytes.

```rust
trait ByteStream: Send + Sync {
    fn id(&self) -> StreamId;
    fn fingerprint(&self) -> StreamFingerprint;
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes>;
}

trait ChunkIndex: Send + Sync {
    fn stream_id(&self) -> StreamId;
    fn stream_fingerprint(&self) -> StreamFingerprint;
    fn prefix_len(&self) -> u64;
    fn max_k(&self) -> usize;
    fn find_candidates(&self, needle: &[u8]) -> SmallVec<[IndexedChunk; 4]>;
}

struct IndexedChunk {
    offset: u64,
    len: u16,
    encoded_cost: u16,
    read_cost_hint: u16,
}
```

Generation and search are separate. `ByteStream` is the deterministic truth. `ChunkIndex` is an optimization artifact that can be rebuilt, cached, memory-mapped, enlarged, or discarded. Plans depend on stream identity and fingerprint, not on a specific index file.

MVP streams:

1. `sha256-counter:v1`
   - Practical.
   - Deterministic.
   - Fast enough.
   - Lets us build the rest of the system without implementing pi digit extraction.
   - Correctness substrate for MVP.
   - Joke drawback: not pi.

2. `pi.hex-fraction-prefix:v1`
   - Use a precomputed checked-in or generated prefix for demo.
   - Define bytes precisely: take hexadecimal fractional digits of pi after `3.`, pair consecutive hex digits into bytes, and let offset 0 be the byte formed from the first two fractional hex digits.
   - Optional BBP-style random access later.
   - For the README, this is the star.

3. `e.hex-fraction-prefix:v1` and `sqrt2.hex-fraction-prefix:v1`
   - Optional later streams.
   - Same prefix-and-fingerprint model.

`sha256-counter:v1` exact definition:

```text
block_i = floor(offset / 32)
block   = SHA256("PiLSMer sha256-counter:v1" || seed || little_endian_u64(block_i))
stream  = block_0 || block_1 || block_2 || ...
read_at slices the concatenated stream.
```

Write fixed test vectors for at least offsets 0, 1, 31, 32, 33, and one multi-block read. Do not leave endian, counter width, or domain separator implicit.

## 9. Chunk Finding

The chunk finder converts raw bytes into a list of references.

Do not scan the stream per chunk. Build indexes over deterministic stream prefixes:

```text
index[1][stream[i..i+1]] -> candidate offsets
index[2][stream[i..i+2]] -> candidate offsets
...
index[K][stream[i..i+K]] -> candidate offsets
```

Index shape:

```rust
struct StreamIndex {
    stream_id: StreamId,
    stream_fingerprint: StreamFingerprint,
    prefix_len: u64,
    max_k: u8,
    memory_budget_bytes: u64,
    by_len: Vec<PackedKGramTable>,
}

struct PackedKGramTable {
    k: u8,
    entries: Mmap<[PackedKGramEntry]>,
    offsets: Mmap<[u64]>,
}

struct PackedKGramEntry {
    packed_key: u64,
    offsets_start: u32,
    offsets_len: u16,
}
```

Do not use `HashMap<Vec<u8>, Vec<u64>>` for the real index. It is fine for a tiny test fixture, but too memory-heavy for a 16 MiB prefix. MVP indexes should use packed k-gram keys sorted by `(packed_key, offset)`, with a bounded number of retained offsets per key. For `max_k <= 8`, the k-gram itself fits in `u64`; larger `k` needs a different index family and is not MVP.

Planning pipeline:

```text
Raw bytes
  -> candidate table per position
  -> dynamic-programming planner
  -> optional chunk coalescer
  -> plan encoder
  -> reconstruction self-check
  -> Envelope::Plan
```

Candidate table:

```rust
struct Candidate {
    stream_ix: u8,
    offset: u64,
    len: u16,
    encoded_cost: u16,
    read_cost_hint: u16,
}
```

For each position, collect all chunks from every enabled index up to `max_k`. Then run DP:

```rust
dp[n] = 0;
for i in (0..n).rev() {
    dp[i] = min(c in candidates[i]) {
        c.encoded_cost + dp[i + c.len]
    };
}
```

Optimize for encoded plan bytes first, chunk count second, reconstruction time third. Greedy longest-match is not optimal:

```text
target: ABCDEF
available: ABCD, ABC, DEF, E, F
greedy: ABCD + E + F = 3 chunks
better: ABC + DEF = 2 chunks
```

Configuration:

```rust
struct PlanOptions {
    max_prefix_len: u64,
    max_k: usize,
    max_index_memory_bytes: u64,
    max_offsets_per_kgram: u16,
    max_plan_millis_per_value: u64,
    allow_literals: bool,
    plan_codec: PlanCodec,
}
```

MVP defaults:

```text
max_prefix_len = 16 MiB
max_k = 3
max_index_memory_bytes = 256 MiB
max_offsets_per_kgram = 4
max_plan_millis_per_value = 500
allow_literals = false
plan_codec = CompactBinary
```

For a random-looking W-byte prefix, the rough probability of finding a fixed k-byte substring is:

```text
P(hit) ~= 1 - exp(-W / 256^k)
```

For W = 16 MiB:

```text
1 byte:  effectively certain
2 bytes: effectively certain
3 bytes: about 63%
4 bytes: about 0.39%
5 bytes: about 0.0015%
```

That means indexed plans over a 16 MiB SHA prefix will be dominated by 2- and 3-byte chunks, with occasional 4-byte wins if `max_k` permits it.

Demo planning should intentionally stage the embarrassment:

```text
COMPACT INTO NONEXISTENCE --humiliation=maximum
  max_k = 1
  plan_codec = CeremonialCbor
  chunks ~= logical_len

VACUUM MEANING --budget 30s
  max_k = 3 or 4
  plan_codec = CompactBinary or CeremonialCbor
  chunks drop substantially
```

## 10. Write Path

```text
PiLsmDb::put(k, user_bytes)
  -> encode ValueEnvelope::Raw(user_bytes)
  -> SlateDB put(k, encoded)
```

Writes should not immediately create plans. The whole joke depends on compaction being the thing that makes data "optimized."

Options:

```rust
struct PutOptions {
    await_durable: bool,
    allow_immediate_plan: bool, // default false
}
```

`allow_immediate_plan` exists for tests only.

## 11. Read Path

```text
PiLsmDb::get(k)
  -> SlateDB get(k)
  -> decode envelope
  -> Raw(bytes): return bytes
  -> Plan(plan): reconstruct and verify logical hash
```

Reads must be transparent by default.

Also expose raw inspection:

```rust
db.get_envelope(k) -> ValueEnvelope
db.explain(k) -> ExplainValue
```

Failure modes:

- Missing key: normal `None`
- Corrupt envelope: `PiLsmError::CorruptEnvelope`
- Plan cannot reconstruct: `PiLsmError::ReconstructionFailed`
- Logical hash mismatch: `PiLsmError::LogicalHashMismatch`

Range scans must reconstruct lazily. A scan over 1,000 planned blobs should not eagerly turn into a CPU furnace.

```rust
struct PiLsmIterator {
    inner: slatedb::DbIterator,
    reconstructor: Arc<Reconstructor>,
}

struct ScanOptions {
    reconstruct: bool, // default true
    max_reconstruct_bytes: Option<u64>,
}
```

Also expose raw inspection:

```rust
db.scan_envelopes(range) -> PiLsmEnvelopeIterator
db.scan_explain(range) -> PiLsmExplainIterator
```

Useful caches:

```text
Stream block cache:
  key = (stream_id, stream_fingerprint, block_number)
  value = bytes

Reconstruction cache:
  key = plan_hash or logical_hash
  value = logical bytes
```

The reconstruction cache is philosophically illegal. Keep it optional and report it with `pilsmer_philosophical_purity_violations_total`.

## 12. Compaction Filter

The compaction filter is the core.

Behavior:

```text
Raw(bytes) -> Plan(plan_indexed_chunks(bytes))
Plan(plan), normal mode -> Keep
Plan(plan), vacuum-meaning mode -> Plan(improve_plan(plan))
Tombstone or Merge -> Keep
unknown/corrupt -> Keep or error depending strictness
```

Rust sketch:

```rust
struct PiLsmCompactionFilter {
    stream_registry: Arc<StreamRegistry>,
    planner: Arc<Planner>,
    mode: CompactionMode,
    strict_envelopes: bool,
    snapshot_safe_filtering: bool,
    context: CompactionJobContext,
    stats: FilterStats,
}

#[async_trait]
impl CompactionFilter for PiLsmCompactionFilter {
    async fn filter(
        &mut self,
        entry: &RowEntry,
    ) -> Result<CompactionFilterDecision, CompactionFilterError> {
        if self.snapshot_safe_filtering && !self.snapshot_safe_to_modify(entry) {
            self.stats.snapshot_protected_entries += 1;
            return Ok(CompactionFilterDecision::Keep);
        }

        let bytes = match &entry.value {
            ValueDeletable::Value(bytes) => bytes,
            ValueDeletable::Merge(_) | ValueDeletable::Tombstone => {
                return Ok(CompactionFilterDecision::Keep);
            }
        };

        let envelope = match ValueEnvelope::decode(bytes.as_ref()) {
            Ok(envelope) => envelope,
            Err(err) if self.strict_envelopes => return Err(err.into()),
            Err(_) => return Ok(CompactionFilterDecision::Keep),
        };

        match (self.mode, envelope) {
            (CompactionMode::Disabled, _) => Ok(CompactionFilterDecision::Keep),
            (_, ValueEnvelope::Raw(bytes)) => self.plan_raw(bytes).await,
            (CompactionMode::VacuumMeaning, ValueEnvelope::Plan(plan)) => {
                self.improve_plan(plan).await
            }
            (_, ValueEnvelope::Plan(_)) => Ok(CompactionFilterDecision::Keep),
        }
    }

    async fn on_compaction_end(&mut self) -> Result<(), CompactionFilterError> {
        self.stats.emit();
        Ok(())
    }
}
```

Important rules:

- The filter must be idempotent. Repeated normal compactions should not keep rewriting plans forever.
- `Normal` should almost never return `Err`; failed planning keeps the raw value.
- `ForceRawToPlan` returns `Err` if pure planning fails.
- `VacuumMeaning` keeps the old plan unless the new plan is strictly better under the score function.
- Use `Err` only for forced planning failure, stream registry misconfiguration, internal invariant violations, or codec bugs.

Snapshot-safe predicate:

```rust
fn snapshot_safe_to_modify(ctx: &CompactionJobContext, entry: &RowEntry) -> bool {
    match ctx.retention_min_seq {
        Some(min_seq) => entry.seq < min_seq,
        None => true,
    }
}
```

Expose an explicit demo override:

```sh
pilsmer compact --into-nonexistence --ignore-snapshot-representation-safety
```

## 13. Compaction Modes

```rust
enum CompactionMode {
    Disabled,
    Normal,
    ForceRawToPlan,
    VacuumMeaning,
}
```

Definitions:

- `Disabled`: no rewriting; pass through all entries.
- `Normal`: raw values become plans when pure planning succeeds; plans are kept.
- `ForceRawToPlan`: same as normal, but errors if a raw value cannot be planned within budget.
- `VacuumMeaning`: attempts to improve existing plans with a larger index, higher `max_k`, more streams, or a different codec.

CLI:

```sh
pilsmer compact --mode normal
pilsmer compact --into-nonexistence --humiliation=maximum
pilsmer compact --mode vacuum-meaning
pilsmer compact --mode disabled
```

## 14. "VACUUM MEANING"

`VACUUM MEANING` is the user-facing operation that deliberately spends compute to reduce plan ugliness.

Input plan:

```text
23 one-byte chunks
```

Output plan:

```text
17 chunks, longest run 3 bytes
```

It is not useful. It is the joke’s second act.

Command:

```sh
pilsmer vacuum-meaning --key invoice:123
pilsmer vacuum-meaning --all --budget 10m
```

Improvement algorithm:

```text
existing Plan
  -> reconstruct logical bytes
  -> replan with larger budget / more streams / higher max_k / alternate codec
  -> reconstruct and verify logical hash
  -> accept only if score improves
```

Score:

```text
score = encoded_plan_len + 16 * chunk_count + estimated_read_cost
```

Accept iff `new_score < old_score`. This avoids "improvements" that lower chunk count but increase encoded bytes or read cost.

Expose the diff:

```text
chunks:                 81103 -> 32744
encoded_plan_bytes:   2457103 -> 391928
longest_run:                1 -> 4
meaning_reclaimed:      48.2%
```

Implementation options:

1. Write a new raw value? Bad. It bypasses compaction.
2. Use a compaction filter mode. Better.
3. Add an app-level rewrite path that reads and writes a better `Plan`. Acceptable for MVP, but less SlateDB-specific.

App-level rewrite concurrency contract:

- `plan_key` and app-level `vacuum_meaning` must not unconditionally write a plan computed from an earlier read.
- The wrapper should acquire a per-key rewrite lock, read the current envelope, compute a `source_envelope_hash`, plan outside the lock if needed, reacquire the lock, reread the envelope, and write only if the source envelope is unchanged.
- If the source changed, return `SkippedStaleSource` and leave the current value untouched.
- This only protects concurrent operations inside one `PiLsmDb` process. Multi-process or multi-writer correctness requires a real conditional write API. Without that, app-level rewrites are demo/single-writer only.
- The compaction filter remains the production-safe transformation path because it rewrites an existing version instead of creating a new latest write that can overwrite a concurrent `put`.

Preferred:

- MVP: guarded app-level rewrite for one key in demo/single-writer mode.
- Later: compaction-filter `VacuumMeaning` mode.

## 15. API

Rust API:

```rust
impl PiLsmDb {
    pub async fn open(
        path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
        opts: PiLsmOptions,
    ) -> Result<Self>;

    pub async fn put<K, V>(&self, key: K, value: V) -> Result<WriteHandle>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>;

    pub async fn get<K>(&self, key: K) -> Result<Option<Bytes>>
    where
        K: AsRef<[u8]>;

    pub async fn delete<K>(&self, key: K) -> Result<WriteHandle>
    where
        K: AsRef<[u8]>;

    pub async fn scan<K, R>(&self, range: R) -> Result<PiLsmIterator>
    where
        K: AsRef<[u8]>,
        R: RangeBounds<K>;

    pub async fn flush(&self) -> Result<()>;

    pub async fn explain<K>(&self, key: K) -> Result<Option<ExplainValue>>
    where
        K: AsRef<[u8]>;

    pub async fn plan_key<K>(&self, key: K, opts: PlanOptions) -> Result<PlanReport>
    where
        K: AsRef<[u8]>;

    pub async fn vacuum_meaning<K>(&self, key: K, opts: VacuumOptions) -> Result<VacuumReport>
    where
        K: AsRef<[u8]>;
}

pub struct PiLsmOptions {
    pub stream_registry: Arc<StreamRegistry>,
    pub planner: Arc<Planner>,
    pub compaction_mode: CompactionMode,
    pub strict_envelopes: bool,
    pub snapshot_safe_filtering: bool,
    pub max_reconstruct_bytes: u64,
}

pub enum RewriteStatus {
    Rewritten,
    KeptAlreadyPlanned,
    SkippedStaleSource,
    SkippedPlanningFailed,
}

pub struct RewritePrecondition {
    pub source_envelope_hash: [u8; 32],
}
```

CLI:

```sh
pilsmer init --path ./demo
pilsmer put ./demo invoice:123 ./invoice.json
pilsmer get ./demo invoice:123
pilsmer explain ./demo invoice:123
pilsmer compact ./demo --mode normal
pilsmer vacuum-meaning ./demo --all
pilsmer bench ./demo --values 1000 --size 1024
```

SQL-like demo shell:

```sql
PUT invoice:123 '{"total":49.99,"status":"paid"}';
GET invoice:123;
EXPLAIN GET invoice:123;
COMPACT INTO NONEXISTENCE;
EXPLAIN GET invoice:123;
VACUUM MEANING;
```

## 16. Metrics

Expose metrics that sound serious and incriminate the system.

```text
pilsmer_raw_values_total
pilsmer_planned_values_total
pilsmer_raw_bytes_converted_total
pilsmer_logical_bytes_total
pilsmer_raw_envelope_bytes_total
pilsmer_plan_envelope_bytes_total
pilsmer_plan_metadata_bytes_total
pilsmer_located_user_bytes_total
pilsmer_literal_user_bytes_total
pilsmer_physical_value_bytes_total
pilsmer_philosophical_user_value_bytes_stored_total
pilsmer_reconstruction_seconds
pilsmer_planner_seconds
pilsmer_chunks_per_value
pilsmer_avg_chunk_len_bytes
pilsmer_longest_natural_run_bytes
pilsmer_stream_prefix_bytes_indexed
pilsmer_metadata_amplification_ratio
pilsmer_philosophical_compression_ratio
pilsmer_compaction_filter_errors_total
pilsmer_snapshot_protected_entries_total
pilsmer_vacuum_meaning_attempts_total
pilsmer_vacuum_meaning_improvements_total
pilsmer_reconstruction_cache_bytes
pilsmer_philosophical_purity_violations_total
pilsmer_representation_entropy_excuses_total
```

Definitions:

```text
physical_value_bytes = encoded envelope length
philosophical_user_value_bytes_stored = literal bytes, normally 0
metadata_amplification_ratio = plan_envelope_bytes / logical_user_bytes
philosophical_compression_ratio = logical_user_bytes / philosophical_user_value_bytes_stored
```

For pure plans, `philosophical_user_value_bytes_stored = 0`, so the ratio is infinity. For zero-length values, print `NaN, but smug`. Do not hide infinity or undefined.

## 17. Demo Script

Demo target: 3 minutes.

1. Initialize a local object-store-backed SlateDB.
2. Insert a small JSON object and a small PNG.
3. Read them back normally.
4. Show `EXPLAIN`: values are raw.
5. Flush.
6. Run `COMPACT INTO NONEXISTENCE --humiliation=maximum`.
7. Show compactor logs converting values with `max_k = 1`.
8. Read values back successfully.
9. Show `EXPLAIN`: values are now reconstruction plans.
10. Show physical value bytes, philosophical user bytes stored, and metadata amplification.
11. Run `VACUUM MEANING --budget 30s`.
12. Show chunks and longest run improve under `max_k = 3` or `4`.
13. End with benchmark table proving normal storage is better.

Expected output:

```text
Original:             81,920 bytes
Physical value:     2,457,103 bytes
User bytes stored:          0 bytes
Plan metadata:      2,457,103 bytes
Chunks:               81,103
Longest run:               3 bytes
Write time:              12 ms
Compaction time:      94,321 ms
Read time:             1,844 ms
Compression:     legally ambiguous
```

## 18. Benchmark Suite

Benchmarks should be honest.

Compare:

- Plain SlateDB raw values
- PiLSMer raw envelope, before compaction
- PiLSMer compact binary plan
- PiLSMer ceremonial plan
- PiLSMer after `VACUUM MEANING`

Workloads:

- 1,000 tiny JSON values, 64-512 bytes
- 4 KiB JSON values
- 64 KiB random blobs
- 64 KiB repeated blobs
- one PNG, 50-500 KiB
- one UUID-heavy dataset, because it is hostile to meaning
- all-byte corpus

Metrics:

- put p50/p95
- flush time
- compaction wall time
- planner CPU time
- read latency p50/p95/p99
- plan bytes
- logical bytes
- object-store GET/PUT count
- chunks per value
- average chunk length
- longest natural run
- stream prefix bytes searched/indexed
- effective user-value bytes stored
- philosophical compression ratio
- reconstruction hash failures

Expected conclusion:

```text
SlateDB is better at storing data.
PiLSMer is better at avoiding accountability.
```

## 19. Safety and Correctness

Correctness target:

```text
get(k) after any successful put/compact/vacuum returns the original logical value
unless the key was deleted.
```

Invariants:

- `Raw(bytes).logical_bytes() == bytes`
- `decode(encode(envelope)) == envelope`
- `Plan(plan).reconstruct()` hashes to `plan.logical_hash`
- plan chunks cover exactly `logical_len` bytes
- plan chunk lengths are nonzero
- plan offsets plus lengths do not overflow `u64`
- every plan carries a logical hash stronger than CRC32C
- compaction filter must not transform tombstones
- compaction filter must not transform merge operands
- compaction filter must be idempotent in normal mode
- normal compaction keeps raw values when planning fails
- corrupt or unknown envelopes are kept unless `strict_envelopes` is enabled
- corrupt plans fail closed with a logical-hash error

Snapshot caveat:

SlateDB warns that compaction filters can affect snapshot consistency when they modify or drop entries. PiLSMer should avoid dropping entries. It modifies value representation while preserving logical value. In default snapshot-safe mode, keep rows protected by `retention_min_seq`; demo mode may override this only with an explicit flag. Raw SlateDB snapshots may still observe representation changes if bypassing `PiLsmDb`.

Compatibility caveat:

All reads and writes must go through the PiLSMer wrapper. Direct SlateDB reads will return envelopes, not user values.

Property tests should cover:

- empty values
- all 256 byte values
- random blobs up to several MiB
- highly repetitive blobs
- JSON-like values
- values that begin with `PLSM`
- truncated envelopes
- unknown versions
- oversized varints
- plans with invalid stream IDs
- plans with chunk-count mismatches
- logical-hash mismatches
- offset and length overflow cases

## 20. Error Policy

Default mode should favor preserving data over completing the joke.

If planning fails during compaction:

- `Normal`: keep raw value unchanged and increment error metric.
- `ForceRawToPlan`: abort compaction.
- `VacuumMeaning`: keep existing plan unchanged.

This avoids data loss from failed stream search.

Compaction should return an engine error only for:

- `ForceRawToPlan` planning failure
- stream registry misconfiguration
- internal invariant violation
- codec bug

## 21. Implementation Phases

Suggested repo layout:

```text
pilsmer-core/
  envelope.rs
  plan.rs
  stream.rs
  stream_index.rs
  planner.rs
  reconstruct.rs
  explain.rs

pilsmer-slate/
  db.rs
  compaction_filter.rs
  compactor.rs
  admin.rs

pilsmer-cli/
  main.rs
  commands/
    put.rs
    get.rs
    explain.rs
    compact.rs
    vacuum_meaning.rs
    bench.rs
```

`pilsmer-core` should not depend on SlateDB. That keeps most correctness tests away from object storage, compaction timing, and async engine behavior.

### Phase 0: Repo skeleton

- Rust workspace
- `pilsmer-core` library crate
- `pilsmer-slate` integration crate
- `pilsmer-cli` binary
- local object store demo
- basic CI

### Phase 1: Envelope and wrapper

- encode/decode `ValueEnvelope`
- raw `PiLsmDb::put/get/delete/scan`
- `explain`
- tests for roundtrip and corrupt envelope handling

### Phase 2: Stream engine

- `Sha256CounterStream`
- fixed SHA256-counter test vectors
- `ByteStream` and `ChunkIndex` split
- `read_at`
- tests with deterministic fixtures

### Phase 3: Indexed planner

- stream prefix index
- bounded packed k-gram tables, not heap-heavy `HashMap<Vec<u8>, Vec<u64>>`
- multiple offset candidates per k-gram
- candidate table generation
- DP planner
- raw bytes to reconstruction plan
- reconstruct plan to bytes
- logical hash validation
- explain stats
- benchmark baseline

### Phase 4: App-level rewrite and demo

- `plan_key`
- `vacuum_meaning`
- per-key in-process rewrite lock
- source-envelope hash recheck before app-level rewrite
- `SkippedStaleSource` handling
- CLI demo using app-level rewrites
- pure-plan and contaminated-literal development modes

### Phase 5: SlateDB compaction filter

- enable `compaction_filters`
- implement `PiLsmCompactionFilter`
- raw-to-plan transformation during compaction
- snapshot-safety behavior
- idempotence tests

### Phase 6: Standalone compactor demo

- writer with embedded compactor disabled
- admin/manual compaction path
- aligned filter-policy configuration
- standalone compactor demo

### Phase 7: Pi prefix stream

- `pi.hex-fraction-prefix:v1`
- prefix generation/checking
- stream fingerprint in plans
- README hero mode

### Phase 8: Benchmarks and polish

- `bench`
- compact vs ceremonial codec comparisons
- demo script
- README

### Phase 9: Optional scheduler work

- custom scheduler if current SlateDB API permits cleanly
- prefer compacting raw-heavy sorted runs if there is a way to observe that
- otherwise wait for stat-driven compaction triggers and use periodic compaction

## 22. Design Decisions

1. Default stream:
   - Use `sha256-counter:v1` for MVP correctness.
   - Use `pi.hex-fraction-prefix:v1` as the hero stream.

2. Default planner:
   - Use indexed DP over a deterministic prefix.
   - Default to `max_k = 3` and compact binary.
   - Use `max_k = 1` and ceremonial encoding for maximum demo humiliation.

3. Plan metrics:
   - Always print physical bytes and philosophical bytes separately.
   - `physical_value_bytes` is the encoded envelope length.
   - `philosophical_user_value_bytes_stored` is literal user bytes only, normally 0.

4. Compaction failure behavior:
   - `Normal` may leave raw values when planning fails.
   - `ForceRawToPlan` and maximum-humiliation demo mode should fail loudly instead.

5. Transforms:
   - It makes finding chunks easier and the joke weaker.
   - Leave out for MVP.

6. App-level rewrite safety:
   - Guard rewrites with a source-envelope hash and per-key in-process lock.
   - Treat app-level rewrite as demo/single-writer unless a real conditional write API exists.

## 23. Open Questions

1. How large can a value be before the demo becomes painful?
   - Keep demo values under 100 KiB unless using precomputed indexes.

2. Can stat-driven compaction make PiLSMer better?
   - Yes. Periodic triggers make transformation predictable.
   - App-defined stats would help more, but the open RFC is storage-engine stats only.

3. Should later index versions use suffix arrays, FM-indexes, or another compact corpus-search structure for larger `max_k`?
   - Packed k-gram tables are the MVP answer.
   - Larger `k` probably needs a different index family.

## 24. README Copy

```text
# PiLSMer

PiLSMer is a SlateDB-backed, data-free key-value store.

It writes your data normally, then uses compaction to replace your values with
instructions for finding equivalent byte sequences inside deterministic infinite
streams. Reads still work. Everything else gets worse.

## Why?

Because exact lookup is too honest.

## What is stored?

Only metadata.

And logical hashes.

And chunk offsets.

And stream identifiers.

And enough regret to reconstruct the original value.
```

## 25. Best Commands

```sh
pilsmer compact --into-nonexistence
pilsmer vacuum-meaning
pilsmer explain --philosophical
pilsmer bench --against-common-sense
pilsmer migrate --constant pi --to e
```

## 26. Final Shape

The project should look like a real storage-engine experiment until the reader notices the metrics.

Keep the implementation competent. The idea is dumb enough on its own.
