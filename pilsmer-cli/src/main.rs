use std::io::{Read, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use clap::{Parser, Subcommand, ValueEnum};
use futures::stream::BoxStream;
use pilsmer_core::{
    pi_hex_fraction_prefix_stream, ByteStream, PhilosophicalCompressionRatio, PlanCodec,
    PlanOptions, Planner, Reconstructor, Sha256CounterStream, StreamIndex, StreamIndexOptions,
    StreamRegistry, PI_HEX_FRACTION_PREFIX_BYTES,
};
use pilsmer_slate::{
    run_compactor_with_options, CompactionMode, PiLsmCompactionFilterStats,
    PiLsmCompactionFilterSupplier, PiLsmCompactorOptions, PiLsmDb, PiLsmMetrics, PiLsmOptions,
    RewriteStatus,
};
use slatedb::object_store::local::LocalFileSystem;
use slatedb::object_store::path::Path as ObjectStorePath;
use slatedb::object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, UploadPart,
};
use slatedb::Db;

#[derive(Parser, Debug)]
#[command(name = "pilsmer")]
#[command(about = "A SlateDB-backed key-value store that locates your data elsewhere.")]
struct Cli {
    #[arg(long, value_enum, default_value_t = StreamKind::Sha256Counter)]
    stream: StreamKind,
    #[arg(long)]
    prefix_bytes: Option<u64>,
    #[arg(long, default_value_t = 3)]
    max_k: usize,
    #[arg(long, value_enum, default_value_t = CliPlanCodec::CompactBinary)]
    plan_codec: CliPlanCodec,
    #[arg(long)]
    allow_literals: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Init {
        path: PathBuf,
    },
    Put {
        path: PathBuf,
        key: String,
        file: PathBuf,
    },
    Get {
        path: PathBuf,
        key: String,
    },
    Delete {
        path: PathBuf,
        key: String,
    },
    Explain {
        path: PathBuf,
        key: String,
        #[arg(long)]
        philosophical: bool,
    },
    PlanKey {
        path: PathBuf,
        key: String,
    },
    VacuumMeaning {
        path: PathBuf,
        key: Option<String>,
        #[arg(long)]
        all: bool,
        #[arg(long, value_parser = parse_duration)]
        budget: Option<Duration>,
    },
    Metrics {
        path: PathBuf,
    },
    Compact {
        path: PathBuf,
        #[arg(long, default_value_t = 1000)]
        run_ms: u64,
        #[arg(long, default_value_t = 1)]
        runs: usize,
        #[arg(long, default_value_t = 50)]
        poll_ms: u64,
        #[arg(long, default_value_t = 4)]
        min_compaction_sources: usize,
        #[arg(long, value_enum)]
        mode: Option<CliCompactionMode>,
        #[arg(long)]
        into_nonexistence: bool,
        #[arg(long, value_enum)]
        humiliation: Option<Humiliation>,
        #[arg(long)]
        strict_envelopes: bool,
        #[arg(long)]
        ignore_snapshot_representation_safety: bool,
    },
    Bench {
        path: PathBuf,
        #[arg(long, value_enum, default_value_t = BenchWorkload::Sha256Stream)]
        workload: BenchWorkload,
        #[arg(long)]
        suite: bool,
        #[arg(long)]
        against_common_sense: bool,
        #[arg(long, default_value_t = 100)]
        values: usize,
        #[arg(long, default_value_t = 1024)]
        size: usize,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum StreamKind {
    Sha256Counter,
    PiPrefix,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum CliPlanCodec {
    CompactBinary,
    CeremonialCbor,
}

impl From<CliPlanCodec> for PlanCodec {
    fn from(value: CliPlanCodec) -> Self {
        match value {
            CliPlanCodec::CompactBinary => PlanCodec::CompactBinary,
            CliPlanCodec::CeremonialCbor => PlanCodec::CeremonialCbor,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum CliCompactionMode {
    Disabled,
    Normal,
    ForceRawToPlan,
    VacuumMeaning,
}

impl From<CliCompactionMode> for CompactionMode {
    fn from(mode: CliCompactionMode) -> Self {
        match mode {
            CliCompactionMode::Disabled => CompactionMode::Disabled,
            CliCompactionMode::Normal => CompactionMode::Normal,
            CliCompactionMode::ForceRawToPlan => CompactionMode::ForceRawToPlan,
            CliCompactionMode::VacuumMeaning => CompactionMode::VacuumMeaning,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Humiliation {
    Modest,
    Maximum,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum BenchWorkload {
    Sha256Stream,
    TinyJson,
    #[value(name = "json-4k")]
    Json4k,
    #[value(name = "random-64k")]
    Random64k,
    #[value(name = "repeated-64k")]
    Repeated64k,
    UuidHeavy,
    AllBytes,
    TinyPng,
    #[value(name = "png-256k")]
    Png256k,
}

impl BenchWorkload {
    fn name(self) -> &'static str {
        match self {
            Self::Sha256Stream => "sha256-stream",
            Self::TinyJson => "tiny-json",
            Self::Json4k => "json-4k",
            Self::Random64k => "random-64k",
            Self::Repeated64k => "repeated-64k",
            Self::UuidHeavy => "uuid-heavy",
            Self::AllBytes => "all-bytes",
            Self::TinyPng => "tiny-png",
            Self::Png256k => "png-256k",
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let stream_kind = cli.stream;
    let plan_options = PlanOptions {
        max_prefix_len: cli
            .prefix_bytes
            .unwrap_or_else(|| default_prefix_bytes(stream_kind)),
        max_k: cli.max_k,
        plan_codec: cli.plan_codec.into(),
        allow_literals: cli.allow_literals,
        ..PlanOptions::default()
    };

    match cli.command {
        Command::Init { path } => {
            let db = open_db(&path, &plan_options, stream_kind).await?;
            db.close().await?;
        }
        Command::Put { path, key, file } => {
            let db = open_db(&path, &plan_options, stream_kind).await?;
            let value = read_value(&file)?;
            db.put(key.as_bytes(), value).await?;
            db.flush().await?;
            db.close().await?;
        }
        Command::Get { path, key } => {
            let db = open_db(&path, &plan_options, stream_kind).await?;
            let Some(value) = db.get(key.as_bytes()).await? else {
                bail!("key not found: {key}");
            };
            std::io::stdout().write_all(&value)?;
            db.close().await?;
        }
        Command::Delete { path, key } => {
            let db = open_db(&path, &plan_options, stream_kind).await?;
            db.delete(key.as_bytes()).await?;
            db.flush().await?;
            db.close().await?;
        }
        Command::Explain {
            path,
            key,
            philosophical: _,
        } => {
            let db = open_db(&path, &plan_options, stream_kind).await?;
            let Some(explain) = db.explain(key.as_bytes()).await? else {
                bail!("key not found: {key}");
            };
            println!("storage_class: {:?}", explain.storage_class);
            println!("logical_user_bytes: {}", explain.logical_user_bytes);
            println!("physical_value_bytes: {}", explain.physical_value_bytes);
            println!("plan_metadata_bytes: {}", explain.plan_metadata_bytes);
            println!("chunks: {}", explain.chunks);
            println!("longest_natural_run: {}", explain.longest_natural_run);
            println!("literal_bytes: {}", explain.literal_bytes);
            match explain.average_chunk_len {
                Some(len) => println!("average_chunk_len: {len:.2}"),
                None => println!("average_chunk_len: undefined"),
            }
            println!(
                "philosophical_user_value_bytes_stored: {}",
                explain.philosophical_user_value_bytes_stored
            );
            println!(
                "philosophical_compression_ratio: {}",
                philosophical_compression_ratio(explain.philosophical_compression_ratio)
            );
            println!("purity: {:?}", explain.purity);
            match explain.metadata_amplification_ratio {
                Some(ratio) => println!("metadata_amplification: {ratio:.2}x"),
                None => println!("metadata_amplification: undefined"),
            }
            db.close().await?;
        }
        Command::PlanKey { path, key } => {
            let db = open_db(&path, &plan_options, stream_kind).await?;
            let report = db.plan_key(key.as_bytes(), plan_options).await?;
            print_rewrite_status(report.status);
            db.flush().await?;
            db.close().await?;
        }
        Command::VacuumMeaning {
            path,
            key,
            all,
            budget,
        } => {
            let db = open_db(&path, &plan_options, stream_kind).await?;
            match (all, key) {
                (true, None) => {
                    let report = vacuum_all(&db, &plan_options, budget).await?;
                    println!("visited: {}", report.visited);
                    println!("rewritten: {}", report.rewritten);
                    println!("kept: {}", report.kept);
                    println!("stale_or_missing: {}", report.stale_or_missing);
                    println!("timed_out: {}", report.timed_out);
                }
                (false, Some(key)) => {
                    let report = db.vacuum_meaning(key.as_bytes(), plan_options).await?;
                    print_rewrite_status(report.status);
                }
                (true, Some(_)) => bail!("--all conflicts with a key argument"),
                (false, None) => bail!("vacuum-meaning requires a key or --all"),
            }
            db.flush().await?;
            db.close().await?;
        }
        Command::Metrics { path } => {
            let db = open_db(&path, &plan_options, stream_kind).await?;
            let metrics = db.metrics().await?;
            print_metrics(&metrics);
            db.close().await?;
        }
        Command::Compact {
            path,
            run_ms,
            runs,
            poll_ms,
            min_compaction_sources,
            mode,
            into_nonexistence,
            humiliation,
            strict_envelopes,
            ignore_snapshot_representation_safety,
        } => {
            if min_compaction_sources < 2 {
                bail!("--min-compaction-sources must be at least 2");
            }
            if runs == 0 {
                bail!("--runs must be at least 1");
            }
            let mut compact_plan_options = plan_options.clone();
            let mode = compact_mode(mode, into_nonexistence, humiliation)?;
            if matches!(mode, CompactionMode::ForceRawToPlan) && compact_plan_options.allow_literals
            {
                bail!("--allow-literals conflicts with forced compaction into plans");
            }
            if humiliation == Some(Humiliation::Maximum) {
                compact_plan_options.max_k = 1;
                compact_plan_options.plan_codec = PlanCodec::CeremonialCbor;
            }
            let (object_store, db_path) = open_local_store(&path)?;
            let runtime = build_runtime(&compact_plan_options, stream_kind).await?;
            let supplier = runtime.supplier.with_options(
                mode,
                strict_envelopes,
                !ignore_snapshot_representation_safety,
            );
            for _ in 0..runs {
                run_compactor_with_options(
                    db_path.clone(),
                    object_store.clone(),
                    supplier.clone(),
                    PiLsmCompactorOptions {
                        run_for: Duration::from_millis(run_ms),
                        poll_interval: Duration::from_millis(poll_ms),
                        min_compaction_sources,
                    },
                )
                .await?;
            }
            print_compaction_filter_stats(&supplier.stats());
        }
        Command::Bench {
            path,
            workload,
            suite,
            against_common_sense,
            values,
            size,
        } => {
            if suite || against_common_sense {
                run_bench_suite(&path, &plan_options, stream_kind).await?;
            } else {
                run_bench(
                    &path,
                    BenchCase {
                        workload,
                        value_count: values,
                        value_size: size,
                    },
                    &plan_options,
                    stream_kind,
                )
                .await?;
            }
        }
    }

    Ok(())
}

fn default_prefix_bytes(stream_kind: StreamKind) -> u64 {
    match stream_kind {
        StreamKind::Sha256Counter => PlanOptions::default().max_prefix_len,
        StreamKind::PiPrefix => PI_HEX_FRACTION_PREFIX_BYTES as u64,
    }
}

fn compact_mode(
    mode: Option<CliCompactionMode>,
    into_nonexistence: bool,
    humiliation: Option<Humiliation>,
) -> Result<CompactionMode> {
    if mode.is_some() && into_nonexistence {
        bail!("--mode conflicts with --into-nonexistence");
    }
    if mode.is_some() && humiliation == Some(Humiliation::Maximum) {
        bail!("--mode conflicts with --humiliation maximum");
    }

    if into_nonexistence || humiliation == Some(Humiliation::Maximum) {
        Ok(CompactionMode::ForceRawToPlan)
    } else {
        Ok(mode.unwrap_or(CliCompactionMode::Normal).into())
    }
}

#[derive(Clone, Debug, Default)]
struct VacuumAllReport {
    visited: u64,
    rewritten: u64,
    kept: u64,
    stale_or_missing: u64,
    timed_out: bool,
}

async fn vacuum_all(
    db: &PiLsmDb,
    plan_options: &PlanOptions,
    budget: Option<Duration>,
) -> Result<VacuumAllReport> {
    let start = Instant::now();
    let mut keys = Vec::new();
    let mut envelopes = db.scan_envelopes::<Vec<u8>, _>(Vec::<u8>::new()..).await?;
    while let Some(kv) = envelopes.next().await? {
        keys.push(kv.key);
    }

    let mut report = VacuumAllReport::default();
    for key in keys {
        if budget.is_some_and(|budget| start.elapsed() >= budget) {
            report.timed_out = true;
            break;
        }

        let rewrite = db.vacuum_meaning(&key, plan_options.clone()).await?;
        report.visited += 1;
        match rewrite.status {
            RewriteStatus::Rewritten => report.rewritten += 1,
            RewriteStatus::SkippedMissingKey | RewriteStatus::SkippedStaleSource => {
                report.stale_or_missing += 1;
            }
            RewriteStatus::KeptAlreadyPlanned
            | RewriteStatus::SkippedPlanningFailed
            | RewriteStatus::SkippedNotImproved => report.kept += 1,
        }
    }

    Ok(report)
}

fn parse_duration(input: &str) -> std::result::Result<Duration, String> {
    let input = input.trim();
    if input.is_empty() {
        return Err("duration must not be empty".to_string());
    }

    let split_at = input
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(input.len());
    let (number, unit) = input.split_at(split_at);
    if number.is_empty() {
        return Err(format!("invalid duration {input:?}"));
    }

    let value = number
        .parse::<u64>()
        .map_err(|err| format!("invalid duration {input:?}: {err}"))?;
    match unit {
        "" | "ms" => Ok(Duration::from_millis(value)),
        "s" => Ok(Duration::from_secs(value)),
        "m" => value
            .checked_mul(60)
            .map(Duration::from_secs)
            .ok_or_else(|| format!("duration is too large: {input:?}")),
        other => Err(format!(
            "unsupported duration unit {other:?}; use ms, s, or m"
        )),
    }
}

#[derive(Clone, Copy, Debug)]
struct BenchCase {
    workload: BenchWorkload,
    value_count: usize,
    value_size: usize,
}

impl BenchCase {
    fn dir_name(self) -> String {
        format!(
            "{}-{}x{}",
            self.workload.name(),
            self.value_count,
            self.value_size
        )
    }
}

async fn run_bench_suite(
    path: &Path,
    plan_options: &PlanOptions,
    stream_kind: StreamKind,
) -> Result<()> {
    if path.exists() {
        bail!("bench path already exists: {}", path.display());
    }

    let cases = [
        BenchCase {
            workload: BenchWorkload::TinyJson,
            value_count: 1_000,
            value_size: 128,
        },
        BenchCase {
            workload: BenchWorkload::Json4k,
            value_count: 64,
            value_size: 4 * 1024,
        },
        BenchCase {
            workload: BenchWorkload::Random64k,
            value_count: 8,
            value_size: 64 * 1024,
        },
        BenchCase {
            workload: BenchWorkload::Repeated64k,
            value_count: 8,
            value_size: 64 * 1024,
        },
        BenchCase {
            workload: BenchWorkload::Png256k,
            value_count: 1,
            value_size: 256 * 1024,
        },
        BenchCase {
            workload: BenchWorkload::UuidHeavy,
            value_count: 1_000,
            value_size: 36,
        },
        BenchCase {
            workload: BenchWorkload::AllBytes,
            value_count: 1,
            value_size: 256,
        },
    ];

    for case in cases {
        println!();
        run_bench(&path.join(case.dir_name()), case, plan_options, stream_kind).await?;
    }

    Ok(())
}

async fn run_bench(
    path: &Path,
    case: BenchCase,
    plan_options: &PlanOptions,
    stream_kind: StreamKind,
) -> Result<()> {
    let value_count = case.value_count;
    let value_size = case.value_size;
    if value_count == 0 {
        bail!("--values must be at least 1");
    }
    if value_size == 0 {
        bail!("--size must be at least 1");
    }
    if path.exists() {
        bail!("bench path already exists: {}", path.display());
    }

    let values = generate_values(case).await?;
    let compact_options = PlanOptions {
        plan_codec: PlanCodec::CompactBinary,
        ..plan_options.clone()
    };
    let ceremonial_options = PlanOptions {
        plan_codec: PlanCodec::CeremonialCbor,
        ..plan_options.clone()
    };
    let humiliation_options = PlanOptions {
        max_k: 1,
        plan_codec: PlanCodec::CeremonialCbor,
        ..plan_options.clone()
    };
    let vacuum_options = PlanOptions {
        max_k: plan_options.max_k.max(3),
        plan_codec: PlanCodec::CompactBinary,
        ..plan_options.clone()
    };

    let plain = bench_plain_slate(&path.join("plain-slate"), &values).await?;
    let raw = bench_pilsmer_raw(
        &path.join("pilsmer-raw"),
        &values,
        &compact_options,
        stream_kind,
    )
    .await?;
    let compact = bench_pilsmer_planned(
        "pilsmer-compact-plan",
        &path.join("pilsmer-compact-plan"),
        &values,
        &compact_options,
        stream_kind,
    )
    .await?;
    let ceremonial = bench_pilsmer_planned(
        "pilsmer-ceremonial-plan",
        &path.join("pilsmer-ceremonial-plan"),
        &values,
        &ceremonial_options,
        stream_kind,
    )
    .await?;
    let vacuumed = bench_pilsmer_vacuumed(
        &path.join("pilsmer-vacuumed"),
        &values,
        &humiliation_options,
        &vacuum_options,
        stream_kind,
    )
    .await?;

    println!("values: {value_count}");
    println!("value_size: {value_size}");
    println!("bench_workload: {}", case.workload.name());
    println!(
        "workload\tput_ms\tflush_ms\tput_p50_us\tput_p95_us\tplan_ms\tread_ms\tread_p50_us\tread_p95_us\tread_p99_us\treconstruction_hash_failures\tobject_store_gets\tobject_store_puts\tlogical_bytes\tphysical_value_bytes\tchunks\tmetadata_amp"
    );
    print_bench_row(&plain);
    print_bench_row(&raw);
    print_bench_row(&compact);
    print_bench_row(&ceremonial);
    print_bench_row(&vacuumed);
    Ok(())
}

#[derive(Clone, Debug)]
struct BenchResult {
    workload: &'static str,
    put_ms: u128,
    flush_ms: Option<u128>,
    put_latency: Option<LatencySummary>,
    plan_ms: Option<u128>,
    read_ms: u128,
    read_latency: Option<LatencySummary>,
    reconstruction_hash_failures: Option<u64>,
    object_store_counts: Option<ObjectStoreCounts>,
    logical_bytes: u64,
    physical_value_bytes: Option<u64>,
    chunks: Option<u64>,
}

impl BenchResult {
    fn metadata_amp(&self) -> Option<f64> {
        match (self.physical_value_bytes, self.logical_bytes) {
            (Some(bytes), logical) if logical > 0 => Some(bytes as f64 / logical as f64),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct LatencySummary {
    p50_us: u128,
    p95_us: u128,
    p99_us: u128,
}

fn latency_summary(mut samples: Vec<u128>) -> Option<LatencySummary> {
    if samples.is_empty() {
        return None;
    }
    samples.sort_unstable();
    Some(LatencySummary {
        p50_us: percentile(&samples, 50),
        p95_us: percentile(&samples, 95),
        p99_us: percentile(&samples, 99),
    })
}

fn percentile(sorted_samples: &[u128], percentile: usize) -> u128 {
    let ix = ((sorted_samples.len() * percentile).div_ceil(100))
        .saturating_sub(1)
        .min(sorted_samples.len() - 1);
    sorted_samples[ix]
}

async fn generate_values(case: BenchCase) -> Result<Vec<Vec<u8>>> {
    match case.workload {
        BenchWorkload::Sha256Stream | BenchWorkload::Random64k => {
            generate_stream_values(case.value_count, case.value_size, [1_u8; 32]).await
        }
        BenchWorkload::TinyJson | BenchWorkload::Json4k => {
            generate_json_values(case.value_count, case.value_size)
        }
        BenchWorkload::Repeated64k => generate_repeated_values(case.value_count, case.value_size),
        BenchWorkload::UuidHeavy => generate_uuid_values(case.value_count, case.value_size).await,
        BenchWorkload::AllBytes => generate_all_byte_values(case.value_count, case.value_size),
        BenchWorkload::TinyPng => Ok((0..case.value_count)
            .map(|_| TINY_PNG_BYTES.to_vec())
            .collect()),
        BenchWorkload::Png256k => generate_png_values(case.value_count, case.value_size),
    }
}

async fn generate_stream_values(
    value_count: usize,
    value_size: usize,
    seed: [u8; 32],
) -> Result<Vec<Vec<u8>>> {
    let stream = Sha256CounterStream::new(seed);
    let mut values = Vec::with_capacity(value_count);
    for ix in 0..value_count {
        let offset = ix
            .checked_mul(value_size)
            .context("benchmark value offset overflow")?;
        values.push(stream.read_at(offset as u64, value_size).await?.to_vec());
    }
    Ok(values)
}

fn generate_json_values(value_count: usize, target_size: usize) -> Result<Vec<Vec<u8>>> {
    let mut values = Vec::with_capacity(value_count);
    for ix in 0..value_count {
        let prefix = format!(
            r#"{{"id":{ix},"status":"paid","total":{},"note":""#,
            10_000 + ix
        );
        let suffix = r#""}"#;
        let fill_len = target_size.saturating_sub(prefix.len() + suffix.len());
        let fill = repeated_ascii(fill_len, b"json-value-field-");
        values.push(format!("{prefix}{fill}{suffix}").into_bytes());
    }
    Ok(values)
}

fn generate_repeated_values(value_count: usize, value_size: usize) -> Result<Vec<Vec<u8>>> {
    Ok((0..value_count)
        .map(|ix| {
            let pattern = format!("PiLSMer repeated blob {ix:08} stores meaning elsewhere. ");
            repeated_ascii(value_size, pattern.as_bytes()).into_bytes()
        })
        .collect())
}

async fn generate_uuid_values(value_count: usize, target_size: usize) -> Result<Vec<Vec<u8>>> {
    let raw = generate_stream_values(value_count.max(1) * 16, 16, [2_u8; 32]).await?;
    let mut values = Vec::with_capacity(value_count);
    for ix in 0..value_count {
        let mut value = uuid_from_bytes(&raw[ix % raw.len()]);
        let mut next = ix + value_count;
        while value.len() < target_size {
            value.push(',');
            value.push_str(&uuid_from_bytes(&raw[next % raw.len()]));
            next += value_count;
        }
        value.truncate(target_size.max(36));
        values.push(value.into_bytes());
    }
    Ok(values)
}

fn generate_all_byte_values(value_count: usize, value_size: usize) -> Result<Vec<Vec<u8>>> {
    Ok((0..value_count)
        .map(|ix| {
            (0..value_size)
                .map(|offset| ((ix + offset) % 256) as u8)
                .collect()
        })
        .collect())
}

fn generate_png_values(value_count: usize, target_size: usize) -> Result<Vec<Vec<u8>>> {
    (0..value_count)
        .map(|ix| generate_png_fixture(target_size, ix))
        .collect()
}

fn generate_png_fixture(target_size: usize, seed: usize) -> Result<Vec<u8>> {
    const WIDTH: usize = 256;
    const CHANNELS: usize = 3;
    const CONTAINER_OVERHEAD: usize = 128;

    let row_len = 1 + WIDTH * CHANNELS;
    let target_pixel_bytes = target_size.saturating_sub(CONTAINER_OVERHEAD).max(row_len);
    let height = target_pixel_bytes.div_ceil(row_len);
    let raw_len = height
        .checked_mul(row_len)
        .context("PNG benchmark fixture is too large")?;
    let height_u32 = u32::try_from(height).context("PNG benchmark height exceeds u32")?;

    let mut raw = Vec::with_capacity(raw_len);
    for y in 0..height {
        raw.push(0);
        for x in 0..WIDTH {
            raw.push(((x + seed) & 0xff) as u8);
            raw.push(((y + seed * 3) & 0xff) as u8);
            raw.push(((x ^ y ^ seed) & 0xff) as u8);
        }
    }

    let mut ihdr = Vec::with_capacity(13);
    ihdr.extend_from_slice(&(WIDTH as u32).to_be_bytes());
    ihdr.extend_from_slice(&height_u32.to_be_bytes());
    ihdr.extend_from_slice(&[8, 2, 0, 0, 0]);

    let mut png = Vec::new();
    png.extend_from_slice(b"\x89PNG\r\n\x1a\n");
    append_png_chunk(&mut png, b"IHDR", &ihdr)?;
    append_png_chunk(&mut png, b"IDAT", &zlib_stored(&raw)?)?;
    append_png_chunk(&mut png, b"IEND", &[])?;
    Ok(png)
}

fn zlib_stored(data: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(data.len() + 6 + data.len().div_ceil(65_535) * 5);
    out.extend_from_slice(&[0x78, 0x01]);

    let mut offset = 0;
    while offset < data.len() {
        let remaining = data.len() - offset;
        let block_len = remaining.min(65_535);
        let is_final = offset + block_len == data.len();
        out.push(if is_final { 0x01 } else { 0x00 });
        let len = u16::try_from(block_len).context("deflate stored block is too large")?;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&(!len).to_le_bytes());
        out.extend_from_slice(&data[offset..offset + block_len]);
        offset += block_len;
    }

    out.extend_from_slice(&adler32(data).to_be_bytes());
    Ok(out)
}

fn append_png_chunk(out: &mut Vec<u8>, chunk_type: &[u8; 4], data: &[u8]) -> Result<()> {
    let len = u32::try_from(data.len()).context("PNG chunk is too large")?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(chunk_type);
    out.extend_from_slice(data);

    let mut crc_input = Vec::with_capacity(chunk_type.len() + data.len());
    crc_input.extend_from_slice(chunk_type);
    crc_input.extend_from_slice(data);
    out.extend_from_slice(&crc32(&crc_input).to_be_bytes());
    Ok(())
}

fn adler32(data: &[u8]) -> u32 {
    const MOD_ADLER: u32 = 65_521;
    let mut a = 1_u32;
    let mut b = 0_u32;
    for byte in data {
        a = (a + u32::from(*byte)) % MOD_ADLER;
        b = (b + a) % MOD_ADLER;
    }
    (b << 16) | a
}

fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xffff_ffff_u32;
    for byte in data {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xedb8_8320 & mask);
        }
    }
    !crc
}

fn repeated_ascii(len: usize, pattern: &[u8]) -> String {
    if len == 0 {
        return String::new();
    }
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        let remaining = len - out.len();
        let take = remaining.min(pattern.len());
        out.extend_from_slice(&pattern[..take]);
    }
    String::from_utf8(out).expect("benchmark pattern is ASCII")
}

fn uuid_from_bytes(bytes: &[u8]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    )
}

const TINY_PNG_BYTES: &[u8] = &[
    0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x48, 0x44, 0x52,
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x04, 0x00, 0x00, 0x00, 0xb5, 0x1c, 0x0c,
    0x02, 0x00, 0x00, 0x00, 0x0b, 0x49, 0x44, 0x41, 0x54, 0x78, 0xda, 0x63, 0xfc, 0xff, 0x1f, 0x00,
    0x03, 0x03, 0x02, 0x00, 0xef, 0xbf, 0xa7, 0xdb, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4e, 0x44,
    0xae, 0x42, 0x60, 0x82,
];

async fn bench_plain_slate(path: &Path, values: &[Vec<u8>]) -> Result<BenchResult> {
    let (object_store, db_path, object_store_counters) = open_counted_local_store(path)?;
    let db = Db::open(db_path, object_store).await?;

    let put_start = Instant::now();
    let mut put_latencies = Vec::with_capacity(values.len());
    for (ix, value) in values.iter().enumerate() {
        let op_start = Instant::now();
        db.put(key_bytes(ix), value.as_slice()).await?;
        put_latencies.push(op_start.elapsed().as_micros());
    }
    let flush_start = Instant::now();
    db.flush().await?;
    let flush_ms = flush_start.elapsed().as_millis();
    let put_ms = put_start.elapsed().as_millis();

    let read_start = Instant::now();
    let mut read_latencies = Vec::with_capacity(values.len());
    let mut logical_bytes = 0_u64;
    for ix in 0..values.len() {
        let op_start = Instant::now();
        let Some(value) = db.get(key_bytes(ix)).await? else {
            bail!("missing plain SlateDB bench key {ix}");
        };
        read_latencies.push(op_start.elapsed().as_micros());
        logical_bytes += value.len() as u64;
    }
    let read_ms = read_start.elapsed().as_millis();
    db.close().await?;
    let object_store_counts = object_store_counters.snapshot();

    Ok(BenchResult {
        workload: "plain-slate",
        put_ms,
        flush_ms: Some(flush_ms),
        put_latency: latency_summary(put_latencies),
        plan_ms: None,
        read_ms,
        read_latency: latency_summary(read_latencies),
        reconstruction_hash_failures: None,
        object_store_counts: Some(object_store_counts),
        logical_bytes,
        physical_value_bytes: Some(logical_bytes),
        chunks: None,
    })
}

async fn bench_pilsmer_raw(
    path: &Path,
    values: &[Vec<u8>],
    plan_options: &PlanOptions,
    stream_kind: StreamKind,
) -> Result<BenchResult> {
    let (db, object_store_counters) = open_counted_db(path, plan_options, stream_kind).await?;

    let put_start = Instant::now();
    let mut put_latencies = Vec::with_capacity(values.len());
    for (ix, value) in values.iter().enumerate() {
        let op_start = Instant::now();
        db.put(key_bytes(ix), value.as_slice()).await?;
        put_latencies.push(op_start.elapsed().as_micros());
    }
    let flush_start = Instant::now();
    db.flush().await?;
    let flush_ms = flush_start.elapsed().as_millis();
    let put_ms = put_start.elapsed().as_millis();

    let read_start = Instant::now();
    let mut read_latencies = Vec::with_capacity(values.len());
    let mut logical_bytes = 0_u64;
    let mut physical_value_bytes = 0_u64;
    for ix in 0..values.len() {
        let key = key_bytes(ix);
        let op_start = Instant::now();
        let Some(value) = db.get(&key).await? else {
            bail!("missing PiLSMer raw bench key {ix}");
        };
        read_latencies.push(op_start.elapsed().as_micros());
        logical_bytes += value.len() as u64;
        let Some(explain) = db.explain(&key).await? else {
            bail!("missing PiLSMer raw explain key {ix}");
        };
        physical_value_bytes += explain.physical_value_bytes;
    }
    let read_ms = read_start.elapsed().as_millis();
    db.close().await?;
    let object_store_counts = object_store_counters.snapshot();

    Ok(BenchResult {
        workload: "pilsmer-raw",
        put_ms,
        flush_ms: Some(flush_ms),
        put_latency: latency_summary(put_latencies),
        plan_ms: None,
        read_ms,
        read_latency: latency_summary(read_latencies),
        reconstruction_hash_failures: Some(0),
        object_store_counts: Some(object_store_counts),
        logical_bytes,
        physical_value_bytes: Some(physical_value_bytes),
        chunks: None,
    })
}

async fn bench_pilsmer_planned(
    workload: &'static str,
    path: &Path,
    values: &[Vec<u8>],
    plan_options: &PlanOptions,
    stream_kind: StreamKind,
) -> Result<BenchResult> {
    let (db, object_store_counters) = open_counted_db(path, plan_options, stream_kind).await?;

    let put_start = Instant::now();
    let mut put_latencies = Vec::with_capacity(values.len());
    for (ix, value) in values.iter().enumerate() {
        let op_start = Instant::now();
        db.put(key_bytes(ix), value.as_slice()).await?;
        put_latencies.push(op_start.elapsed().as_micros());
    }
    let flush_start = Instant::now();
    db.flush().await?;
    let flush_ms = flush_start.elapsed().as_millis();
    let put_ms = put_start.elapsed().as_millis();

    let plan_start = Instant::now();
    for ix in 0..values.len() {
        let report = db.plan_key(key_bytes(ix), plan_options.clone()).await?;
        if !matches!(
            report.status,
            RewriteStatus::Rewritten | RewriteStatus::KeptAlreadyPlanned
        ) {
            bail!("planning failed for bench key {ix}: {:?}", report.status);
        }
    }
    db.flush().await?;
    let plan_ms = plan_start.elapsed().as_millis();

    let mut result = collect_pilsmer_read_result(&db, workload, values.len()).await?;
    result.put_ms = put_ms;
    result.flush_ms = Some(flush_ms);
    result.put_latency = latency_summary(put_latencies);
    result.plan_ms = Some(plan_ms);
    db.close().await?;
    result.object_store_counts = Some(object_store_counters.snapshot());
    Ok(result)
}

async fn bench_pilsmer_vacuumed(
    path: &Path,
    values: &[Vec<u8>],
    seed_options: &PlanOptions,
    vacuum_options: &PlanOptions,
    stream_kind: StreamKind,
) -> Result<BenchResult> {
    let (object_store, db_path, object_store_counters) = open_counted_local_store(path)?;
    let seed_runtime = build_runtime(seed_options, stream_kind).await?;
    let db = PiLsmDb::open(db_path.clone(), object_store.clone(), seed_runtime.opts).await?;

    let put_start = Instant::now();
    let mut put_latencies = Vec::with_capacity(values.len());
    for (ix, value) in values.iter().enumerate() {
        let op_start = Instant::now();
        db.put(key_bytes(ix), value.as_slice()).await?;
        put_latencies.push(op_start.elapsed().as_micros());
    }
    let flush_start = Instant::now();
    db.flush().await?;
    let flush_ms = flush_start.elapsed().as_millis();
    let put_ms = put_start.elapsed().as_millis();

    let rewrite_start = Instant::now();
    for ix in 0..values.len() {
        let report = db.plan_key(key_bytes(ix), seed_options.clone()).await?;
        if !matches!(
            report.status,
            RewriteStatus::Rewritten | RewriteStatus::KeptAlreadyPlanned
        ) {
            bail!(
                "humiliation planning failed for bench key {ix}: {:?}",
                report.status
            );
        }
    }
    db.flush().await?;
    db.close().await?;

    let vacuum_runtime = build_runtime(vacuum_options, stream_kind).await?;
    let db = PiLsmDb::open(db_path, object_store, vacuum_runtime.opts).await?;
    for ix in 0..values.len() {
        let report = db
            .vacuum_meaning(key_bytes(ix), vacuum_options.clone())
            .await?;
        if !matches!(
            report.status,
            RewriteStatus::Rewritten
                | RewriteStatus::KeptAlreadyPlanned
                | RewriteStatus::SkippedNotImproved
        ) {
            bail!(
                "vacuum planning failed for bench key {ix}: {:?}",
                report.status
            );
        }
    }
    db.flush().await?;
    let rewrite_ms = rewrite_start.elapsed().as_millis();

    let mut result = collect_pilsmer_read_result(&db, "pilsmer-vacuumed", values.len()).await?;
    result.put_ms = put_ms;
    result.flush_ms = Some(flush_ms);
    result.put_latency = latency_summary(put_latencies);
    result.plan_ms = Some(rewrite_ms);
    db.close().await?;
    result.object_store_counts = Some(object_store_counters.snapshot());
    Ok(result)
}

async fn collect_pilsmer_read_result(
    db: &PiLsmDb,
    workload: &'static str,
    value_count: usize,
) -> Result<BenchResult> {
    let read_start = Instant::now();
    let mut read_latencies = Vec::with_capacity(value_count);
    let mut logical_bytes = 0_u64;
    let mut physical_value_bytes = 0_u64;
    let mut chunks = 0_u64;
    for ix in 0..value_count {
        let key = key_bytes(ix);
        let op_start = Instant::now();
        let Some(value) = db.get(&key).await? else {
            bail!("missing PiLSMer bench key {ix}");
        };
        read_latencies.push(op_start.elapsed().as_micros());
        logical_bytes += value.len() as u64;
        let Some(explain) = db.explain(&key).await? else {
            bail!("missing PiLSMer bench explain key {ix}");
        };
        physical_value_bytes += explain.physical_value_bytes;
        chunks += explain.chunks;
    }
    let read_ms = read_start.elapsed().as_millis();

    Ok(BenchResult {
        workload,
        put_ms: 0,
        flush_ms: None,
        put_latency: None,
        plan_ms: None,
        read_ms,
        read_latency: latency_summary(read_latencies),
        reconstruction_hash_failures: Some(0),
        object_store_counts: None,
        logical_bytes,
        physical_value_bytes: Some(physical_value_bytes),
        chunks: Some(chunks),
    })
}

fn key_bytes(ix: usize) -> Vec<u8> {
    format!("k:{ix:08}").into_bytes()
}

fn print_bench_row(result: &BenchResult) {
    println!(
        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        result.workload,
        result.put_ms,
        optional_u128(result.flush_ms),
        optional_latency(result.put_latency, |latency| latency.p50_us),
        optional_latency(result.put_latency, |latency| latency.p95_us),
        optional_u128(result.plan_ms),
        result.read_ms,
        optional_latency(result.read_latency, |latency| latency.p50_us),
        optional_latency(result.read_latency, |latency| latency.p95_us),
        optional_latency(result.read_latency, |latency| latency.p99_us),
        optional_u64(result.reconstruction_hash_failures),
        optional_u64(result.object_store_counts.map(|counts| counts.gets)),
        optional_u64(result.object_store_counts.map(|counts| counts.puts)),
        result.logical_bytes,
        optional_u64(result.physical_value_bytes),
        optional_u64(result.chunks),
        optional_ratio(result.metadata_amp()),
    );
}

fn optional_u128(value: Option<u128>) -> String {
    value.map_or_else(|| "-".to_string(), |value| value.to_string())
}

fn optional_latency(
    value: Option<LatencySummary>,
    select: impl FnOnce(LatencySummary) -> u128,
) -> String {
    value.map_or_else(|| "-".to_string(), |value| select(value).to_string())
}

fn optional_u64(value: Option<u64>) -> String {
    value.map_or_else(|| "-".to_string(), |value| value.to_string())
}

fn optional_ratio(value: Option<f64>) -> String {
    value.map_or_else(|| "-".to_string(), |value| format!("{value:.2}x"))
}

fn print_metrics(metrics: &PiLsmMetrics) {
    println!("pilsmer_raw_values_total {}", metrics.raw_values_total);
    println!(
        "pilsmer_planned_values_total {}",
        metrics.planned_values_total
    );
    println!(
        "pilsmer_logical_bytes_total {}",
        metrics.logical_bytes_total
    );
    println!(
        "pilsmer_planned_logical_bytes_total {}",
        metrics.planned_logical_bytes_total
    );
    println!(
        "pilsmer_raw_bytes_converted_total {}",
        metrics.raw_bytes_converted_total
    );
    println!(
        "pilsmer_raw_envelope_bytes_total {}",
        metrics.raw_envelope_bytes_total
    );
    println!(
        "pilsmer_plan_envelope_bytes_total {}",
        metrics.plan_envelope_bytes_total
    );
    println!(
        "pilsmer_plan_metadata_bytes_total {}",
        metrics.plan_metadata_bytes_total
    );
    println!(
        "pilsmer_located_user_bytes_total {}",
        metrics.located_user_bytes_total
    );
    println!(
        "pilsmer_literal_user_bytes_total {}",
        metrics.literal_user_bytes_total
    );
    println!(
        "pilsmer_physical_value_bytes_total {}",
        metrics.physical_value_bytes_total
    );
    println!(
        "pilsmer_philosophical_user_value_bytes_stored_total {}",
        metrics.philosophical_user_value_bytes_stored_total
    );
    println!(
        "pilsmer_reconstruction_seconds {}",
        metric_seconds(metrics.reconstruction_seconds)
    );
    println!(
        "pilsmer_planner_seconds {}",
        metric_seconds(metrics.planner_seconds)
    );
    println!("pilsmer_chunks_total {}", metrics.chunks_total);
    println!(
        "pilsmer_chunks_per_value {}",
        metric_optional(metrics.chunks_per_value)
    );
    println!(
        "pilsmer_avg_chunk_len_bytes {}",
        metric_optional(metrics.avg_chunk_len_bytes)
    );
    println!(
        "pilsmer_longest_natural_run_bytes {}",
        metrics.longest_natural_run_bytes
    );
    println!(
        "pilsmer_stream_prefix_bytes_indexed {}",
        metrics.stream_prefix_bytes_indexed
    );
    println!(
        "pilsmer_metadata_amplification_ratio {}",
        metric_optional(metrics.metadata_amplification_ratio)
    );
    println!(
        "pilsmer_philosophical_compression_ratio {}",
        metric_philosophical_ratio(metrics.philosophical_compression_ratio)
    );
    println!(
        "pilsmer_compaction_filter_errors_total {}",
        metrics.compaction_filter_errors_total
    );
    println!(
        "pilsmer_snapshot_protected_entries_total {}",
        metrics.snapshot_protected_entries_total
    );
    println!(
        "pilsmer_vacuum_meaning_attempts_total {}",
        metrics.vacuum_meaning_attempts_total
    );
    println!(
        "pilsmer_vacuum_meaning_improvements_total {}",
        metrics.vacuum_meaning_improvements_total
    );
    println!(
        "pilsmer_reconstruction_cache_bytes {}",
        metrics.reconstruction_cache_bytes
    );
    println!(
        "pilsmer_philosophical_purity_violations_total {}",
        metrics.philosophical_purity_violations_total
    );
    println!(
        "pilsmer_representation_entropy_excuses_total {}",
        metrics.representation_entropy_excuses_total
    );
}

fn print_compaction_filter_stats(stats: &PiLsmCompactionFilterStats) {
    println!("raw_values_converted: {}", stats.raw_values_converted);
    println!("raw_bytes_converted: {}", stats.raw_bytes_converted);
    println!("plans_improved: {}", stats.plans_improved);
    println!("plans_kept: {}", stats.plans_kept);
    println!(
        "raw_values_kept_after_planning_failure: {}",
        stats.raw_values_kept_after_planning_failure
    );
    println!("corrupt_or_unknown_kept: {}", stats.corrupt_or_unknown_kept);
    println!(
        "snapshot_protected_entries: {}",
        stats.snapshot_protected_entries
    );
    println!(
        "tombstones_or_merges_kept: {}",
        stats.tombstones_or_merges_kept
    );
    println!("errors: {}", stats.errors);
}

fn metric_optional(value: Option<f64>) -> String {
    value.map_or_else(|| "undefined".to_string(), |value| format!("{value:.6}"))
}

fn metric_seconds(value: f64) -> String {
    format!("{value:.9}")
}

fn metric_philosophical_ratio(value: PhilosophicalCompressionRatio) -> String {
    match value {
        PhilosophicalCompressionRatio::Finite(value) => format!("{value:.6}"),
        PhilosophicalCompressionRatio::Infinite => "infinity".to_string(),
        PhilosophicalCompressionRatio::Revoked => "revoked".to_string(),
        PhilosophicalCompressionRatio::Undefined => "NaN, but smug".to_string(),
    }
}

fn philosophical_compression_ratio(value: PhilosophicalCompressionRatio) -> String {
    match value {
        PhilosophicalCompressionRatio::Finite(value) => format!("{value:.2}x"),
        PhilosophicalCompressionRatio::Infinite => "infinity".to_string(),
        PhilosophicalCompressionRatio::Revoked => "revoked".to_string(),
        PhilosophicalCompressionRatio::Undefined => "NaN, but smug".to_string(),
    }
}

async fn open_db(
    path: &Path,
    plan_options: &PlanOptions,
    stream_kind: StreamKind,
) -> Result<PiLsmDb> {
    let (object_store, db_path) = open_local_store(path)?;
    let runtime = build_runtime(plan_options, stream_kind).await?;
    Ok(PiLsmDb::open(db_path, object_store, runtime.opts).await?)
}

async fn open_counted_db(
    path: &Path,
    plan_options: &PlanOptions,
    stream_kind: StreamKind,
) -> Result<(PiLsmDb, ObjectStoreCounters)> {
    let (object_store, db_path, counters) = open_counted_local_store(path)?;
    let runtime = build_runtime(plan_options, stream_kind).await?;
    Ok((
        PiLsmDb::open(db_path, object_store, runtime.opts).await?,
        counters,
    ))
}

fn open_local_store(path: &Path) -> Result<(Arc<dyn ObjectStore>, String)> {
    let (root, db_path) = split_db_path(path)?;
    std::fs::create_dir_all(&root)
        .with_context(|| format!("creating object-store root {}", root.display()))?;
    Ok((Arc::new(LocalFileSystem::new_with_prefix(&root)?), db_path))
}

fn open_counted_local_store(
    path: &Path,
) -> Result<(Arc<dyn ObjectStore>, String, ObjectStoreCounters)> {
    let (object_store, db_path) = open_local_store(path)?;
    let counters = ObjectStoreCounters::default();
    Ok((
        Arc::new(CountingObjectStore {
            inner: object_store,
            counters: counters.clone(),
        }),
        db_path,
        counters,
    ))
}

#[derive(Clone, Debug, Default)]
struct ObjectStoreCounters {
    inner: Arc<ObjectStoreCounterState>,
}

#[derive(Debug, Default)]
struct ObjectStoreCounterState {
    gets: AtomicU64,
    puts: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default)]
struct ObjectStoreCounts {
    gets: u64,
    puts: u64,
}

impl ObjectStoreCounters {
    fn add_get(&self) {
        self.inner.gets.fetch_add(1, Ordering::Relaxed);
    }

    fn add_put(&self) {
        self.inner.puts.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> ObjectStoreCounts {
        ObjectStoreCounts {
            gets: self.inner.gets.load(Ordering::Relaxed),
            puts: self.inner.puts.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone)]
struct CountingObjectStore {
    inner: Arc<dyn ObjectStore>,
    counters: ObjectStoreCounters,
}

impl std::fmt::Display for CountingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingObjectStore({})", self.inner)
    }
}

impl std::fmt::Debug for CountingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CountingObjectStore").finish()
    }
}

#[async_trait]
impl ObjectStore for CountingObjectStore {
    async fn put_opts(
        &self,
        location: &ObjectStorePath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> slatedb::object_store::Result<PutResult> {
        self.counters.add_put();
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectStorePath,
        opts: PutMultipartOptions,
    ) -> slatedb::object_store::Result<Box<dyn MultipartUpload>> {
        self.counters.add_put();
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(CountingMultipartUpload {
            inner,
            counters: self.counters.clone(),
        }))
    }

    async fn get_opts(
        &self,
        location: &ObjectStorePath,
        options: GetOptions,
    ) -> slatedb::object_store::Result<GetResult> {
        self.counters.add_get();
        self.inner.get_opts(location, options).await
    }

    async fn get_range(
        &self,
        location: &ObjectStorePath,
        range: Range<u64>,
    ) -> slatedb::object_store::Result<bytes::Bytes> {
        self.counters.add_get();
        self.inner.get_range(location, range).await
    }

    async fn get_ranges(
        &self,
        location: &ObjectStorePath,
        ranges: &[Range<u64>],
    ) -> slatedb::object_store::Result<Vec<bytes::Bytes>> {
        self.counters.add_get();
        self.inner.get_ranges(location, ranges).await
    }

    async fn head(&self, location: &ObjectStorePath) -> slatedb::object_store::Result<ObjectMeta> {
        self.counters.add_get();
        self.inner.head(location).await
    }

    async fn delete(&self, location: &ObjectStorePath) -> slatedb::object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&ObjectStorePath>,
    ) -> BoxStream<'static, slatedb::object_store::Result<ObjectMeta>> {
        self.counters.add_get();
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&ObjectStorePath>,
        offset: &ObjectStorePath,
    ) -> BoxStream<'static, slatedb::object_store::Result<ObjectMeta>> {
        self.counters.add_get();
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectStorePath>,
    ) -> slatedb::object_store::Result<ListResult> {
        self.counters.add_get();
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(
        &self,
        from: &ObjectStorePath,
        to: &ObjectStorePath,
    ) -> slatedb::object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &ObjectStorePath,
        to: &ObjectStorePath,
    ) -> slatedb::object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

struct CountingMultipartUpload {
    inner: Box<dyn MultipartUpload>,
    counters: ObjectStoreCounters,
}

impl std::fmt::Debug for CountingMultipartUpload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CountingMultipartUpload").finish()
    }
}

#[async_trait]
impl MultipartUpload for CountingMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.counters.add_put();
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> slatedb::object_store::Result<PutResult> {
        self.counters.add_put();
        self.inner.complete().await
    }

    async fn abort(&mut self) -> slatedb::object_store::Result<()> {
        self.inner.abort().await
    }
}

struct Runtime {
    opts: PiLsmOptions,
    supplier: CompactionSupplierBuilder,
}

struct CompactionSupplierBuilder {
    registry: StreamRegistry,
    planner: Planner,
}

impl CompactionSupplierBuilder {
    fn with_options(
        &self,
        mode: CompactionMode,
        strict_envelopes: bool,
        snapshot_safe_filtering: bool,
    ) -> Arc<PiLsmCompactionFilterSupplier> {
        Arc::new(
            PiLsmCompactionFilterSupplier::new(
                self.planner.clone(),
                Reconstructor::new(self.registry.clone()),
            )
            .with_mode(mode)
            .with_strict_envelopes(strict_envelopes)
            .with_snapshot_safe_filtering(snapshot_safe_filtering),
        )
    }
}

async fn build_runtime(plan_options: &PlanOptions, stream_kind: StreamKind) -> Result<Runtime> {
    let stream: Arc<dyn ByteStream> = match stream_kind {
        StreamKind::Sha256Counter => Arc::new(Sha256CounterStream::new([0_u8; 32])),
        StreamKind::PiPrefix => {
            Arc::new(pi_hex_fraction_prefix_stream(plan_options.max_prefix_len)?)
        }
    };
    let mut registry = StreamRegistry::new();
    registry.register(stream.clone());
    let index = Arc::new(
        StreamIndex::build(
            stream,
            StreamIndexOptions {
                max_prefix_len: plan_options.max_prefix_len,
                max_k: plan_options.max_k,
                max_index_memory_bytes: plan_options.max_index_memory_bytes,
                max_offsets_per_kgram: plan_options.max_offsets_per_kgram,
            },
        )
        .await?,
    );
    let planner = Planner::new(vec![index], registry.clone(), plan_options.clone());
    Ok(Runtime {
        opts: PiLsmOptions::new(registry.clone(), planner.clone()),
        supplier: CompactionSupplierBuilder { registry, planner },
    })
}

fn split_db_path(path: &Path) -> Result<(PathBuf, String)> {
    let root = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let Some(name) = path.file_name() else {
        bail!("database path must name a directory");
    };
    Ok((root.to_path_buf(), name.to_string_lossy().to_string()))
}

fn read_value(file: &Path) -> Result<Vec<u8>> {
    if file == Path::new("-") {
        let mut value = Vec::new();
        std::io::stdin().read_to_end(&mut value)?;
        return Ok(value);
    }
    std::fs::read(file).with_context(|| format!("reading {}", file.display()))
}

fn print_rewrite_status(status: RewriteStatus) {
    println!("status: {status:?}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_png_fixture_is_png_shaped_and_sized() {
        let png = generate_png_fixture(64 * 1024, 7).unwrap();
        assert!(png.starts_with(b"\x89PNG\r\n\x1a\n"));
        assert!(png.windows(4).any(|window| window == b"IHDR"));
        assert!(png.windows(4).any(|window| window == b"IDAT"));
        assert!(png.windows(4).any(|window| window == b"IEND"));
        assert!(png.len() >= 64 * 1024);
    }

    #[test]
    fn png_crc_and_zlib_checksums_match_known_values() {
        assert_eq!(crc32(b"IEND"), 0xae42_6082);
        assert_eq!(adler32(b""), 1);
    }

    #[test]
    fn undefined_philosophical_ratio_uses_spec_wording() {
        assert_eq!(
            philosophical_compression_ratio(PhilosophicalCompressionRatio::Undefined),
            "NaN, but smug"
        );
        assert_eq!(
            metric_philosophical_ratio(PhilosophicalCompressionRatio::Undefined),
            "NaN, but smug"
        );
    }
}
