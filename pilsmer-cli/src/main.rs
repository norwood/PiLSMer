use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use pilsmer_core::{
    pi_hex_fraction_prefix_stream, ByteStream, PlanOptions, Planner, Reconstructor,
    Sha256CounterStream, StreamIndex, StreamIndexOptions, StreamRegistry,
    PI_HEX_FRACTION_PREFIX_BYTES,
};
use pilsmer_slate::{
    run_compactor_with_options, CompactionMode, PiLsmCompactionFilterSupplier,
    PiLsmCompactorOptions, PiLsmDb, PiLsmOptions, RewriteStatus,
};
use slatedb::object_store::local::LocalFileSystem;
use slatedb::object_store::ObjectStore;
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
    Explain {
        path: PathBuf,
        key: String,
    },
    PlanKey {
        path: PathBuf,
        key: String,
    },
    VacuumMeaning {
        path: PathBuf,
        key: String,
    },
    Compact {
        path: PathBuf,
        #[arg(long, default_value_t = 1000)]
        run_ms: u64,
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

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let stream_kind = cli.stream;
    let plan_options = PlanOptions {
        max_prefix_len: cli
            .prefix_bytes
            .unwrap_or_else(|| default_prefix_bytes(stream_kind)),
        max_k: cli.max_k,
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
        Command::Explain { path, key } => {
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
            println!(
                "philosophical_user_value_bytes_stored: {}",
                explain.philosophical_user_value_bytes_stored
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
        Command::VacuumMeaning { path, key } => {
            let db = open_db(&path, &plan_options, stream_kind).await?;
            let report = db.vacuum_meaning(key.as_bytes(), plan_options).await?;
            print_rewrite_status(report.status);
            db.flush().await?;
            db.close().await?;
        }
        Command::Compact {
            path,
            run_ms,
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
            let mut compact_plan_options = plan_options.clone();
            let mode = compact_mode(mode, into_nonexistence, humiliation)?;
            if matches!(mode, CompactionMode::ForceRawToPlan) && compact_plan_options.allow_literals
            {
                bail!("--allow-literals conflicts with forced compaction into plans");
            }
            if humiliation == Some(Humiliation::Maximum) {
                compact_plan_options.max_k = 1;
            }
            let (object_store, db_path) = open_local_store(&path)?;
            let runtime = build_runtime(&compact_plan_options, stream_kind).await?;
            let supplier = runtime.supplier.with_options(
                mode,
                strict_envelopes,
                !ignore_snapshot_representation_safety,
            );
            run_compactor_with_options(
                db_path,
                object_store,
                supplier,
                PiLsmCompactorOptions {
                    run_for: Duration::from_millis(run_ms),
                    poll_interval: Duration::from_millis(poll_ms),
                    min_compaction_sources,
                },
            )
            .await?;
        }
        Command::Bench { path, values, size } => {
            run_bench(&path, values, size, &plan_options, stream_kind).await?;
        }
    }

    Ok(())
}

fn default_prefix_bytes(stream_kind: StreamKind) -> u64 {
    match stream_kind {
        StreamKind::Sha256Counter => 1_048_576,
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

async fn run_bench(
    path: &Path,
    value_count: usize,
    value_size: usize,
    plan_options: &PlanOptions,
    stream_kind: StreamKind,
) -> Result<()> {
    if value_count == 0 {
        bail!("--values must be at least 1");
    }
    if value_size == 0 {
        bail!("--size must be at least 1");
    }
    if path.exists() {
        bail!("bench path already exists: {}", path.display());
    }

    let values = generate_values(value_count, value_size).await?;
    let plain = bench_plain_slate(&path.join("plain-slate"), &values).await?;
    let raw = bench_pilsmer_raw(
        &path.join("pilsmer-raw"),
        &values,
        plan_options,
        stream_kind,
    )
    .await?;
    let planned = bench_pilsmer_planned(
        &path.join("pilsmer-planned"),
        &values,
        plan_options,
        stream_kind,
    )
    .await?;

    println!("values: {value_count}");
    println!("value_size: {value_size}");
    println!(
        "workload\tput_ms\tplan_ms\tread_ms\tlogical_bytes\tphysical_value_bytes\tchunks\tmetadata_amp"
    );
    print_bench_row(&plain);
    print_bench_row(&raw);
    print_bench_row(&planned);
    Ok(())
}

#[derive(Clone, Debug)]
struct BenchResult {
    workload: &'static str,
    put_ms: u128,
    plan_ms: Option<u128>,
    read_ms: u128,
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

async fn generate_values(value_count: usize, value_size: usize) -> Result<Vec<Vec<u8>>> {
    let stream = Sha256CounterStream::new([1_u8; 32]);
    let mut values = Vec::with_capacity(value_count);
    for ix in 0..value_count {
        let offset = ix
            .checked_mul(value_size)
            .context("benchmark value offset overflow")?;
        values.push(stream.read_at(offset as u64, value_size).await?.to_vec());
    }
    Ok(values)
}

async fn bench_plain_slate(path: &Path, values: &[Vec<u8>]) -> Result<BenchResult> {
    let (object_store, db_path) = open_local_store(path)?;
    let db = Db::open(db_path, object_store).await?;

    let put_start = Instant::now();
    for (ix, value) in values.iter().enumerate() {
        db.put(key_bytes(ix), value.as_slice()).await?;
    }
    db.flush().await?;
    let put_ms = put_start.elapsed().as_millis();

    let read_start = Instant::now();
    let mut logical_bytes = 0_u64;
    for ix in 0..values.len() {
        let Some(value) = db.get(key_bytes(ix)).await? else {
            bail!("missing plain SlateDB bench key {ix}");
        };
        logical_bytes += value.len() as u64;
    }
    let read_ms = read_start.elapsed().as_millis();
    db.close().await?;

    Ok(BenchResult {
        workload: "plain-slate",
        put_ms,
        plan_ms: None,
        read_ms,
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
    let db = open_db(path, plan_options, stream_kind).await?;

    let put_start = Instant::now();
    for (ix, value) in values.iter().enumerate() {
        db.put(key_bytes(ix), value.as_slice()).await?;
    }
    db.flush().await?;
    let put_ms = put_start.elapsed().as_millis();

    let read_start = Instant::now();
    let mut logical_bytes = 0_u64;
    let mut physical_value_bytes = 0_u64;
    for ix in 0..values.len() {
        let key = key_bytes(ix);
        let Some(value) = db.get(&key).await? else {
            bail!("missing PiLSMer raw bench key {ix}");
        };
        logical_bytes += value.len() as u64;
        let Some(explain) = db.explain(&key).await? else {
            bail!("missing PiLSMer raw explain key {ix}");
        };
        physical_value_bytes += explain.physical_value_bytes;
    }
    let read_ms = read_start.elapsed().as_millis();
    db.close().await?;

    Ok(BenchResult {
        workload: "pilsmer-raw",
        put_ms,
        plan_ms: None,
        read_ms,
        logical_bytes,
        physical_value_bytes: Some(physical_value_bytes),
        chunks: None,
    })
}

async fn bench_pilsmer_planned(
    path: &Path,
    values: &[Vec<u8>],
    plan_options: &PlanOptions,
    stream_kind: StreamKind,
) -> Result<BenchResult> {
    let db = open_db(path, plan_options, stream_kind).await?;

    let put_start = Instant::now();
    for (ix, value) in values.iter().enumerate() {
        db.put(key_bytes(ix), value.as_slice()).await?;
    }
    db.flush().await?;
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

    let read_start = Instant::now();
    let mut logical_bytes = 0_u64;
    let mut physical_value_bytes = 0_u64;
    let mut chunks = 0_u64;
    for ix in 0..values.len() {
        let key = key_bytes(ix);
        let Some(value) = db.get(&key).await? else {
            bail!("missing PiLSMer planned bench key {ix}");
        };
        logical_bytes += value.len() as u64;
        let Some(explain) = db.explain(&key).await? else {
            bail!("missing PiLSMer planned explain key {ix}");
        };
        physical_value_bytes += explain.physical_value_bytes;
        chunks += explain.chunks;
    }
    let read_ms = read_start.elapsed().as_millis();
    db.close().await?;

    Ok(BenchResult {
        workload: "pilsmer-planned",
        put_ms,
        plan_ms: Some(plan_ms),
        read_ms,
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
        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        result.workload,
        result.put_ms,
        optional_u128(result.plan_ms),
        result.read_ms,
        result.logical_bytes,
        optional_u64(result.physical_value_bytes),
        optional_u64(result.chunks),
        optional_ratio(result.metadata_amp()),
    );
}

fn optional_u128(value: Option<u128>) -> String {
    value.map_or_else(|| "-".to_string(), |value| value.to_string())
}

fn optional_u64(value: Option<u64>) -> String {
    value.map_or_else(|| "-".to_string(), |value| value.to_string())
}

fn optional_ratio(value: Option<f64>) -> String {
    value.map_or_else(|| "-".to_string(), |value| format!("{value:.2}x"))
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

fn open_local_store(path: &Path) -> Result<(Arc<dyn ObjectStore>, String)> {
    let (root, db_path) = split_db_path(path)?;
    std::fs::create_dir_all(&root)
        .with_context(|| format!("creating object-store root {}", root.display()))?;
    Ok((Arc::new(LocalFileSystem::new_with_prefix(&root)?), db_path))
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
