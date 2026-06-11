use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

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
