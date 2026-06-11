use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use pilsmer_core::{
    ByteStream, PlanOptions, Planner, Sha256CounterStream, StreamIndex, StreamIndexOptions,
    StreamRegistry,
};
use pilsmer_slate::{PiLsmDb, PiLsmOptions, RewriteStatus};
use slatedb::object_store::local::LocalFileSystem;

#[derive(Parser, Debug)]
#[command(name = "pilsmer")]
#[command(about = "A SlateDB-backed key-value store that locates your data elsewhere.")]
struct Cli {
    #[arg(long, default_value_t = 1_048_576)]
    prefix_bytes: u64,
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
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let plan_options = PlanOptions {
        max_prefix_len: cli.prefix_bytes,
        max_k: cli.max_k,
        allow_literals: cli.allow_literals,
        ..PlanOptions::default()
    };

    match cli.command {
        Command::Init { path } => {
            let db = open_db(&path, &plan_options).await?;
            db.close().await?;
        }
        Command::Put { path, key, file } => {
            let db = open_db(&path, &plan_options).await?;
            let value = read_value(&file)?;
            db.put(key.as_bytes(), value).await?;
            db.flush().await?;
            db.close().await?;
        }
        Command::Get { path, key } => {
            let db = open_db(&path, &plan_options).await?;
            let Some(value) = db.get(key.as_bytes()).await? else {
                bail!("key not found: {key}");
            };
            std::io::stdout().write_all(&value)?;
            db.close().await?;
        }
        Command::Explain { path, key } => {
            let db = open_db(&path, &plan_options).await?;
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
            let db = open_db(&path, &plan_options).await?;
            let report = db.plan_key(key.as_bytes(), plan_options).await?;
            print_rewrite_status(report.status);
            db.flush().await?;
            db.close().await?;
        }
        Command::VacuumMeaning { path, key } => {
            let db = open_db(&path, &plan_options).await?;
            let report = db.vacuum_meaning(key.as_bytes(), plan_options).await?;
            print_rewrite_status(report.status);
            db.flush().await?;
            db.close().await?;
        }
    }

    Ok(())
}

async fn open_db(path: &Path, plan_options: &PlanOptions) -> Result<PiLsmDb> {
    let (root, db_path) = split_db_path(path)?;
    std::fs::create_dir_all(&root)
        .with_context(|| format!("creating object-store root {}", root.display()))?;
    let object_store = Arc::new(LocalFileSystem::new_with_prefix(&root)?);
    let stream: Arc<dyn ByteStream> = Arc::new(Sha256CounterStream::new([0_u8; 32]));
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
    let opts = PiLsmOptions::new(registry, planner);
    Ok(PiLsmDb::open(db_path, object_store, opts).await?)
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
