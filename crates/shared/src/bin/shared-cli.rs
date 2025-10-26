use anyhow::{Context, Result};
use clap::Parser;
use std::path::PathBuf;
use tangent_shared::Config;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Path to config file (YAML)
    #[arg(value_name = "CONFIG", default_value = "tangent.yaml")]
    config: PathBuf,

    #[arg(long)]
    json: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let cfg = Config::from_file(&args.config)
        .with_context(|| format!("loading config from {}", &args.config.display()))?;

    let sources = &cfg.sources;
    let batch_kb = cfg.batch_size_kb();
    let batch_bytes = batch_kb << 10;
    let batch_age_ms = cfg.batch_age_ms();
    let workers = cfg.runtime.workers;

    if args.json {
        println!(
            "{{
              \"sources\":\"{c:?}\",\"batch_size_kb\":{kb},\"batch_size_bytes\":{bytes},\
              \"batch_age_ms\":{age:?},\"workers\":{workers}}}",
            c = sources,
            kb = batch_kb,
            bytes = batch_bytes,
            age = batch_age_ms,
            workers = workers
        );
    } else {
        println!("Config:");
        println!("  consumers : {:?}", sources);
        println!("  batch_size  : {} KB ({} bytes)", batch_kb, batch_bytes);
        println!("  batch_age   : {:?} ms", batch_age_ms);
        println!("  workers     : {}", workers);
    }

    Ok(())
}
