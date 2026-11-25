//! Stage a verified VSDB universe on MMDB v5 at a previously absent path.
use std::{env, error::Error, path::PathBuf};

fn main() -> Result<(), Box<dyn Error>> {
    let paths: Vec<PathBuf> = env::args_os().skip(1).map(PathBuf::from).collect();
    let [source, destination] = paths.as_slice() else {
        return Err("usage: vsdb-stage-v5 SOURCE GENERATION".into());
    };
    vsdb_migrate::stage_universe(source, destination)?;
    println!("Verified VSDB base: {}", destination.join("base").display());
    Ok(())
}
