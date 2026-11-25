use std::{env, path::PathBuf};

/// A fresh path owned by this test or benchmark under the invocation's scratch root.
pub fn database_dir() -> PathBuf {
    let root = env::var_os("VSDB_BASE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(env::temp_dir);
    root.join(format!(
        "vsdb-{}-{}",
        env!("CARGO_CRATE_NAME"),
        rand::random::<u128>()
    ))
}
