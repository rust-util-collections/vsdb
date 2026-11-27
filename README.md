# VSDB

[![Crates.io](https://img.shields.io/crates/v/vsdb.svg)](https://crates.io/crates/vsdb)
[![Docs.rs](https://docs.rs/vsdb/badge.svg)](https://docs.rs/vsdb)
[![Rust](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml/badge.svg)](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml)

VSDB is an embedded Rust database. Use `vsdb` for typed maps, branch history,
Merkle proofs, slot pagination, parent overlays and vector search. Use
`vsdb_core` directly for byte keys and values or namespace administration.
[Choose a collection](strata/README.md#choosing-a-collection) · [Raw storage API](core/README.md)

## Quick start

From your Rust application, add the crate from a local checkout. The supported
architectures are `x86_64` and `aarch64`. Building MMDB's bundled Zstd backend
requires a C toolchain.

```sh
cargo add vsdb --path /path/to/vsdb/strata
```

Set `VSDB_BASE_DIR` before running the application to choose where it stores
data. With no explicit configuration, VSDB uses that variable, then
`$HOME/.vsdb`, then a per-process temporary path. [Configuration](core/docs/api.md#configure-the-universe)

```rust
use vsdb::{MapxOrd, Result};

fn main() -> Result<()> {
    let mut counts = MapxOrd::<String, u64>::new();
    let mut batch = counts.batch();
    batch.insert(&String::from("apples"), &3);
    batch.commit_sync()?;

    let id = counts.save_meta()?;
    drop(counts);
    let restored = MapxOrd::<String, u64>::from_meta(id)?;
    assert_eq!(restored.get("apples"), Some(3));
    Ok(())
}
```

`commit_sync()` makes the batch durable on the map's shard. `save_meta()`
saves the handle in the database and returns an `InstanceId`. Keep that ID in
application storage to reopen the map in a later process using the same database.

## Documentation

- Browse the application guides, database operations and storage design in the [documentation index](docs/README.md).
- Reopen a native v18 database in v19, or convert an older format with the [upgrade guide](docs/upgrade.md).
- Develop and test the workspace with the [contributor guide](AGENTS.md). Release changes are recorded in the [changelog](CHANGELOG.md).

## License

[MIT](LICENSE)
