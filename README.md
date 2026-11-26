![GitHub top language](https://img.shields.io/github/languages/top/rust-util-collections/vsdb)
[![Crates.io](https://img.shields.io/crates/v/vsdb.svg)](https://crates.io/crates/vsdb)
[![Docs.rs](https://docs.rs/vsdb/badge.svg)](https://docs.rs/vsdb)
[![Rust](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml/badge.svg)](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml)

# vsdb

VSDB is an embedded Rust database with persistent collections, versioned maps
and specialized indexes. Applications use the `vsdb` crate in `strata/`. The storage engine
is MMDB, a Rust LSM tree whose bundled Zstd backend requires a C toolchain.
[Crate guide](strata/README.md)

VSDB guarantees correct behavior on 64-bit x86 (`x86_64`) and 64-bit Arm
(`aarch64`). Other architectures carry no testing or correctness guarantee.

**Upgrading an existing store?** VSDB v18 now uses MMDB v5. With all
processes stopped, stage a v17/v4 or v18/v4 universe with `vsdb-stage-v5`.
Select the verified generation's `base` path. For v17 data, also convert
saved and embedded collection handles before resuming writes. Fresh v18/v5
universes can be created directly. [Migration guide](migration/README.md)

## Quick start

To try the v18 workspace from a local checkout, add the high-level crate to
your application with `cargo add vsdb --path /path/to/vsdb/strata`.

```rust
use vsdb::{MapxOrd, Result};

fn main() -> Result<()> {
    let mut counts = MapxOrd::<String, u64>::new();
    let mut batch = counts.batch();
    batch.insert(&String::from("apples"), &3);
    batch.commit_sync()?;

    // Persist this token to reopen the same collection in this universe.
    let id = counts.save_meta()?;
    let restored = MapxOrd::<String, u64>::from_meta(id)?;
    assert_eq!(restored.get("apples"), Some(3));
    Ok(())
}
```

Set `VSDB_BASE_DIR` or call `vsdb_configure(VsdbOptions::new(path))` before
first use to choose the database directory. The default is `$HOME/.vsdb`.
[Configuration and raw map examples](core/docs/api.md)

## Documentation

- [Choose a collection](strata/README.md#choosing-a-collection) for ordered scans, version history, proofs, overlays, pagination or vector search.
- [v18 architecture](docs/v18.md) — diagrams of shard routing, batch publication, version history and offline migration.
- [Documentation index](docs/README.md) — API examples, namespace placement, read-only deployment, memory sizing and benchmarks.
- [Working on VSDB](AGENTS.md) — workspace layout, build commands, conventions and project skills.
- [Changelog](CHANGELOG.md) — release history and v18 API changes.

## License

[MIT](LICENSE)
