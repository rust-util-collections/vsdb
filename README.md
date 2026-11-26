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

VSDB v19 opens native v18 universes in place. The [v18 maintenance branch](https://github.com/rust-util-collections/vsdb/tree/v18)
ships 18.x releases and maintains the converter for upgrading v17 data to v18.
[Upgrade guide](docs/README.md#upgrade-an-existing-universe)

## Quick start

To try the workspace from a local checkout, add the high-level crate to
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
- [Storage architecture](docs/v18.md) — diagrams of shard routing, batch publication and version history.
- [Documentation index](docs/README.md) — API examples, namespace placement, read-only deployment, memory sizing and benchmarks.
- [Working on VSDB](AGENTS.md) — workspace layout, build commands, conventions and project skills.
- [Changelog](CHANGELOG.md) — release history and API changes.

## License

[MIT](LICENSE)
