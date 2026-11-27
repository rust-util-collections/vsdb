# VSDB

[![Crates.io](https://img.shields.io/crates/v/vsdb.svg)](https://crates.io/crates/vsdb)
[![Docs.rs](https://docs.rs/vsdb/badge.svg)](https://docs.rs/vsdb)
[![Rust](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml/badge.svg)](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml)

VSDB is an embedded Rust database. Use `vsdb` for typed maps, branch history,
Merkle proofs, slot pagination, parent overlays and vector search. Use
`vsdb_core` directly for byte keys and values or namespace administration.
[Choose a collection](strata/README.md#choosing-a-collection) · [Raw storage API](core/README.md)

## Quick start

```sh
cargo add vsdb@19
```

VSDB supports `x86_64` and `aarch64`. Building MMDB's bundled Zstd backend
requires a C toolchain.

Set `VSDB_BASE_DIR` to choose the database directory. [Default paths and configuration](core/docs/api.md#configure-the-universe)

```rust
use vsdb::{Mapx, Result};

type Stock = Mapx<String, u64>;

fn main() -> Result<()> {
    let mut stock = Stock::new();
    stock.insert(&"apples".into(), &3);
    stock.try_sync_wal()?;

    let id = stock.save_meta()?;
    let reopened = Stock::from_meta(id)?;
    assert_eq!(reopened.get("apples"), Some(3));
    Ok(())
}
```

`try_sync_wal()` makes the write durable. Keep the `InstanceId` returned by
`save_meta()` to reopen the map in a later process using the same database.

## Documentation

- [Application and operations guides](docs/README.md)
- [Upgrade an existing database](docs/upgrade.md)
- [Contributor guide](AGENTS.md) · [Changelog](CHANGELOG.md)

## License

[MIT](LICENSE)
