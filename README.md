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

## Choose a collection

| If you need | Use |
| --- | --- |
| Key/value lookups, ordered scans, raw byte keys, or one persisted value | `Mapx`, `MapxOrd`, `MapxOrdRawKey`, or `Orphan`. [Collection examples](strata/docs/api.md) |
| Named branches, commits, snapshots, diff, merge, and rollback | `VerMap`. [Versioned storage](strata/docs/versioned.md) |
| A Merkle root or a membership/absence proof | `MptCalc` or `SmtCalc` for in-memory computation. `VerMapWithProof` computes roots and proofs over a branch or commit. [Trie examples](strata/docs/api.md) |
| Forward or descending pages over ordered slots such as timestamps | `SlotDex` stores entries and tier counts in one prefix. [Slot index examples](strata/docs/api.md#slotdex) |
| Child maps that inherit a parent's values until overridden or deleted | `DagMap` and its raw variants store each connected overlay tree on one shard. [DAG map example](strata/docs/api.md#dagmap) |
| Approximate nearest neighbors with optional key filters | `VecDex` indexes vectors with HNSW. `VecDexDyn` selects the metric at runtime. [Vector index guide](strata/docs/vecdex.md) |

### Branches and commits

```rust
use vsdb::{Result, VerMap};

fn main() -> Result<()> {
    let mut map = VerMap::<u32, String>::new();
    let main = map.main_branch();
    map.insert(main, &1, &String::from("first"))?;
    let commit = map.commit(main)?;
    let original = map.at(commit)?;

    let feature = map.create_branch("feature", main)?;
    map.insert(feature, &1, &String::from("updated"))?;
    map.commit(feature)?;
    map.merge(feature, main)?;
    assert_eq!(map.get(main, &1)?.as_deref(), Some("updated"));
    assert_eq!(original.get(&1).as_deref(), Some("first"));
    Ok(())
}
```

A branch's working edits become durable history at commit. Snapshots pin the
tree root they captured and support reads while the branch advances.
[Durability and recovery](strata/docs/versioned.md#durability-and-recovery)

## Storage configuration

### Namespaces

A namespace owns an MMDB engine. The default engine has 16 shards. Additional
namespaces can choose a directory, shard count and memory budget at creation.
`ns.scope(|| Mapx::new())` and `Mapx::new_in(&ns)` select the engine for a new
collection. Reads and restores use the namespace recorded in its handle.
Ordinary batches publish within one prefix, and a checked co-located insert
can publish across two prefixes on one shard. [Namespace design](docs/namespaces.md)

### Read-only mode

Configure `VsdbOptions::read_only(path)` before opening a completed universe
to query it on a read-only filesystem. The setting applies to all namespaces
in the process. [Read-only guide](strata/docs/read-only.md)

### Memory sizing

The default engine budget is 2 GiB. Each additional namespace defaults to
512 MiB. These budgets size caches and write buffers. Process memory also
includes pinned blocks, runtime metadata and application allocations.
[Memory sizing and overrides](docs/storage.md#memory-sizing)

## Documentation

- [v18 design and measured performance](docs/v18.md) — storage behavior, migration guarantees and operation-specific upgrade results.
- [Documentation index](docs/README.md) — collection APIs, configuration and benchmark data.
- [Working on VSDB](AGENTS.md) — workspace layout, build commands, conventions and project skills.
- [Changelog](CHANGELOG.md) — release history and v18 API changes.

## License

[MIT](LICENSE)
