![GitHub top language](https://img.shields.io/github/languages/top/rust-util-collections/VSDB)
[![Latest Version](https://img.shields.io/crates/v/VSDB.svg)](https://crates.io/crates/VSDB)
[![Rust Documentation](https://img.shields.io/badge/api-rustdoc-blue.svg)](https://docs.rs/VSDB)
![GitHub Workflow Status](https://img.shields.io/github/workflow/status/rust-util-collections/VSDB/Rust)
[![Minimum rustc version](https://img.shields.io/badge/rustc-1.63+-lightgray.svg)](https://github.com/rust-random/rand#rust-version-requirements)

# VSDB

VSDB is a 'Git' in the form of KV-database.

Based on the powerful version control function of VSDB, you can easily give your data structure the ability to version management.

**Make everything versioned !!**

[**To view the change log check here**](https://github.com/rust-util-collections/vsdb/blob/master/CHANGELOG.md).

### Highlights

- Support Git-like verison operations, such as:
    - Create countless branches and merge them to their parents
    - Rolling back a 'branch' to a specified historical 'version'
    - Querying the historical value of a key on the specified 'branch'
- Most APIs is similar as the coresponding data structures in the standard library
    - Use `Vecx` just like `Vec`
    - Use `Mapx` just like `HashMap`
    - Use `MapxOrd` just like `BTreeMap`
- ...

### Practical scenarios

- Process `CheckTx`, `DeliverTx`, `Commit` ... in 'Tendermint ABCI'
- Handle folk chain branches, e.g.:
   - Handling 'chain folk' or 'uncle blocks' in non-deterministic consensus like 'POW'
   - Handle temporary 'chain folk' in a hybrid consensus like 'Babe + Grandpa'(substrate)
- Support some special APIs of 'ETH Web3' in the form of 'trial run'
- ...

### Examples

Suppose you have a great algorithm like this:

```rust
struct GreatAlgo {
    a: Vec<...>,
    b: BTreeMap<...>,
    c: u128,
    d: HashMap<...>,
    e: ...
}
```

Simply replace the original structure with the corresponding VSDB data structure,
and your algorithm get the powerful version control ability at once!

```rust
#[dervive(Vs, Default)]
struct GreatAlgo {
    a: VecxVs<...>,
    b: MapxOrdVs<...>,
    c: OrphanVs<u128>,
    d: MapxVs<...>,
    e: ...
}

let algo = GreatAlgo::default();

algo.get_by_branch_version(...);
algo.branch_create(...);
algo.branch_create_by_base_branch(...);
algo.branch_create_by_base_branch_version(...);
algo.branch_remove(...);
algo.version_pop(...);
algo.prune();
```

**NOTE !!**

the `#[derive(Vs)]` macro can be applied to structures
whose internal fields are all types defined in VSDB
(primitive types and their collections are also supported),
but can not be applied to nesting wrapper among VSDB-types,
we recommend you to use the [**multi-key APIs**](src/versioned_multi_key)
if you indeed require these functions(better performance also),
or you will have to implement the `VsMgmt` trait manually.

This data structure can be handled correctly by `#[derive(Vs)]`:

```rust
#[derive(Vs)]
struct GoodCase<K, T> {
    a: VecxVs<i64>,
    b: SubItem0,
    c: SubItem1,
    d: SubItem2,
    e: u8,
    f: Vec<i16>,
    g: VecDeque<i64>,
    h: BTreeSet<u16>,
    i: HashMap<K, AtomicU64>,
    j: HashSet<i32>,
    k: LinkedList<()>,
    l: Box<dyn AsRef<bool>,
    m: Box<dyn AsRef<[Vec<u128>]>>,
    n: PhantomData<T>,
}

#[derive(Vs)]
struct SubItem0(MapxVs<u8, u8>, VecxVs<u8>);

#[derive(Vs)]
struct SubItem1 {
    a: OrphanVs<i16>,
    b: MapxOrdVs<String, u8>
}

#[derive(Vs)]
struct SubItem2 {
    a: i8,
    b: u128
}

// // A nope implementation of `VsMgmt` for custom stateless types.
// // the `#[derive(Vs)]` on 'SubItem2' is same as this implementation.
// impl VsMgmt for SubItem2 {
//     impl_vs_methods_nope!();
// }
```

**But** this one can NOT be handled correctly by `#[derive(Vs)]`:

```rust
// It can be compiled, but the result is wrong !
// The versioned methods of the inner 'MapxVs<u8, u8>' will missing,
// We recommend you to use the 'multi-key' APIs of VSDB, or
// you will have to implement the 'VsMgmt' trait manually.
#[derive(Vs)]
struct BadCase {
    a: VecxVs<MapxVs<u8, u8>>,
}
```

Please check the [**multi-key functions**](src/versioned_multi_key)
if you have requirements of the above or similar scenes.

Some complete examples:

- Versioned examples:
    - [**Simple blockchain**](examples/blockchain_state.rs)
    - [**Operations one by one**](src/versioned/mapx_raw/test.rs)
- Unversioned examples:
    - [**Simple web server**](examples/web_server.rs)
    - [**Operations one by one**](src/basic/mapx/test.rs)

### Compilation features

- [**default**] `sled_engine`, use sled as the backend database
    - Faster compilation speed
    - Faster running speed in the versioned functions
    - Support for compiling into a statically linked object
- `rocks_engine`, use rocksdb as the backend database
    - Faster running speed in the unversioned functions
    - Can not be compiled into a statically linked object
- [**default**] `msgpack_codec`, use msgpack as the codec
    - Faster running speed
- `bcs_codec`, use bcs as the codec
    - Created by the 'Libre' project of Facebook
    - Security reinforcement for blockchain scenarios
- [**default**] `derive`, enable the `Vs` procedural macro
- `compress`, enable compression in the backend database
- `extra_types`, implement `VsMgmt` for some common extra types
  - For example: `H256` and `H160` of the `primitive-types` crate

### NOTE

- The serialized result of a VSDB instance can not be used as the basis for distributed consensus
  - The serialized result only contains some meta-information(storage paths, etc.)
  - These meta-information are likely to be different in different environments
  - The correct way is to read what you need from it, and then process the real content
- Version names must be globally unique
  - Using a same version name on different branches is also not allowed
- The low-level "does not exist" expression has been changed from `None` to `[]` since v0.42.0
  - If you assign an empty value(`[u8;0]`, `&[]` ...) to a key, the key will be treated as 'deleted'

### LICENSE

- [**MIT**](https://choosealicense.com/licenses/mit) for v0.40 and earlier
- [**GPL-3.0**](../LICENSE) for v0.41 and later
