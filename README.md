![](https://tokei.rs/b1/github/ccmlm/vsdb)
![GitHub top language](https://img.shields.io/github/languages/top/ccmlm/vsdb)

# VSDB

VSDB is a 'Git' in the form of a KV database.

Based on the powerful version control function of VSDB, you can easily give your data structure the ability to version management.

**Make everything versioned !!**

## Highlights

- Support Git-like verison operations, such as:
    - Create countless branches and merge them to their parents
    - Rolling back a 'branch' to a specified historical 'version'
    - Querying the historical value of a key on the specified 'branch'
- Most APIs is similar as the coresponding data structures in the standard library
    - Use `Vecx` just like `Vec`
    - Use `Mapx` just like `HashMap`
    - Use `MapxOrd` just like `BTreeMap`
- ...

## Examples

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
struct GreatAlgo {
    a: VecxVs<...>,
    b: MapxOrdVs<...>,
    c: OrphanVs<u128>,
    d: MapxVs<...>,
    e: ...
}

impl GreatAlgo {
    fn version_create(&self, name: &str) {
        self.a.version_create(VersionName(name)).unwrap();
        self.b.version_create(VersionName(name)).unwrap();
        self.c.version_create(VersionName(name)).unwrap();
        self.d.version_create(VersionName(name)).unwrap();
        ...
    }
    fn branch_create(&self, name: &str) {
        self.a.branch_create(BranchName(name)).unwrap();
        self.b.branch_create(BranchName(name)).unwrap();
        self.c.branch_create(BranchName(name)).unwrap();
        self.d.branch_create(BranchName(name)).unwrap();
        ...
    }
    ...
}
```

> Some complete examples:
>
> - Versioned examples:
>     - [**Simple blockchain**](tests/blockchain_state.rs)
>     - [**Operations one by one**](src/versioned/mapx_raw/test.rs)
> - Unversioned examples:
>     - [**Simple web server**](tests/web_server.rs)
>     - [**Operations one by one**](src/basic/mapx/test.rs)

## Compilation features

- [**default**] `sled_engine`, use sled as the backend database
    - Faster compilation speed
    - Support for compiling into a statically linked binary
- `rocks_engine`, use rocksdb as the backedn database
    - Faster running speed
    - Can not be compiled into a statically linked binary
- [**default**] `cbor_ende`, use cbor as the `en/de`coder
    - Faster running speed
- `bcs_ende`, use bcs as the `en/de`coder
    - Created by the libre project of facebook
    - Security reinforcement for blockchain scenarios

## Low-level design

Based on the underlying one-dimensional linear storage structure (native kv-database, such as sled/rocksdb, etc.), multiple different namespaces are divided, and then abstract each dimension in the multi-dimensional logical structure based on these divided namespaces.

In the category of kv-database, namespaces can be expressed as different key ranges, or different key prefix.

This is the same as expressing complex data structures in computer memory(the memory itself is just a one-dimensional linear structure).

User data will be divided into two dimensions: 'branch' and 'version', the functions of the 'basic' category are stateless, and the functions of the 'versioned' category are stateful. In the internal implementation, each stateful function is implemented based on its corresponding stateless function,
all stateful data has two additional identification dimensions ('branch' and 'version'), somewhat like the logic in Git. Stateless functions do not have the feature of 'version' management, but they have higher performance.
