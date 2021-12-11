![](https://tokei.rs/b1/github/ccmlm/vsdb)
![GitHub top language](https://img.shields.io/github/languages/top/ccmlm/vsdb)
![GitHub issues](https://img.shields.io/github/issues-raw/ccmlm/vsdb)
![GitHub pull requests](https://img.shields.io/github/issues-pr-raw/ccmlm/vsdb)

# VSDB

VSDB, **V**ersioned **S**tateful **D**ata**B**ase, mainly used in blockchain scene.

## Highlights

- Support GIT-like verison operations, such as:
    - Rolling back a 'branch' to a specified historical 'version'
    - Querying the historical value of a key on the specified 'branch'
    - Merge branches(different data versions) just like 'git merge BRANCH'
    - ...
- The definition of most APIs is same as the coresponding data structures of the standard library
    - Use `Vecx` just like `Vec`, but data will be automatically stored in disk
    - Use `Mapx` just like `HashMap`, but data will be automatically stored in disk
    - Use `MapxOrd` just like `BTreeMap`, but data will be automatically stored in disk
    - ...

## Design concept

Based on the underlying one-dimensional linear storage structure (native kv-database, such as sled/rocksdb, etc.), multiple different namespaces are divided, and then abstract each dimension in the multi-dimensional logical structure based on these divided namespaces.

In the category of kv-database, namespaces can be expressed as different key ranges, or different key prefix.

This is the same as expressing complex data structures in computer memory, you know, the memory itself is just a one-dimensional linear structure.

User data will be divided into two dimensions: 'branch' and 'version', the functions of the 'basic' category are stateless, and the functions of the 'versioned' category are stateful.

In the internal implementation, each stateful function is implemented based on its corresponding stateless function,
all stateful data has two additional identification dimensions ('branch' and 'version'), somewhat like the logic in GIT.

Stateless functions do not have the feature of 'version' management, but they have higher performance.

## Compilation features

- [**default**] `sled_engine`, use sled as the backend database
- `rocks_engine`, use rocksdb as the backedn database
- [**default**] `cbor_ende`, use cbor as the `en/de`coder
- `bcs_ende`, use bcs(created by the facebook libre project) as the `en/de`coder
