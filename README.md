![GitHub top language](https://img.shields.io/github/languages/top/rust-util-collections/VSDB)
[![Minimum rustc version](https://img.shields.io/badge/rustc-1.65+-lightgray.svg)](https://github.com/rust-random/rand#rust-version-requirements)
![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/rust-util-collections/vsdb/rust.yml?branch=master)

# VSDB

VSDB is a 'Git' in the form of KV-database.

Some known practical scenarios:

- Process `CheckTx`, `DeliverTx`, `Commit` ... in 'Tendermint ABCI'
- Handle folk chain branches, e.g.:
   - Handling 'chain folk' or 'uncle blocks' in non-deterministic consensus like 'POW'
   - Handle temporary 'chain folk' in a hybrid consensus like 'Babe + Grandpa'(substrate)
- Support some special APIs of 'ETH Web3' in the form of 'trial run'
- ...

Check [**here**](wrappers/README.md) for a detailed description.

### Crate List

|Name|Version|Doc|Description|
|:-|:-|:-|:-|
|[**vsdb**](wrappers)|[![](https://img.shields.io/crates/v/vsdb.svg)](https://crates.io/crates/vsdb)|[![](https://img.shields.io/badge/api-rustdoc-blue.svg)](https://docs.rs/vsdb)|`./wrappers` High-level APIs|
|[**vsdb_core**](core)|[![](https://img.shields.io/crates/v/vsdb_core.svg)](https://crates.io/crates/vsdb_core)|[![](https://img.shields.io/badge/api-rustdoc-blue.svg)](https://docs.rs/vsdb_core)|`./core` Low-level implementations|
|[**vsdb_derive**](derive)|[![](https://img.shields.io/crates/v/vsdb_derive.svg)](https://crates.io/crates/vsdb_derive)|[![](https://img.shields.io/badge/api-rustdoc-blue.svg)](https://docs.rs/vsdb_derive)|`./derive` Procedure macro collection|

### Gratitude

Thanks to all the people who already contributed!

<a href="https://github.com/rust-util-collections/vsdb/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=rust-util-collections/vsdb"/>
</a>

### LICENSE

- [**MIT**](https://choosealicense.com/licenses/mit) for v0.40 and earlier
- [**GPL-3.0**](LICENSE) for v0.41 and later
