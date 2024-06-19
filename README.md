![GitHub top language](https://img.shields.io/github/languages/top/rust-util-collections/VSDB)
[![Rust](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml/badge.svg)](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml)
[![Minimum rustc version](https://img.shields.io/badge/rustc-1.70+-lightgray.svg)](https://github.com/rust-random/rand#rust-version-requirements)

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

|Name|Version|Doc|Path|Description|
|:-|:-|:-|:-|:-|
|[**vsdb**](wrappers)|[![](https://img.shields.io/crates/v/vsdb.svg)](https://crates.io/crates/vsdb)|[![](https://img.shields.io/badge/api-rustdoc-blue.svg)](https://docs.rs/vsdb)|`./wrappers`|High-level APIs|
|[**vsdb_core**](core)|[![](https://img.shields.io/crates/v/vsdb_core.svg)](https://crates.io/crates/vsdb_core)|[![](https://img.shields.io/badge/api-rustdoc-blue.svg)](https://docs.rs/vsdb_core)|`./core`|Low-level implementations|
|[**vsdb_derive**](derive)|[![](https://img.shields.io/crates/v/vsdb_derive.svg)](https://crates.io/crates/vsdb_derive)|[![](https://img.shields.io/badge/api-rustdoc-blue.svg)](https://docs.rs/vsdb_derive)|`./derive`|Procedure macro collection|
|[**vsdb_trie_db**](utils/trie_db)|[![](https://img.shields.io/crates/v/vsdb_trie_db.svg)](https://crates.io/crates/vsdb_trie_db)|[![](https://img.shields.io/badge/api-rustdoc-blue.svg)](https://docs.rs/vsdb_trie_db)|`./utils/trie_db`|trie based structures with </br> limited version capabilities|
|[**vsdb_slot_db**](utils/slot_db)|[![](https://img.shields.io/crates/v/vsdb_slot_db.svg)](https://crates.io/crates/vsdb_slot_db)|[![](https://img.shields.io/badge/api-rustdoc-blue.svg)](https://docs.rs/vsdb_slot_db)|`./utils/slot_db`|A skip-list like timestamp DB|

### Gratitude

Thanks to all the people who already contributed!

<a href="https://github.com/rust-util-collections/vsdb/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=rust-util-collections/vsdb"/>
</a>

### LICENSE

- [**MIT**](https://choosealicense.com/licenses/mit) for v0.40 and earlier
- [**GPL-3.0**](LICENSE) for v0.41 and later
