![GitHub top language](https://img.shields.io/github/languages/top/rust-util-collections/VSDB)
[![Latest Version](https://img.shields.io/crates/v/vsdb_slot_db.svg)](https://crates.io/crates/vsdb_slot_db)
[![Rust Documentation](https://img.shields.io/badge/api-rustdoc-blue.svg)](https://docs.rs/vsdb_slot_db)
[![Minimum rustc version](https://img.shields.io/badge/rustc-1.78+-lightgray.svg)](https://github.com/rust-random/rand#rust-version-requirements)

# Slot DB

A `Skip List`-like index cache, based on the powerful [`vsdb`](https://crates.io/crates/vsdb) crate.

If you have a big key-value database, and you need high-performance pagination display or data analysis based on that data, then this crate may be a great tool for you.

## Usage

For examples, please check [**the embed test cases**](src/test.rs).
