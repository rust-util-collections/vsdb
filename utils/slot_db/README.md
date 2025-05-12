# vsdb_slot_db

[![Crates.io](https://img.shields.io/crates/v/vsdb_slot_db.svg)](https://crates.io/crates/vsdb_slot_db)
[![Docs.rs](https://docs.rs/vsdb_slot_db/badge.svg)](https://docs.rs/vsdb_slot_db)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](../../LICENSE)
[![Rust](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml/badge.svg)](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml)

> A skip-list-like index for efficient, timestamp-based paged queries.

This crate provides `SlotDB`, a data structure that uses a tiered, skip-list-like index to enable fast pagination and range queries over large, ordered datasets. It is ideal for applications where data is associated with a "slot" (such as a timestamp or block number) and needs to be queried in pages.

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
vsdb_slot_db = "7.0.0"
```

## Usage

`SlotDB` provides an efficient way to query large, ordered datasets in pages.

```rust
use vsdb_slot_db::SlotDB;

// Create a new SlotDB with a tier capacity of 10 and normal order.
let mut db = SlotDB::<String>::new(10, false);

// Insert some keys into different slots.
db.insert(100, "entry_a".to_string()).unwrap();
db.insert(100, "entry_b".to_string()).unwrap();
db.insert(200, "entry_c".to_string()).unwrap();
db.insert(300, "entry_d".to_string()).unwrap();

// Check the total number of entries.
assert_eq!(db.total(), 4);

// Get entries by page.
// Get the first page with a size of 2, in reverse order.
let entries = db.get_entries_by_page(2, 0, true);
assert_eq!(entries, vec!["entry_d".to_string(), "entry_c".to_string()]);

// Get entries within a slot range.
let entries_in_slot = db.get_entries_by_page_slot(Some(100), Some(100), 10, 0, false);
assert_eq!(entries_in_slot.len(), 2);
assert!(entries_in_slot.contains(&"entry_a".to_string()));
assert!(entries_in_slot.contains(&"entry_b".to_string()));
```

## License

For API examples, see [API Examples](docs/api.md).

This project is licensed under the **MIT** license.
