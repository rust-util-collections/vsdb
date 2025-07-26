# vsdb_slot_db

[![Crates.io](https://img.shields.io/crates/v/vsdb_slot_db.svg)](https://crates.io/crates/vsdb_slot_db)
[![Docs.rs](https://docs.rs/vsdb_slot_db/badge.svg)](https://docs.rs/vsdb_slot_db)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](../../LICENSE)
[![Rust](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml/badge.svg)](https://github.com/rust-util-collections/vsdb/actions/workflows/rust.yml)

> A skip-list like index cache.

A `Skip List`-like index cache, based on the powerful [`vsdb`](https://crates.io/crates/vsdb) crate.

If you have a large key-value database and need high-performance pagination or data analysis, this crate could be a great tool for you.

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
vsdb_slot_db = "4.0.4"
```

## Usage

`SlotDB` is a skip-list-like index cache that can be used for high-performance pagination and data analysis.

```rust
use vsdb_slot_db::{SlotDB, Slot};

// Create a new SlotDB with a maximum of 16 entries per slot
let mut db: SlotDB<String> = SlotDB::new(16, false);

// Insert some data with slot numbers
db.insert(1, "data1".to_string()).unwrap();
db.insert(1, "data2".to_string()).unwrap();
db.insert(2, "data3".to_string()).unwrap();

// Get entries by page
let page1 = db.get_entries_by_page(10, 0, false);
assert_eq!(page1.len(), 3);

// Get entries by page with a specific slot range
let page2 = db.get_entries_by_page_slot(Some(1), Some(1), 10, 0, false);
assert_eq!(page2.len(), 2);
assert_eq!(page2[0], "data1");
assert_eq!(page2[1], "data2");
```

## License

This project is licensed under the **MIT** license.