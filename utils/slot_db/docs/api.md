# vsdb_slot_db API Examples

This document provides examples for the public APIs in the `vsdb_slot_db` crate.

## SlotDB

`SlotDB` is a skip-list-like data structure designed for fast paged queries.

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

// Remove an entry.
db.remove(100, &"entry_a".to_string());
assert_eq!(db.total(), 3);

// Clear the database.
db.clear();
assert_eq!(db.total(), 0);
```
