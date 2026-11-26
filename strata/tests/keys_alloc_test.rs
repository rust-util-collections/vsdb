//! Key iteration skips value bytes instead of copying them.
#[path = "common/scratch.rs"]
mod scratch;

use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
};
use vsdb::{MapxOrdRawKey, VsdbOptions, vsdb_configure};

thread_local! {
    static ALLOCATED: Cell<usize> = const { Cell::new(0) };
}

struct Counting;

// SAFETY: every call forwards to `System` with the caller's arguments and
// only updates a const-initialized thread-local counter without a destructor.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = ALLOCATED.try_with(|n| n.set(n.get() + layout.size()));
        // SAFETY: the caller upholds `GlobalAlloc::alloc`'s contract for `layout`.
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` was allocated by `System` with this `layout`.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

#[test]
fn keys_do_not_copy_values() {
    // An explicit budget keeps every row in a memtable, whose cursor lends
    // values without reading blocks, whatever `VSDB_MEM_BUDGET_MB` says.
    vsdb_configure(VsdbOptions::new(scratch::database_dir()).with_mem_budget_mb(2048))
        .unwrap();
    let value = vec![7u8; 256 * 1024];
    let mut raw = MapxOrdRawKey::<Vec<u8>>::new();
    let mut ord = vsdb::MapxOrd::<u32, Vec<u8>>::new();
    let mut map = vsdb::Mapx::<u32, Vec<u8>>::new();
    for i in 0u8..8 {
        raw.insert([i], &value);
        ord.insert(&u32::from(i), &value);
        map.insert(&u32::from(i), &value);
    }

    let before = ALLOCATED.with(Cell::get);
    assert_eq!(raw.keys().count(), 8);
    assert_eq!(raw.keys().next_back(), Some(vec![7]));
    assert_eq!(ord.keys().count(), 8);
    assert_eq!(map.keys().count(), 8);
    let allocated = ALLOCATED.with(Cell::get) - before;
    assert!(
        allocated < value.len(),
        "listing keys allocated {allocated} bytes"
    );
}
