//! Caller-thread allocation counts for warmed successful operations.
use super::{MapxRaw, Namespace, black_box};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
};

thread_local! {
    static COUNTS: Cell<Option<(usize,usize)>> = const { Cell::new(None) };
}

struct Count;
#[global_allocator]
static ALLOCATOR: Count = Count;

fn note(bytes: usize) {
    let _ = COUNTS.try_with(|c| {
        if let Some((n, b)) = c.get() {
            c.set(Some((n + 1, b + bytes)));
        }
    });
}

// SAFETY: pointers/layouts pass straight through to System, with no new
// allocation, synchronization, or owner in the accounting path.
unsafe impl GlobalAlloc for Count {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        note(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        note(size);
        unsafe { System.realloc(pointer, layout, size) }
    }
}
fn capture(name: &str, calls: usize, f: impl FnOnce() -> usize) {
    COUNTS.set(Some((0, 0)));
    let result = black_box(f());
    let (count, bytes) = COUNTS.replace(None).unwrap();
    println!("allocation case={name} calls={calls} count={count} bytes={bytes} checksum={result}");
}

pub(super) fn run(ns: &Namespace) {
    const ROWS: usize = 2048;
    let mut map = MapxRaw::new_in(ns);
    for n in 0..ROWS {
        map.insert((n as u64).to_be_bytes(), [(n & 255) as u8; 4096]);
    }
    ns.flush();
    let view = map.read_view();
    for n in 0..ROWS {
        assert!(view.get((n as u64).to_be_bytes()).is_some());
    }
    capture("exists", ROWS, || {
        (0..ROWS)
            .map(|n| usize::from(black_box(view.contains_key((n as u64).to_be_bytes()))))
            .sum()
    });
    capture("owned-hit", ROWS, || {
        (0..ROWS)
            .map(|n| usize::from(black_box(view.get((n as u64).to_be_bytes()).unwrap())[0]))
            .sum()
    });
    capture("decode", ROWS, || {
        (0..ROWS)
            .map(|n| {
                let key = (n as u64).to_be_bytes();
                #[cfg(feature = "v5")]
                let v = view
                    .try_get_with(key, |bytes| Ok(usize::from(black_box(bytes)[0])))
                    .unwrap()
                    .unwrap();
                #[cfg(not(feature = "v5"))]
                let v = usize::from(black_box(view.get(key).unwrap())[0]);
                v
            })
            .sum()
    });
    capture("scan", ROWS, || {
        let mut cursor = view.iter();
        let mut sum = 0;
        #[cfg(feature = "v5")]
        while let Some((k, v)) = cursor.try_step_next().unwrap() {
            sum += black_box(k).len() + usize::from(black_box(v)[0]);
        }
        #[cfg(not(feature = "v5"))]
        for (k, v) in &mut cursor {
            sum += black_box(k).len() + usize::from(black_box(v)[0]);
        }
        sum
    });
}
