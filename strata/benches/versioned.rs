#![allow(warnings)]

use criterion::{Criterion, criterion_group, criterion_main};
use std::ops::Bound;
use std::sync::atomic::{AtomicUsize, Ordering};
use vsdb::versioned::BranchId;
use vsdb::versioned::map::VerMap;

fn setup() {
    let dir = format!("/tmp/vsdb_bench_versioned/{}", rand::random::<u128>());
    let _ = vsdb_core::vsdb_set_base_dir(&dir);
}

// =====================================================================
// Single-branch CRUD
// =====================================================================

fn single_branch_crud(c: &mut Criterion) {
    let mut group = c.benchmark_group("** versioned::single_branch **");
    group
        .measurement_time(std::time::Duration::from_secs(3))
        .sample_size(10);

    setup();
    let counter = AtomicUsize::new(0);
    let mut m: VerMap<u64, Vec<u8>> = VerMap::new();
    let main = m.main_branch();

    group.bench_function("insert", |b| {
        b.iter(|| {
            let n = counter.fetch_add(1, Ordering::SeqCst) as u64;
            m.insert(main, &n, &vec![0u8; 128]).unwrap();
        })
    });

    // Commit so reads hit committed data.
    m.commit(main).unwrap();

    group.bench_function("get (hit)", |b| {
        let max = counter.load(Ordering::SeqCst) as u64;
        let mut i = 0u64;
        b.iter(|| {
            i = (i + 1) % max;
            m.get(main, &i).unwrap();
        })
    });

    group.bench_function("get (miss)", |b| {
        let base = counter.load(Ordering::SeqCst) as u64 + 1_000_000;
        let mut i = 0u64;
        b.iter(|| {
            i += 1;
            m.get(main, &(base + i)).unwrap();
        })
    });

    group.bench_function("contains_key", |b| {
        let max = counter.load(Ordering::SeqCst) as u64;
        let mut i = 0u64;
        b.iter(|| {
            i = (i + 1) % max;
            m.contains_key(main, &i).unwrap();
        })
    });

    group.bench_function("remove", |b| {
        // Remove from a high range that was previously inserted.
        let rm = AtomicUsize::new(counter.load(Ordering::SeqCst));
        b.iter(|| {
            let n = rm.fetch_sub(1, Ordering::SeqCst) as u64;
            m.remove(main, &n).unwrap();
        })
    });

    group.finish();
}

// =====================================================================
// Commit / Rollback
// =====================================================================

fn commit_rollback(c: &mut Criterion) {
    let mut group = c.benchmark_group("** versioned::commit_rollback **");
    group
        .measurement_time(std::time::Duration::from_secs(3))
        .sample_size(10);

    setup();
    let mut m: VerMap<u64, Vec<u8>> = VerMap::new();
    let main = m.main_branch();
    let counter = AtomicUsize::new(0);

    group.bench_function("insert + commit (1 key)", |b| {
        b.iter(|| {
            let n = counter.fetch_add(1, Ordering::SeqCst) as u64;
            m.insert(main, &n, &vec![0u8; 64]).unwrap();
            m.commit(main).unwrap();
        })
    });

    group.bench_function("insert 10 + commit", |b| {
        b.iter(|| {
            for _ in 0..10 {
                let n = counter.fetch_add(1, Ordering::SeqCst) as u64;
                m.insert(main, &n, &vec![0u8; 64]).unwrap();
            }
            m.commit(main).unwrap();
        })
    });

    group.finish();
}

// =====================================================================
// Branching
// =====================================================================

fn branching(c: &mut Criterion) {
    let mut group = c.benchmark_group("** versioned::branching **");
    group
        .measurement_time(std::time::Duration::from_secs(3))
        .sample_size(10);

    setup();
    let mut m: VerMap<u64, Vec<u8>> = VerMap::new();
    let main = m.main_branch();

    // Pre-populate with 1000 keys on main.
    for i in 0..1000u64 {
        m.insert(main, &i, &vec![0u8; 64]).unwrap();
    }
    m.commit(main).unwrap();

    let branch_counter = AtomicUsize::new(0);

    group.bench_function("create_branch (from 1k keys)", |b| {
        b.iter(|| {
            let n = branch_counter.fetch_add(1, Ordering::SeqCst);
            m.create_branch(&format!("b{n}"), main).unwrap();
        })
    });

    // Create a branch, insert on it, commit, then merge.
    group.bench_function("branch + insert 10 + commit + merge", |b| {
        b.iter(|| {
            let n = branch_counter.fetch_add(1, Ordering::SeqCst);
            let br = m.create_branch(&format!("m{n}"), main).unwrap();
            for j in 0..10u64 {
                let key = 100_000 + (n as u64) * 10 + j;
                m.insert(br, &key, &vec![0u8; 64]).unwrap();
            }
            m.commit(br).unwrap();
            m.merge(br, main).unwrap();
            m.delete_branch(br).unwrap();
        })
    });

    group.finish();
}

// =====================================================================
// Iteration & Range
// =====================================================================

fn iteration(c: &mut Criterion) {
    let mut group = c.benchmark_group("** versioned::iteration **");
    group
        .measurement_time(std::time::Duration::from_secs(3))
        .sample_size(10);

    setup();
    let mut m: VerMap<u64, Vec<u8>> = VerMap::new();
    let main = m.main_branch();

    // Populate 5000 keys.
    for i in 0..5000u64 {
        m.insert(main, &i, &vec![0u8; 64]).unwrap();
    }
    m.commit(main).unwrap();

    group.bench_function("iter full (5k keys)", |b| {
        b.iter(|| {
            let count = m.iter(main).unwrap().count();
            assert_eq!(count, 5000);
        })
    });

    group.bench_function("range [1000, 2000) (1k keys)", |b| {
        b.iter(|| {
            let count = m
                .range(main, Bound::Included(&1000), Bound::Excluded(&2000))
                .unwrap()
                .count();
            assert_eq!(count, 1000);
        })
    });

    group.bench_function("range [0, 100) (100 keys)", |b| {
        b.iter(|| {
            let count = m
                .range(main, Bound::Included(&0), Bound::Excluded(&100))
                .unwrap()
                .count();
            assert_eq!(count, 100);
        })
    });

    group.finish();
}

// =====================================================================
// Historical reads
// =====================================================================

fn historical(c: &mut Criterion) {
    let mut group = c.benchmark_group("** versioned::historical **");
    group
        .measurement_time(std::time::Duration::from_secs(3))
        .sample_size(10);

    setup();
    let mut m: VerMap<u64, Vec<u8>> = VerMap::new();
    let main = m.main_branch();

    // Create 20 commits, each adding 50 keys.
    let mut commits = Vec::new();
    for c_idx in 0..20u64 {
        for j in 0..50u64 {
            let key = c_idx * 50 + j;
            m.insert(main, &key, &vec![0u8; 64]).unwrap();
        }
        commits.push(m.commit(main).unwrap());
    }

    group.bench_function("get_at_commit", |b| {
        let mut i = 0usize;
        b.iter(|| {
            let c_idx = i % commits.len();
            let key = (c_idx as u64) * 50; // first key of that commit
            m.get_at_commit(commits[c_idx], &key).unwrap();
            i += 1;
        })
    });

    group.bench_function("iter_at_commit (oldest, 50 keys)", |b| {
        b.iter(|| {
            let count = m.iter_at_commit(commits[0]).unwrap().count();
            assert_eq!(count, 50);
        })
    });

    group.bench_function("iter_at_commit (latest, 1000 keys)", |b| {
        b.iter(|| {
            let count = m.iter_at_commit(*commits.last().unwrap()).unwrap().count();
            assert_eq!(count, 1000);
        })
    });

    group.finish();
}

// =====================================================================
// Three-way merge
// =====================================================================

fn merge_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("** versioned::merge **");
    group
        .measurement_time(std::time::Duration::from_secs(5))
        .sample_size(10);

    setup();
    let mut m: VerMap<u64, Vec<u8>> = VerMap::new();
    let main = m.main_branch();

    // Common ancestor: 1000 keys.
    for i in 0..1000u64 {
        m.insert(main, &i, &vec![0u8; 64]).unwrap();
    }
    m.commit(main).unwrap();

    let branch_counter = AtomicUsize::new(0);

    group.bench_function("merge (100 changed in each side)", |b| {
        b.iter(|| {
            let n = branch_counter.fetch_add(1, Ordering::SeqCst) as u64;
            let br = m.create_branch(&format!("mg{n}"), main).unwrap();

            // Feature changes keys 0..100.
            for i in 0..100u64 {
                m.insert(br, &i, &vec![1u8; 64]).unwrap();
            }
            m.commit(br).unwrap();

            // Main changes keys 100..200.
            for i in 100..200u64 {
                m.insert(main, &i, &vec![2u8; 64]).unwrap();
            }
            m.commit(main).unwrap();

            m.merge(br, main).unwrap();
            m.delete_branch(br).unwrap();
        })
    });

    group.finish();
}

// =====================================================================
// GC
// =====================================================================

fn gc_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("** versioned::gc **");
    group
        .measurement_time(std::time::Duration::from_secs(5))
        .sample_size(10);

    setup();
    let mut m: VerMap<u64, Vec<u8>> = VerMap::new();
    let main = m.main_branch();

    // Build up history: 50 commits on main, each with 20 inserts.
    for c_idx in 0..50u64 {
        for j in 0..20u64 {
            m.insert(main, &(c_idx * 20 + j), &vec![0u8; 64]).unwrap();
        }
        m.commit(main).unwrap();
    }

    // Create and delete 20 branches to leave orphan commits.
    for i in 0..20u64 {
        let br = m.create_branch(&format!("gc{i}"), main).unwrap();
        for j in 0..10u64 {
            let key = 10_000 + i * 10 + j;
            m.insert(br, &key, &vec![0u8; 64]).unwrap();
        }
        m.commit(br).unwrap();
        m.delete_branch(br).unwrap();
    }

    group.bench_function("gc (50 commits + 20 deleted branches)", |b| {
        b.iter(|| {
            m.gc();
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    single_branch_crud,
    commit_rollback,
    branching,
    iteration,
    historical,
    merge_bench,
    gc_bench,
);

criterion_main!(benches);
