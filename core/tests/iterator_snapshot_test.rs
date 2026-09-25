use std::{
    borrow::Cow,
    sync::{Arc, Barrier},
    thread,
};
use vsdb_core::{MapxRaw, Namespace, NamespaceOpts};

fn replace(map: &mut MapxRaw, generation: u64) {
    let mut batch = map.batch_wiped();
    for key in 0u64..128 {
        batch.insert(key.to_be_bytes(), generation.to_be_bytes());
    }
    batch.commit().unwrap();
}

#[test]
fn immutable_views_survive_concurrent_replacements_and_flushes() {
    let ns = Namespace::create_with(NamespaceOpts {
        path: None,
        shards: 1,
        mem_budget_mb: Some(64),
    })
    .unwrap();
    let mut writer = MapxRaw::new_in(&ns);
    replace(&mut writer, 0);
    ns.flush();
    // SAFETY: only the original handle writes. This alias is used for
    // immutable point reads and snapshot iteration throughout the test.
    let reader = unsafe { writer.shadow() };
    let mut old = reader.iter();
    assert_eq!(old.next().unwrap().0, 0u64.to_be_bytes());
    assert_eq!(old.next_back().unwrap().0, 127u64.to_be_bytes());
    let bounded = reader.range(
        Cow::Owned(32u64.to_be_bytes().to_vec())
            ..Cow::Owned(64u64.to_be_bytes().to_vec()),
    );
    let detached = writer.range_detached(..);
    let start = Arc::new(Barrier::new(2));
    let worker_start = start.clone();
    let worker = thread::spawn(move || {
        worker_start.wait();
        for generation in 1..=32 {
            replace(&mut writer, generation);
            if generation % 4 == 0 {
                writer.namespace().flush();
            }
            thread::yield_now();
        }
        writer
    });
    start.wait();
    for _ in 0..64 {
        let rows: Vec<_> = reader.iter().collect();
        assert_eq!(rows.len(), 128);
        let generation = &rows[0].1;
        for (key, (bytes, value)) in rows.iter().enumerate() {
            assert_eq!(*bytes, (key as u64).to_be_bytes());
            assert_eq!(value, generation, "one iterator must not mix batches");
        }
        thread::yield_now();
    }
    let writer = worker.join().unwrap();
    let old: Vec<_> = old.collect();
    assert_eq!(old.len(), 126);
    for (key, (bytes, value)) in (1u64..127).zip(old) {
        assert_eq!(bytes, key.to_be_bytes());
        assert_eq!(value, 0u64.to_be_bytes());
    }
    let bounded: Vec<_> = bounded.collect();
    assert_eq!(bounded.len(), 32);
    for (key, (bytes, value)) in (32u64..64).zip(bounded) {
        assert_eq!(bytes, key.to_be_bytes());
        assert_eq!(value, 0u64.to_be_bytes());
    }
    let detached: Vec<_> = detached.rev().collect();
    assert_eq!(detached.len(), 128);
    for (key, (bytes, value)) in (0u64..128).rev().zip(detached) {
        assert_eq!(bytes, key.to_be_bytes());
        assert_eq!(value, 0u64.to_be_bytes());
    }
    assert_eq!(reader.get(0u64.to_be_bytes()).unwrap(), 32u64.to_be_bytes());
    assert_eq!(writer.get(0u64.to_be_bytes()).unwrap(), 32u64.to_be_bytes());
}
