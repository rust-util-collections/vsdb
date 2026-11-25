use std::{borrow::Cow, ops::Bound};
use vsdb_core::{
    MAX_GROUPED_READ_KEYS, MapxRaw, Namespace, NamespaceOpts, StorageErrorKind,
    VsdbError,
};

mod common;

#[test]
fn pins_and_bounded_visit_preserve_snapshot_and_slots() {
    common::setup();
    let mut map = MapxRaw::new();
    let large_key = vec![b'x'; 513];
    let mut setup = map.batch();
    setup.insert(b"", b"");
    setup.insert(&large_key, b"kept");
    setup.insert(b"removed", b"old");
    setup.commit_sync_no_slowdown().unwrap();

    let reader = map.reader();
    let view = reader.read_view();
    let mut replace = map.batch_wiped();
    replace.insert(&large_key, b"replaced");
    replace.commit_no_slowdown().unwrap();
    map.namespace().flush();
    let len = |bytes: &[u8]| Ok(bytes.len());
    assert_eq!(view.try_get_with(&large_key, len).unwrap(), Some(4));
    assert_eq!(reader.try_get_with(&large_key, len).unwrap(), Some(8));
    assert_eq!(map.try_get_with(b"", len).unwrap(), None);
    assert_eq!(view.try_get_with(b"", len).unwrap(), Some(0));
    assert!(view.try_contains_key(b"removed").unwrap());
    assert!(!map.try_contains_key(b"removed").unwrap());

    let keys: &[&[u8]] = &[&large_key, b"missing", b"", &large_key];
    let mut seen = Vec::new();
    view.try_visit_many(keys, |slot, value| {
        seen.push((slot, value.map(<[u8]>::to_vec)));
        Ok(())
    })
    .unwrap();
    assert_eq!(
        seen,
        vec![
            (0, Some(b"kept".to_vec())),
            (1, None),
            (2, Some(Vec::new())),
            (3, Some(b"kept".to_vec()))
        ]
    );
    let mut seen = 0;
    let failure = view.try_visit_many(keys, |slot, _| {
        seen += 1;
        if slot == 1 {
            return Err(VsdbError::Other {
                detail: "decoder stopped".into(),
            });
        }
        Ok(())
    });
    assert!(matches!(failure, Err(VsdbError::Other { .. })));
    assert_eq!(seen, 2);
    view.try_visit_many(&[], |_, _| panic!("no slots")).unwrap();
    let err = view
        .try_visit_many(&[b"".as_slice(); MAX_GROUPED_READ_KEYS + 1], |_, _| {
            panic!("oversized request should fail")
        })
        .unwrap_err();
    assert!(matches!(
        err,
        VsdbError::Storage {
            kind: StorageErrorKind::InvalidArgument,
            ..
        }
    ));
}

#[test]
fn borrowed_cursor_bounds_are_shared_between_forward_and_reverse_steps() {
    common::setup();
    let mut map = MapxRaw::new();
    for key in [b"a".as_slice(), b"a\0", b"a\0\0", b"b"] {
        map.insert(key, key);
    }
    let view = map.read_view();
    let bounds = (
        Bound::Excluded(Cow::Borrowed(b"a".as_slice())),
        Bound::Included(Cow::Borrowed(b"b".as_slice())),
    );
    let mut cursor = view.try_range(bounds).unwrap();
    assert_eq!(
        cursor.try_step_prev().unwrap(),
        Some((b"b".as_slice(), b"b".as_slice()))
    );
    assert_eq!(
        cursor.try_step_next().unwrap(),
        Some((b"a\0".as_slice(), b"a\0".as_slice()))
    );
    assert_eq!(
        cursor.try_step_prev().unwrap(),
        Some((b"a\0\0".as_slice(), b"a\0\0".as_slice()))
    );
    assert_eq!(cursor.try_step_next().unwrap(), None);
    let mut cursor = map.try_range(..).unwrap();
    assert_eq!(cursor.try_step_next().unwrap().unwrap().0, b"a");
}

#[test]
fn namespace_reports_one_pool_and_each_members_own_memtables() {
    common::setup();
    let ns = Namespace::create_with(NamespaceOpts {
        shards: 2,
        mem_budget_mb: Some(32),
        ..Default::default()
    })
    .unwrap();
    let first = ns.memory_usage();
    assert_eq!(first.shards.len(), 2);
    assert_eq!(first.pool.segmented_capacity_bytes, 4 << 20);
    assert_eq!(first.pool.overflow_capacity_bytes, 4 << 20);
    assert_eq!(first.write_buffer_bytes_per_shard, 768 << 10);
    assert_eq!(first.immutable_limit_per_shard, 4);
    let mut map = MapxRaw::new_in(&ns);
    map.insert(b"test", vec![1; 128 << 10]);
    let before_flush = ns.memory_usage();
    let shard = map.instance_id().map_id as usize % 2;
    assert!(
        before_flush.shards[shard].active_memtable_bytes
            > first.shards[shard].active_memtable_bytes
    );
    assert_eq!(before_flush.shards[1 - shard], first.shards[1 - shard]);
    ns.flush();
    let after = ns.memory_usage();
    assert_eq!(after.shards[shard].immutable_memtable_count, 0);
    assert_eq!(after.shards[shard].immutable_memtable_bytes, 0);
    assert!(
        after.shards[shard].active_memtable_bytes
            < before_flush.shards[shard].active_memtable_bytes
    );
}
