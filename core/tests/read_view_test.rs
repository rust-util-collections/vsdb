use std::{borrow::Cow, ops::Bound, thread};
use vsdb_core::{MapxRaw, Namespace, NamespaceOpts};

#[test]
fn read_view_survives_publication_clear_and_flush() {
    let ns = Namespace::create_with(NamespaceOpts {
        shards: 1,
        mem_budget_mb: Some(64),
        ..Default::default()
    })
    .unwrap();
    let mut map = MapxRaw::new_in(&ns);
    let mut adjacent = map.new_colocated();
    adjacent.insert(b"c", b"unrelated");
    let mut batch = map.batch();
    for key in [b"a", b"b", b"c"] {
        batch.insert(key, b"old");
    }
    batch.commit_sync().unwrap();
    let reader = map.reader();
    let view = reader.read_view();
    let ns_writer = ns.clone();
    let writer = thread::spawn(move || {
        for _ in 0..16 {
            map.clear();
            let mut batch = map.batch();
            batch.insert(b"a", b"new");
            batch.insert(b"b", b"new");
            batch.commit_sync().unwrap();
            ns_writer.flush();
        }
        map
    });
    for _ in 0..64 {
        assert_eq!(view.get(b"a").unwrap(), b"old");
        assert_eq!(view.get(b"b").unwrap(), b"old");
        assert!(view.contains_key(b"c"));
        let pairs: Vec<_> = view
            .range((
                Bound::Excluded(Cow::Borrowed(b"a".as_slice())),
                Bound::Included(Cow::Borrowed(b"c".as_slice())),
            ))
            .rev()
            .collect();
        assert_eq!(
            pairs,
            vec![
                (b"c".to_vec(), b"old".to_vec()),
                (b"b".to_vec(), b"old".to_vec())
            ]
        );
        thread::yield_now();
    }
    let map = writer.join().unwrap();
    assert_eq!(view.iter().count(), 3);
    assert_eq!(reader.get(b"a").unwrap(), b"new");
    assert_eq!(reader.read_view().get(b"c"), None);
    assert_eq!(map.read_view().iter().count(), 2);
    drop(view);
    drop((map, adjacent));
    let ns = ns.close().unwrap_err().0.unwrap();
    drop(reader);
    ns.close().unwrap();
}

#[test]
fn independent_views_and_reader_clones_share_identity_not_state() {
    let mut map = MapxRaw::new();
    map.insert(b"", b"first");
    let reader = map.reader();
    let other = reader.clone();
    let before = reader.read_view();
    map.insert(b"", b"second");
    let after = other.read_view();
    assert_eq!(before.get(b"").unwrap(), b"first");
    assert_eq!(after.get(b"").unwrap(), b"second");
    assert_eq!(before.range(..).next().unwrap().0, b"");
}
