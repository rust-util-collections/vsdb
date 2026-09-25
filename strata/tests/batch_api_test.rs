use vsdb::{Mapx, MapxOrd, MapxOrdRawKey, VsdbOptions, vsdb_configure};

#[test]
fn test_batch_basic() {
    let dir = format!(
        "/tmp/vsdb_testing/batch_api_test_{}",
        rand::random::<u128>()
    );
    vsdb_configure(VsdbOptions::new(&dir)).unwrap();

    let mut map = Mapx::new();

    // 1. Basic insert and commit
    let mut batch = map.batch();
    batch.insert(&1, &"one".to_string());
    batch.insert(&2, &"two".to_string());
    batch.commit().unwrap();

    assert_eq!(map.get(&1), Some("one".to_string()));
    assert_eq!(map.get(&2), Some("two".to_string()));

    // 2. Remove in batch
    let mut batch = map.batch();
    batch.remove(&1);
    batch.insert(&3, &"three".to_string());
    batch.commit().unwrap();

    assert_eq!(map.get(&1), None);
    assert_eq!(map.get(&2), Some("two".to_string()));
    assert_eq!(map.get(&3), Some("three".to_string()));

    // 3. Drop without commit (should discard changes? No, batch is only a buffer)
    // Actually, the backend WriteBatch is just a list of operations.
    // If we don't commit, nothing happens to DB.
    {
        let mut batch = map.batch();
        batch.insert(&4, &"four".to_string());
        // dropped here
    }
    assert_eq!(map.get(&4), None);

    let mut batch = map.batch();
    batch.insert(&4, &"four".to_string());
    batch.commit_sync().unwrap();
    assert_eq!(map.get(&4), Some("four".to_string()));
    let mut ordered = MapxOrd::<u64, u64>::new();
    let mut batch = ordered.batch();
    batch.insert(&42, &7);
    batch.commit_sync().unwrap();
    assert_eq!(ordered.get(&42), Some(7));
    let mut raw_key = MapxOrdRawKey::<u64>::new();
    let mut batch = raw_key.batch();
    batch.insert(b"key", &7);
    batch.commit_sync().unwrap();
    assert_eq!(raw_key.get(b"key"), Some(7));
}
