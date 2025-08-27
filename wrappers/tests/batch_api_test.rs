use vsdb::Mapx;
use vsdb::vsdb_set_base_dir;

#[test]
fn test_batch_entry_basic() {
    let dir = format!(
        "/tmp/vsdb_testing/batch_api_test_{}",
        rand::random::<u128>()
    );
    vsdb_set_base_dir(&dir).unwrap();

    let mut map = Mapx::new();

    // 1. Basic insert and commit
    let mut batch = map.batch_entry();
    batch.insert(&1, &"one".to_string());
    batch.insert(&2, &"two".to_string());
    batch.commit().unwrap();

    assert_eq!(map.get(&1), Some("one".to_string()));
    assert_eq!(map.get(&2), Some("two".to_string()));

    // 2. Remove in batch
    let mut batch = map.batch_entry();
    batch.remove(&1);
    batch.insert(&3, &"three".to_string());
    batch.commit().unwrap();

    assert_eq!(map.get(&1), None);
    assert_eq!(map.get(&2), Some("two".to_string()));
    assert_eq!(map.get(&3), Some("three".to_string()));

    // 3. Drop without commit (should discard changes? No, batch is only a buffer)
    // Actually, RocksDB WriteBatch is just a list of operations.
    // If we don't commit, nothing happens to DB.
    {
        let mut batch = map.batch_entry();
        batch.insert(&4, &"four".to_string());
        // dropped here
    }
    assert_eq!(map.get(&4), None);
}
