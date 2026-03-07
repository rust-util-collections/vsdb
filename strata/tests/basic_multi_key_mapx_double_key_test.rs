use ruc::*;
use vsdb::{basic_multi_key::mapx_double_key::MapxDk, vsdb_set_base_dir};

#[test]
fn basic_cases() {
    info_omit!(vsdb_set_base_dir(&format!(
        "/tmp/vsdb_testing/{}",
        rand::random::<u64>()
    )));

    let mut map = MapxDk::new();

    map.insert(&(&1u8, &1u8), &9u8);
    map.insert(&(&1, &2), &8);
    map.insert(&(&1, &3), &7);

    assert_eq!(map.get(&(&1, &1)).unwrap(), 9);
    assert_eq!(map.get(&(&1, &2)).unwrap(), 8);
    assert_eq!(map.get(&(&1, &3)).unwrap(), 7);

    // does not exist
    map.remove(&(&1, Some(&4)));

    map.remove(&(&1, Some(&1)));
    assert!(map.get(&(&1, &1)).is_none());

    // partial-path remove
    map.remove(&(&1, None));
    assert!(map.get(&(&1, &2)).is_none());
    assert!(map.get(&(&1, &3)).is_none());

    map.entry(&(&1, &99)).or_insert(100);
    assert_eq!(map.get(&(&1, &99)).unwrap(), 100);
}
