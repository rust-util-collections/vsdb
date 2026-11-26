//! Handles and rows written by v17.0.21 for populated u64 and u128 indexes.
//! The capacities are 2^32 and 2^64. The second index reverses slot order.
#[path = "common/scratch.rs"]
mod scratch;

use std::fs;
use vsdb::{SlotDex64, SlotDex128, VsdbOptions, slotdex::Order, vsdb_configure};
use vsdb_core::MapxRaw;
use vsdb_migrate::{LegacyRaw, slotdex::*};

type Fixture = (Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>);

fn install((meta, rows): Fixture) -> (MapxRaw, Vec<u8>) {
    let id = MapxRaw::new().instance_id();
    let frame: &[u8] = postcard::from_bytes(&meta).unwrap();
    let ((_, LegacyRaw(mut store)), _) =
        postcard::take_from_bytes::<(u8, LegacyRaw)>(&frame[16..]).unwrap();
    assert_eq!(store.instance_id(), id);
    let mut batch = store.batch();
    for (k, v) in rows {
        batch.insert(k, v);
    }
    batch.commit_sync().unwrap();
    (store, meta)
}

#[test]
fn oversized_capacities_preserve_entries_and_resume_at_the_selected_capacity() {
    vsdb_configure(VsdbOptions::new(scratch::database_dir())).unwrap();
    let [first, second]: [Fixture; 2] =
        postcard::from_bytes(include_bytes!("fixtures/slotdex-wide-v17.postcard")).unwrap();
    let (mut store, old) = install(first);
    let id = store.instance_id();
    let path = store.namespace().meta_path(id.map_id);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, &old).unwrap();
    let original: Vec<_> = store.range(..[1, 1].as_slice()).collect();
    let old_total = store.get([2]).unwrap();
    let (slot, n) = store.range([1, 0]..[1, 1]).next().unwrap();
    store.insert(&slot, 0u64.to_le_bytes());
    assert!(migrate_v17_with_capacity::<u64, u64>(id, 3).is_err());
    assert_eq!(fs::read(&path).unwrap(), old);
    assert!(!path.with_extension("slotdex-v17").exists());
    store.insert(slot, n);
    let bad_floor = [&[1, 1][..], &0u64.to_be_bytes()].concat();
    store.insert(&bad_floor, 99u64.to_le_bytes());
    assert!(migrate_v17_with_capacity::<u64, u64>(id, 3).is_err());
    assert!(store.contains_key(&bad_floor));
    assert!(!path.with_extension("slotdex-v17").exists());
    store.remove(&bad_floor);
    assert!(migrate_v17_with_capacity::<u64, u64>(id, 1).is_err());

    let dex: SlotDex64<u64> = migrate_v17_with_capacity(id, 3).unwrap();
    assert_eq!(dex.instance_id(), id);
    assert_eq!(dex.page(.., 20, 0, Order::Asc), (0..12).collect::<Vec<_>>());
    assert_eq!(dex.count(3..=1024), 6);
    assert!(store.range([1, 2]..[1, 3]).next().is_some());
    let new = fs::read(&path).unwrap();
    drop(dex);

    // Recreate an interruption after the first tier and before the final total
    // and saved metadata were published. An embedded alias resumes the same plan.
    fs::write(&path, &old).unwrap();
    let mut batch = store.batch();
    batch.delete_range(&[1, 2], &[2]);
    batch.insert([2], &old_total);
    batch.commit_sync().unwrap();
    assert!(migrate_handle_v17_with_capacity::<u64, u64>(&old, 4).is_err());
    assert_eq!(store.get([2]).unwrap(), old_total);
    assert_eq!(migrate_handle_v17::<u64, u64>(&old).unwrap(), new);
    let mut dex: SlotDex64<u64> = migrate_v17(id).unwrap();
    assert_eq!(fs::read(&path).unwrap(), new);
    assert_eq!(
        store.range(..[1, 1].as_slice()).collect::<Vec<_>>(),
        original
    );
    assert_eq!(dex.page(.., 2, 1, Order::Asc), [2, 3]);
    dex.remove(3, &3).unwrap();
    assert_eq!(
        migrate_handle_v17_with_capacity::<u64, u64>(&old, 3).unwrap(),
        new
    );
    let restored: SlotDex64<u64> = postcard::from_bytes(&new).unwrap();
    assert_eq!(restored.count(3..=1024), 5);
    assert!(migrate_v17_with_capacity::<u64, u64>(id, 2).is_err());

    let (raw, embedded) = install(second);
    let new = migrate_handle_v17::<u128, u64>(&embedded).unwrap();
    let dex: SlotDex128<u64> = postcard::from_bytes(&new).unwrap();
    assert_eq!(dex.instance_id(), raw.instance_id());
    assert_eq!(dex.page(.., 20, 0, Order::Asc), (0..12).collect::<Vec<_>>());
    assert_eq!(dex.count(u128::MAX..), 2);
    assert_eq!(
        migrate_handle_v17_with_capacity::<u128, u64>(&embedded, u32::MAX).unwrap(),
        new
    );
    assert!(!raw.namespace().meta_path(raw.instance_id().map_id).exists());
}
