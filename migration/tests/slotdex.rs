//! Golden metadata and raw rows produced by v17.0.21 (da9a325) for a
//! SlotDex64<u64> with capacity 3. Run in an isolated universe so the
//! original captured prefix can be retained verbatim.
use vsdb::{SlotDex64, slotdex::Order};
use vsdb_core::MapxRaw;
use vsdb_migrate::{
    LegacyRaw,
    slotdex::{migrate_custom_v17, migrate_v17},
};

type Rows = Vec<(Vec<u8>, Vec<u8>)>;

#[test]
fn recovers_and_upgrades_v17_in_place() {
    let dir =
        std::env::temp_dir().join(format!("vsdb-slotdex-migration-{}", rand::random::<u128>()));
    vsdb::vsdb_configure(vsdb::VsdbOptions::new(dir)).unwrap();
    // Reserve the fixture prefix as a newly issued map in this empty universe.
    let issued = MapxRaw::new().instance_id();
    let (meta, rows): (Vec<u8>, Rows) =
        postcard::from_bytes(include_bytes!("fixtures/slotdex-v17.postcard")).unwrap();
    let envelope: &[u8] = postcard::from_bytes(&meta).unwrap();
    assert_eq!(&envelope[..8], b"VSTYPE03");
    let (_, LegacyRaw(mut raw), cap, _): (u8, LegacyRaw, u64, bool) =
        postcard::from_bytes(&envelope[16..]).unwrap();
    assert_eq!(cap, 3);
    let id = raw.instance_id();
    assert_eq!(id, issued);
    let mut batch = raw.batch();
    for (key, value) in &rows {
        batch.insert(key, value);
    }
    batch.commit().unwrap();
    let path = raw.namespace().meta_path(id.map_id);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, &meta).unwrap();
    assert!(SlotDex64::<u64>::from_meta(id).is_err());
    assert!(migrate_custom_v17::<u64, u64>(id, 9).is_err());
    let ragged = [1, 0, 0];
    raw.insert(ragged, 1u64.to_le_bytes());
    assert!(migrate_custom_v17::<u64, u64>(id, 3).is_err());
    assert_eq!(raw.get([2]).unwrap().len(), 8);
    raw.remove(ragged);
    let mut dex: SlotDex64<u64> = migrate_custom_v17(id, 3).unwrap();
    assert_eq!(dex.instance_id(), id);
    assert_eq!(dex.total(), 6);
    assert_eq!(dex.count(2..=4), 3);
    assert_eq!(dex.page(.., 6, 0, Order::Desc), [14, 13, 12, 11, 10, 9]);
    assert_eq!(raw.get([2]).unwrap().len(), 16);
    let retry: SlotDex64<u64> = migrate_v17(id).unwrap();
    assert_eq!(retry.instance_id(), id);
    dex.insert(5, 15).unwrap();
    assert_eq!(
        raw.get([2]).unwrap(),
        [7u64.to_le_bytes(), 1u64.to_le_bytes()].concat()
    );
    let saved: SlotDex64<u64> = SlotDex64::from_meta(dex.save_meta().unwrap()).unwrap();
    assert_eq!(saved.instance_id(), id);
    assert_eq!(
        saved.iter(.., Order::Asc).collect::<Vec<_>>(),
        [9, 10, 11, 12, 13, 15, 14]
    );
}
