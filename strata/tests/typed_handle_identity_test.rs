mod common;

use serde::{Deserialize, Serialize};
use std::fs;
use vsdb::{Mapx, MapxOrd};

mod accounting {
    #[derive(super::Serialize, super::Deserialize)]
    pub struct Record {
        pub amount: u64,
    }

    #[derive(super::Serialize, super::Deserialize, PartialEq, Eq, Hash)]
    pub struct Key(pub u64);
}

mod access {
    #[derive(super::Serialize, super::Deserialize)]
    pub struct Record {
        pub enabled: bool,
    }

    #[derive(super::Serialize, super::Deserialize, PartialEq, Eq, Hash)]
    pub struct Key(pub u64);
}

#[test]
fn same_named_value_cannot_restore_or_rewrite_another_schema() {
    common::setup();
    let mut original = MapxOrd::<u64, accounting::Record>::new();
    original.insert(&7, &accounting::Record { amount: 1 });
    let id = original.save_meta().unwrap();
    let bytes = postcard::to_allocvec(&original).unwrap();
    drop(original);

    // Postcard can interpret the value 1 as either u64 or bool. The handle
    // identity must reject this before a wrong typed writer can change it.
    assert!(MapxOrd::<u64, access::Record>::from_meta(id).is_err());
    assert!(postcard::from_bytes::<MapxOrd<u64, access::Record>>(&bytes).is_err());

    type RecordAlias = accounting::Record;
    let restored = MapxOrd::<u64, RecordAlias>::from_meta(id).unwrap();
    assert_eq!(restored.get(&7).unwrap().amount, 1);
}

#[test]
fn same_named_keys_and_nested_values_keep_their_identity() {
    common::setup();
    let mut original = Mapx::<accounting::Key, Vec<Option<accounting::Record>>>::new();
    original.insert(
        &accounting::Key(7),
        &vec![Some(accounting::Record { amount: 1 })],
    );
    let bytes = postcard::to_allocvec(&original).unwrap();
    drop(original);

    assert!(
        postcard::from_bytes::<Mapx<access::Key, Vec<Option<accounting::Record>>>>(
            &bytes
        )
        .is_err()
    );
    assert!(
        postcard::from_bytes::<Mapx<accounting::Key, Vec<Option<access::Record>>>>(
            &bytes
        )
        .is_err()
    );
    let restored: Mapx<accounting::Key, Vec<Option<accounting::Record>>> =
        postcard::from_bytes(&bytes).unwrap();
    assert_eq!(
        restored.get(&accounting::Key(7)).unwrap()[0]
            .as_ref()
            .unwrap()
            .amount,
        1
    );
}

#[test]
fn handles_nested_inside_values_reject_the_wrong_type() {
    common::setup();
    let mut inner = MapxOrd::<u64, accounting::Record>::new();
    inner.insert(&7, &accounting::Record { amount: 1 });
    let mut outer = Mapx::<u64, MapxOrd<u64, accounting::Record>>::new();
    outer.insert(&1, &inner);
    drop(inner);
    let bytes = postcard::to_allocvec(&outer).unwrap();
    drop(outer);

    assert!(
        postcard::from_bytes::<Mapx<u64, MapxOrd<u64, access::Record>>>(&bytes).is_err()
    );
    let restored: Mapx<u64, MapxOrd<u64, accounting::Record>> =
        postcard::from_bytes(&bytes).unwrap();
    assert_eq!(restored.get(&1).unwrap().get(&7).unwrap().amount, 1);
}

#[test]
fn saving_an_empty_typed_map_propagates_prefix_io_errors() {
    const PROBE: &str = "VSDB_TYPED_PREFIX_IO_PROBE";
    if let Some(path) = std::env::var_os(PROBE) {
        let base = std::path::Path::new(&path);
        vsdb::vsdb_configure(vsdb::VsdbOptions::new(base)).unwrap();
        let map = MapxOrd::<u64, u64>::new();
        let mut raw_batch_map = vsdb::vsdb_core::MapxRaw::new();
        let ceiling = base.join("__SYSTEM__/__prefix_ceiling__");
        let saved = fs::read(&ceiling).unwrap();
        fs::remove_file(&ceiling).unwrap();
        fs::create_dir(&ceiling).unwrap();
        // Untouched sync_wal is a no-op and must not force prefix allocation.
        vsdb::vsdb_core::MapxRaw::new().sync_wal();
        // Fallible batch and SlotDex construction propagate allocator I/O errors.
        let mut batch = raw_batch_map.batch();
        batch.insert(b"k", b"v");
        assert!(matches!(batch.commit(), Err(vsdb::VsdbError::Io(_))));
        assert!(matches!(
            vsdb::SlotDex::<u64, u64>::new(64, false),
            Err(vsdb::VsdbError::Io(_))
        ));
        assert!(matches!(map.save_meta(), Err(vsdb::VsdbError::Io(_))));
        assert!(postcard::to_allocvec(&map).is_err());
        fs::remove_dir(&ceiling).unwrap();
        fs::write(ceiling, saved).unwrap();
        let id = map.save_meta().unwrap();
        let restored = MapxOrd::<u64, u64>::from_meta(id).unwrap();
        assert_eq!(restored.instance_id(), id);
        return;
    }
    let scratch = std::env::temp_dir()
        .join(format!("vsdb-typed-prefix-{}", rand::random::<u128>()));
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "saving_an_empty_typed_map_propagates_prefix_io_errors",
            "--nocapture",
        ])
        .env(PROBE, &scratch)
        .output()
        .unwrap();
    fs::remove_dir_all(scratch).unwrap();
    assert!(out.status.success(), "{out:?}");
}
