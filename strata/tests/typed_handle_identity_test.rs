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
    use std::error::Error as _;

    const PROBE: &str = "VSDB_TYPED_PREFIX_IO_PROBE";
    const SEED_MAP: &str = "VSDB_TYPED_PREFIX_SEED_MAP";
    const SEED_SD: &str = "VSDB_TYPED_PREFIX_SEED_SD";
    const PROBE_MAP_ID: u64 = 1 << 30;
    const PROBE_SD_ID: u64 = (1 << 30) + 1;
    if let Some(path) = std::env::var_os(PROBE) {
        let base = std::path::Path::new(&path);
        vsdb::vsdb_configure(vsdb::VsdbOptions::new(base)).unwrap();
        let map = MapxOrd::<u64, u64>::new();
        let ns = map.namespace();
        fs::create_dir_all(ns.meta_path(PROBE_MAP_ID).parent().unwrap()).unwrap();
        fs::copy(
            std::env::var_os(SEED_MAP).unwrap(),
            ns.meta_path(PROBE_MAP_ID),
        )
        .unwrap();
        fs::copy(
            std::env::var_os(SEED_SD).unwrap(),
            ns.meta_path(PROBE_SD_ID),
        )
        .unwrap();

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

        // open_raw inside from_meta must preserve VsdbError::Io and its source.
        let map_err = MapxOrd::<u64, u64>::from_meta(PROBE_MAP_ID).unwrap_err();
        assert!(matches!(map_err, vsdb::VsdbError::Io(_)), "got {map_err:?}");
        assert!(map_err.source().is_some());
        let sd_err = vsdb::SlotDex::<u64, u64>::from_meta(PROBE_SD_ID).unwrap_err();
        assert!(matches!(sd_err, vsdb::VsdbError::Io(_)), "got {sd_err:?}");
        assert!(sd_err.source().is_some());

        fs::remove_dir(&ceiling).unwrap();
        fs::write(ceiling, saved).unwrap();
        let id = map.save_meta().unwrap();
        let restored = MapxOrd::<u64, u64>::from_meta(id).unwrap();
        assert_eq!(restored.instance_id(), id);
        return;
    }
    common::setup();
    let patch_frame = |encoded: Vec<u8>, dst_id: u64| {
        let (frame, rest): (&[u8], _) = postcard::take_from_bytes(&encoded).unwrap();
        assert!(rest.is_empty());
        let mut patched = frame.to_vec();
        patched[16..24].copy_from_slice(&dst_id.to_le_bytes());
        patched[24..32].copy_from_slice(&0u64.to_le_bytes());
        postcard::to_allocvec(&patched.as_slice()).unwrap()
    };
    let mut seed_m = MapxOrd::<u64, u64>::new();
    seed_m.insert(&1, &2);
    let seed_sd = vsdb::SlotDex::<u64, u64>::new(16, false).unwrap();

    let scratch = std::env::temp_dir()
        .join(format!("vsdb-typed-prefix-{}", rand::random::<u128>()));
    let seed_dir =
        std::env::temp_dir().join(format!("vsdb-typed-seed-{}", rand::random::<u128>()));
    fs::create_dir_all(&seed_dir).unwrap();
    let seed_map_path = seed_dir.join("map.meta");
    let seed_sd_path = seed_dir.join("sd.meta");
    fs::write(
        &seed_map_path,
        patch_frame(postcard::to_allocvec(&seed_m).unwrap(), PROBE_MAP_ID),
    )
    .unwrap();
    fs::write(
        &seed_sd_path,
        patch_frame(postcard::to_allocvec(&seed_sd).unwrap(), PROBE_SD_ID),
    )
    .unwrap();

    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "saving_an_empty_typed_map_propagates_prefix_io_errors",
            "--nocapture",
        ])
        .env(PROBE, &scratch)
        .env(SEED_MAP, &seed_map_path)
        .env(SEED_SD, &seed_sd_path)
        .output()
        .unwrap();
    fs::remove_dir_all(scratch).unwrap();
    fs::remove_dir_all(seed_dir).unwrap();
    assert!(out.status.success(), "{out:?}");
}

#[test]
fn from_meta_preserves_corrupt_classifications() {
    use vsdb::{
        DagMap, DagMapRaw, DagMapRawKey, VsdbError,
        basic::persistent_btree::PersistentBTree,
        vecdex::{HnswConfig, VecDex, distance::L2},
    };

    common::setup();
    let raw_prefix = |map_id: u64| {
        let frame = [
            b"VSMAPX02".as_slice(),
            &map_id.to_le_bytes(),
            &0u64.to_le_bytes(),
        ]
        .concat();
        vsdb::vsdb_core::MapxRaw::from_prefix_meta(&frame).unwrap()
    };

    // Corrupt collection headers/rows must surface as VsdbError::Corrupt
    // rather than VsdbError::Encoding(SerdeDeCustom).
    let bt = PersistentBTree::new();
    let bt_id = bt.save_meta().unwrap();
    raw_prefix(bt_id.map_id).remove([9]);
    assert!(matches!(
        PersistentBTree::from_meta(bt_id),
        Err(VsdbError::Corrupt { .. })
    ));

    let vd = VecDex::<u64, L2>::new(HnswConfig {
        dim: 2,
        ..Default::default()
    })
    .unwrap();
    let vd_id = vd.save_meta().unwrap();
    raw_prefix(vd_id.map_id).remove([0x06]);
    assert!(matches!(
        VecDex::<u64, L2>::from_meta(vd_id),
        Err(VsdbError::Corrupt { .. })
    ));

    let dag = DagMap::<u64, u64>::new(None);
    let dag_id = dag.save_meta().unwrap();
    raw_prefix(dag_id.map_id).remove([0]);
    assert!(matches!(
        DagMap::<u64, u64>::from_meta(dag_id),
        Err(VsdbError::Corrupt { .. })
    ));

    let dag_rk = DagMapRawKey::<u64>::new(None);
    let dag_rk_id = dag_rk.save_meta().unwrap();
    raw_prefix(dag_rk_id.map_id).remove([0]);
    assert!(matches!(
        DagMapRawKey::<u64>::from_meta(dag_rk_id),
        Err(VsdbError::Corrupt { .. })
    ));

    let dag_r = DagMapRaw::new(None);
    let dag_r_id = dag_r.save_meta().unwrap();
    raw_prefix(dag_r_id.map_id).remove([0]);
    assert!(matches!(
        DagMapRaw::from_meta(dag_r_id),
        Err(VsdbError::Corrupt { .. })
    ));
}
