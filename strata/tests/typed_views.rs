mod common;
use vsdb::{
    DagMap, DagMapRawKey, Mapx, MapxOrd, MapxOrdRawKey, Namespace, Orphan, VsdbError,
    basic::mapx_ord::Entry,
};

#[test]
fn reader_pins_sequence_and_entries_write_once() {
    common::setup();
    let mut map = MapxOrd::<String, u32>::new();
    *map.entry("a").or_default() = 1;
    map.entry("b").or_insert_with(|| 2);
    let reader = map.reader().clone();
    let view = reader.read_view();
    if let Entry::Occupied(mut e) = map.entry("a").and_modify(|v| *v += 2) {
        assert_eq!(e.insert(8), 3);
    } else {
        panic!("present key was vacant");
    }
    assert_eq!(view.get("a"), Some(1));
    assert_eq!(view.range("a".."z").count(), 2);
    assert_eq!(reader.read_view().get("a"), Some(8));
    let mut arrays = MapxOrd::<[u8; 64], u32>::new();
    arrays.insert(&[1; 64], &42);
    assert_eq!(arrays.get(&[1; 63][..]), None);
    assert_eq!(arrays.range([0; 64]..=[1; 64]).count(), 1);
    assert_eq!(arrays.range(&[0; 64]..=&[1; 64]).count(), 1);
    assert_eq!(arrays.range(..).count(), 1);
    let mut nums = MapxOrd::<u64, u32>::new();
    nums.insert(&5, &6);
    assert_eq!(nums.range(1u64..10).count(), 1);
    assert_eq!(nums.range(&1u64..&10).count(), 1);
    assert_eq!(nums.get_le(5u64), Some((5, 6)));
    let mut m = Mapx::<[u8; 2], u32>::new();
    m.insert(&[0, 7], &1);
    assert_eq!(m.read_view().get(&[0, 7][..]), Some(1));
    assert_eq!(m.reader().read_view().get(&[7][..]), None);
    let mut o = Orphan::new(7);
    let r = o.reader();
    o.update(|v| *v += 2).unwrap();
    assert_eq!(r.get(), 9);
}

#[test]
fn failed_guard_commit_keeps_existing_and_vacant_rows_intact() {
    struct Fallible(u8);
    impl vsdb::ValueEnDe for Fallible {
        fn encode_value_into(&self, dst: &mut Vec<u8>) -> vsdb::Result<()> {
            dst.push(self.0);
            if self.0 == 255 {
                return Err(vsdb::VsdbError::InvalidConfig {
                    detail: "invalid test value".into(),
                });
            }
            Ok(())
        }
        fn decode_value(bytes: &[u8]) -> vsdb::Result<Self> {
            Ok(Self(bytes[0]))
        }
    }
    common::setup();
    let mut m = Mapx::<u32, Fallible>::new();
    m.insert(&7, &Fallible(3));
    assert!(m.update(&7, |v| v.0 = 255).is_err());
    assert_eq!(m.get(&7).unwrap().0, 3);
    assert!(m.entry(&8).or_insert(Fallible(255)).commit().is_err());
    assert!(!m.contains_key(&8));
}

#[test]
fn guard_created_by_unwind_cleanup_observes_writeback_rules() {
    use std::{
        cell::RefCell,
        panic::{AssertUnwindSafe, catch_unwind},
    };
    struct Cleanup<'a>(&'a mut Mapx<u32, RefCell<u32>>);
    impl Drop for Cleanup<'_> {
        fn drop(&mut self) {
            // Debug diagnostics must not cause a second panic during cleanup.
            *self.0.get_mut(&1).unwrap().borrow_mut() = 41;
            self.0.update(&2, |v| *v.get_mut() = 42).unwrap();
        }
    }
    common::setup();
    let mut map = Mapx::new();
    map.insert(&1, &RefCell::new(1));
    map.insert(&2, &RefCell::new(2));
    let unwind = catch_unwind(AssertUnwindSafe(|| {
        let _cleanup = Cleanup(&mut map);
        panic!("begin cleanup");
    }));
    assert!(unwind.is_err());
    assert_eq!(map.get(&1).unwrap().into_inner(), 1);
    assert_eq!(map.get(&2).unwrap().into_inner(), 42);
}

#[test]
fn update_propagates_stored_value_decode_error_before_callback() {
    use vsdb::{KeyEnDe, MapxRaw, ValueEnDe};

    common::setup();
    let retired_ns = Namespace::create().unwrap();
    let retired_id = retired_ns.id();
    let nested = Mapx::<u32, u32>::new_in(&retired_ns);
    let nested_bytes = nested.encode_value();

    let mut mapx = Mapx::<u32, Mapx<u32, u32>>::new();
    mapx.insert(&1, &nested);
    let mut mapx_ord = MapxOrd::<u32, Mapx<u32, u32>>::new();
    mapx_ord.insert(&1, &nested);
    let mut rawkey = MapxOrdRawKey::<Mapx<u32, u32>>::new();
    rawkey.insert(b"k", &nested);
    let mut orphan = Orphan::<Mapx<u32, u32>>::new(nested);

    let nested2 = Mapx::<u32, u32>::new_in(&retired_ns);
    let expected_bytes = nested2.encode_value();
    let mut dag_rawkey = DagMapRawKey::<Mapx<u32, u32>>::new(None);
    dag_rawkey.insert(b"k", &nested2);
    let mut dag = DagMap::<u32, Mapx<u32, u32>>::new(None);
    dag.insert(&1, &nested2);

    drop(nested2);
    retired_ns.close().unwrap();
    Namespace::destroy(retired_id).unwrap();

    assert!(matches!(rawkey.try_get(b"k"), Err(VsdbError::Encoding(_))));

    let mut callbacks = 0usize;
    assert!(matches!(
        mapx.update(&1, |_| callbacks += 1),
        Err(VsdbError::Encoding(_))
    ));
    assert!(matches!(
        mapx_ord.update(&1, |_| callbacks += 1),
        Err(VsdbError::Encoding(_))
    ));
    assert!(matches!(
        rawkey.update(b"k", |_| callbacks += 1),
        Err(VsdbError::Encoding(_))
    ));
    assert!(matches!(
        orphan.update(|_| callbacks += 1),
        Err(VsdbError::Encoding(_))
    ));
    assert!(matches!(
        dag_rawkey.update(b"k", |_| callbacks += 1),
        Err(VsdbError::Encoding(_))
    ));
    assert!(matches!(
        dag.update(&1, |_| callbacks += 1),
        Err(VsdbError::Encoding(_))
    ));
    assert_eq!(callbacks, 0);

    // Confirm the original encoded payloads were untouched on disk.
    use serde::{
        Deserialize,
        de::value::{BytesDeserializer, Error as ValueError},
    };
    let open_raw = |id: vsdb::InstanceId| -> MapxRaw {
        let frame = [
            b"VSMAPX02".as_slice(),
            &id.map_id.to_le_bytes(),
            &id.ns.unwrap_or_default().to_le_bytes(),
        ]
        .concat();
        MapxRaw::deserialize(BytesDeserializer::<ValueError>::new(&frame)).unwrap()
    };
    assert_eq!(
        open_raw(rawkey.instance_id()).get(b"k").as_deref(),
        Some(nested_bytes.as_slice())
    );
    let mut dag_key = vec![2u8];
    dag_key.extend_from_slice(&1u32.encode_key());
    assert_eq!(
        &open_raw(dag.instance_id()).get(&dag_key).unwrap()[1..],
        expected_bytes.as_slice()
    );
}

#[test]
fn fallible_typed_edits_propagate_key_encoding_errors() {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct CheckedKey(u8);
    impl vsdb::KeyEnDe for CheckedKey {
        fn encode_key_into(&self, dst: &mut Vec<u8>) -> vsdb::Result<()> {
            if self.0 == 0xff {
                return Err(VsdbError::InvalidConfig {
                    detail: "rejected key".into(),
                });
            }
            dst.push(self.0);
            Ok(())
        }
        fn decode_key(bytes: &[u8]) -> vsdb::Result<Self> {
            Ok(Self(bytes[0]))
        }
    }

    common::setup();
    let valid = UNIX_EPOCH + Duration::from_secs(1);
    let pre_epoch = UNIX_EPOCH - Duration::from_secs(1);

    let mut map = Mapx::<SystemTime, u32>::new();
    map.insert(&valid, &10);
    assert!(matches!(
        map.update(&pre_epoch, |_| panic!("callback must not run")),
        Err(VsdbError::Encoding(_))
    ));
    assert_eq!(map.update(&valid, |v| *v += 5).unwrap(), Some(()));
    assert_eq!(map.get(&valid), Some(15));

    let mut strings = Mapx::<String, u32>::new();
    let long = "x".repeat(200);
    strings.insert(&long, &7);
    assert_eq!(
        strings
            .update(long.as_str(), |v| {
                *v += 1;
                *v
            })
            .unwrap(),
        Some(8)
    );

    let mut arrays = Mapx::<[u8; 4], u32>::new();
    arrays.insert(&[1, 2, 3, 4], &9);
    assert_eq!(
        arrays
            .update(&[1, 2, 3][..], |_| panic!("short slice must miss"))
            .unwrap(),
        None
    );

    let mut dag = DagMap::<SystemTime, u32>::new(None);
    dag.insert(&valid, &20);
    assert!(matches!(
        dag.update(&pre_epoch, |_| panic!("callback must not run")),
        Err(VsdbError::Encoding(_))
    ));
    assert_eq!(dag.get(&valid), Some(20));

    let mut custom_map = Mapx::<CheckedKey, u32>::new();
    custom_map.insert(&CheckedKey(1), &3);
    assert!(matches!(
        custom_map.update(&CheckedKey(0xff), |_| panic!("callback must not run")),
        Err(VsdbError::InvalidConfig { .. })
    ));
    let mut custom_dag = DagMap::<CheckedKey, u32>::new(None);
    custom_dag.insert(&CheckedKey(1), &3);
    assert!(matches!(
        custom_dag.update(&CheckedKey(0xff), |_| panic!("callback must not run")),
        Err(VsdbError::InvalidConfig { .. })
    ));
}
