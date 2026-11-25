mod common;
use vsdb::{Mapx, MapxOrd, Orphan, basic::mapx_ord::Entry};

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
