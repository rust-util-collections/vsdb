use super::{Mapx, ValueEnDe};
use crate::basic::{mapx_ord::MapxOrd, orphan::Orphan};
use ruc::*;
use std::{cell::RefCell, fs};

#[test]
fn test_insert() {
    let mut hdr: Mapx<usize, usize> = Mapx::new();
    let max = 100;
    (0..max)
        .map(|i: usize| (i, (max + i)))
        .for_each(|(key, value)| {
            assert!(hdr.get(&key).is_none());
            hdr.insert(&key, &value);
            assert!(hdr.contains_key(&key));
            assert_eq!(pnk!(hdr.get(&key)), value);
            hdr.remove(&key);
            assert!(hdr.get(&key).is_none());
        });
    hdr.clear();
    (0..max).map(|i: usize| i).for_each(|key| {
        assert!(hdr.get(&key).is_none());
    });
}

#[test]
fn xx_test_valueende() {
    let cnt = 100;
    let dehdr = {
        let mut hdr: Mapx<usize, usize> = Mapx::new();
        (0..cnt).map(|i: usize| (i, i)).for_each(|(key, value)| {
            hdr.insert(&key, &value);
        });
        hdr.encode()
    };
    let reloaded = pnk!(<Mapx<usize, usize> as ValueEnDe>::decode(&dehdr));
    (0..cnt).map(|i: usize| i).for_each(|i| {
        assert_eq!(i, reloaded.get(&i).unwrap());
    });
}

#[test]
fn test_iter() {
    let mut hdr: Mapx<usize, usize> = Mapx::new();
    let max = 100;
    (0..max).map(|i: usize| (i, i)).for_each(|(key, value)| {
        hdr.insert(&key, &value);
    });
    for (key, value) in hdr.iter().collect::<Vec<_>>().into_iter() {
        assert_eq!(key, value);
        hdr.remove(&key);
    }
}

#[test]
fn test_first_last() {
    let mut hdr: Mapx<usize, usize> = Mapx::new();
    let max = 100;
    (0..max).map(|i: usize| (i, i)).for_each(|(key, value)| {
        hdr.insert(&key, &value);
    });
    let (key, value) = pnk!(hdr.iter().next());
    assert_eq!(key, value);
    // order is not guaranteed in Mapx
    // assert_eq!(0, key);

    let (key, value) = pnk!(hdr.iter().next_back());
    assert_eq!(key, value);
    // assert_eq!(max - 1, key);
}

#[test]
fn test_values() {
    let mut hdr: Mapx<usize, usize> = Mapx::new();
    let max = 100usize;
    (0..max).map(|i| (i, i)).for_each(|(key, value)| {
        hdr.insert(&key, &value);
    });
    for (k, v) in hdr.iter() {
        assert_eq!(k, v);
    }
}

#[test]
fn test_values_first_last() {
    let mut hdr: Mapx<usize, usize> = Mapx::new();
    let max = 100;
    (0..max).map(|i: usize| (i, i)).for_each(|(key, value)| {
        hdr.insert(&key, &value);
    });
    let _value = pnk!(hdr.values().next());
    // assert_eq!(0, value);

    let _value = pnk!(hdr.values().next_back());
    // assert_eq!(max - 1, value);
}

#[test]
fn test_save_and_from_meta() {
    let mut hdr: Mapx<usize, String> = Mapx::new();
    hdr.insert(&1, &"hello".to_string());
    hdr.insert(&2, &"world".to_string());

    let id = pnk!(hdr.save_meta());
    assert_eq!(id, hdr.instance_id());

    let restored: Mapx<usize, String> = pnk!(Mapx::from_meta(id));
    assert_eq!(restored.get(&1), Some("hello".to_string()));
    assert_eq!(restored.get(&2), Some("world".to_string()));
    assert!(restored.is_the_same_instance(&hdr));
}

#[test]
fn test_from_meta_rejects_legacy_prefix_payload() {
    // The pre-v13.4 meta format was a bare 8-byte prefix; v14 removed the
    // legacy acceptance path, so such a payload must be rejected instead
    // of silently forging a handle.
    let mut hdr: Mapx<u32, String> = Mapx::new();
    hdr.insert(&1, &"legacy".to_string());

    let id = hdr.instance_id();
    let legacy_payload = postcard::to_allocvec(&hdr.as_bytes()).unwrap();
    fs::write(crate::common::vsdb_meta_path(id.map_id), legacy_payload).unwrap();

    assert!(Mapx::<u32, String>::from_meta(id).is_err());

    // Re-saving under the current format restores access.
    let id = hdr.save_meta().unwrap();
    let restored: Mapx<u32, String> = Mapx::from_meta(id).unwrap();
    assert_eq!(restored.get(&1), Some("legacy".to_string()));
    assert!(restored.is_the_same_instance(&hdr));
}

#[test]
fn test_from_meta_rejects_wrong_typed_handle() {
    let mut hdr: Mapx<u32, u32> = Mapx::new();
    hdr.insert(&1, &10);

    let id = hdr.save_meta().unwrap();
    assert!(Mapx::<String, u32>::from_meta(id).is_err());
}

/// Nested: `Mapx<String, MapxOrd<u32, String>>` — only the outer meta
/// is saved; restoring it must bring back all inner maps and their data.
#[test]
fn test_nested_mapx_of_mapxord() {
    let mut outer: Mapx<String, MapxOrd<u32, String>> = Mapx::new();

    let mut inner1: MapxOrd<u32, String> = MapxOrd::new();
    inner1.insert(&1, &"a".to_string());
    inner1.insert(&2, &"b".to_string());

    let mut inner2: MapxOrd<u32, String> = MapxOrd::new();
    inner2.insert(&10, &"x".to_string());
    inner2.insert(&20, &"y".to_string());
    inner2.insert(&30, &"z".to_string());

    outer.insert(&"first".to_string(), &inner1);
    outer.insert(&"second".to_string(), &inner2);

    // Save only the outermost instance
    let id = pnk!(outer.save_meta());

    // Restore and verify the full nested structure
    let restored: Mapx<String, MapxOrd<u32, String>> = pnk!(Mapx::from_meta(id));

    let r1 = restored.get(&"first".to_string()).unwrap();
    assert_eq!(r1.get(&1), Some("a".to_string()));
    assert_eq!(r1.get(&2), Some("b".to_string()));

    let r2 = restored.get(&"second".to_string()).unwrap();
    assert_eq!(r2.get(&10), Some("x".to_string()));
    assert_eq!(r2.get(&20), Some("y".to_string()));
    assert_eq!(r2.get(&30), Some("z".to_string()));
}

/// Triple nesting: `Orphan< Mapx<String, MapxOrd<u32, String>> >`
/// Save only the Orphan; everything underneath must survive.
#[test]
fn test_nested_orphan_of_mapx_of_mapxord() {
    let mut inner_map: MapxOrd<u32, String> = MapxOrd::new();
    inner_map.insert(&1, &"hello".to_string());
    inner_map.insert(&2, &"world".to_string());

    let mut mid: Mapx<String, MapxOrd<u32, String>> = Mapx::new();
    mid.insert(&"data".to_string(), &inner_map);

    let outer: Orphan<Mapx<String, MapxOrd<u32, String>>> = Orphan::new(mid);

    let id = pnk!(outer.save_meta());

    let restored: Orphan<Mapx<String, MapxOrd<u32, String>>> =
        pnk!(Orphan::from_meta(id));
    let restored_mid = restored.get_value();
    let restored_inner = restored_mid.get(&"data".to_string()).unwrap();
    assert_eq!(restored_inner.get(&1), Some("hello".to_string()));
    assert_eq!(restored_inner.get(&2), Some("world".to_string()));
}

/// Postcard serde roundtrip for Mapx — verifies the hand-written
/// Serialize/Deserialize impl produces a valid, live handle.
#[test]
fn test_serde_roundtrip() {
    let mut hdr: Mapx<u32, String> = Mapx::new();
    hdr.insert(&1, &"alpha".into());
    hdr.insert(&2, &"beta".into());
    hdr.insert(&3, &"gamma".into());

    let bytes = postcard::to_allocvec(&hdr).unwrap();
    let restored: Mapx<u32, String> = postcard::from_bytes(&bytes).unwrap();

    assert!(restored.is_the_same_instance(&hdr));
    assert_eq!(restored.get(&1), Some("alpha".into()));
    assert_eq!(restored.get(&2), Some("beta".into()));
    assert_eq!(restored.get(&3), Some("gamma".into()));
}

/// Serialized size should stay compact.
#[test]
fn test_serde_size() {
    let hdr: Mapx<String, String> = Mapx::new();
    let bytes = postcard::to_allocvec(&hdr).unwrap();
    assert!(bytes.len() <= 64, "expected ≤64 bytes, got {}", bytes.len());
}

/// from_meta with a nonexistent ID must return an error.
#[test]
fn test_from_meta_nonexistent() {
    assert!(Mapx::<u32, u32>::from_meta(u64::MAX).is_err());
}

/// Restore from meta, mutate, verify both handles share storage.
#[test]
fn test_meta_restore_then_mutate() {
    let mut hdr: Mapx<u32, u32> = Mapx::new();
    hdr.insert(&1, &100);
    hdr.insert(&2, &200);

    let id = pnk!(hdr.save_meta());
    let mut restored: Mapx<u32, u32> = pnk!(Mapx::from_meta(id));

    restored.insert(&3, &300);
    restored.remove(&1);

    // Both handles see the same state
    assert!(hdr.get(&1).is_none());
    assert_eq!(hdr.get(&3), Some(300));
}

/// ValueEnDe roundtrip: encode a Mapx as a value, then decode it.
/// This is the path exercised when Mapx is used as a nested value type.
#[test]
fn test_valueende_roundtrip() {
    let mut inner: Mapx<u32, String> = Mapx::new();
    inner.insert(&10, &"ten".into());
    inner.insert(&20, &"twenty".into());

    let encoded = inner.encode();
    let decoded: Mapx<u32, String> = Mapx::decode(&encoded).unwrap();

    assert!(decoded.is_the_same_instance(&inner));
    assert_eq!(decoded.get(&10), Some("ten".into()));
    assert_eq!(decoded.get(&20), Some("twenty".into()));
}

// =====================================================================
// IntoIterator + keys()
// =====================================================================

#[test]
fn test_into_iter_ref() {
    let mut hdr: Mapx<u32, u32> = Mapx::new();
    for i in 0..10u32 {
        hdr.insert(&i, &(i * 10));
    }
    let mut count = 0;
    for (k, v) in &hdr {
        assert_eq!(v, k * 10);
        count += 1;
    }
    assert_eq!(count, 10);
}

#[test]
fn test_into_iter_mut() {
    let mut hdr: Mapx<u32, u32> = Mapx::new();
    for i in 0..5u32 {
        hdr.insert(&i, &i);
    }
    for (_k, mut v) in &mut hdr {
        *v += 100;
    }
    for i in 0..5u32 {
        assert_eq!(hdr.get(&i), Some(i + 100));
    }
}

#[test]
fn test_get_mut_persists_interior_mutability_change() {
    let mut hdr: Mapx<u32, RefCell<u32>> = Mapx::new();
    hdr.insert(&1, &RefCell::new(10));

    {
        let value = hdr.get_mut(&1).unwrap();
        *value.borrow_mut() = 20;
    }

    assert_eq!(*hdr.get(&1).unwrap().borrow(), 20);
}

#[test]
fn test_keys() {
    let mut hdr: Mapx<u32, String> = Mapx::new();
    hdr.insert(&3, &"c".into());
    hdr.insert(&1, &"a".into());
    hdr.insert(&2, &"b".into());
    let mut keys: Vec<u32> = hdr.keys().collect();
    keys.sort();
    assert_eq!(keys, vec![1, 2, 3]);
}

#[test]
fn test_keys_empty() {
    let hdr: Mapx<u32, u32> = Mapx::new();
    assert_eq!(hdr.keys().count(), 0);
}

// =====================================================================
// Persistence / nesting
// =====================================================================

/// Deep nesting: Mapx<String, Mapx<String, Mapx<u32, u64>>>
/// Only outer meta needed; all 3 layers survive.
#[test]
fn test_deep_triple_nesting() {
    let mut innermost: Mapx<u32, u64> = Mapx::new();
    innermost.insert(&1, &111);
    innermost.insert(&2, &222);

    let mut mid: Mapx<String, Mapx<u32, u64>> = Mapx::new();
    mid.insert(&"a".into(), &innermost);

    let mut outer: Mapx<String, Mapx<String, Mapx<u32, u64>>> = Mapx::new();
    outer.insert(&"top".into(), &mid);

    let id = pnk!(outer.save_meta());
    let restored: Mapx<String, Mapx<String, Mapx<u32, u64>>> = pnk!(Mapx::from_meta(id));

    let r_mid = restored.get(&"top".into()).unwrap();
    let r_inner = r_mid.get(&"a".into()).unwrap();
    assert_eq!(r_inner.get(&1), Some(111));
    assert_eq!(r_inner.get(&2), Some(222));
}

// =====================================================================
// PartialEq (regression: must compare *decoded* values, not raw bytes)
// =====================================================================

/// `0.0_f64 == -0.0_f64` decodes equal despite encoding to different
/// bytes — `Mapx`'s `PartialEq` must agree with `f64`'s own, not a
/// byte-level comparison.
#[test]
fn test_partial_eq_decodes_values_not_bytes() {
    let mut m1: Mapx<u32, f64> = Mapx::new();
    m1.insert(&1, &0.0_f64);

    let mut m2: Mapx<u32, f64> = Mapx::new();
    m2.insert(&1, &-0.0_f64);

    assert_eq!(0.0_f64, -0.0_f64);
    assert_eq!(
        m1, m2,
        "maps holding decode-equal values must compare equal"
    );
}

/// Conversely, two bit-identical NaNs decode as *unequal* per IEEE-754
/// (`NaN != NaN`) — a byte-derived `PartialEq` would wrongly call them
/// equal.
#[test]
fn test_partial_eq_nan_values_are_never_equal() {
    let mut m1: Mapx<u32, f64> = Mapx::new();
    m1.insert(&1, &f64::NAN);

    let mut m2: Mapx<u32, f64> = Mapx::new();
    m2.insert(&1, &f64::NAN);

    assert_ne!(m1, m2, "NaN must never compare equal, even to itself");
}
