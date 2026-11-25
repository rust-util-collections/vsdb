use super::*;

#[test]
fn test_insert() {
    let mut hdr: MapxOrd<u64, u64> = MapxOrd::new();
    let max = 100;
    (0..max)
        .map(|i: u64| (i, (max + i)))
        .for_each(|(key, value)| {
            assert!(hdr.get(&key).is_none());
            hdr.insert(&key, &value);
            assert!(hdr.contains_key(&key));
            assert_eq!(hdr.get(&key).unwrap(), value);
            hdr.remove(&key);
            assert!(hdr.get(&key).is_none());
        });
    hdr.clear();
    (0..max).map(|i: u64| i).for_each(|key| {
        assert!(hdr.get(&key).is_none());
    });
}

#[test]
fn test_valueende() {
    let cnt = 100;
    let dehdr = {
        let mut hdr: MapxOrd<u64, u64> = MapxOrd::new();
        (0..cnt).map(|i: u64| (i, i)).for_each(|(key, value)| {
            hdr.insert(&key, &value);
        });
        <MapxOrd<u64, u64> as ValueEnDe>::encode_value(&hdr)
    };
    let reloaded = <MapxOrd<u64, u64> as ValueEnDe>::decode_value(&dehdr).unwrap();
    (0..cnt).map(|i: u64| i).for_each(|i| {
        assert_eq!(i, reloaded.get(&i).unwrap());
    });
}

#[test]
fn test_iter() {
    let mut hdr: MapxOrd<u64, u64> = MapxOrd::new();
    let max = 100;
    (0..max).map(|i: u64| (i, i)).for_each(|(key, value)| {
        hdr.insert(&key, &value);
    });
    for (key, value) in hdr.iter().collect::<Vec<_>>().into_iter() {
        assert_eq!(key, value);
        hdr.remove(&key);
    }
}

#[test]
fn test_first_last() {
    let mut hdr: MapxOrd<u64, u64> = MapxOrd::new();
    let max = 100;
    (0..max).map(|i: u64| (i, i)).for_each(|(key, value)| {
        hdr.insert(&key, &value);
    });
    let (key, value) = hdr.first().unwrap();
    assert_eq!(key, value);
    assert_eq!(0, key);

    let (key, value) = hdr.last().unwrap();
    assert_eq!(key, value);
    assert_eq!(max - 1, key);
}

#[test]
fn test_values() {
    let mut hdr: MapxOrd<u64, u64> = MapxOrd::new();
    let max = 100;
    (0..max).map(|i: u64| (i, i)).for_each(|(key, value)| {
        hdr.insert(&key, &value);
    });
    for (i, it) in hdr.values().enumerate() {
        assert_eq!(i as u64, it);
    }
}

#[test]
fn test_values_first_last() {
    let mut hdr: MapxOrd<u64, u64> = MapxOrd::new();
    let max = 100;
    (0..max).map(|i: u64| (i, i)).for_each(|(key, value)| {
        hdr.insert(&key, &value);
    });
    let value = hdr.values().next().unwrap();
    assert_eq!(0, value);

    let value = hdr.values().next_back().unwrap();
    assert_eq!(max - 1, value);
}

#[test]
fn test_save_and_from_meta() {
    let mut hdr: MapxOrd<u32, String> = MapxOrd::new();
    hdr.insert(&1, &"hello".to_string());
    hdr.insert(&2, &"world".to_string());

    let id = hdr.save_meta().unwrap();
    assert_eq!(id, hdr.instance_id());

    let restored: MapxOrd<u32, String> = MapxOrd::from_meta(id).unwrap();
    assert_eq!(restored.get(&1), Some("hello".to_string()));
    assert_eq!(restored.get(&2), Some("world".to_string()));
    assert_eq!(restored.first(), Some((1, "hello".to_string())));
    assert!(restored.is_the_same_instance(&hdr));
}

/// Postcard serde roundtrip for MapxOrd.
#[test]
fn test_serde_roundtrip() {
    let mut hdr: MapxOrd<u64, u64> = MapxOrd::new();
    for i in 0..20 {
        hdr.insert(&i, &(i * 100));
    }

    let bytes = postcard::to_allocvec(&hdr).unwrap();
    let restored: MapxOrd<u64, u64> = postcard::from_bytes(&bytes).unwrap();

    assert!(restored.is_the_same_instance(&hdr));
    for i in 0..20 {
        assert_eq!(restored.get(&i), Some(i * 100));
    }
    // Verify ordering is preserved
    let keys: Vec<u64> = restored.iter().map(|(k, _)| k).collect();
    assert_eq!(keys, (0..20).collect::<Vec<_>>());
}

/// from_meta nonexistent.
#[test]
fn test_from_meta_nonexistent() {
    assert!(MapxOrd::<u32, u32>::from_meta(u64::MAX).is_err());
}

/// Restore from meta and mutate — verifies shared storage.
#[test]
fn test_meta_restore_then_mutate() {
    let mut hdr: MapxOrd<u32, String> = MapxOrd::new();
    hdr.insert(&1, &"a".into());

    let id = hdr.save_meta().unwrap();
    let mut restored: MapxOrd<u32, String> = MapxOrd::from_meta(id).unwrap();
    restored.insert(&2, &"b".into());

    assert_eq!(hdr.get(&2), Some("b".into()));
}

// =====================================================================
// IntoIterator + keys()
// =====================================================================

#[test]
fn test_into_iter_ref() {
    let mut hdr: MapxOrd<u32, u32> = MapxOrd::new();
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
    let mut hdr: MapxOrd<u32, u32> = MapxOrd::new();
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
fn test_keys_ordered() {
    let mut hdr: MapxOrd<u32, String> = MapxOrd::new();
    hdr.insert(&30, &"c".into());
    hdr.insert(&10, &"a".into());
    hdr.insert(&20, &"b".into());
    // keys() must return ascending order for MapxOrd
    let keys: Vec<u32> = hdr.keys().collect();
    assert_eq!(keys, vec![10, 20, 30]);
}

#[test]
fn test_keys_empty() {
    let hdr: MapxOrd<u32, u32> = MapxOrd::new();
    assert_eq!(hdr.keys().count(), 0);
}

#[test]
fn test_keys_never_decodes_values() {
    // A value type whose `decode` panics unconditionally — `encode` is
    // a normal, valid encoding, so inserts succeed and the on-disk
    // bytes are well-formed; only `decode` is instrumented to detect
    // whether `keys()` ever touches the value bytes.
    #[derive(Clone, Debug)]
    struct PanicsOnDecode(u32);

    impl crate::common::ende::ValueEnDe for PanicsOnDecode {
        fn encode_value_into(
            &self,
            dst: &mut Vec<u8>,
        ) -> crate::common::error::Result<()> {
            dst.extend_from_slice(&self.0.to_le_bytes());
            Ok(())
        }
        fn decode_value(_bytes: &[u8]) -> crate::common::error::Result<Self> {
            panic!("keys() must never decode values");
        }
    }

    let mut hdr: MapxOrd<u32, PanicsOnDecode> = MapxOrd::new();
    hdr.insert(&30, &PanicsOnDecode(300));
    hdr.insert(&10, &PanicsOnDecode(100));
    hdr.insert(&20, &PanicsOnDecode(200));

    // Must not panic: keys() decodes only K.
    let keys: Vec<u32> = hdr.keys().collect();
    assert_eq!(keys, vec![10, 20, 30]);

    // Control: iterating full entries DOES decode V and panics,
    // confirming the test type is wired correctly.
    let panicked =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| hdr.iter().count()))
            .is_err();
    assert!(panicked, "expected iter() to decode V and panic");
}

// =====================================================================
// Persistence
// =====================================================================

/// ValueEnDe roundtrip for MapxOrd (used when MapxOrd is a nested value).
#[test]
fn test_valueende_roundtrip() {
    let mut m: MapxOrd<u32, String> = MapxOrd::new();
    m.insert(&5, &"five".into());
    m.insert(&10, &"ten".into());

    let encoded = m.encode_value();
    let decoded: MapxOrd<u32, String> = MapxOrd::decode_value(&encoded).unwrap();
    assert!(decoded.is_the_same_instance(&m));
    assert_eq!(decoded.get(&5), Some("five".into()));
}

// =====================================================================
// PartialEq (regression: must compare *decoded* values, not raw bytes)
// =====================================================================

/// `0.0_f64 == -0.0_f64` decodes equal despite encoding to different
/// bytes — `MapxOrd`'s `PartialEq` must agree with `f64`'s own, not a
/// byte-level comparison.
#[test]
fn test_partial_eq_decodes_values_not_bytes() {
    let mut m1: MapxOrd<u32, f64> = MapxOrd::new();
    m1.insert(&1, &0.0_f64);

    let mut m2: MapxOrd<u32, f64> = MapxOrd::new();
    m2.insert(&1, &-0.0_f64);

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
    let mut m1: MapxOrd<u32, f64> = MapxOrd::new();
    m1.insert(&1, &f64::NAN);

    let mut m2: MapxOrd<u32, f64> = MapxOrd::new();
    m2.insert(&1, &f64::NAN);

    assert_ne!(m1, m2, "NaN must never compare equal, even to itself");
}
