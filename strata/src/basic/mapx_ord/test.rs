use super::*;
use ruc::*;

#[test]
fn test_insert() {
    let mut hdr: MapxOrd<usize, usize> = MapxOrd::new();
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
fn test_valueende() {
    let cnt = 100;
    let dehdr = {
        let mut hdr: MapxOrd<usize, usize> = MapxOrd::new();
        (0..cnt).map(|i: usize| (i, i)).for_each(|(key, value)| {
            hdr.insert(&key, &value);
        });
        <MapxOrd<usize, usize> as ValueEnDe>::encode(&hdr)
    };
    let mut reloaded = pnk!(<MapxOrd<usize, usize> as ValueEnDe>::decode(&dehdr));
    (0..cnt).map(|i: usize| i).for_each(|i| {
        assert_eq!(i, reloaded.get(&i).unwrap());
    });
}

#[test]
fn test_iter() {
    let mut hdr: MapxOrd<usize, usize> = MapxOrd::new();
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
    let mut hdr: MapxOrd<usize, usize> = MapxOrd::new();
    let max = 100;
    (0..max).map(|i: usize| (i, i)).for_each(|(key, value)| {
        hdr.insert(&key, &value);
    });
    let (key, value) = pnk!(hdr.first());
    assert_eq!(key, value);
    assert_eq!(0, key);

    let (key, value) = pnk!(hdr.last());
    assert_eq!(key, value);
    assert_eq!(max - 1, key);
}

#[test]
fn test_values() {
    let mut hdr: MapxOrd<usize, usize> = MapxOrd::new();
    let max = 100;
    (0..max).map(|i: usize| (i, i)).for_each(|(key, value)| {
        hdr.insert(&key, &value);
    });
    let mut i = 0;
    for it in hdr.values() {
        assert_eq!(i, it);
        i = i + 1;
    }
}

#[test]
fn test_values_first_last() {
    let mut hdr: MapxOrd<usize, usize> = MapxOrd::new();
    let max = 100;
    (0..max).map(|i: usize| (i, i)).for_each(|(key, value)| {
        hdr.insert(&key, &value);
    });
    let value = pnk!(hdr.values().next());
    assert_eq!(0, value);

    let value = pnk!(hdr.values().next_back());
    assert_eq!(max - 1, value);
}

#[test]
fn test_save_and_from_meta() {
    let mut hdr: MapxOrd<u32, String> = MapxOrd::new();
    hdr.insert(&1, &"hello".to_string());
    hdr.insert(&2, &"world".to_string());

    let id = pnk!(hdr.save_meta());
    assert_eq!(id, hdr.instance_id());

    let restored: MapxOrd<u32, String> = pnk!(MapxOrd::from_meta(id));
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

    let id = pnk!(hdr.save_meta());
    let mut restored: MapxOrd<u32, String> = pnk!(MapxOrd::from_meta(id));
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

// =====================================================================
// Persistence
// =====================================================================

/// ValueEnDe roundtrip for MapxOrd (used when MapxOrd is a nested value).
#[test]
fn test_valueende_roundtrip() {
    let mut m: MapxOrd<u32, String> = MapxOrd::new();
    m.insert(&5, &"five".into());
    m.insert(&10, &"ten".into());

    let encoded = m.encode();
    let decoded: MapxOrd<u32, String> = MapxOrd::decode(&encoded).unwrap();
    assert!(decoded.is_the_same_instance(&m));
    assert_eq!(decoded.get(&5), Some("five".into()));
}
