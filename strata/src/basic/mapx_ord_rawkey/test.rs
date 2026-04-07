use super::*;
use ruc::*;

#[test]
fn test_insert() {
    let mut hdr: MapxOrdRawKey<usize> = MapxOrdRawKey::new();
    let max = 100;
    (0..max)
        .map(|i: usize| (i.to_be_bytes(), (max + i)))
        .for_each(|(key, value)| {
            let key = key.to_vec();
            assert!(hdr.get(&key).is_none());
            hdr.entry(&key[..]).or_insert(value);
            hdr.insert(&key[..], &value);
            assert!(hdr.contains_key(&key[..]));
            assert_eq!(pnk!(hdr.get(&key[..])), value);
            hdr.remove(&key[..]);
            assert!(hdr.get(&key[..]).is_none());
            hdr.insert(&key[..], &value);
        });
    hdr.clear();
    (0..max).map(|i: usize| i.to_be_bytes()).for_each(|key| {
        assert!(hdr.get(&key).is_none());
    });
}

#[test]
fn test_valueende() {
    let cnt = 100;
    let dehdr = {
        let mut hdr: MapxOrdRawKey<usize> = MapxOrdRawKey::new();
        (0..cnt)
            .map(|i: usize| (i.to_be_bytes(), i))
            .for_each(|(key, value)| {
                hdr.insert(&key[..], &value);
            });
        <MapxOrdRawKey<usize> as ValueEnDe>::encode(&hdr)
    };
    let mut reloaded = pnk!(<MapxOrdRawKey<usize> as ValueEnDe>::decode(&dehdr));
    (0..cnt).map(|i: usize| i).for_each(|i| {
        assert_eq!(i, reloaded.get(&i.to_be_bytes()).unwrap());
    });
}

#[test]
fn test_iter() {
    let mut hdr: MapxOrdRawKey<usize> = MapxOrdRawKey::new();
    let max = 100;
    (0..max)
        .map(|i: usize| (i.to_be_bytes(), i))
        .for_each(|(key, value)| {
            hdr.insert(&key[..], &value);
        });
    for (key, _) in hdr.iter().collect::<Vec<_>>().into_iter() {
        hdr.remove(&key);
    }
}

#[test]
fn test_first_last() {
    let mut hdr: MapxOrdRawKey<usize> = MapxOrdRawKey::new();
    let max = 100;
    (0..max)
        .map(|i: usize| (i.to_be_bytes(), i))
        .for_each(|(key, value)| {
            hdr.insert(&key[..], &value);
        });
    let (_, value) = pnk!(hdr.first());
    assert_eq!(0, value);

    let (_, value) = pnk!(hdr.last());
    assert_eq!(max - 1, value);
}

#[test]
fn test_values() {
    let mut hdr: MapxOrdRawKey<usize> = MapxOrdRawKey::new();
    let max = 100;
    (0..max)
        .map(|i: usize| (i.to_be_bytes(), i))
        .for_each(|(key, value)| {
            hdr.insert(&key[..], &value);
        });
    let mut i = 0;
    for (_, it) in hdr.iter() {
        assert_eq!(i, it);
        i = i + 1;
    }
}

#[test]
fn test_values_first_last() {
    let mut hdr: MapxOrdRawKey<usize> = MapxOrdRawKey::new();
    let max = 100;
    (0..max)
        .map(|i: usize| (i.to_be_bytes(), i))
        .for_each(|(key, value)| {
            hdr.insert(&key[..], &value);
        });
    let (_, value) = pnk!(hdr.iter().next());
    assert_eq!(0, value);

    let (_, value) = pnk!(hdr.iter().next_back());
    assert_eq!(max - 1, value);
}

#[test]
fn test_save_and_from_meta() {
    let mut hdr: MapxOrdRawKey<String> = MapxOrdRawKey::new();
    hdr.insert(&[1u8], &"hello".to_string());
    hdr.insert(&[2u8], &"world".to_string());

    let id = pnk!(hdr.save_meta());
    assert_eq!(id, hdr.instance_id());

    let restored: MapxOrdRawKey<String> = pnk!(MapxOrdRawKey::from_meta(id));
    assert_eq!(restored.get(&[1u8]), Some("hello".to_string()));
    assert_eq!(restored.get(&[2u8]), Some("world".to_string()));
    assert!(restored.is_the_same_instance(&hdr));
}

/// Postcard serde roundtrip for MapxOrdRawKey.
#[test]
fn test_serde_roundtrip() {
    let mut hdr: MapxOrdRawKey<Vec<u8>> = MapxOrdRawKey::new();
    hdr.insert(b"key1", &vec![1, 2, 3]);
    hdr.insert(b"key2", &vec![4, 5, 6]);

    let bytes = postcard::to_allocvec(&hdr).unwrap();
    let restored: MapxOrdRawKey<Vec<u8>> = postcard::from_bytes(&bytes).unwrap();

    assert!(restored.is_the_same_instance(&hdr));
    assert_eq!(restored.get(b"key1"), Some(vec![1, 2, 3]));
    assert_eq!(restored.get(b"key2"), Some(vec![4, 5, 6]));
}

/// from_meta nonexistent.
#[test]
fn test_from_meta_nonexistent() {
    assert!(MapxOrdRawKey::<String>::from_meta(u64::MAX).is_err());
}

/// Restore from meta and mutate.
#[test]
fn test_meta_restore_then_mutate() {
    let mut hdr: MapxOrdRawKey<u64> = MapxOrdRawKey::new();
    hdr.insert(&[1u8], &100);

    let id = pnk!(hdr.save_meta());
    let mut restored: MapxOrdRawKey<u64> = pnk!(MapxOrdRawKey::from_meta(id));
    restored.insert(&[2u8], &200);

    assert_eq!(hdr.get(&[2u8]), Some(200));
}
