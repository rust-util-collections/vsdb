use super::*;
use ruc::*;
use std::mem::size_of;

#[test]
fn test_insert() {
    let mut hdr = MapxRaw::new();
    let max = 500;
    (0..max)
        .map(|i: u64| (to_bytes(i), to_bytes(max + i)))
        .for_each(|(key, value)| {
            assert!(hdr.get(&key).is_none());
            hdr.entry(&key).or_insert(&value);
            assert!(hdr.insert(&key, &value).is_some());
            assert!(hdr.contains_key(&key));
            assert_eq!(&pnk!(hdr.get(&key))[..], &value[..]);
            assert_eq!(&pnk!(hdr.remove(&key))[..], &value[..]);
            assert!(hdr.get(&key).is_none());
            assert!(hdr.insert(&key, &value).is_none());
        });
    hdr.clear();
    (0..max).map(|i: u64| to_bytes(i)).for_each(|key| {
        assert!(hdr.get(&key).is_none());
    });
    assert!(hdr.is_empty());
}
#[test]
fn test_len() {
    let mut hdr = MapxRaw::new();
    let max = 500;
    (0..max)
        .map(|i: u64| (to_bytes(i), to_bytes(max + i)))
        .for_each(|(key, value)| {
            assert!(hdr.insert(&key, &value).is_none());
        });
    assert_eq!(500, hdr.len());

    for key in 0..max {
        assert!(hdr.remove(&to_bytes(key)).is_some());
    }
    assert_eq!(0, hdr.len());
}

#[test]
fn test_iter() {
    let mut hdr = MapxRaw::new();
    let max = 500;
    (0..max)
        .map(|i: u64| (to_bytes(i), to_bytes(i)))
        .for_each(|(key, value)| {
            assert!(hdr.insert(&key, &value).is_none());
        });

    hdr.iter_mut().for_each(|(k, mut v)| {
        *v = to_bytes(to_u64(&v) + 1).to_vec().into();
    });

    for (idx, key) in hdr
        .iter()
        .map(|(k, _)| k)
        .collect::<Vec<_>>()
        .into_iter()
        .enumerate()
    {
        assert_eq!(idx as u64 + 1, to_u64(&hdr.remove(&key).unwrap()));
    }

    assert_eq!(0, hdr.len());
}

#[test]
fn test_first_last() {
    let mut hdr = MapxRaw::new();
    let max = 500;
    (0..max)
        .map(|i: u64| (to_bytes(i), to_bytes(i)))
        .for_each(|(key, value)| {
            assert!(hdr.insert(&key, &value).is_none());
        });
    let (_, value) = pnk!(hdr.iter().next());
    let val = to_u64(&value);
    assert_eq!(0, val);

    let (_, value) = pnk!(hdr.iter().next_back());
    let val = to_u64(&value);
    assert_eq!(max - 1, val);
}

fn to_u64(bytes: &[u8]) -> u64 {
    u64::from_be_bytes(<[u8; size_of::<u64>()]>::try_from(bytes).unwrap())
}

fn to_bytes(i: u64) -> [u8; size_of::<u64>()] {
    i.to_be_bytes()
}
