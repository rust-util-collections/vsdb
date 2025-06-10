use super::*;
use ruc::*;
use std::mem::size_of;

#[test]
fn test_insert() {
    let mut hdr = MapxRaw::new();
    let max = 100;
    (0..max)
        .map(|i: u64| (to_bytes(i), to_bytes(max + i)))
        .for_each(|(key, value)| {
            assert!(hdr.get(&key).is_none());
            hdr.entry(&key).or_insert(&value);

            // After inserting, should exist
            assert!(hdr.contains_key(&key));
            assert_eq!(&pnk!(hdr.get(&key))[..], &value[..]);

            // Remove it
            hdr.remove(&key);
            assert!(hdr.get(&key).is_none());

            // Insert again
            hdr.insert(&key, &value);
            assert!(hdr.contains_key(&key));
        });

    hdr.clear();
    (0..max).map(|i: u64| to_bytes(i)).for_each(|key| {
        assert!(hdr.get(&key).is_none());
    });
}

#[test]
fn test_iter() {
    let mut hdr = MapxRaw::new();
    let max = 100;
    (0..max)
        .map(|i: u64| (to_bytes(i), to_bytes(i)))
        .for_each(|(key, value)| {
            hdr.insert(&key, &value);
        });

    hdr.iter_mut().for_each(|(k, mut v)| {
        *v = to_bytes(to_u64(&v) + 1).to_vec().into();
    });

    for (idx, (key, value)) in hdr.iter().enumerate() {
        assert_eq!(idx as u64 + 1, to_u64(&value));
    }
}

#[test]
fn test_first_last() {
    let mut hdr = MapxRaw::new();
    let max = 100;
    (0..max)
        .map(|i: u64| (to_bytes(i), to_bytes(i)))
        .for_each(|(key, value)| {
            hdr.insert(&key, &value);
        });

    let (_, value) = pnk!(hdr.iter().next());
    let val = to_u64(&value);
    assert_eq!(0, val);

    let (_, value) = pnk!(hdr.iter().next_back());
    let val = to_u64(&value);
    assert_eq!(max - 1, val);
}

#[test]
fn test_batch() {
    let mut hdr = MapxRaw::new();
    let max = 100u64;

    hdr.batch(|batch| {
        for i in 0..max {
            let key = to_bytes(i);
            let value = to_bytes(max + i);
            batch.insert(&key, &value);
        }
    });

    for i in 0..max {
        let key = to_bytes(i);
        let value = to_bytes(max + i);
        assert_eq!(&pnk!(hdr.get(&key))[..], &value[..]);
    }

    hdr.batch(|batch| {
        for i in 0..max {
            let key = to_bytes(i);
            batch.remove(&key);
        }
    });

    for i in 0..max {
        let key = to_bytes(i);
        assert!(hdr.get(&key).is_none());
    }
}

fn to_u64(bytes: &[u8]) -> u64 {
    u64::from_be_bytes(<[u8; size_of::<u64>()]>::try_from(bytes).unwrap())
}

fn to_bytes(i: u64) -> [u8; size_of::<u64>()] {
    i.to_be_bytes()
}
