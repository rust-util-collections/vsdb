use super::{Mapx, ValueEnDe};
use ruc::*;

#[test]
fn test_insert() {
    let mut hdr: Mapx<usize, usize> = Mapx::new();
    let max = 100;
    (0..max)
        .map(|i: usize| (i, (max + i)))
        .for_each(|(key, value)| {
            assert!(hdr.get(&key).is_none());
            hdr.set_value(&key, &value);
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
    let mut reloaded = pnk!(<Mapx<usize, usize> as ValueEnDe>::decode(&dehdr));
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
    let value = pnk!(hdr.values().next());
    // assert_eq!(0, value);

    let value = pnk!(hdr.values().next_back());
    // assert_eq!(max - 1, value);
}
