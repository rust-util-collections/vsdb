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
        hdr.unset_value(&key);
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
