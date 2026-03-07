use crate::ValueEnDe;

use super::*;
use ruc::*;

#[test]
fn test_insert() {
    let mut hdr: MapxDk<usize, usize, usize> = MapxDk::new();
    assert_eq!(2, hdr.key_size());
    let max = 100;
    (0..max)
        .map(|i: usize| (i, max + i))
        .for_each(|(i, value)| {
            let key = &(&i, &i);
            assert!(hdr.get(key).is_none());
            hdr.entry(key).or_insert(value);
            hdr.insert(key, &value);
            assert!(hdr.contains_key(&key));
            assert_eq!(pnk!(hdr.get(&key)), value);
            hdr.remove(&(&i, Some(&i)));
            assert!(hdr.get(&key).is_none());
            hdr.insert(&key, &value);
        });
    hdr.clear();
    (0..max).map(|i: usize| i).for_each(|i| {
        let key = &(&i, &i);
        assert!(hdr.get(key).is_none());
    });
}

#[test]
fn test_valueende() {
    let cnt = 100;
    let dehdr = {
        let mut hdr: MapxDk<usize, usize, usize> = MapxDk::new();
        let max = 100;
        (0..max).map(|i: usize| (i, i)).for_each(|(key, value)| {
            let key = &(&key, &key);
            hdr.insert(key, &value);
        });
        <MapxDk<usize, usize, usize> as ValueEnDe>::encode(&hdr)
    };
    let mut reloaded: MapxDk<usize, usize, usize> =
        pnk!(<MapxDk<usize, usize, usize> as ValueEnDe>::decode(&dehdr));

    (0..cnt).map(|i: usize| i).for_each(|i| {
        let key = &(&i, &i);
        assert_eq!(i, pnk!(reloaded.get(key)));
    });
}
