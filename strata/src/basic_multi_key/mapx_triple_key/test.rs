use crate::ValueEnDe;

use super::*;
use ruc::*;

#[test]
fn test_insert() {
    let mut hdr: MapxTk<usize, usize, usize, usize> = MapxTk::new();
    assert_eq!(3, hdr.key_size());
    let max = 100;
    (0..max)
        .map(|i: usize| (i, max + i))
        .for_each(|(i, value)| {
            let key = &(&i, &i, &i);
            assert!(hdr.get(key).is_none());
            hdr.entry(key).or_insert(value);
            hdr.insert(key, &value);
            assert!(hdr.contains_key(&key));
            assert_eq!(pnk!(hdr.get(&key)), value);
            hdr.remove(&(&i, Some((&i, Some(&i)))));
            assert!(hdr.get(&key).is_none());
            hdr.insert(key, &value);
        });
    hdr.clear();
    (0..max).map(|i: usize| i).for_each(|i| {
        let key = &(&i, &i, &i);
        assert!(hdr.get(key).is_none());
    });
}

#[test]
fn test_valueende() {
    let cnt = 100;
    let dehdr = {
        let mut hdr: MapxTk<usize, usize, usize, usize> = MapxTk::new();
        let max = 100;
        (0..max).map(|i: usize| (i, i)).for_each(|(key, value)| {
            let key = &(&key, &key, &key);
            hdr.insert(key, &value);
        });
        <MapxTk<usize, usize, usize, usize> as ValueEnDe>::encode(&hdr)
    };
    let mut reloaded: MapxTk<usize, usize, usize, usize> =
        pnk!(<MapxTk<usize, usize, usize, usize> as ValueEnDe>::decode(
            &dehdr
        ));

    (0..cnt).map(|i: usize| i).for_each(|i| {
        let key = &(&i, &i, &i);
        assert_eq!(i, pnk!(reloaded.get(key)));
    });
}
