use crate::ValueEnDe;

use super::*;
use ruc::*;

#[test]
fn test_insert() {
    let mut hdr: MapxOrdRawValue<usize> = MapxOrdRawValue::new();
    let max = 100;
    (0..max)
        .map(|i: usize| (i, (max + i).to_be_bytes()))
        .for_each(|(key, value)| {
            assert!(hdr.get(&key).is_none());
            hdr.entry(key).or_insert(&value);
            hdr.set_value(&key, &value[..]);
            hdr.insert(&key, &value[..]);
            assert!(hdr.contains_key(&key));

            assert_eq!(*pnk!(hdr.get(&key)), value);
            hdr.remove(&key);

            assert!(hdr.get(&key).is_none());
            hdr.insert(&key, &value[..]);
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
        let mut hdr: MapxOrdRawValue<usize> = MapxOrdRawValue::new();
        (0..cnt)
            .map(|i: usize| (i, <usize as ValueEnDe>::encode(&i)))
            .for_each(|(key, value)| {
                hdr.insert(&key, &value);
            });
        <MapxOrdRawValue<usize> as ValueEnDe>::encode(&hdr)
    };
    let mut reloaded = pnk!(<MapxOrdRawValue<usize> as ValueEnDe>::decode(&dehdr));
    (0..cnt).map(|i: usize| i).for_each(|i| {
        let val = pnk!(<usize as ValueEnDe>::decode(&pnk!(reloaded.get(&i))));
        assert_eq!(i, val);
    });
}

#[test]
fn test_iter() {
    let mut hdr: MapxOrdRawValue<usize> = MapxOrdRawValue::new();
    let max = 100;
    (0..max)
        .map(|i: usize| (i, i.to_be_bytes()))
        .for_each(|(key, value)| {
            hdr.insert(&key, &value[..]);
        });
    for (key, _) in hdr.iter().collect::<Vec<_>>().into_iter() {
        hdr.unset_value(&key);
    }
}

#[test]
fn test_first_last() {
    let mut hdr: MapxOrdRawValue<usize> = MapxOrdRawValue::new();
    let max = 100;
    (0..max)
        .map(|i: usize| (i, <usize as ValueEnDe>::encode(&i)))
        .for_each(|(key, value)| {
            hdr.insert(&key, &value);
        });

    let (_, value) = pnk!(hdr.first());
    let val = pnk!(<usize as ValueEnDe>::decode(&value));
    assert_eq!(0, val);

    let (_, value) = pnk!(hdr.last());
    let val = pnk!(<usize as ValueEnDe>::decode(&value));
    assert_eq!(max - 1, val);
}
