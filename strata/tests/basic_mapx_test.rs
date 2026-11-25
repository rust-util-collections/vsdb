use serde::{Deserialize, Serialize};
use vsdb::{Mapx, ValueEnDe, VsdbOptions, vsdb_configure};

#[derive(Serialize, Deserialize, Default, Debug, Eq, PartialEq, Clone)]
struct SampleBlock {
    idx: usize,
    data: Vec<usize>,
}

fn gen_sample(idx: usize) -> SampleBlock {
    SampleBlock {
        idx,
        data: vec![idx],
    }
}

#[test]
fn basic_cases() {
    let cnt = 200;
    vsdb_configure(VsdbOptions::new(format!(
        "/tmp/vsdb_testing/{}",
        rand::random::<u64>()
    )))
    .unwrap();

    let hdr = {
        let mut hdr_i = Mapx::new();

        (0..cnt).for_each(|i| {
            assert!(hdr_i.get(&i).is_none());
        });

        (0..cnt).map(|i| (i, gen_sample(i))).for_each(|(i, b)| {
            hdr_i.entry(&i).or_insert(b.clone());
            assert_eq!(hdr_i.get(&i).unwrap().idx, i);
            hdr_i.remove(&i);
            assert!(hdr_i.get(&i).is_none());
            hdr_i.insert(&i, &b);
            hdr_i.insert(&i, &b);
        });

        <Mapx<usize, SampleBlock> as ValueEnDe>::encode_value(&hdr_i)
    };

    let mut reloaded =
        <Mapx<usize, SampleBlock> as ValueEnDe>::decode_value(&hdr).unwrap();

    (0..cnt).for_each(|i| {
        assert_eq!(i, reloaded.get(&i).unwrap().idx);
    });

    (1..cnt).for_each(|i| {
        reloaded.get_mut(&i).unwrap().idx = 1 + i;
        assert_eq!(reloaded.get(&i).unwrap().idx, 1 + i);
        assert!(reloaded.contains_key(&i));
        reloaded.remove(&i);
        assert!(!reloaded.contains_key(&i));
    });

    reloaded.clear();
}
