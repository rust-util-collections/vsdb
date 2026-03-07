use ruc::*;
use serde::{Deserialize, Serialize};
use vsdb::{Mapx, ValueEnDe, vsdb_set_base_dir};

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
    info_omit!(vsdb_set_base_dir(&format!(
        "/tmp/vsdb_testing/{}",
        rand::random::<u64>()
    )));

    let hdr = {
        let mut hdr_i = Mapx::new();

        (0..cnt).for_each(|i| {
            assert!(hdr_i.get(&i).is_none());
        });

        (0..cnt).map(|i| (i, gen_sample(i))).for_each(|(i, b)| {
            hdr_i.entry(&i).or_insert(b.clone());
            assert_eq!(pnk!(hdr_i.get(&i)).idx, i);
            hdr_i.remove(&i);
            assert!(hdr_i.get(&i).is_none());
            hdr_i.insert(&i, &b);
            hdr_i.insert(&i, &b);
        });

        <Mapx<usize, SampleBlock> as ValueEnDe>::encode(&hdr_i)
    };

    let mut reloaded = pnk!(<Mapx<usize, SampleBlock> as ValueEnDe>::decode(&hdr));

    (0..cnt).for_each(|i| {
        assert_eq!(i, reloaded.get(&i).unwrap().idx);
    });

    (1..cnt).for_each(|i| {
        pnk!(reloaded.get_mut(&i)).idx = 1 + i;
        assert_eq!(pnk!(reloaded.get(&i)).idx, 1 + i);
        assert!(reloaded.contains_key(&i));
        reloaded.remove(&i);
        assert!(!reloaded.contains_key(&i));
    });

    reloaded.clear();
}
