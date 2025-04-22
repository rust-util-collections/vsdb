use ruc::*;
use serde::{Deserialize, Serialize};
use vsdb::{ValueEnDe, basic::mapx_ord_rawkey::MapxOrdRawKey, vsdb_set_base_dir};

#[derive(Serialize, Deserialize, Default, Debug, Eq, PartialEq, Clone)]
struct SampleBlock {
    data: Vec<u8>,
}

fn gen_sample(bytes: &[u8]) -> SampleBlock {
    SampleBlock {
        data: bytes.to_vec(),
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
        let mut hdr_i = MapxOrdRawKey::new();

        assert_eq!(0, hdr_i.len());
        (0usize..cnt).map(|i| i.to_be_bytes()).for_each(|i| {
            assert!(hdr_i.get(&i).is_none());
        });

        (0usize..cnt)
            .map(|i| i.to_be_bytes())
            .map(|i| (i, gen_sample(&i)))
            .for_each(|(i, b)| {
                hdr_i.entry(&i[..]).or_insert(b.clone());
                assert_eq!(&hdr_i.get(&i).unwrap().data, &i);
                assert_eq!(hdr_i.remove(&i), Some(b.clone()));
                assert!(hdr_i.get(&i).is_none());
                assert!(hdr_i.insert(&i, &b).is_none());
                assert!(hdr_i.insert(&i, &b).is_some());
            });

        assert_eq!(cnt, hdr_i.len());

        <MapxOrdRawKey<SampleBlock> as ValueEnDe>::encode(&hdr_i)
    };

    let mut reloaded = pnk!(<MapxOrdRawKey<SampleBlock> as ValueEnDe>::decode(&hdr));

    assert_eq!(cnt, reloaded.len());

    (0usize..cnt).map(|i| i.to_be_bytes()).for_each(|i| {
        assert_eq!(&i[..], &reloaded.get(&i).unwrap().data);
    });

    (1usize..cnt).for_each(|i| {
        (*reloaded.get_mut(&i.to_be_bytes()).unwrap()).data =
            (1 + i).to_be_bytes().to_vec();
        assert_eq!(
            &reloaded.get(&i.to_be_bytes()).unwrap().data,
            &(1 + i).to_be_bytes()[..]
        );
        assert!(reloaded.contains_key(&i.to_be_bytes()));
        assert!(reloaded.remove(&i.to_be_bytes()).is_some());
        assert!(!reloaded.contains_key(&i.to_be_bytes()));
    });

    assert_eq!(1, reloaded.len());
    reloaded.clear();
    assert!(reloaded.is_empty());
}
