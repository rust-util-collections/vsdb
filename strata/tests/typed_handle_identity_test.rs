use serde::{Deserialize, Serialize};
use std::sync::Once;
use vsdb::{Mapx, MapxOrd, VsdbOptions, vsdb_configure};

mod accounting {
    #[derive(super::Serialize, super::Deserialize)]
    pub struct Record {
        pub amount: u64,
    }

    #[derive(super::Serialize, super::Deserialize)]
    pub struct Key(pub u64);
}

mod access {
    #[derive(super::Serialize, super::Deserialize)]
    pub struct Record {
        pub enabled: bool,
    }

    #[derive(super::Serialize, super::Deserialize)]
    pub struct Key(pub u64);
}

fn setup() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        vsdb_configure(VsdbOptions::new(format!(
            "/tmp/vsdb_testing/typed-identity-{}",
            rand::random::<u128>()
        )))
        .unwrap();
    });
}

#[test]
fn same_named_value_cannot_restore_or_rewrite_another_schema() {
    setup();
    let mut original = MapxOrd::<u64, accounting::Record>::new();
    original.insert(&7, &accounting::Record { amount: 1 });
    let id = original.save_meta().unwrap();
    let bytes = postcard::to_allocvec(&original).unwrap();
    drop(original);

    // Postcard can interpret the value 1 as either u64 or bool. The handle
    // identity must reject this before a wrong typed writer can change it.
    assert!(MapxOrd::<u64, access::Record>::from_meta(id).is_err());
    assert!(postcard::from_bytes::<MapxOrd<u64, access::Record>>(&bytes).is_err());

    type RecordAlias = accounting::Record;
    let restored = MapxOrd::<u64, RecordAlias>::from_meta(id).unwrap();
    assert_eq!(restored.get(&7).unwrap().amount, 1);
}

#[test]
fn same_named_keys_and_nested_values_keep_their_identity() {
    setup();
    let mut original = Mapx::<accounting::Key, Vec<Option<accounting::Record>>>::new();
    original.insert(
        &accounting::Key(7),
        &vec![Some(accounting::Record { amount: 1 })],
    );
    let bytes = postcard::to_allocvec(&original).unwrap();
    drop(original);

    assert!(
        postcard::from_bytes::<Mapx<access::Key, Vec<Option<accounting::Record>>>>(
            &bytes
        )
        .is_err()
    );
    assert!(
        postcard::from_bytes::<Mapx<accounting::Key, Vec<Option<access::Record>>>>(
            &bytes
        )
        .is_err()
    );
    let restored: Mapx<accounting::Key, Vec<Option<accounting::Record>>> =
        postcard::from_bytes(&bytes).unwrap();
    assert_eq!(
        restored.get(&accounting::Key(7)).unwrap()[0]
            .as_ref()
            .unwrap()
            .amount,
        1
    );
}

#[test]
fn handles_nested_inside_values_reject_the_wrong_type() {
    setup();
    let mut inner = MapxOrd::<u64, accounting::Record>::new();
    inner.insert(&7, &accounting::Record { amount: 1 });
    let mut outer = Mapx::<u64, MapxOrd<u64, accounting::Record>>::new();
    outer.insert(&1, &inner);
    drop(inner);
    let bytes = postcard::to_allocvec(&outer).unwrap();
    drop(outer);

    assert!(
        postcard::from_bytes::<Mapx<u64, MapxOrd<u64, access::Record>>>(&bytes).is_err()
    );
    let restored: Mapx<u64, MapxOrd<u64, accounting::Record>> =
        postcard::from_bytes(&bytes).unwrap();
    assert_eq!(restored.get(&1).unwrap().get(&7).unwrap().amount, 1);
}
