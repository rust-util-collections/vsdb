//!
//! Common utils.
//!

pub mod hash {
    use blake3::{Hasher, OUT_LEN};
    use std::hash::Hasher as StdHasher;

    pub type Hash = [u8; HASH_SIZ];
    pub const HASH_SIZ: usize = OUT_LEN;

    pub fn hash(data: &[&[u8]]) -> Hash {
        let mut hasher = Hasher::new();
        for bytes in data {
            hasher.update(bytes);
        }
        hasher.finalize().into()
    }

    #[derive(Default)]
    pub struct Mocker;

    impl StdHasher for Mocker {
        fn finish(&self) -> u64 {
            0
        }
        fn write(&mut self, _: &[u8]) {}
    }

    pub(crate) struct Blake3Hasher;

    impl hash_db::Hasher for Blake3Hasher {
        type Out = Hash;
        type StdHasher = Mocker;
        const LENGTH: usize = HASH_SIZ;
        fn hash(data: &[u8]) -> Self::Out {
            hash(&[data])
        }
    }

    pub fn trie_root<I, A, B>(entries: I) -> Vec<u8>
    where
        I: IntoIterator<Item = (A, B)>,
        A: AsRef<[u8]>,
        B: AsRef<[u8]>,
    {
        triehash::sec_trie_root::<Blake3Hasher, _, _, _>(entries).to_vec()
    }
}
