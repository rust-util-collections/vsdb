//!
//! Common utils.
//!

#[cfg(feature = "hash")]
pub(crate) mod hash {
    use blake3::{Hasher, OUT_LEN};

    pub type Hash = [u8; OUT_LEN];

    pub const HASH_SIZ: usize = OUT_LEN;

    pub fn hash(data: &[&[u8]]) -> Hash {
        let mut hasher = Hasher::new();
        for bytes in data {
            hasher.update(bytes);
        }
        hasher.finalize().into()
    }
}
