#![doc = include_str!("../README.md")]
#![deny(warnings)]
#![allow(clippy::new_without_default)]

mod substrate_trie;

#[cfg(test)]
mod test;

pub use vsdb::{RawBytes, RawKey, RawValue, ValueEnDe};

use ruc::*;
use serde::{Deserialize, Serialize};
use trie_db::{
    CError, DBValue, HashDB, Hasher as _, Trie, TrieHash, TrieItem, TrieIterator, TrieKeyItem,
    TrieMut,
};
use vsdb::{MapxOrdRawKey, Orphan};
use vsdb_hash_db::{KeccakHasher as H, TrieBackend, sp_hash_db::EMPTY_PREFIX};

type L = substrate_trie::LayoutV1<H>;
type TrieDB<'a, 'cache> = trie_db::TrieDB<'a, 'cache, L>;
type TrieDBBuilder<'a, 'cache> = trie_db::TrieDBBuilder<'a, 'cache, L>;
type TrieDBMut<'a> = trie_db::TrieDBMut<'a, L>;
type TrieDBMutBuilder<'a> = trie_db::TrieDBMutBuilder<'a, L>;

/// The root hash of a Merkle Patricia Trie.
pub type TrieRoot = TrieHash<L>;

/// An iterator over the items of a Merkle Patricia Trie.
pub type TrieIter<'a> = Box<dyn TrieIterator<L, Item = TrieItem<TrieHash<L>, CError<L>>> + 'a>;
/// An iterator over the keys of a Merkle Patricia Trie.
pub type TrieKeyIter<'a> =
    Box<dyn TrieIterator<L, Item = TrieKeyItem<TrieHash<L>, CError<L>>> + 'a>;

// root hash ==> backend instance
type HeaderSet = MapxOrdRawKey<TrieBackend>;

/// A store for Merkle Patricia Tries (MPTs).
///
/// `MptStore` manages multiple MPTs, each identified by a `backend_key`.
/// It handles the creation, derivation, and lifecycle of tries.
#[derive(Deserialize, Serialize)]
pub struct MptStore {
    // backend key ==> backend instance
    //
    // the backend key
    // - for the world state MPT, it is `[0]`(just an example)
    // - for the storage MPT, it is the bytes of a H160 address
    meta: MapxOrdRawKey<HeaderSet>,
}

impl MptStore {
    /// Creates a new, empty `MptStore`.
    #[inline(always)]
    pub fn new() -> Self {
        Self {
            meta: MapxOrdRawKey::new(),
        }
    }

    /// Creates a "shadow" copy of the `MptStore`.
    ///
    /// This method creates a new `MptStore` that shares the same underlying data source.
    ///
    /// # Safety
    ///
    /// This API breaks Rust's semantic safety guarantees. It is safe to use only in a
    /// race-free environment.
    #[inline(always)]
    pub unsafe fn shadow(&self) -> Self {
        unsafe {
            Self {
                meta: self.meta.shadow(),
            }
        }
    }

    /// Initializes a new trie from scratch (with no parent).
    ///
    /// # Arguments
    ///
    /// * `backend_key` - The key to identify the new trie's backend.
    ///
    /// # Returns
    ///
    /// A `Result` containing a new `MptOnce` instance or an error.
    #[inline(always)]
    pub fn trie_init(&mut self, backend_key: &[u8]) -> Result<MptOnce> {
        let b = TrieBackend::new(&mut Orphan::new(None)).unwrap();
        self.trie_create(backend_key, b).c(d!())
    }

    /// Creates a new trie from a specified backend.
    ///
    /// # Arguments
    ///
    /// * `backend_key` - The key to identify the new trie's backend.
    /// * `backend` - The `TrieBackend` to use for the new trie.
    ///
    /// # Returns
    ///
    /// A `Result` containing a new `MptOnce` instance or an error.
    #[inline(always)]
    pub fn trie_create(&mut self, backend_key: &[u8], backend: TrieBackend) -> Result<MptOnce> {
        let hdr = self.meta.entry(backend_key).or_insert(HeaderSet::new());
        MptOnce::create_with_backend(backend, &hdr).c(d!())
    }

    /// Re-derives a trie handler from a specified trie root.
    ///
    /// The returned handler is a new child of the target trie node.
    ///
    /// # Arguments
    ///
    /// * `backend_key` - The key of the trie's backend.
    /// * `root` - The `TrieRoot` to re-derive from.
    ///
    /// # Returns
    ///
    /// A `Result` containing a new `MptOnce` instance or an error.
    #[inline(always)]
    pub fn trie_rederive(&self, backend_key: &[u8], root: TrieRoot) -> Result<MptOnce> {
        self.meta.get(backend_key).c(d!()).and_then(|hs| {
            hs.get(root)
                .c(d!())
                .and_then(|b| MptOnce::rederive(&b, root, &hs).c(d!()))
        })
    }

    /// Prunes the trie, merging all nodes into the genesis node.
    ///
    /// This includes the target node itself.
    ///
    /// # Arguments
    ///
    /// * `backend_key` - The key of the trie's backend.
    /// * `root` - The `TrieRoot` of the trie to prune.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure.
    pub fn trie_prune(&mut self, backend_key: &[u8], root: TrieRoot) -> Result<()> {
        let mut hs = self.meta.get(backend_key).c(d!())?;
        let backend = hs.get(root).c(d!())?;

        let new_backend = backend.prune().c(d!())?;

        let hs_ro = unsafe { hs.shadow() };
        for k in hs_ro
            .iter()
            .filter(|(_, i)| i.is_dead() || i.is_the_same_instance(&new_backend))
            .map(|(key, _)| key)
        {
            hs.remove(k);
        }

        hs.insert(root, &new_backend);

        Ok(())
    }

    /// Destroys the entire trie associated with the target `backend_key`.
    ///
    /// # Arguments
    ///
    /// * `backend_key` - The key of the trie's backend to destroy.
    #[inline(always)]
    pub fn trie_destroy(&mut self, backend_key: &[u8]) {
        if let Some(mut hs) = self.meta.remove(backend_key) {
            for (_root, mut b) in hs.iter() {
                b.clear();
            }
            hs.clear();
        }
    }
}

/// An owned, mutable Merkle Patricia Trie instance.
///
/// # NOTE
///
/// The referenced field **MUST** be placed after the field that references it
/// to ensure that `Drop::drop` is executed in the correct order, preventing UB.
pub struct MptOnce {
    mpt: MptMut<'static>,
    root: TrieRoot,

    // self-reference
    #[allow(dead_code)]
    backend: Box<TrieBackend>,

    // A shadow of the instance in MptStore
    header_set: HeaderSet,
}

impl MptOnce {
    fn create_with_backend(backend: TrieBackend, header_set: &HeaderSet) -> Result<Self> {
        let backend = Box::into_raw(Box::new(backend));
        let mut mpt = MptMut::new(unsafe { &mut *backend });
        let root = mpt.commit();
        Ok(Self {
            mpt,
            root,
            backend: unsafe { Box::from_raw(backend) },
            header_set: unsafe { header_set.shadow() },
        })
    }

    fn rederive(
        parent_backend: &TrieBackend,
        root: TrieRoot,
        header_set: &HeaderSet,
    ) -> Result<Self> {
        let b = TrieBackend::new(&mut Orphan::new(Some(
            unsafe { parent_backend.shadow_backend() }.into_inner(),
        )))
        .c(d!())
        .map(|b| Box::into_raw(Box::new(b)))?;

        let mpt = MptMut::from_existing(unsafe { &mut *b }, root).c(d!())?;

        Ok(Self {
            mpt,
            root,
            backend: unsafe { Box::from_raw(b) },
            header_set: unsafe { header_set.shadow() },
        })
    }

    /// Retrieves a value for a key from the trie.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to look up.
    ///
    /// # Returns
    ///
    /// A `Result` containing an `Option<Vec<u8>>` with the value, or an error.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.mpt.get(key).c(d!())
    }

    /// Checks if a key exists in the trie.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to check.
    ///
    /// # Returns
    ///
    /// A `Result` containing a boolean or an error.
    pub fn contains(&self, key: &[u8]) -> Result<bool> {
        self.mpt.contains(key).c(d!())
    }

    /// Inserts a key-value pair into the trie.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to insert.
    /// * `value` - The value to associate with the key.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.mpt.insert(key, value).c(d!())
    }

    /// Removes a key-value pair from the trie.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to remove.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure.
    pub fn remove(&mut self, key: &[u8]) -> Result<()> {
        self.mpt.remove(key).c(d!()).map(|_| ())
    }

    /// Removes all key-value pairs in the current snapshot (version).
    pub fn clear(&mut self) -> Result<()> {
        self.mpt.clear().c(d!())
    }

    /// Checks if the trie is empty.
    pub fn is_empty(&self) -> bool {
        self.mpt.is_empty()
    }

    /// Commits all changes to the trie, consumes the current handler,
    /// and derives a new handler as a child of the current one.
    ///
    /// # Returns
    ///
    /// A `Result` containing the new `MptOnce` instance or an error.
    pub fn commit(mut self) -> Result<Self> {
        let root = self.mpt.commit();

        // if self.header_set.contains_key(root) {
        //     return Err(eg!("the root value exists!"));
        // }

        // root ==> its data version
        self.header_set.insert(root, &self.backend);

        Self::rederive(&self.backend, root, &self.header_set).c(d!())
    }

    /// Gets the cached trie root without triggering a commit.
    pub fn root(&self) -> TrieRoot {
        self.root
    }

    /// Derives a read-only handler for the trie.
    ///
    /// # Arguments
    ///
    /// * `root` - The `TrieRoot` to create the read-only handler from.
    ///
    /// # Returns
    ///
    /// A `Result` containing a new `MptRo` instance or an error.
    pub fn ro_handle(&self, root: TrieRoot) -> Result<MptRo<'_>> {
        MptRo::from_existing(&self.backend, root).c(d!())
    }
}

impl ValueEnDe for MptOnce {
    fn try_encode(&self) -> Result<RawBytes> {
        Ok(self.encode())
    }

    fn encode(&self) -> RawBytes {
        [
            self.root.to_vec(),
            self.backend.encode(),
            self.header_set.encode(),
        ]
        .encode()
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let [r, b, h] = <[Vec<u8>; 3]>::decode(bytes).c(d!())?;

        alt!(H::LENGTH > r.len(), return Err(eg!("Invalid length")));
        let mut root = [0; H::LENGTH];
        root.copy_from_slice(&r[..H::LENGTH]);

        let backend = TrieBackend::decode(&b).c(d!())?;
        let header_set = HeaderSet::decode(&h).c(d!())?;

        Self::rederive(&backend, root, &header_set).c(d!())
    }
}

//
// # NOTE
//
// The referenced field **MUST** be placed after the field that references it,
// this is to ensure that the `drop`s can be executed in the correct order,
// so that UB will not occur
// A mutable MPT instance
struct MptMut<'a> {
    trie: TrieDBMut<'a>,

    // self-reference
    #[allow(dead_code)]
    meta: MptMeta,
}

impl<'a> MptMut<'a> {
    // keep private !!
    fn new(backend: &'a mut TrieBackend) -> Self {
        // The buf will be rewrited when building the target `Trie`,
        // so its original contents can be arbitrary values.
        let root_buf = Default::default();

        let meta = MptMeta::new(root_buf);

        let trie = TrieDBMutBuilder::new(backend, unsafe { &mut *meta.root }).build();

        Self { trie, meta }
    }

    fn from_existing(backend: &'a mut TrieBackend, root: TrieRoot) -> Result<Self> {
        if !backend.contains(&root, EMPTY_PREFIX) {
            return Err(eg!("Invalid state root: {:02x?}", root));
        }

        let meta = MptMeta::new(root);

        let trie = TrieDBMutBuilder::from_existing(backend, unsafe { &mut *meta.root }).build();

        Ok(Self { trie, meta })
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.trie.get(key).c(d!())
    }

    fn contains(&self, key: &[u8]) -> Result<bool> {
        self.trie.contains(key).c(d!())
    }

    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.trie.insert(key, value).c(d!()).map(|_| ())
    }

    fn remove(&mut self, key: &[u8]) -> Result<()> {
        self.trie.remove(key).c(d!()).map(|_| ())
    }

    fn clear(&mut self) -> Result<()> {
        let root = self.commit();
        let keys = self.ro_handle(root).unwrap().key_iter().collect::<Vec<_>>();
        for k in keys.iter().map(|k| k.as_ref().unwrap()) {
            self.remove(k).c(d!())?;
        }
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.trie.is_empty()
    }

    fn commit(&mut self) -> TrieRoot {
        *self.trie.root()
    }

    fn ro_handle(&self, root: TrieRoot) -> Result<MptRo<'_>> {
        MptRo::from_existing_dyn(self.trie.db(), root).c(d!())
    }
}

//
// # NOTE
//
// The referenced field **MUST** be placed after the field that references it,
// this is to ensure that the `drop`s can be executed in the correct order,
// so that UB will not occur
/// A read-only Merkle Patricia Trie instance.
pub struct MptRo<'a> {
    trie: TrieDB<'a, 'a>,

    // self-reference
    #[allow(dead_code)]
    meta: MptMeta,
}

impl<'a> MptRo<'a> {
    /// Creates a new read-only MPT instance from an existing backend and root.
    ///
    /// # Arguments
    ///
    /// * `backend` - The `TrieBackend` to use.
    /// * `root` - The `TrieRoot` to create the instance from.
    ///
    /// # Returns
    ///
    /// A `Result` containing a new `MptRo` instance or an error.
    pub fn from_existing(backend: &'a TrieBackend, root: TrieRoot) -> Result<Self> {
        if !backend.contains(&root, EMPTY_PREFIX) {
            return Err(eg!("Invalid state root: {:02x?}", root));
        }

        let meta = MptMeta::new(root);

        let trie = TrieDBBuilder::new(backend, unsafe { &*meta.root }).build();

        Ok(Self { trie, meta })
    }

    /// Creates a new read-only MPT instance from a dynamic `HashDB` object and root.
    ///
    /// # Arguments
    ///
    /// * `backend` - A dynamic reference to a `HashDB`.
    /// * `root` - The `TrieRoot` to create the instance from.
    ///
    /// # Returns
    ///
    /// A `Result` containing a new `MptRo` instance or an error.
    pub fn from_existing_dyn(backend: &dyn HashDB<H, DBValue>, root: TrieRoot) -> Result<Self> {
        let backend = &backend as *const &dyn HashDB<H, DBValue>;
        let backend = backend.cast::<&TrieBackend>();
        let backend = unsafe { *backend };
        MptRo::from_existing(backend, root).c(d!())
    }

    /// Retrieves a value for a key from the trie.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to look up.
    ///
    /// # Returns
    ///
    /// A `Result` containing an `Option<Vec<u8>>` with the value, or an error.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.trie.get(key).c(d!())
    }

    /// Checks if a key exists in the trie.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to check.
    ///
    /// # Returns
    ///
    /// A `Result` containing a boolean or an error.
    pub fn contains(&self, key: &[u8]) -> Result<bool> {
        self.trie.contains(key).c(d!())
    }

    /// Returns an iterator over the trie's items.
    pub fn iter(&self) -> TrieIter<'_> {
        pnk!(self.trie.iter())
    }

    /// Returns an iterator over the trie's keys.
    pub fn key_iter(&self) -> TrieKeyIter<'_> {
        pnk!(self.trie.key_iter())
    }

    /// Gets the trie root.
    pub fn root(&self) -> TrieRoot {
        *self.trie.root()
    }
}

struct MptMeta {
    // self-reference
    #[allow(dead_code)]
    root: *mut TrieRoot,
}

impl MptMeta {
    fn new(root: TrieRoot) -> Self {
        Self {
            root: Box::into_raw(Box::new(root)),
        }
    }
}

impl Drop for MptMeta {
    fn drop(&mut self) {
        unsafe {
            drop(Box::from_raw(self.root));
        }
    }
}
